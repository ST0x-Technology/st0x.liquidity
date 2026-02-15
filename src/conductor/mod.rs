//! Orchestrates the main bot loop: subscribes to DEX events, queues them,
//! processes trades, places offsetting broker orders, and manages background
//! tasks (order polling, rebalancing, inventory tracking). [`Conductor`] owns
//! the task handles; [`run_market_hours_loop`] drives the lifecycle.

mod builder;
mod manifest;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, IntoLogData};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types;
use futures_util::{Stream, StreamExt};
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use st0x_dto::ServerMessage;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::alpaca_trading_api::AlpacaTradingApiError;
use st0x_execution::{ExecutionError, Executor, FractionalShares, SupportedExecutor};

use st0x_event_sorcery::{Cons, Nil, Projection, SendError, Store, StoreBuilder, Unwired};

use crate::bindings::IOrderBookV5::{ClearV3, IOrderBookV5Instance, TakeOrderV3};
use crate::config::{Ctx, CtxError};
use crate::equity_redemption::{EquityRedemption, Redeemer};
use crate::inventory::{
    InventoryPollingService, InventorySnapshot, InventorySnapshotReactor, InventoryView,
};
use crate::offchain::order_poller::OrderStatusPoller;
use crate::offchain_order::{
    ExecutorOrderPlacer, OffchainOrder, OffchainOrderCommand, OffchainOrderId, OrderPlacer,
};
use crate::onchain::USDC_BASE;
use crate::onchain::accumulator::{ExecutionCtx, check_all_positions, check_execution_readiness};
use crate::onchain::backfill::backfill_events;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain::trade::{TradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain::{EvmCtx, OnChainError, OnchainTrade};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeId};
use crate::position::{Position, PositionCommand, TradeId};
use crate::queue::{
    EventQueueError, QueuedEvent, enqueue, get_next_unprocessed_event, mark_event_processed,
};
use crate::rebalancing::redemption::RedemptionService;
use crate::rebalancing::{
    RebalancingCqrsFrameworks, RebalancingCtx, RebalancingTriggerConfig, RedemptionDependencies,
    build_rebalancing_queries, spawn_rebalancer,
};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand, VaultRegistryId};

use self::manifest::QueryManifest;
pub(crate) use builder::{ConductorBuilder, CqrsFrameworks};

/// Bundles CQRS frameworks used throughout the trade processing pipeline.
struct TradeProcessingCqrs {
    onchain_trade: Arc<Store<OnChainTrade>>,
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    offchain_order: Arc<Store<OffchainOrder>>,
    execution_threshold: ExecutionThreshold,
}

pub(crate) struct Conductor {
    pub(crate) executor_maintenance: Option<JoinHandle<()>>,
    pub(crate) rebalancer: Option<JoinHandle<()>>,
    pub(crate) inventory_poller: Option<JoinHandle<()>>,
    pub(crate) trading_tasks: Option<TradingTasks>,
}

pub(crate) struct TradingTasks {
    pub(crate) order_poller: JoinHandle<()>,
    pub(crate) dex_event_receiver: JoinHandle<()>,
    pub(crate) event_processor: JoinHandle<()>,
    pub(crate) position_checker: JoinHandle<()>,
    pub(crate) queue_processor: JoinHandle<()>,
}

/// Event processing errors for live event handling.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventProcessingError {
    #[error("Event queue error: {0}")]
    Queue(#[from] EventQueueError),
    #[error("Failed to enqueue ClearV3 event: {0}")]
    EnqueueClearV3(#[source] EventQueueError),
    #[error("Failed to enqueue TakeOrderV3 event: {0}")]
    EnqueueTakeOrderV3(#[source] EventQueueError),
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Ctx error: {0}")]
    Ctx(#[from] CtxError),
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Alpaca trading API error: {0}")]
    AlpacaTradingApi(#[from] AlpacaTradingApiError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
}

pub(crate) async fn run_market_hours_loop<E>(
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    executor_maintenance: Option<JoinHandle<()>>,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let (mut conductor, shared) = start_infrastructure(
        &ctx,
        &pool,
        executor.clone(),
        executor_maintenance,
        event_sender,
    )
    .await?;

    loop {
        if try_start_trading(&mut conductor, &ctx, &pool, &executor, &shared).await {
            break;
        }
    }

    info!("Conductor running");
    let result = conductor.wait_for_completion().await;
    conductor.abort_all();
    result
}

async fn try_start_trading<E>(
    conductor: &mut Conductor,
    ctx: &Ctx,
    pool: &SqlitePool,
    executor: &E,
    shared: &SharedState,
) -> bool
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    const RETRY_DELAY_SECS: u64 = 10;

    match start_trading_tasks(ctx, pool, executor.clone(), shared).await {
        Ok(tasks) => {
            conductor.trading_tasks = Some(tasks);
            true
        }
        Err(e) => {
            error!("Failed to start trading tasks: {e}, retrying in {RETRY_DELAY_SECS} seconds");
            tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
            conductor.executor_maintenance = executor.run_executor_maintenance().await;
            false
        }
    }
}

async fn start_conductor<E>(
    ctx: &Ctx,
    pool: &SqlitePool,
    executor_maintenance: Option<JoinHandle<()>>,
    event_sender: &broadcast::Sender<ServerMessage>,
) -> anyhow::Result<Conductor>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let conductor = Conductor::start(
        ctx,
        pool,
        executor.clone(),
        executor_maintenance,
        event_sender.clone(),
    )
    .await?;

    info!("Market opened, conductor running");
    Ok(conductor)
}

async fn retry_after_failure<E>(
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
    error: anyhow::Error,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    error!("Failed to start conductor: {error}, retrying in {RERUN_DELAY_SECS} seconds");
    tokio::time::sleep(std::time::Duration::from_secs(RERUN_DELAY_SECS)).await;

    let new_maintenance = executor.run_executor_maintenance().await;
    Box::pin(run_market_hours_loop(
        executor,
        ctx,
        pool,
        new_maintenance,
        event_sender,
    ))
    .await
}

async fn run_until_market_close<E>(
    conductor: &mut Conductor,
    timeout: Duration,
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    tokio::select! {
        result = conductor.wait_for_completion() => {
            handle_conductor_completion(conductor, result)
        }
        () = tokio::time::sleep(timeout) => {
            handle_market_close(conductor, executor, ctx, pool, event_sender).await
        }
    }
}

fn handle_conductor_completion(
    conductor: &mut Conductor,
    result: anyhow::Result<()>,
) -> anyhow::Result<()> {
    info!("Conductor completed");
    conductor.abort_all();
    result?;
    info!(
        "Conductor completed successfully, \
         continuing to next market session"
    );
    Ok(())
}

async fn handle_market_close<E>(
    conductor: &mut Conductor,
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    info!("Market closed, shutting down trading tasks");
    conductor.abort_trading_tasks();
    let next_maintenance = conductor.executor_maintenance.take();
    info!("Trading tasks shutdown, DEX events buffering");
    Box::pin(run_market_hours_loop(
        executor,
        ctx,
        pool,
        next_maintenance,
        event_sender,
    ))
    .await
}

/// Context for vault discovery operations during trade processing.
struct VaultDiscoveryCtx<'a> {
    vault_registry: &'a Store<VaultRegistry>,
    orderbook: Address,
    order_owner: Address,
}

impl Conductor {
    pub(crate) async fn start<E>(
        ctx: &Ctx,
        pool: &SqlitePool,
        executor: E,
        executor_maintenance: Option<JoinHandle<()>>,
        event_sender: broadcast::Sender<ServerMessage>,
    ) -> anyhow::Result<Self>
    where
        E: Executor + Clone + Send + 'static,
        EventProcessingError: From<E::Error>,
    {
        let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let cache = SymbolCache::default();
        let orderbook = IOrderBookV5Instance::new(ctx.evm.orderbook, &provider);

        let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        let cutoff_block =
            get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, pool).await?;

        if let Some(end_block) = cutoff_block.checked_sub(1) {
            backfill_events(pool, &provider, &ctx.evm, end_block).await?;
        }

        let onchain_trade = Arc::new(
            StoreBuilder::<OnChainTrade>::new(pool.clone())
                .build(())
                .await?,
        );

        let inventory = Arc::new(RwLock::new(InventoryView::default()));

        let (position, position_projection, rebalancer) =
            if let Some(rebalancing_ctx) = ctx.rebalancing_ctx() {
                let signer = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key)?;
                let market_maker_wallet = signer.address();

                let (position, position_projection, rebalancer_handle) =
                    spawn_rebalancing_infrastructure(
                        rebalancing_ctx,
                        pool,
                        ctx,
                        &inventory,
                        event_sender,
                        &provider,
                        market_maker_wallet,
                    )
                    .await?;

                (position, position_projection, Some(rebalancer_handle))
            } else {
                let (position, position_projection) = build_position_cqrs(pool).await?;
                (position, position_projection, None)
            };

        let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer(executor.clone()));
        let offchain_order_projection =
            Arc::new(Projection::<OffchainOrder>::sqlite(pool.clone())?);

        let offchain_order = Arc::new(
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .with((*offchain_order_projection).clone())
                .build(order_placer)
                .await?,
        );

        let vault_registry = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await?;

        let snapshot = build_inventory_snapshot_store(pool, inventory.clone()).await?;

        let frameworks = CqrsFrameworks {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            offchain_order_projection,
            vault_registry,
            snapshot,
        };

        let mut builder = ConductorBuilder::new(
            ctx.clone(),
            pool.clone(),
            cache,
            provider,
            executor,
            ctx.execution_threshold,
            frameworks,
        )
        .with_executor_maintenance(executor_maintenance)
        .with_dex_event_streams(clear_stream, take_stream);

        if let Some(rebalancer_handle) = rebalancer {
            builder = builder.with_rebalancer(rebalancer_handle);
        }

        Ok(builder.spawn())
    }
}

impl Conductor {
    pub(crate) async fn wait_for_completion(&mut self) -> Result<(), anyhow::Error> {
        let infra = wait_for_infrastructure(
            &mut self.executor_maintenance,
            &mut self.rebalancer,
            &mut self.inventory_poller,
        );

        if let Some(tasks) = self.trading_tasks.as_mut() {
            wait_for_all_tasks(infra, tasks).await
        } else {
            infra.await;
            Ok(())
        }
    }

    pub(crate) fn abort_trading_tasks(&mut self) {
        let Some(tasks) = self.trading_tasks.take() else {
            info!("No trading tasks to abort");
            return;
        };

        info!(
            "Aborting trading tasks (keeping rebalancer, inventory poller, and broker maintenance alive)"
        );
        tasks.order_poller.abort();
        tasks.dex_event_receiver.abort();
        tasks.event_processor.abort();
        tasks.position_checker.abort();
        tasks.queue_processor.abort();
        info!("Trading tasks aborted successfully");
    }

    pub(crate) fn abort_all(&mut self) {
        self.abort_trading_tasks();

        if let Some(ref handle) = self.rebalancer {
            handle.abort();
        }
        if let Some(ref handle) = self.inventory_poller {
            handle.abort();
        }
        if let Some(ref handle) = self.executor_maintenance {
            handle.abort();
        }
    }
}

async fn spawn_rebalancing_infrastructure<P: Provider + Clone + Send + Sync + 'static>(
    rebalancing_ctx: &RebalancingCtx,
    pool: &SqlitePool,
    ctx: &Ctx,
    inventory: &Arc<RwLock<InventoryView>>,
    event_sender: broadcast::Sender<ServerMessage>,
    provider: &P,
    market_maker_wallet: Address,
) -> anyhow::Result<(
    Arc<Store<Position>>,
    Arc<Projection<Position>>,
    JoinHandle<()>,
)> {
    info!("Initializing rebalancing infrastructure");

    const OPERATION_CHANNEL_CAPACITY: usize = 100;
    let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

    let manifest = QueryManifest::new(
        RebalancingTriggerConfig {
            equity: rebalancing_ctx.equity_threshold,
            usdc: rebalancing_ctx.usdc.clone(),
        },
        pool.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        inventory.clone(),
        operation_sender,
        event_sender,
    )?;

    let (built, wired) = manifest.wire(pool.clone()).await?;

    let frameworks = RebalancingCqrsFrameworks {
        mint: Arc::new(built.mint),
        usdc: Arc::new(built.usdc),
    };

    // Create services needed for redemption CQRS
    let broker_auth = &rebalancing_ctx.alpaca_broker_auth;
    let vault_service = Arc::new(RaindexService::new(provider.clone(), ctx.evm.orderbook));
    let tokenization_service = Arc::new(AlpacaTokenizationService::new(
        broker_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_account_id,
        broker_auth.api_key.clone(),
        broker_auth.api_secret.clone(),
        provider.clone(),
        rebalancing_ctx.redemption_wallet,
    ));

    let redemption_service = Arc::new(RedemptionService::new(
        vault_service.clone(),
        tokenization_service.clone(),
        vault_registry_query.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
    ));

    let redemption_deps = RedemptionDependencies {
        vault_service,
        tokenization_service,
        vault_registry_query,
        service: redemption_service,
        cqrs: Arc::new(built.redemption),
    };

    let handle = spawn_rebalancer(
        rebalancing_ctx,
        provider.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        operation_receiver,
        frameworks,
        redemption_deps,
    )
    .await?;

    Ok((
        Arc::new(built.position),
        Arc::new(wired.position_view),
        handle,
    ))
}

async fn wait_for_infrastructure(
    executor_maintenance: &mut Option<JoinHandle<()>>,
    rebalancer: &mut Option<JoinHandle<()>>,
    inventory_poller: &mut Option<JoinHandle<()>>,
) {
    tokio::join!(
        wait_for_optional_task(executor_maintenance, "Executor maintenance"),
        wait_for_optional_task(rebalancer, "Rebalancer"),
        wait_for_optional_task(inventory_poller, "Inventory poller"),
    );
}

async fn wait_for_all_tasks(
    infrastructure: impl std::future::Future<Output = ()>,
    tasks: &mut TradingTasks,
) -> anyhow::Result<()> {
    let ((), poller, dex, processor, position, queue) = tokio::join!(
        infrastructure,
        &mut tasks.order_poller,
        &mut tasks.dex_event_receiver,
        &mut tasks.event_processor,
        &mut tasks.position_checker,
        &mut tasks.queue_processor
    );

    log_task_result(poller, "Order poller");
    log_task_result(dex, "DEX event receiver");
    log_task_result(processor, "Event processor");
    log_task_result(position, "Position checker");
    log_task_result(queue, "Queue processor");

    Ok(())
}

async fn wait_for_optional_task(handle: &mut Option<JoinHandle<()>>, task_name: &str) {
    let Some(handle) = handle else { return };

    match handle.await {
        Ok(()) => info!("{task_name} completed successfully"),
        Err(error) if error.is_cancelled() => {
            info!("{task_name} cancelled (expected during shutdown)");
        }
        Err(error) => error!("{task_name} task panicked: {error}"),
    }
}

fn log_task_result(result: Result<(), tokio::task::JoinError>, task_name: &str) {
    if let Err(error) = result {
        error!("{task_name} task panicked: {error}");
    }
}

/// Constructs the position CQRS framework with its view query
/// (without rebalancing trigger). Used when rebalancing is disabled.
async fn build_position_cqrs(
    pool: &SqlitePool,
) -> anyhow::Result<(Arc<Store<Position>>, Arc<Projection<Position>>)> {
    let position_view = Projection::<Position>::sqlite(pool.clone())?;

    let store = StoreBuilder::<Position>::new(pool.clone())
        .with(position_view.clone())
        .build(())
        .await?;

    Ok((Arc::new(store), Arc::new(position_view)))
}

/// Constructs the inventory snapshot store with its query.
async fn build_inventory_snapshot_store(
    pool: &SqlitePool,
    inventory: Arc<RwLock<InventoryView>>,
) -> anyhow::Result<Store<InventorySnapshot>> {
    let snapshot_reactor = InventorySnapshotReactor::new(inventory);

    let query: Unwired<InventorySnapshotReactor, Cons<InventorySnapshot, Nil>> =
        Unwired::new(snapshot_reactor);

    let (store, (_query, ())) = StoreBuilder::<InventorySnapshot>::new(pool.clone())
        .wire(query)
        .build(())
        .await?;

    Ok(store)
}

fn spawn_order_poller<E: Executor + Clone + Send + 'static>(
    ctx: &Ctx,
    executor: E,
    offchain_order_projection: Projection<OffchainOrder>,
    offchain_order: Arc<Store<OffchainOrder>>,
    position: Arc<Store<Position>>,
) -> JoinHandle<()> {
    let poller_ctx = ctx.get_order_poller_ctx();
    info!(
        "Starting order status poller with interval: {:?}, max jitter: {:?}",
        poller_ctx.polling_interval, poller_ctx.max_jitter
    );

    let poller = OrderStatusPoller::new(
        poller_ctx,
        executor,
        offchain_order_projection,
        offchain_order,
        position,
    );
    tokio::spawn(async move {
        if let Err(error) = poller.run().await {
            error!("Order poller failed: {error}");
        } else {
            info!("Order poller completed successfully");
        }
    })
}

fn spawn_onchain_event_receiver(
    event_sender: UnboundedSender<(TradeEvent, Log)>,
    clear_stream: impl Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send + 'static,
    take_stream: impl Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>>
    + Unpin
    + Send
    + 'static,
) -> JoinHandle<()> {
    info!("Starting blockchain event receiver");
    tokio::spawn(receive_blockchain_events(
        clear_stream,
        take_stream,
        event_sender,
    ))
}

fn spawn_event_processor(
    pool: SqlitePool,
    mut event_receiver: tokio::sync::mpsc::UnboundedReceiver<(TradeEvent, Log)>,
) -> JoinHandle<()> {
    info!("Starting event processor");
    tokio::spawn(async move {
        while let Some((event, log)) = event_receiver.recv().await {
            trace!(
                "Processing live event: tx_hash={:?}, log_index={:?}",
                log.transaction_hash, log.log_index
            );
            if let Err(error) = process_live_event(&pool, event, log).await {
                error!("Failed to process live event: {error}");
            }
        }
        info!("Event processing loop ended");
    })
}

fn spawn_queue_processor<P, E>(
    executor: E,
    ctx: &Ctx,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
    cqrs: TradeProcessingCqrs,
    vault_registry: Store<VaultRegistry>,
) -> JoinHandle<()>
where
    P: Provider + Clone + Send + 'static,
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    info!("Starting queue processor service");
    let ctx_clone = ctx.clone();
    let pool_clone = pool.clone();
    let cache_clone = cache.clone();

    tokio::spawn(async move {
        run_queue_processor(
            &executor,
            &ctx_clone,
            &pool_clone,
            &cache_clone,
            provider,
            &cqrs,
            &vault_registry,
        )
        .await;
    })
}

fn spawn_periodic_accumulated_position_check<E>(
    executor: E,
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    offchain_order: Arc<Store<OffchainOrder>>,
    execution_threshold: ExecutionThreshold,
) -> JoinHandle<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    info!("Starting periodic accumulated position checker");

    tokio::spawn(async move {
        const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

        let mut interval = tokio::time::interval(CHECK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running periodic accumulated position check");
            if let Err(error) = check_and_execute_accumulated_positions(
                &executor,
                &position,
                &position_projection,
                &offchain_order,
                &execution_threshold,
            )
            .await
            {
                error!("Periodic accumulated position check failed: {error}");
            }
        }
    })
}

fn spawn_inventory_poller<P, E>(
    pool: SqlitePool,
    raindex_service: Arc<RaindexService<P>>,
    executor: E,
    orderbook: Address,
    order_owner: Address,
    snapshot: Store<InventorySnapshot>,
) -> JoinHandle<()>
where
    P: Provider + Clone + Send + 'static,
    E: Executor + Clone + Send + 'static,
{
    info!("Starting inventory poller");

    let service = InventoryPollingService::new(
        raindex_service,
        executor,
        pool,
        orderbook,
        order_owner,
        snapshot,
    );

    tokio::spawn(async move {
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running inventory poll");
            if let Err(error) = service.poll_and_record().await {
                error!(%error, "Inventory polling failed");
            }
        }
    })
}

async fn receive_blockchain_events<S1, S2>(
    mut clear_stream: S1,
    mut take_stream: S2,
    event_sender: UnboundedSender<(TradeEvent, Log)>,
) where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
{
    loop {
        let event_result = tokio::select! {
            Some(result) = clear_stream.next() => {
                result.map(|(event, log)| (TradeEvent::ClearV3(Box::new(event)), log))
            }
            Some(result) = take_stream.next() => {
                result.map(|(event, log)| (TradeEvent::TakeOrderV3(Box::new(event)), log))
            }
            else => {
                error!("All event streams ended, shutting down event receiver");
                break;
            }
        };

        if !dispatch_blockchain_event(event_result, &event_sender) {
            break;
        }
    }
}

/// Returns `false` if the event loop should stop.
fn dispatch_blockchain_event(
    event_result: Result<(TradeEvent, Log), sol_types::Error>,
    event_sender: &UnboundedSender<(TradeEvent, Log)>,
) -> bool {
    match event_result {
        Ok((event, log)) => {
            trace!(
                "Received blockchain event: tx_hash={:?}, \
                 log_index={:?}, block_number={:?}",
                log.transaction_hash, log.log_index, log.block_number
            );
            if event_sender.send((event, log)).is_err() {
                error!("Event receiver dropped, shutting down");
                return false;
            }
            true
        }
        Err(error) => {
            error!("Error in event stream: {error}");
            true
        }
    }
}

pub(crate) async fn get_cutoff_block<S1, S2, P>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    provider: &P,
    pool: &SqlitePool,
) -> anyhow::Result<u64>
where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
    P: Provider + Clone,
{
    info!("Starting WebSocket subscriptions and waiting for first event...");

    let first_event_result = wait_for_first_event_with_timeout(
        clear_stream,
        take_stream,
        std::time::Duration::from_secs(5),
    )
    .await;

    let Some((mut event_buffer, block_number)) = first_event_result else {
        let current_block = provider.get_block_number().await?;
        info!(
            "No subscription events within timeout, \
             using current block {current_block} as cutoff"
        );
        return Ok(current_block);
    };

    buffer_live_events(clear_stream, take_stream, &mut event_buffer, block_number).await;

    crate::queue::enqueue_buffer(pool, event_buffer).await;

    Ok(block_number)
}

async fn process_live_event(
    pool: &SqlitePool,
    event: TradeEvent,
    log: Log,
) -> Result<(), EventProcessingError> {
    match &event {
        TradeEvent::ClearV3(clear_event) => {
            info!(
                "Enqueuing ClearV3 event: tx_hash={:?}, log_index={:?}",
                log.transaction_hash, log.log_index
            );

            enqueue(pool, clear_event.as_ref(), &log)
                .await
                .map_err(EventProcessingError::EnqueueClearV3)?;
        }
        TradeEvent::TakeOrderV3(take_event) => {
            info!(
                "Enqueuing TakeOrderV3 event: tx_hash={:?}, log_index={:?}",
                log.transaction_hash, log.log_index
            );

            enqueue(pool, take_event.as_ref(), &log)
                .await
                .map_err(EventProcessingError::EnqueueTakeOrderV3)?;
        }
    }

    Ok(())
}

async fn run_queue_processor<P, E>(
    executor: &E,
    ctx: &Ctx,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
    cqrs: &TradeProcessingCqrs,
    vault_registry: &Store<VaultRegistry>,
) where
    P: Provider + Clone,
    E: Executor + Clone,
    EventProcessingError: From<E::Error>,
{
    info!("Starting queue processor service");

    let feed_id_cache = FeedIdCache::default();

    log_unprocessed_count(pool).await;

    let executor_type = executor.to_supported_executor();

    let queue_context = QueueProcessingCtx {
        cache,
        feed_id_cache: &feed_id_cache,
        vault_registry,
    };

    loop {
        let delay =
            process_queue_step(executor_type, ctx, pool, &provider, cqrs, &queue_context).await;
        sleep(delay).await;
    }
}

async fn log_unprocessed_count(pool: &SqlitePool) {
    match crate::queue::count_unprocessed(pool).await {
        Ok(count) if count > 0 => {
            info!("Found {count} unprocessed events from previous sessions to process");
        }
        Ok(_) => info!("No unprocessed events found, starting fresh"),
        Err(error) => error!("Failed to count unprocessed events: {error}"),
    }
}

async fn process_queue_step<P: Provider + Clone>(
    executor_type: SupportedExecutor,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    cqrs: &TradeProcessingCqrs,
    queue_context: &QueueProcessingCtx<'_>,
) -> Duration {
    match process_next_queued_event(executor_type, ctx, pool, provider, cqrs, queue_context).await {
        Ok(Some(offchain_order_id)) => {
            info!(%offchain_order_id, "Offchain order placed successfully");
            std::time::Duration::ZERO
        }
        Ok(None) => std::time::Duration::from_millis(100),
        Err(error) => {
            error!("Error processing queued event: {error}");
            std::time::Duration::from_millis(500)
        }
    }
}

/// Context for queue event processing containing caches and CQRS components.
struct QueueProcessingCtx<'a> {
    cache: &'a SymbolCache,
    feed_id_cache: &'a FeedIdCache,
    vault_registry: &'a Store<VaultRegistry>,
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn process_next_queued_event<P: Provider + Clone>(
    executor_type: SupportedExecutor,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    cqrs: &TradeProcessingCqrs,
    queue_context: &QueueProcessingCtx<'_>,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    let queued_event = get_next_unprocessed_event(pool).await?;
    let Some(queued_event) = queued_event else {
        return Ok(None);
    };

    let event_id = queued_event.id.ok_or(EventProcessingError::Queue(
        EventQueueError::MissingQueuedEventId,
    ))?;

    let onchain_trade = convert_event_to_trade(
        ctx,
        queue_context.cache,
        provider,
        &queued_event,
        queue_context.feed_id_cache,
    )
    .await?;

    let Some(trade) = onchain_trade else {
        info!(
            "Event filtered out (no matching owner): event_type={:?}, tx_hash={:?}, log_index={}",
            match &queued_event.event {
                TradeEvent::ClearV3(_) => "ClearV3",
                TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
            },
            queued_event.tx_hash,
            queued_event.log_index
        );
        mark_event_processed(pool, event_id).await?;
        return Ok(None);
    };

    let vault_discovery_ctx = VaultDiscoveryCtx {
        vault_registry: queue_context.vault_registry,
        orderbook: ctx.evm.orderbook,
        order_owner: ctx.order_owner()?,
    };

    process_valid_trade(
        executor_type,
        pool,
        &queued_event,
        event_id,
        trade,
        cqrs,
        &vault_discovery_ctx,
    )
    .await
}

/// Discovers vaults from a trade and emits VaultRegistryCommands.
///
/// This function is called AFTER trade conversion succeeds, using the trade's
/// already-resolved symbol. It extracts vault information from the queued event
/// and registers vaults owned by the specified order_owner.
///
/// Vaults are classified as:
/// - USDC vault: token == USDC_BASE
/// - Equity vault: token matches the trade's symbol (via cache lookup)
pub(super) async fn discover_vaults_for_trade(
    queued_event: &QueuedEvent,
    trade: &OnchainTrade,
    context: &VaultDiscoveryCtx<'_>,
) -> Result<(), EventProcessingError> {
    let tx_hash = queued_event.tx_hash;
    let base_symbol = trade.symbol.base();
    let expected_equity_token = trade.equity_token;

    let owned_vaults = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => extract_vaults_from_clear(clear_event),
        TradeEvent::TakeOrderV3(take_event) => extract_owned_vaults(
            &take_event.config.order,
            take_event.config.inputIOIndex,
            take_event.config.outputIOIndex,
        ),
    };

    let our_vaults = owned_vaults
        .into_iter()
        .filter(|vault| vault.owner == context.order_owner);

    let vault_registry_id = VaultRegistryId {
        orderbook: context.orderbook,
        owner: context.order_owner,
    };

    for owned_vault in our_vaults {
        let vault = owned_vault.vault;

        let command = if vault.token == USDC_BASE {
            VaultRegistryCommand::DiscoverUsdcVault {
                vault_id: vault.vault_id,
                discovered_in: tx_hash,
            }
        } else if vault.token == expected_equity_token {
            VaultRegistryCommand::DiscoverEquityVault {
                token: vault.token,
                vault_id: vault.vault_id,
                discovered_in: tx_hash,
                symbol: base_symbol.clone(),
            }
        } else {
            warn!(
                vault_id = %vault.vault_id,
                token = %vault.token,
                usdc = %USDC_BASE,
                expected_equity_token = %expected_equity_token,
                "Vault token does not match USDC or expected equity token, skipping"
            );
            continue;
        };

        context
            .vault_registry
            .send(&vault_registry_id, command)
            .await?;
    }

    Ok(())
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn convert_event_to_trade<P: Provider + Clone>(
    ctx: &Ctx,
    cache: &SymbolCache,
    provider: &P,
    queued_event: &QueuedEvent,
    feed_id_cache: &FeedIdCache,
) -> Result<Option<OnchainTrade>, EventProcessingError> {
    let reconstructed_log = reconstruct_log_from_queued_event(&ctx.evm, queued_event);
    let order_owner = ctx.order_owner()?;

    let onchain_trade = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => {
            OnchainTrade::try_from_clear_v3(
                &ctx.evm,
                cache,
                provider,
                *clear_event.clone(),
                reconstructed_log,
                feed_id_cache,
                order_owner,
            )
            .await?
        }
        TradeEvent::TakeOrderV3(take_event) => {
            OnchainTrade::try_from_take_order_if_target_owner(
                cache,
                provider,
                *take_event.clone(),
                reconstructed_log,
                order_owner,
                feed_id_cache,
            )
            .await?
        }
    };

    Ok(onchain_trade)
}

#[tracing::instrument(
    skip(pool, queued_event, trade, cqrs, vault_discovery_ctx),
    fields(event_id, symbol = %trade.symbol),
    level = tracing::Level::INFO
)]
async fn process_valid_trade(
    executor_type: SupportedExecutor,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
    vault_discovery_ctx: &VaultDiscoveryCtx<'_>,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    info!(
        "Event successfully converted to trade: event_type={:?}, \
         tx_hash={:?}, log_index={}, symbol={}, amount={}",
        match &queued_event.event {
            TradeEvent::ClearV3(_) => "ClearV3",
            TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
        },
        trade.tx_hash,
        trade.log_index,
        trade.symbol,
        trade.amount
    );

    discover_vaults_for_trade(queued_event, &trade, vault_discovery_ctx).await?;

    let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
    let _guard = symbol_lock.lock().await;

    info!(
        "Processing queued trade: symbol={}, amount={}, \
         direction={:?}, tx_hash={:?}, log_index={}",
        trade.symbol, trade.amount, trade.direction, trade.tx_hash, trade.log_index
    );

    process_queued_trade(executor_type, pool, queued_event, event_id, trade, cqrs).await
}

async fn execute_witness_trade(
    onchain_trade: &Store<OnChainTrade>,
    trade: &OnchainTrade,
    block_number: u64,
) {
    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let Some(block_timestamp) = trade.block_timestamp else {
        error!(
            "Missing block_timestamp for OnChainTrade::Witness: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        );
        return;
    };

    let command = OnChainTradeCommand::Witness {
        symbol: trade.symbol.base().clone(),
        amount,
        direction: trade.direction,
        price_usdc,
        block_number,
        block_timestamp,
    };

    match onchain_trade.send(&trade_id, command).await {
        Ok(()) => info!(
            "Successfully executed OnChainTrade::Witness command: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        ),
        Err(error) => error!(
            "Failed to execute OnChainTrade::Witness command: {error}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn execute_acknowledge_fill(
    position: &Store<Position>,
    trade: &OnchainTrade,
    threshold: ExecutionThreshold,
) {
    let base_symbol = trade.symbol.base();

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let Some(block_timestamp) = trade.block_timestamp else {
        error!(
            "Missing block_timestamp for Position::AcknowledgeOnChainFill: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        );
        return;
    };

    let command = PositionCommand::AcknowledgeOnChainFill {
        symbol: base_symbol.clone(),
        threshold,
        trade_id: TradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        },
        amount: FractionalShares::new(amount),
        direction: trade.direction,
        price_usdc,
        block_timestamp,
    };

    match position.send(base_symbol, command).await {
        Ok(()) => info!(
            "Successfully executed Position::AcknowledgeOnChainFill command: tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
        Err(error) => error!(
            "Failed to execute Position::AcknowledgeOnChainFill command: {error}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn process_queued_trade(
    executor_type: SupportedExecutor,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    // Update Position aggregate FIRST so threshold check sees current state
    execute_acknowledge_fill(&cqrs.position, &trade, cqrs.execution_threshold).await;

    mark_event_processed(pool, event_id)
        .await
        .map_err(|error| {
            error!("Failed to mark event {event_id} as processed: {error}");
            EventProcessingError::Queue(error)
        })?;

    info!(
        "Successfully marked event as processed: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    execute_witness_trade(&cqrs.onchain_trade, &trade, queued_event.block_number).await;

    let base_symbol = trade.symbol.base();

    let Some(execution) =
        check_execution_readiness(&cqrs.position_projection, base_symbol, executor_type).await?
    else {
        return Ok(None);
    };

    place_offchain_order(pool, &execution, cqrs).await
}

async fn place_offchain_order(
    pool: &SqlitePool,
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    let offchain_order_id = OffchainOrderId::new();

    execute_place_offchain_order(execution, cqrs, offchain_order_id).await;
    execute_create_offchain_order(execution, cqrs, offchain_order_id).await;

    let aggregate = cqrs.offchain_order.load(&offchain_order_id).await;

    if let Ok(Some(OffchainOrder::Failed { error, .. })) = aggregate {
        warn!(
            %offchain_order_id,
            symbol = %execution.symbol,
            %error,
            "Broker rejected order, clearing position pending state"
        );
        execute_fail_offchain_order_position(&cqrs.position, offchain_order_id, execution, error)
            .await;
    }

    Ok(Some(offchain_order_id))
}

async fn execute_fail_offchain_order_position(
    position: &Store<Position>,
    offchain_order_id: OffchainOrderId,
    execution: &ExecutionCtx,
    error: String,
) {
    let command = PositionCommand::FailOffChainOrder {
        offchain_order_id,
        error,
    };

    match position.send(&execution.symbol, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::FailOffChainOrder succeeded"
        ),
        Err(e) => error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::FailOffChainOrder failed: {e}"
        ),
    }
}

async fn execute_place_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
    offchain_order_id: OffchainOrderId,
) {
    let command = PositionCommand::PlaceOffChainOrder {
        offchain_order_id,
        shares: execution.shares,
        direction: execution.direction,
        executor: execution.executor,
        threshold: cqrs.execution_threshold,
    };

    match cqrs.position.send(&execution.symbol, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::PlaceOffChainOrder succeeded"
        ),
        Err(error) => error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::PlaceOffChainOrder failed: {error}"
        ),
    }
}

async fn execute_create_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
    offchain_order_id: OffchainOrderId,
) {
    let command = OffchainOrderCommand::Place {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
        executor: execution.executor,
    };

    match cqrs.offchain_order.send(&offchain_order_id, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "OffchainOrder::Place succeeded"
        ),
        Err(error) => error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "OffchainOrder::Place failed: {error}"
        ),
    }
}

fn reconstruct_log_from_queued_event(
    ctx: &EvmCtx,
    queued_event: &crate::queue::QueuedEvent,
) -> Log {
    let log_data = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TradeEvent::TakeOrderV3(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = queued_event
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: ctx.orderbook,
            data: log_data,
        },
        block_hash: None,
        block_number: Some(queued_event.block_number),
        block_timestamp,
        transaction_hash: Some(queued_event.tx_hash),
        transaction_index: None,
        log_index: Some(queued_event.log_index),
        removed: false,
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn check_and_execute_accumulated_positions<E>(
    executor: &E,
    position: &Store<Position>,
    position_projection: &Projection<Position>,
    offchain_order: &Arc<Store<OffchainOrder>>,
    threshold: &ExecutionThreshold,
) -> Result<(), EventProcessingError>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let executor_type = executor.to_supported_executor();
    let ready_positions = check_all_positions(executor, position_projection, executor_type).await?;

    if ready_positions.is_empty() {
        debug!("No accumulated positions ready for execution");
        return Ok(());
    }

    info!(
        "Found {} accumulated positions ready for execution",
        ready_positions.len()
    );

    for execution in ready_positions {
        let offchain_order_id = OffchainOrderId::new();

        info!(
            symbol = %execution.symbol,
            shares = %execution.shares,
            direction = ?execution.direction,
            %offchain_order_id,
            "Executing accumulated position"
        );

        // TODO: add broker rejection handling (load_offchain_order_failure +
        // execute_fail_offchain_order_position) like in place_offchain_order
        let command = PositionCommand::PlaceOffChainOrder {
            offchain_order_id,
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
            threshold: *threshold,
        };

        match position.send(&execution.symbol, command).await {
            Ok(()) => info!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder succeeded"
            ),
            Err(error) => error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder failed: {error}"
            ),
        }

        let command = OffchainOrderCommand::Place {
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
        };

        match offchain_order.send(&offchain_order_id, command).await {
            Ok(()) => info!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder::Place succeeded"
            ),
            Err(error) => error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder::Place failed: {error}"
            ),
        }
    }

    Ok(())
}

/// Maps database symbols to current executor-recognized tickers.
async fn wait_for_first_event_with_timeout<S1, S2>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    timeout: std::time::Duration,
) -> Option<(Vec<(TradeEvent, Log)>, u64)>
where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
{
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    let mut events = Vec::new();

    loop {
        let event_result = tokio::select! {
            Some(result) = clear_stream.next() => {
                result.map(|(event, log)| (TradeEvent::ClearV3(Box::new(event)), log))
            }
            Some(result) = take_stream.next() => {
                result.map(|(event, log)| (TradeEvent::TakeOrderV3(Box::new(event)), log))
            }
            () = &mut deadline => return None,
        };

        match event_result {
            Ok((event, log)) => {
                let Some(block_number) = log.block_number else {
                    error!("Event missing block number during startup");
                    continue;
                };
                events.push((event, log));
                return Some((std::mem::take(&mut events), block_number));
            }
            Err(error) => {
                error!("Error in event stream during startup: {error}");
            }
        }
    }
}

async fn buffer_live_events<S1, S2>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    event_buffer: &mut Vec<(TradeEvent, Log)>,
    cutoff_block: u64,
) where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
{
    loop {
        tokio::select! {
            Some(result) = clear_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::ClearV3(Box::new(event)), log));
                }
                Err(error) => error!("Error in clear event stream during backfill: {error}"),
                _ => {}
            },
            Some(result) = take_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::TakeOrderV3(Box::new(event)), log));
                }
                Err(error) => error!("Error in take event stream during backfill: {error}"),
                _ => {}
            },
            else => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, IntoLogData, TxHash, U256, address, bytes, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types;
    use futures_util::stream;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_execution::{
        Direction, ExecutorOrderId, MarketOrder, MockExecutor, MockExecutorCtx, Positive, Symbol,
        TryIntoExecutor,
    };

    use super::*;
    use crate::bindings::IOrderBookV5::{
        ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4, TakeOrderConfigV4,
    };
    use crate::conductor::builder::CqrsFrameworks;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::inventory::ImbalanceThreshold;
    use crate::offchain_order::{OffchainOrderId, PriceCents};
    use crate::onchain::trade::OnchainTrade;
    use crate::position::PositionEvent;
    use crate::rebalancing::trigger::UsdcRebalancingConfig;
    use crate::rebalancing::{RebalancingTrigger, TriggeredOperation};
    use crate::test_utils::{OnchainTradeBuilder, get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn trade_processing_cqrs(frameworks: &CqrsFrameworks) -> TradeProcessingCqrs {
        TradeProcessingCqrs {
            onchain_trade: frameworks.onchain_trade.clone(),
            position: frameworks.position.clone(),
            position_projection: frameworks.position_projection.clone(),
            offchain_order: frameworks.offchain_order.clone(),
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    #[tokio::test]
    async fn test_event_enqueued_when_trade_conversion_returns_none() {
        let pool = setup_test_db().await;

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_clear_v2_event_filtering_without_errors() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice,
            bob,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry: &frameworks.vault_registry,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        let result = process_next_queued_event(
            &MockExecutor::new(),
            &config,
            &pool,
            &provider,
            &cqrs,
            &queue_context,
        )
        .await;

        assert_eq!(result.unwrap(), None);

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_logs_info_when_event_is_filtered_out() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice,
            bob,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry: &frameworks.vault_registry,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        process_next_queued_event(
            &MockExecutor::new(),
            &config,
            &pool,
            &provider,
            &cqrs,
            &queue_context,
        )
        .await
        .unwrap();

        assert!(logs_contain("Event filtered out"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_logs_event_type_when_processing() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice,
            bob,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry: &frameworks.vault_registry,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        process_next_queued_event(
            &MockExecutor::new(),
            &config,
            &pool,
            &provider,
            &cqrs,
            &queue_context,
        )
        .await
        .unwrap();

        assert!(logs_contain("ClearV3"));
    }

    #[tokio::test]
    async fn test_conductor_abort_all() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .spawn();

        conductor.abort_all_tasks();
    }

    #[tokio::test]
    async fn test_conductor_individual_abort() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .spawn();

        let tasks = conductor.trading_tasks.as_ref().unwrap();
        tasks.order_poller.abort();
        tasks.event_processor.abort();
        tasks.position_checker.abort();
        tasks.queue_processor.abort();
        tasks.dex_event_receiver.abort();
    }

    #[tokio::test]
    async fn test_conductor_builder_returns_immediately() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .spawn();

        let tasks = conductor.trading_tasks.as_ref().unwrap();
        assert!(!tasks.order_poller.is_finished());
        assert!(!tasks.event_processor.is_finished());
        assert!(!tasks.position_checker.is_finished());
        assert!(!tasks.queue_processor.is_finished());

        conductor.abort_all_tasks();
    }

    #[tokio::test]
    async fn test_conductor_without_rebalancer() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .spawn();

        assert!(conductor.rebalancer.is_none());

        conductor.abort_all_tasks();
    }

    #[tokio::test]
    async fn test_conductor_with_rebalancer() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .with_rebalancer(fake_rebalancer)
        .spawn();

        assert!(conductor.rebalancer.is_some());

        conductor.abort_all_tasks();
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_aborted_on_abort_all() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });

        let conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .with_rebalancer(fake_rebalancer)
        .spawn();

        let rebalancer_handle = conductor.rebalancer.as_ref().unwrap();
        assert!(!rebalancer_handle.is_finished());

        conductor.abort_all_tasks();
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_survives_abort_trading_tasks() {
        let pool = setup_test_db().await;
        let config = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });

        let mut conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .with_rebalancer(fake_rebalancer)
        .spawn();

        conductor.abort_trading_tasks();

        // abort_trading_tasks does NOT abort the rebalancer
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!conductor.rebalancer.as_ref().unwrap().is_finished());

        // Trading tasks should be gone
        assert!(conductor.trading_tasks.is_none());

        // Clean up remaining infrastructure tasks
        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_trading_tasks_aborted_on_abort_trading_tasks() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let mut conductor = ConductorBuilder::new(
            config,
            pool,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
            frameworks,
        )
        .with_executor_maintenance(None)
        .with_dex_event_streams(clear_stream, take_stream)
        .spawn();

        // Capture handles before abort
        let order_poller = conductor
            .trading_tasks
            .as_ref()
            .unwrap()
            .order_poller
            .abort_handle();
        let event_processor = conductor
            .trading_tasks
            .as_ref()
            .unwrap()
            .event_processor
            .abort_handle();

        conductor.abort_trading_tasks();

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(order_poller.is_finished());
        assert!(event_processor.is_finished());
        assert!(conductor.trading_tasks.is_none());

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_idempotency_bot_restart_during_processing() {
        let pool = setup_test_db().await;

        let event1 = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log1 = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &event1, &log1).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();
        crate::queue::mark_event_processed(&pool, queued_event.id.unwrap())
            .await
            .unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        crate::queue::enqueue(&pool, &event1, &log1).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        let mut log2 = crate::test_utils::get_test_log();
        log2.log_index = Some(2);
        crate::queue::enqueue(&pool, &event1, &log2).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 1);

        let next_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next_event.log_index, 2);
        crate::queue::mark_event_processed(&pool, next_event.id.unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_deterministic_processing_order() {
        let pool = setup_test_db().await;

        let events_and_logs = vec![(100, 5), (99, 3), (100, 1), (101, 2), (99, 8)];

        for (block_num, log_idx) in &events_and_logs {
            let event = ClearV3 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: get_test_order(),
                bob: get_test_order(),
                clearConfig: ClearConfigV2 {
                    aliceInputIOIndex: U256::from(0),
                    aliceOutputIOIndex: U256::from(1),
                    bobInputIOIndex: U256::from(1),
                    bobOutputIOIndex: U256::from(0),
                    aliceBountyVaultId: B256::ZERO,
                    bobBountyVaultId: B256::ZERO,
                },
            };
            let mut log = crate::test_utils::get_test_log();
            log.block_number = Some(*block_num);
            log.log_index = Some(*log_idx);
            log.transaction_hash = Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ));

            crate::queue::enqueue(&pool, &event, &log).await.unwrap();
        }

        let expected_order = vec![(99, 3), (99, 8), (100, 1), (100, 5), (101, 2)];

        for (expected_block, expected_log_idx) in expected_order {
            let event = crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(event.block_number, expected_block);
            assert_eq!(event.log_index, expected_log_idx);
            crate::queue::mark_event_processed(&pool, event.id.unwrap())
                .await
                .unwrap();
        }

        assert!(
            crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_restart_scenarios_edge_cases() {
        let pool = setup_test_db().await;

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        let mut events = vec![];
        for i in 0..5 {
            let event = ClearV3 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: get_test_order(),
                bob: get_test_order(),
                clearConfig: ClearConfigV2 {
                    aliceInputIOIndex: U256::from(0),
                    aliceOutputIOIndex: U256::from(1),
                    bobInputIOIndex: U256::from(1),
                    bobOutputIOIndex: U256::from(0),
                    aliceBountyVaultId: B256::ZERO,
                    bobBountyVaultId: B256::ZERO,
                },
            };
            let mut log = crate::test_utils::get_test_log();
            log.log_index = Some(i);
            let mut hash_bytes = [0u8; 32];
            hash_bytes[31] = u8::try_from(i).unwrap_or(0);
            log.transaction_hash = Some(B256::from(hash_bytes));

            crate::queue::enqueue(&pool, &event, &log).await.unwrap();
            events.push((event, log));
        }

        for _ in 0..2 {
            let event = crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .unwrap();
            crate::queue::mark_event_processed(&pool, event.id.unwrap())
                .await
                .unwrap();
        }

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 3);

        let mut processed_count = 0;
        while let Some(event) = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
        {
            crate::queue::mark_event_processed(&pool, event.id.unwrap())
                .await
                .unwrap();
            processed_count += 1;
        }

        assert_eq!(processed_count, 3);
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        for (event, log) in &events {
            crate::queue::enqueue(&pool, event, log).await.unwrap();
        }

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_process_queued_event_deserialization() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(queued_event.event, TradeEvent::ClearV3(_)));

        let reconstructed_log = reconstruct_log_from_queued_event(&ctx.evm, &queued_event);
        assert_eq!(reconstructed_log.inner.address, ctx.evm.orderbook);
        assert_eq!(
            reconstructed_log.transaction_hash.unwrap(),
            queued_event.tx_hash
        );
        assert_eq!(reconstructed_log.log_index.unwrap(), queued_event.log_index);
        assert_eq!(
            reconstructed_log.block_number.unwrap(),
            queued_event.block_number
        );

        let original_log_data = clear_event.into_log_data();
        assert_eq!(reconstructed_log.inner.data, original_log_data);

        crate::queue::mark_event_processed(&pool, queued_event.id.unwrap())
            .await
            .unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_get_cutoff_block_with_timeout() {
        let pool = setup_test_db().await;
        let asserter = Asserter::new();

        asserter.push_success(&serde_json::Value::from(12345u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream = futures_util::stream::empty();
        let mut take_stream = futures_util::stream::empty();

        let cutoff_block = get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, &pool)
            .await
            .unwrap();

        assert_eq!(cutoff_block, 12345);
    }

    #[tokio::test]
    async fn test_wait_for_first_event_with_timeout_no_events() {
        let mut clear_stream = stream::empty();
        let mut take_stream = stream::empty();

        let result = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_millis(10),
        )
        .await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_wait_for_first_event_with_clear_event() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.block_number = Some(1000);

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log.clone()))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let (events, block_number) = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(block_number, 1000);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].0, TradeEvent::ClearV3(_)));
    }

    #[tokio::test]
    async fn test_wait_for_first_event_missing_block_number() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.block_number = None;

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        assert!(
            wait_for_first_event_with_timeout(
                &mut clear_stream,
                &mut take_stream,
                std::time::Duration::from_millis(100),
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn test_buffer_live_events_filtering() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut early_log = get_test_log();
        early_log.block_number = Some(99);

        let mut late_log = get_test_log();
        late_log.block_number = Some(101);

        let events = vec![
            Ok((clear_event.clone(), early_log)),
            Ok((clear_event, late_log)),
        ];

        let mut clear_stream = stream::iter(events);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();
        let mut event_buffer = Vec::new();

        buffer_live_events(&mut clear_stream, &mut take_stream, &mut event_buffer, 100).await;

        assert_eq!(event_buffer.len(), 1);
        assert_eq!(event_buffer[0].1.block_number.unwrap(), 101);
    }

    #[tokio::test]
    async fn test_process_live_event_clear_v2() {
        let pool = setup_test_db().await;

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        process_live_event(&pool, TradeEvent::ClearV3(Box::new(clear_event)), log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const ORDER_OWNER: Address = address!("0xdddddddddddddddddddddddddddddddddddddddd");
    const OTHER_OWNER: Address = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    const TEST_EQUITY_TOKEN: Address = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const TEST_VAULT_ID: B256 =
        fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
    const TEST_TX_HASH: B256 =
        fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

    fn create_order_with_usdc_and_equity_vaults(owner: Address) -> OrderV4 {
        OrderV4 {
            owner,
            evaluable: EvaluableV4 {
                interpreter: address!("0x2222222222222222222222222222222222222222"),
                store: address!("0x3333333333333333333333333333333333333333"),
                bytecode: bytes!("0x00"),
            },
            nonce: fixed_bytes!(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            validInputs: vec![
                IOV2 {
                    token: USDC_BASE,
                    vaultId: TEST_VAULT_ID,
                },
                IOV2 {
                    token: TEST_EQUITY_TOKEN,
                    vaultId: TEST_VAULT_ID,
                },
            ],
            validOutputs: vec![
                IOV2 {
                    token: USDC_BASE,
                    vaultId: TEST_VAULT_ID,
                },
                IOV2 {
                    token: TEST_EQUITY_TOKEN,
                    vaultId: TEST_VAULT_ID,
                },
            ],
        }
    }

    fn create_queued_clear_event(alice: OrderV4, bob: OrderV4) -> QueuedEvent {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice,
            bob,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        QueuedEvent {
            id: Some(1),
            tx_hash: TEST_TX_HASH,
            log_index: 0,
            block_number: 12345,
            event: TradeEvent::ClearV3(Box::new(clear_event)),
            processed: false,
            created_at: None,
            processed_at: None,
            block_timestamp: None,
        }
    }

    fn create_queued_take_event(order: OrderV4) -> QueuedEvent {
        let take_event = TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![],
            },
            input: B256::ZERO,
            output: B256::ZERO,
        };

        QueuedEvent {
            id: Some(1),
            tx_hash: TEST_TX_HASH,
            log_index: 0,
            block_number: 12345,
            event: TradeEvent::TakeOrderV3(Box::new(take_event)),
            processed: false,
            created_at: None,
            processed_at: None,
            block_timestamp: None,
        }
    }

    async fn get_vault_registry_events(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar!("SELECT event_type FROM events WHERE aggregate_type = 'VaultRegistry'")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    fn create_test_trade(symbol: &str) -> OnchainTrade {
        let tokenized_symbol = format!("t{symbol}");
        OnchainTradeBuilder::default()
            .with_symbol(&tokenized_symbol)
            .with_equity_token(TEST_EQUITY_TOKEN)
            .build()
    }

    fn test_trade_with_amount(amount: Decimal, log_index: u64) -> OnchainTrade {
        OnchainTradeBuilder::default()
            .with_symbol("tAAPL")
            .with_equity_token(TEST_EQUITY_TOKEN)
            .with_amount(amount)
            .with_log_index(log_index)
            .build()
    }

    fn create_vault_discovery_context(
        vault_registry: &Store<VaultRegistry>,
    ) -> VaultDiscoveryCtx<'_> {
        VaultDiscoveryCtx {
            vault_registry,
            orderbook: TEST_ORDERBOOK,
            order_owner: ORDER_OWNER,
        }
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_usdc_vault() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|event_name| event_name == "VaultRegistryEvent::UsdcVaultDiscovered"),
            "Expected UsdcVaultDiscovered event, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_equity_vault() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|event_name| event_name == "VaultRegistryEvent::EquityVaultDiscovered"),
            "Expected EquityVaultDiscovered event, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_from_take_event() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let order = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let queued_event = create_queued_take_event(order);
        let trade = create_test_trade("MSFT");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|event_name| event_name == "VaultRegistryEvent::UsdcVaultDiscovered"),
            "Expected UsdcVaultDiscovered event from take order"
        );
        assert!(
            events
                .iter()
                .any(|event_name| event_name == "VaultRegistryEvent::EquityVaultDiscovered"),
            "Expected EquityVaultDiscovered event from take order"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_filters_non_owner_vaults() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed even when no vaults match");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events.is_empty(),
            "Expected no events when vaults don't belong to order_owner, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_uses_correct_aggregate_id() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed");

        let expected_aggregate_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: ORDER_OWNER,
        }
        .to_string();

        let aggregate_ids: Vec<String> = sqlx::query_scalar!(
            "SELECT DISTINCT aggregate_id FROM events WHERE aggregate_type = 'VaultRegistry'"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(aggregate_ids.len(), 1, "Expected exactly one aggregate ID");
        assert_eq!(
            aggregate_ids[0], expected_aggregate_id,
            "Aggregate ID should be {expected_aggregate_id}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_uses_trade_symbol_for_equity() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("GOOG");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed");

        let events: Vec<(String, String)> = sqlx::query_as(
            "SELECT event_type, payload FROM events WHERE aggregate_type = 'VaultRegistry'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        let equity_event = events
            .iter()
            .find(|(event_type, _)| event_type == "VaultRegistryEvent::EquityVaultDiscovered")
            .expect("Should have EquityVaultDiscovered event");

        assert!(
            equity_event.1.contains("GOOG"),
            "Equity vault should use the trade's symbol (GOOG), got payload: {}",
            equity_event.1
        );
    }

    fn succeeding_order_placer() -> Arc<dyn OrderPlacer> {
        struct TestOrderPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for TestOrderPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
                Ok(ExecutorOrderId::new("TEST_BROKER_ORD"))
            }
        }

        Arc::new(TestOrderPlacer)
    }

    async fn create_cqrs_frameworks_with_order_placer(
        pool: &SqlitePool,
        order_placer: Arc<dyn OrderPlacer>,
    ) -> (CqrsFrameworks, Arc<Projection<OffchainOrder>>) {
        let onchain_trade = Arc::new(st0x_event_sorcery::test_store(pool.clone(), ()));

        let position_projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position = Arc::new(
            StoreBuilder::<Position>::new(pool.clone())
                .with(position_projection.clone())
                .build(())
                .await
                .unwrap(),
        );

        let offchain_order_projection = Projection::<OffchainOrder>::sqlite(pool.clone()).unwrap();
        let offchain_order = Arc::new(
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .with(offchain_order_projection.clone())
                .build(order_placer)
                .await
                .unwrap(),
        );

        let vault_registry_projection =
            Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone()).unwrap());
        let vault_registry = st0x_event_sorcery::test_store(pool.clone(), ());
        let snapshot = st0x_event_sorcery::test_store(pool.clone(), ());

        let offchain_order_projection = Arc::new(offchain_order_projection);

        (
            CqrsFrameworks {
                onchain_trade,
                position,
                position_projection: Arc::new(position_projection),
                offchain_order,
                offchain_order_projection: offchain_order_projection.clone(),
                vault_registry,
                vault_registry_projection,
                snapshot,
            },
            offchain_order_projection,
        )
    }

    fn trade_processing_cqrs_with_threshold(
        frameworks: &CqrsFrameworks,
        threshold: ExecutionThreshold,
    ) -> TradeProcessingCqrs {
        TradeProcessingCqrs {
            onchain_trade: frameworks.onchain_trade.clone(),
            position: frameworks.position.clone(),
            position_projection: frameworks.position_projection.clone(),
            offchain_order: frameworks.offchain_order.clone(),
            execution_threshold: threshold,
        }
    }

    /// Enqueues a ClearV3 event into the event_queue and returns the
    /// queued event with its database-assigned ID.
    async fn enqueue_and_fetch(pool: &SqlitePool, log_index: u64) -> (QueuedEvent, i64) {
        let event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.log_index = Some(log_index);
        let mut hash_bytes = [0u8; 32];
        hash_bytes[31] = u8::try_from(log_index).unwrap_or(0);
        log.transaction_hash = Some(B256::from(hash_bytes));

        crate::queue::enqueue(pool, &event, &log).await.unwrap();

        let queued = crate::queue::get_next_unprocessed_event(pool)
            .await
            .unwrap()
            .expect("should have unprocessed event");

        let event_id = queued.id.expect("queued event should have id");
        (queued, event_id)
    }

    #[tokio::test]
    async fn trade_below_threshold_does_not_place_order() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_and_fetch(&pool, 10).await;
        let trade = test_trade_with_amount(dec!(0.5), 10);

        let result = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event,
            event_id,
            trade,
            &cqrs,
        )
        .await;

        assert_eq!(
            result.unwrap(),
            None,
            "0.5 shares should not trigger execution with 1-share threshold"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.net.inner(),
            dec!(0.5),
            "Position net should reflect the accumulated trade"
        );
        assert!(
            position.pending_offchain_order_id.is_none(),
            "No offchain order should be pending"
        );

        assert_eq!(
            crate::queue::count_unprocessed(&pool).await.unwrap(),
            0,
            "Event should be marked as processed"
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_places_offchain_order() {
        let pool = setup_test_db().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_and_fetch(&pool, 20).await;
        let trade = test_trade_with_amount(dec!(1.5), 20);

        let result = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event,
            event_id,
            trade,
            &cqrs,
        )
        .await;

        let offchain_order_id = result
            .unwrap()
            .expect("1.5 shares should trigger execution with 1-share threshold");

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(position.net.inner(), dec!(1.5));
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "Position should track the pending offchain order"
        );

        let offchain_order = offchain_order_projection
            .load(&offchain_order_id)
            .await
            .expect("offchain order should not be in failed lifecycle state")
            .expect("offchain order view should exist");

        assert!(
            matches!(offchain_order, OffchainOrder::Submitted { .. }),
            "Offchain order should be Submitted after successful placement, got: {offchain_order:?}"
        );
    }

    #[tokio::test]
    async fn multiple_trades_accumulate_then_trigger() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event_1, event_id_1) = enqueue_and_fetch(&pool, 30).await;
        let trade_1 = test_trade_with_amount(dec!(0.5), 30);

        let result_1 = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_1,
            event_id_1,
            trade_1,
            &cqrs,
        )
        .await;

        assert_eq!(
            result_1.unwrap(),
            None,
            "First trade of 0.5 shares should not trigger"
        );

        let (queued_event_2, event_id_2) = enqueue_and_fetch(&pool, 31).await;
        let trade_2 = test_trade_with_amount(dec!(0.7), 31);

        let result_2 = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_2,
            event_id_2,
            trade_2,
            &cqrs,
        )
        .await;

        assert!(
            result_2.unwrap().is_some(),
            "Accumulated 1.2 shares should trigger execution with 1-share threshold"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(position.net.inner(), dec!(1.2));
        assert!(position.pending_offchain_order_id.is_some());
    }

    #[tokio::test]
    async fn pending_order_blocks_new_execution() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event_1, event_id_1) = enqueue_and_fetch(&pool, 40).await;
        let trade_1 = test_trade_with_amount(dec!(1.5), 40);

        let first_order_id = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_1,
            event_id_1,
            trade_1,
            &cqrs,
        )
        .await
        .unwrap()
        .expect("first trade should place an order");

        let (queued_event_2, event_id_2) = enqueue_and_fetch(&pool, 41).await;
        let trade_2 = test_trade_with_amount(dec!(1.5), 41);

        let result_2 = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_2,
            event_id_2,
            trade_2,
            &cqrs,
        )
        .await;

        assert_eq!(
            result_2.unwrap(),
            None,
            "Second trade should not trigger execution while first order is pending"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.net.inner(),
            dec!(3.0),
            "Both fills should be accumulated"
        );
        assert_eq!(
            position.pending_offchain_order_id,
            Some(first_order_id),
            "Only the first order should be pending"
        );
    }

    #[tokio::test]
    async fn periodic_checker_executes_after_order_completion() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        // Process first trade -> places order
        let (queued_event_1, event_id_1) = enqueue_and_fetch(&pool, 50).await;
        let trade_1 = test_trade_with_amount(dec!(1.5), 50);

        let first_order_id = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_1,
            event_id_1,
            trade_1,
            &cqrs,
        )
        .await
        .unwrap()
        .expect("first trade should place an order");

        // Process second trade -> blocked by pending order
        let (queued_event_2, event_id_2) = enqueue_and_fetch(&pool, 51).await;
        let trade_2 = test_trade_with_amount(dec!(1.5), 51);

        process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_2,
            event_id_2,
            trade_2,
            &cqrs,
        )
        .await
        .unwrap();

        // Complete the first order via CQRS
        let symbol = Symbol::new("AAPL").unwrap();

        cqrs.position
            .send(
                &symbol,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id: first_order_id,
                    shares_filled: Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
                    direction: st0x_execution::Direction::Sell,
                    executor_order_id: ExecutorOrderId::new("TEST_BROKER_ORD"),
                    price_cents: crate::offchain_order::PriceCents(15000),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Verify position is unblocked
        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_none(),
            "Order should be cleared after completion"
        );

        // Run the periodic checker - it should find the remaining net and place a new order
        let executor = st0x_execution::MockExecutor::new();

        check_and_execute_accumulated_positions(
            &executor,
            &cqrs.position,
            &cqrs.position_projection,
            &cqrs.offchain_order,
            &cqrs.execution_threshold,
        )
        .await
        .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Periodic checker should have placed a new order for remaining net position"
        );
        assert_ne!(
            position.pending_offchain_order_id.unwrap(),
            first_order_id,
            "New order should have a different ID than the completed one"
        );
    }

    #[tokio::test]
    async fn restart_recovery_processes_unprocessed_queue_items() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        // Enqueue an event (simulating events persisted before a crash)
        let (queued_event, event_id) = enqueue_and_fetch(&pool, 60).await;
        let trade = test_trade_with_amount(dec!(2.0), 60);

        // Simulate restart: process the unprocessed event
        let result = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event,
            event_id,
            trade,
            &cqrs,
        )
        .await;

        assert!(
            result.unwrap().is_some(),
            "Recovered event should trigger execution"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(position.net.inner(), dec!(2.0));
        assert!(position.pending_offchain_order_id.is_some());

        assert_eq!(
            crate::queue::count_unprocessed(&pool).await.unwrap(),
            0,
            "All events should be marked as processed after recovery"
        );
    }

    #[tokio::test]
    async fn idempotent_event_queue_prevents_double_processing() {
        let pool = setup_test_db().await;

        let event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();

        // Enqueue same event twice (same tx_hash + log_index -> INSERT OR IGNORE)
        crate::queue::enqueue(&pool, &event, &log).await.unwrap();
        crate::queue::enqueue(&pool, &event, &log).await.unwrap();

        assert_eq!(
            crate::queue::count_unprocessed(&pool).await.unwrap(),
            1,
            "Duplicate enqueue should result in only 1 row"
        );

        // Process and mark as processed
        let queued = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .expect("should have one event");

        crate::queue::mark_event_processed(&pool, queued.id.unwrap())
            .await
            .unwrap();

        assert_eq!(
            crate::queue::count_unprocessed(&pool).await.unwrap(),
            0,
            "No unprocessed events should remain"
        );

        // Re-enqueue same event after processing
        crate::queue::enqueue(&pool, &event, &log).await.unwrap();

        assert_eq!(
            crate::queue::count_unprocessed(&pool).await.unwrap(),
            0,
            "Re-enqueue of already processed event should be ignored"
        );
    }

    /// Builds an `InventoryView` with an equity imbalance: 20% onchain, 80% offchain.
    /// With a 50% target +/- 20% deviation, 20% < 30% lower bound -> TooMuchOffchain.
    fn imbalanced_inventory(symbol: &Symbol) -> InventoryView {
        InventoryView::default()
            .with_equity(symbol.clone())
            .apply_position_event(
                symbol,
                &PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(dec!(20)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                    seen_at: chrono::Utc::now(),
                },
            )
            .unwrap()
            .apply_position_event(
                symbol,
                &PositionEvent::OffChainOrderFilled {
                    offchain_order_id: OffchainOrderId::new(),
                    shares_filled: Positive::new(FractionalShares::new(dec!(80))).unwrap(),
                    direction: Direction::Buy,
                    executor_order_id: ExecutorOrderId::new("ORD1"),
                    price_cents: PriceCents(15000),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn position_events_reach_rebalancing_trigger() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");
        let test_token = address!("0x1234567890123456789012345678901234567890");

        // Seed vault registry so the trigger can resolve the token address.
        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());
        vault_registry
            .send(
                &VaultRegistryId {
                    orderbook,
                    owner: order_owner,
                },
                VaultRegistryCommand::DiscoverEquityVault {
                    token: test_token,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    discovered_in: TxHash::ZERO,
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();

        let inventory = Arc::new(RwLock::new(imbalanced_inventory(&symbol)));
        let (operation_sender, mut operation_receiver) = mpsc::channel(10);

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
                usdc: UsdcRebalancingConfig::Enabled {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
            },
            pool.clone(),
            orderbook,
            order_owner,
            inventory,
            operation_sender,
        ));

        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position_store = Arc::new(
            StoreBuilder::<Position>::new(pool.clone())
                .with(projection.clone())
                .with(Arc::clone(&trigger))
                .build(())
                .await
                .unwrap(),
        );

        // Acknowledge a fill -> fires position events -> trigger should react.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(dec!(1)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let triggered = operation_receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint operation from trigger on position event, got {triggered:?}"
        );
    }

    #[tokio::test]
    async fn inventory_updated_after_fill_via_cqrs() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");

        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        // Seed inventory with 50 offchain shares. CQRS will add 50 onchain.
        let initial_inventory = InventoryView::default()
            .with_equity(symbol.clone())
            .apply_position_event(
                &symbol,
                &PositionEvent::OffChainOrderFilled {
                    offchain_order_id: OffchainOrderId::new(),
                    shares_filled: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
                    direction: Direction::Buy,
                    executor_order_id: ExecutorOrderId::new("SEED"),
                    price_cents: PriceCents(15000),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .unwrap();

        let inventory = Arc::new(RwLock::new(initial_inventory));
        let (operation_sender, _operation_receiver) = mpsc::channel(10);

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: threshold,
                usdc: UsdcRebalancingConfig::Enabled {
                    target: threshold.target,
                    deviation: threshold.deviation,
                },
            },
            pool.clone(),
            orderbook,
            order_owner,
            Arc::clone(&inventory),
            operation_sender,
        ));

        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position_store = Arc::new(
            StoreBuilder::<Position>::new(pool.clone())
                .with(projection.clone())
                .with(Arc::clone(&trigger))
                .build(())
                .await
                .unwrap(),
        );

        // Add 50 onchain shares via CQRS -> trigger applies to inventory.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(dec!(50)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // 50 onchain / 50 offchain = 50% ratio, within 30%-70% bounds -> balanced.
        assert!(
            inventory
                .read()
                .await
                .check_equity_imbalance(&symbol, &threshold)
                .is_none(),
            "50/50 inventory should be balanced (no imbalance detected)"
        );
    }

    #[tokio::test]
    async fn balanced_position_event_does_not_trigger_rebalancing() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");

        // Start balanced: 50 onchain, 50 offchain.
        let mut initial_inventory = InventoryView::default().with_equity(symbol.clone());
        initial_inventory = initial_inventory
            .apply_position_event(
                &symbol,
                &PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(dec!(50)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                    seen_at: chrono::Utc::now(),
                },
            )
            .unwrap();
        initial_inventory = initial_inventory
            .apply_position_event(
                &symbol,
                &PositionEvent::OffChainOrderFilled {
                    offchain_order_id: OffchainOrderId::new(),
                    shares_filled: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
                    direction: Direction::Buy,
                    executor_order_id: ExecutorOrderId::new("ORD1"),
                    price_cents: PriceCents(15000),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .unwrap();

        let inventory = Arc::new(RwLock::new(initial_inventory));
        let (operation_sender, mut receiver) = mpsc::channel(10);

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
                usdc: UsdcRebalancingConfig::Enabled {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
            },
            pool.clone(),
            orderbook,
            order_owner,
            inventory,
            operation_sender,
        ));

        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position_store = Arc::new(
            StoreBuilder::new(pool.clone())
                .with(projection.clone())
                .with(trigger.clone())
                .build(())
                .await
                .unwrap(),
        );

        // Small onchain fill: 55/105 = 52.4%, within 30%-70%.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(dec!(5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "Balanced position event should not trigger rebalancing"
        );
    }

    /// Sets up an initialized position with 65% onchain / 35% offchain inventory
    /// (within the 30%-70% threshold bounds), a seeded vault registry, and the
    /// trigger wired into the position store via `build_position_cqrs`.
    async fn setup_near_upper_threshold_position() -> (
        Arc<Store<Position>>,
        mpsc::Receiver<TriggeredOperation>,
        Symbol,
    ) {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");
        let test_token = address!("0x1234567890123456789012345678901234567890");

        let vault_registry: Store<VaultRegistry> = st0x_event_sorcery::test_store(pool.clone(), ());
        vault_registry
            .send(
                &VaultRegistryId {
                    orderbook,
                    owner: order_owner,
                },
                VaultRegistryCommand::DiscoverEquityVault {
                    token: test_token,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    discovered_in: TxHash::ZERO,
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();

        // 65 onchain, 35 offchain = 65% < 70% upper bound -> within bounds.
        let initial_inventory = InventoryView::default()
            .with_equity(symbol.clone())
            .apply_position_event(
                &symbol,
                &PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(dec!(65)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                    seen_at: chrono::Utc::now(),
                },
            )
            .unwrap()
            .apply_position_event(
                &symbol,
                &PositionEvent::OffChainOrderFilled {
                    offchain_order_id: OffchainOrderId::new(),
                    shares_filled: Positive::new(FractionalShares::new(dec!(35))).unwrap(),
                    direction: Direction::Buy,
                    executor_order_id: ExecutorOrderId::new("ORD1"),
                    price_cents: PriceCents(15000),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .unwrap();

        let inventory = Arc::new(RwLock::new(initial_inventory));
        let (operation_sender, receiver) = mpsc::channel(10);

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
                usdc: UsdcRebalancingConfig::Enabled {
                    target: dec!(0.5),
                    deviation: dec!(0.2),
                },
            },
            pool.clone(),
            orderbook,
            order_owner,
            inventory,
            operation_sender,
        ));

        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position_store = Arc::new(
            StoreBuilder::new(pool.clone())
                .with(projection.clone())
                .with(trigger)
                .build(())
                .await
                .unwrap(),
        );

        (position_store, receiver, symbol)
    }

    #[tokio::test]
    async fn position_event_causing_imbalance_triggers_redemption() {
        let (position_store, mut receiver, symbol) = setup_near_upper_threshold_position().await;
        let test_token = address!("0x1234567890123456789012345678901234567890");

        // No position commands yet -> no triggers fired.
        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "No commands executed yet, channel should be empty"
        );

        // +20 onchain: 85 onchain, 35 offchain, total=120, target=60, excess=25.
        // 85/120 = 70.8% > 70% upper bound -> triggers Redemption.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(dec!(20)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let triggered = receiver.try_recv().unwrap();
        assert_eq!(
            triggered,
            TriggeredOperation::Redemption {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(dec!(25)),
                token: test_token,
            }
        );
    }

    #[tokio::test]
    async fn in_progress_guard_blocks_duplicate_trigger() {
        let (position_store, mut receiver, symbol) = setup_near_upper_threshold_position().await;
        let test_token = address!("0x1234567890123456789012345678901234567890");

        // First fill pushes over: 85/120 = 70.8% > 70% -> triggers Redemption(25).
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(dec!(20)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let first = receiver.try_recv().unwrap();
        assert_eq!(
            first,
            TriggeredOperation::Redemption {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(dec!(25)),
                token: test_token,
            }
        );

        // Second fill while in-progress NOT cleared -> no second trigger.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: FractionalShares::new(dec!(5)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "In-progress guard should block duplicate trigger"
        );
    }

    #[tokio::test]
    async fn restart_hydrates_position_view_from_persisted_events() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // First "session": create position store, execute commands.
        {
            let (position_store, position_projection) = build_position_cqrs(&pool).await.unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::AcknowledgeOnChainFill {
                        symbol: symbol.clone(),
                        threshold: ExecutionThreshold::whole_share(),
                        trade_id: TradeId {
                            tx_hash: TxHash::random(),
                            log_index: 0,
                        },
                        amount: FractionalShares::new(dec!(10)),
                        direction: Direction::Buy,
                        price_usdc: dec!(150.0),
                        block_timestamp: chrono::Utc::now(),
                    },
                )
                .await
                .unwrap();

            let position = position_projection
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after first session");

            assert_eq!(position.net, FractionalShares::new(dec!(10)));
            assert_eq!(position.accumulated_long, FractionalShares::new(dec!(10)));
        }
        // position_store dropped here, simulating shutdown.

        // Second "session": create NEW position store against the same pool.
        {
            let (_position_store, position_projection) = build_position_cqrs(&pool).await.unwrap();

            let position = position_projection
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after restart");

            assert_eq!(
                position.net,
                FractionalShares::new(dec!(10)),
                "Net position should survive restart via persisted view"
            );
            assert_eq!(
                position.accumulated_long,
                FractionalShares::new(dec!(10)),
                "Accumulated long should survive restart via persisted view"
            );
        }
    }

    #[tokio::test]
    async fn broker_rejection_does_not_leave_position_stuck() {
        fn failing_order_placer() -> crate::offchain_order::OffchainOrderServices {
            struct RejectingOrderPlacer;

            #[async_trait::async_trait]
            impl crate::offchain_order::OrderPlacer for RejectingOrderPlacer {
                async fn place_market_order(
                    &self,
                    _order: MarketOrder,
                ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("API error (403 Forbidden): trade denied due to pattern day trading protection".into())
                }
            }

            Arc::new(RejectingOrderPlacer)
        }

        let pool = setup_test_db().await;
        let frameworks = create_cqrs_frameworks_with_order_placer(&pool, failing_order_placer());
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_and_fetch(&pool, 70).await;
        let trade = test_trade_with_amount(1.5, 70);

        process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event,
            event_id,
            trade,
            &cqrs,
        )
        .await
        .unwrap();

        let position =
            crate::position::load_position(&cqrs.position_query, &Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("position should exist");

        // The broker rejected the order. The position must NOT be stuck with a
        // phantom pending order that will never complete.
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position should not have a pending order after broker rejection, \
             but got {:?}. This leaves the position permanently stuck.",
            position.pending_offchain_order_id
        );

        // After clearing the stuck state, the periodic position checker should
        // find the position still has a net imbalance and place a new order.
        // Rebuild CQRS with a broker that accepts orders (simulating the
        // transient PDT restriction being lifted).
        let frameworks = create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer());
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let executor = st0x_execution::MockExecutor::new();

        check_and_execute_accumulated_positions(
            &executor,
            &pool,
            &cqrs.position_cqrs,
            &cqrs.position_query,
            &cqrs.offchain_order_cqrs,
            &cqrs.offchain_order_query,
            &cqrs.execution_threshold,
        )
        .await
        .unwrap();

        let position =
            crate::position::load_position(&cqrs.position_query, &Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Periodic checker should retry execution after broker rejection is cleared"
        );
    }

    /// Tests that `replay_all_views` populates views from historical events.
    ///
    /// This test reproduces the production bug where:
    /// 1. Events were persisted without their query processors wired
    /// 2. After fixing the wiring, views remain empty because cqrs-es
    ///    doesn't automatically replay historical events
    ///
    /// The test verifies that `replay_all_views` recovers from this state.
    #[tokio::test]
    async fn replay_all_views_populates_views_from_historical_events() {
        let pool = setup_test_db().await;
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let owner = address!("0x2222222222222222222222222222222222222222");
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        // Step 1: Create historical events WITHOUT a query processor
        // (simulating events created before the wiring fix)
        let bare_cqrs = wire::test_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![], ());

        bare_cqrs
            .execute(
                &aggregate_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: address!("0x3333333333333333333333333333333333333333"),
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    discovered_in: TxHash::ZERO,
                    symbol: Symbol::new("AAPL").unwrap(),
                },
            )
            .await
            .unwrap();

        // Step 2: Call the production replay function
        super::replay_all_views(&pool).await.unwrap();

        // Step 3: Verify the view is populated from historical events
        let view_repo = SqliteViewRepository::<VaultRegistryAggregate, VaultRegistryAggregate>::new(
            pool,
            "vault_registry_view".to_string(),
        );
        let view = view_repo.load(&aggregate_id).await.unwrap();

        assert!(
            view.is_some(),
            "View should be populated after replay_all_views"
        );
    }

    /// Tests that wired CQRS frameworks correctly pick up from where replay left off.
    ///
    /// This verifies the handoff between:
    /// 1. Unwired GenericQuery instances used during replay
    /// 2. Wired GenericQuery instances in the CQRS framework
    ///
    /// The wired framework must process new events without re-processing
    /// already-replayed events or corrupting the view state.
    #[tokio::test]
    async fn wired_cqrs_picks_up_after_replay() {
        let pool = setup_test_db().await;
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let owner = address!("0x2222222222222222222222222222222222222222");
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        // Step 1: Create historical event WITHOUT a query processor
        let bare_cqrs = wire::test_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![], ());

        let first_token = address!("0x3333333333333333333333333333333333333333");
        let first_vault_id =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        bare_cqrs
            .execute(
                &aggregate_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: first_token,
                    vault_id: first_vault_id,
                    discovered_in: TxHash::ZERO,
                    symbol: Symbol::new("AAPL").unwrap(),
                },
            )
            .await
            .unwrap();

        // Step 2: Run replay to populate view from historical events
        super::replay_all_views(&pool).await.unwrap();

        // Step 3: Create a properly wired CQRS framework (simulating post-startup)
        let wired_cqrs = super::setup_vault_registry(pool.clone());

        // Step 4: Execute a NEW command through the wired framework
        let second_token = address!("0x4444444444444444444444444444444444444444");
        let second_vault_id =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000002");

        wired_cqrs
            .execute(
                &aggregate_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: second_token,
                    vault_id: second_vault_id,
                    discovered_in: TxHash::ZERO,
                    symbol: Symbol::new("GOOGL").unwrap(),
                },
            )
            .await
            .unwrap();

        // Step 5: Verify the view contains BOTH vaults
        let view_repo = SqliteViewRepository::<VaultRegistryAggregate, VaultRegistryAggregate>::new(
            pool,
            "vault_registry_view".to_string(),
        );
        let lifecycle = view_repo.load(&aggregate_id).await.unwrap().unwrap();

        let Lifecycle::Live(view) = lifecycle else {
            panic!("Expected Live state, got: {lifecycle:?}");
        };

        assert!(
            view.equity_vaults.contains_key(&first_token),
            "View should contain vault from replayed event"
        );
        assert!(
            view.equity_vaults.contains_key(&second_token),
            "View should contain vault from post-replay event"
        );
        assert_eq!(
            view.equity_vaults.len(),
            2,
            "View should have exactly 2 vaults"
        );
    }
}
