//! Orchestrates the main bot loop: subscribes to DEX events, queues them,
//! processes trades, places offsetting broker orders, and manages background
//! tasks (order polling, rebalancing, inventory tracking). [`Conductor`] owns
//! the task handles; [`run_market_hours_loop`] drives the lifecycle.

mod builder;

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types;
use cqrs_es::{AggregateError, Query};
use futures_util::{Stream, StreamExt};
use sqlite_es::{SqliteCqrs, sqlite_cqrs};
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

use st0x_execution::{
    AlpacaBrokerApiError, AlpacaTradingApiError, EmptySymbolError, ExecutionError, Executor,
    MarketOrder, SchwabError, SupportedExecutor, Symbol,
};

use crate::bindings::IOrderBookV5::{ClearV3, IOrderBookV5Instance, TakeOrderV3};
use crate::cctp::USDC_BASE;
use crate::config::{Ctx, CtxError};
use crate::dashboard::ServerMessage;
use crate::dual_write::DualWriteContext;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::{
    InventoryPollingService, InventorySnapshotAggregate, InventorySnapshotQuery, InventoryView,
};
use crate::offchain::execution::{OffchainExecution, find_execution_by_id};
use crate::offchain::order_poller::OrderStatusPoller;
use crate::offchain_order::BrokerOrderId;
use crate::onchain::accumulator::{
    CleanedUpExecution, TradeProcessingResult, check_all_accumulated_positions,
};
use crate::onchain::backfill::backfill_events;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::{TradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain::vault::VaultService;
use crate::onchain::{EvmCtx, OnChainError, OnchainTrade, accumulator};
use crate::queue::{
    EventQueueError, QueuedEvent, enqueue, get_next_unprocessed_event, mark_event_processed,
};
use crate::rebalancing::{
    RebalancingCqrsFrameworks, RebalancingCtx, RebalancingTrigger, RebalancingTriggerConfig,
    build_rebalancing_queries, spawn_rebalancer,
};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::vault_registry::{
    VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand, VaultRegistryError,
};
pub(crate) use builder::{ConductorBuilder, CqrsFrameworks};

pub(crate) struct Conductor {
    pub(crate) executor_maintenance: Option<JoinHandle<()>>,
    pub(crate) order_poller: JoinHandle<()>,
    pub(crate) dex_event_receiver: JoinHandle<()>,
    pub(crate) event_processor: JoinHandle<()>,
    pub(crate) position_checker: JoinHandle<()>,
    pub(crate) queue_processor: JoinHandle<()>,
    pub(crate) rebalancer: Option<JoinHandle<()>>,
    pub(crate) inventory_poller: Option<JoinHandle<()>>,
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
    #[error("Database transaction error: {0}")]
    Transaction(#[from] sqlx::Error),
    #[error("Execution with ID {0} not found")]
    ExecutionNotFound(i64),
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Schwab execution error: {0}")]
    Schwab(#[from] SchwabError),
    #[error("Alpaca Broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Alpaca Trading API error: {0}")]
    AlpacaTradingApi(#[from] AlpacaTradingApiError),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error("Config error: {0}")]
    Config(#[from] CtxError),
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] AggregateError<VaultRegistryError>),
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
    const RERUN_DELAY_SECS: u64 = 10;

    let timeout = executor
        .wait_until_market_open()
        .await
        .map_err(|e| anyhow::anyhow!("Market hours check failed: {e}"))?;

    log_market_status(timeout);

    let mut conductor = match Conductor::start(
        &ctx,
        &pool,
        executor.clone(),
        executor_maintenance,
        event_sender.clone(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return retry_after_failure(executor, ctx, pool, e, RERUN_DELAY_SECS, event_sender)
                .await;
        }
    };

    info!("Market opened, conductor running");

    run_conductor_until_market_close(&mut conductor, executor, ctx, pool, timeout, event_sender)
        .await
}

fn log_market_status(timeout: Duration) {
    let timeout_minutes = timeout.as_secs() / 60;
    if timeout_minutes < 60 * 24 {
        info!(
            "Market is open, starting conductor \
             (will timeout in {timeout_minutes} minutes)"
        );
    } else {
        info!("Starting conductor (no market hours restrictions)");
    }
}

async fn retry_after_failure<E>(
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    error: anyhow::Error,
    delay_secs: u64,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    error!("Failed to start conductor: {error}, retrying in {delay_secs} seconds");
    tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;

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

async fn run_conductor_until_market_close<E>(
    conductor: &mut Conductor,
    executor: E,
    ctx: Ctx,
    pool: SqlitePool,
    timeout: Duration,
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
    conductor: &Conductor,
    result: Result<(), anyhow::Error>,
) -> anyhow::Result<()> {
    info!("Conductor completed");
    conductor.order_poller.abort();
    conductor.dex_event_receiver.abort();
    conductor.event_processor.abort();
    conductor.position_checker.abort();
    conductor.queue_processor.abort();
    if let Some(ref handle) = conductor.executor_maintenance {
        handle.abort();
    }
    if let Some(ref handle) = conductor.rebalancer {
        handle.abort();
    }
    if let Some(ref handle) = conductor.inventory_poller {
        handle.abort();
    }
    result?;
    info!("Conductor completed successfully, continuing to next market session");
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
    vault_registry_cqrs: &'a SqliteCqrs<VaultRegistryAggregate>,
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

        backfill_events(pool, &provider, &ctx.evm, cutoff_block - 1).await?;

        let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
        let position_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
        let offchain_order_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
        let vault_registry_cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let dual_write_context = DualWriteContext::with_threshold(
            pool.clone(),
            onchain_trade_cqrs,
            position_cqrs,
            offchain_order_cqrs,
            ctx.execution_threshold,
        );

        let inventory = Arc::new(RwLock::new(InventoryView::default()));

        let snapshot_query = InventorySnapshotQuery::new(inventory.clone());
        let snapshot_cqrs = sqlite_cqrs(
            pool.clone(),
            vec![Box::new(snapshot_query) as Box<dyn Query<InventorySnapshotAggregate>>],
            (),
        );

        let rebalancer = match ctx.rebalancing_ctx() {
            Some(rebalancing_config) => Some(
                spawn_rebalancing_infrastructure(
                    rebalancing_config,
                    pool,
                    ctx,
                    &inventory,
                    event_sender,
                    &provider,
                )
                .await?,
            ),
            None => None,
        };

        let frameworks = CqrsFrameworks {
            dual_write_context,
            vault_registry_cqrs,
            snapshot_cqrs,
        };

        let mut builder = ConductorBuilder::new(
            ctx.clone(),
            pool.clone(),
            cache,
            provider,
            executor,
            frameworks,
        )
        .with_executor_maintenance(executor_maintenance)
        .with_dex_event_streams(clear_stream, take_stream);

        if let Some(rebalancer_handle) = rebalancer {
            builder = builder.with_rebalancer(rebalancer_handle);
        }

        Ok(builder.spawn())
    }

    pub(crate) async fn wait_for_completion(&mut self) -> Result<(), anyhow::Error> {
        let maintenance_task =
            wait_for_optional_task(&mut self.executor_maintenance, "Executor maintenance");
        let rebalancer_task = wait_for_optional_task(&mut self.rebalancer, "Rebalancer");
        let inventory_poller_task =
            wait_for_optional_task(&mut self.inventory_poller, "Inventory poller");

        let ((), (), (), poller, dex, processor, position, queue) = tokio::join!(
            maintenance_task,
            rebalancer_task,
            inventory_poller_task,
            &mut self.order_poller,
            &mut self.dex_event_receiver,
            &mut self.event_processor,
            &mut self.position_checker,
            &mut self.queue_processor
        );

        log_task_result(poller, "Order poller");
        log_task_result(dex, "DEX event receiver");
        log_task_result(processor, "Event processor");
        log_task_result(position, "Position checker");
        log_task_result(queue, "Queue processor");

        Ok(())
    }

    pub(crate) fn abort_trading_tasks(&self) {
        info!(
            "Aborting trading tasks \
             (keeping broker maintenance and DEX event receiver alive)"
        );

        self.order_poller.abort();
        self.event_processor.abort();
        self.position_checker.abort();
        self.queue_processor.abort();

        if let Some(ref handle) = self.rebalancer {
            handle.abort();
        }
        if let Some(ref handle) = self.inventory_poller {
            handle.abort();
        }

        info!("Trading tasks aborted successfully (DEX events will continue buffering)");
    }
}

async fn spawn_rebalancing_infrastructure<P: Provider + Clone + Send + 'static>(
    rebalancing_ctx: &RebalancingCtx,
    pool: &SqlitePool,
    ctx: &Ctx,
    inventory: &Arc<RwLock<InventoryView>>,
    event_sender: broadcast::Sender<ServerMessage>,
    provider: &P,
) -> anyhow::Result<JoinHandle<()>> {
    info!("Initializing rebalancing infrastructure");

    let signer = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key)?;
    let market_maker_wallet = signer.address();

    const OPERATION_CHANNEL_CAPACITY: usize = 100;
    let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

    let trigger = Arc::new(RebalancingTrigger::new(
        RebalancingTriggerConfig {
            equity_threshold: rebalancing_ctx.equity_threshold,
            usdc_threshold: rebalancing_ctx.usdc_threshold,
        },
        pool.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        inventory.clone(),
        operation_sender,
    ));

    let event_broadcast = Some(event_sender);

    let frameworks = RebalancingCqrsFrameworks {
        mint: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<TokenizedEquityMint>(
                trigger.clone(),
                event_broadcast.clone(),
            ),
            (),
        )),
        redemption: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<EquityRedemption>(trigger.clone(), event_broadcast.clone()),
            (),
        )),
        usdc: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<UsdcRebalance>(trigger, event_broadcast),
            (),
        )),
    };

    Ok(spawn_rebalancer(
        rebalancing_ctx,
        provider.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        operation_receiver,
        frameworks,
    )
    .await?)
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
    if let Err(e) = result {
        error!("{task_name} task panicked: {e}");
    }
}

fn spawn_order_poller<E: Executor + Clone + Send + 'static>(
    ctx: &Ctx,
    pool: &SqlitePool,
    executor: E,
    dual_write_context: DualWriteContext,
) -> JoinHandle<()> {
    let poller_config = ctx.get_order_poller_config();
    info!(
        "Starting order status poller with interval: {:?}, max jitter: {:?}",
        poller_config.polling_interval, poller_config.max_jitter
    );

    let poller = OrderStatusPoller::new(poller_config, pool.clone(), executor, dual_write_context);
    tokio::spawn(async move {
        if let Err(e) = poller.run().await {
            error!("Order poller failed: {e}");
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
            if let Err(e) = process_live_event(&pool, event, log).await {
                error!("Failed to process live event: {e}");
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
    dual_write_context: DualWriteContext,
    vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate>,
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
            &dual_write_context,
            &vault_registry_cqrs,
        )
        .await;
    })
}

fn spawn_periodic_accumulated_position_check<E>(
    executor: E,
    pool: SqlitePool,
    dual_write_context: DualWriteContext,
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
            if let Err(e) =
                check_and_execute_accumulated_positions(&executor, &pool, &dual_write_context).await
            {
                error!("Periodic accumulated position check failed: {e}");
            }
        }
    })
}

fn spawn_inventory_poller<P, E>(
    pool: SqlitePool,
    vault_service: Arc<VaultService<P>>,
    executor: E,
    orderbook: Address,
    order_owner: Address,
    snapshot_cqrs: SqliteCqrs<InventorySnapshotAggregate>,
) -> JoinHandle<()>
where
    P: Provider + Clone + Send + 'static,
    E: Executor + Clone + Send + 'static,
{
    info!("Starting inventory poller");

    let service = InventoryPollingService::new(
        vault_service,
        executor,
        pool,
        orderbook,
        order_owner,
        snapshot_cqrs,
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

        if !handle_event_result(event_result, &event_sender) {
            break;
        }
    }
}

fn handle_event_result(
    result: Result<(TradeEvent, Log), sol_types::Error>,
    sender: &UnboundedSender<(TradeEvent, Log)>,
) -> bool {
    match result {
        Ok((event, log)) => {
            trace!(
                "Received blockchain event: tx_hash={:?}, \
                 log_index={:?}, block_number={:?}",
                log.transaction_hash, log.log_index, log.block_number
            );
            if sender.send((event, log)).is_err() {
                error!("Event receiver dropped, shutting down");
                return false;
            }
        }
        Err(e) => error!("Error in event stream: {e}"),
    }
    true
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
    dual_write_context: &DualWriteContext,
    vault_registry_cqrs: &SqliteCqrs<VaultRegistryAggregate>,
) where
    P: Provider + Clone,
    E: Executor + Clone,
    EventProcessingError: From<E::Error>,
{
    info!("Starting queue processor service");

    let feed_id_cache = FeedIdCache::default();

    log_unprocessed_event_count(pool).await;

    let executor_type = executor.to_supported_executor();

    let queue_context = QueueProcessingCtx {
        cache,
        feed_id_cache: &feed_id_cache,
        vault_registry_cqrs,
    };

    loop {
        let result = process_next_queued_event(
            executor_type,
            ctx,
            pool,
            &provider,
            dual_write_context,
            &queue_context,
        )
        .await;

        handle_queue_processing_result(executor, pool, dual_write_context, result).await;
    }
}

async fn log_unprocessed_event_count(pool: &SqlitePool) {
    match crate::queue::count_unprocessed(pool).await {
        Ok(count) if count > 0 => {
            info!("Found {count} unprocessed events from previous sessions to process");
        }
        Ok(_) => info!("No unprocessed events found, starting fresh"),
        Err(e) => error!("Failed to count unprocessed events: {e}"),
    }
}

async fn handle_queue_processing_result<E>(
    executor: &E,
    pool: &SqlitePool,
    dual_write_context: &DualWriteContext,
    result: Result<Option<OffchainExecution>, EventProcessingError>,
) where
    E: Executor + Clone,
    EventProcessingError: From<E::Error>,
{
    match result {
        Ok(Some(execution)) => {
            if let Some(exec_id) = execution.id
                && let Err(e) =
                    execute_pending_offchain_execution(executor, pool, dual_write_context, exec_id)
                        .await
            {
                error!("Failed to execute offchain order {exec_id}: {e}");
            }
        }
        Ok(None) => sleep(Duration::from_millis(100)).await,
        Err(e) => {
            error!("Error processing queued event: {e}");
            sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Context for queue event processing containing caches and CQRS components.
struct QueueProcessingCtx<'a> {
    cache: &'a SymbolCache,
    feed_id_cache: &'a FeedIdCache,
    vault_registry_cqrs: &'a SqliteCqrs<VaultRegistryAggregate>,
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn process_next_queued_event<P: Provider + Clone>(
    executor_type: SupportedExecutor,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    dual_write_context: &DualWriteContext,
    queue_context: &QueueProcessingCtx<'_>,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    let queued_event = get_next_unprocessed_event(pool).await?;
    let Some(queued_event) = queued_event else {
        trace!("No unprocessed events in queue");
        return Ok(None);
    };

    let event_id = extract_event_id(&queued_event)?;

    let onchain_trade = convert_event_to_trade(
        ctx,
        queue_context.cache,
        provider,
        &queued_event,
        queue_context.feed_id_cache,
    )
    .await?;

    let Some(trade) = onchain_trade else {
        return handle_filtered_event(pool, &queued_event, event_id).await;
    };

    let vault_discovery_context = VaultDiscoveryCtx {
        vault_registry_cqrs: queue_context.vault_registry_cqrs,
        orderbook: ctx.evm.orderbook,
        order_owner: ctx.order_owner()?,
    };

    process_valid_trade(
        executor_type,
        pool,
        &queued_event,
        event_id,
        trade,
        dual_write_context,
        &vault_discovery_context,
    )
    .await
}

fn extract_event_id(queued_event: &QueuedEvent) -> Result<i64, EventProcessingError> {
    queued_event
        .id
        .ok_or(EventProcessingError::Queue(EventQueueError::MissingEventId))
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
async fn discover_vaults_for_trade(
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

    let aggregate_id = VaultRegistry::aggregate_id(context.orderbook, context.order_owner);

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
            .vault_registry_cqrs
            .execute(&aggregate_id, command)
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

#[tracing::instrument(skip(pool, queued_event), fields(event_id), level = tracing::Level::DEBUG)]
async fn handle_filtered_event(
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    info!(
        "Event filtered out (no matching owner): \
         event_type={:?}, tx_hash={:?}, log_index={}",
        match &queued_event.event {
            TradeEvent::ClearV3(_) => "ClearV3",
            TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
        },
        queued_event.tx_hash,
        queued_event.log_index
    );

    let mut sql_tx = pool.begin().await.inspect_err(|e| {
        error!("Failed to begin transaction for filtered event: {e}");
    })?;

    mark_event_processed(&mut sql_tx, event_id).await?;

    sql_tx.commit().await.inspect_err(|e| {
        error!("Failed to commit transaction for filtered event: {e}");
    })?;

    Ok(None)
}

#[tracing::instrument(
    skip(pool, queued_event, trade, dual_write_context, vault_discovery_context),
    fields(event_id, symbol = %trade.symbol),
    level = tracing::Level::INFO
)]
async fn process_valid_trade(
    executor_type: SupportedExecutor,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    dual_write_context: &DualWriteContext,
    vault_discovery_context: &VaultDiscoveryCtx<'_>,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
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

    discover_vaults_for_trade(queued_event, &trade, vault_discovery_context).await?;

    let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
    let _guard = symbol_lock.lock().await;

    info!(
        "Processing queued trade: symbol={}, amount={}, \
         direction={:?}, tx_hash={:?}, log_index={}",
        trade.symbol, trade.amount, trade.direction, trade.tx_hash, trade.log_index
    );

    process_trade_within_transaction(
        executor_type,
        pool,
        queued_event,
        event_id,
        trade,
        dual_write_context,
    )
    .await
}

async fn execute_witness_trade(
    dual_write_context: &DualWriteContext,
    trade: &OnchainTrade,
    block_number: u64,
) {
    match crate::dual_write::witness_trade(dual_write_context, trade, block_number).await {
        Ok(()) => info!(
            "Successfully executed OnChainTrade::Witness \
             command: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        ),
        Err(e) => error!(
            "Failed to execute OnChainTrade::Witness \
             command: {e}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn execute_initialize_position(dual_write_context: &DualWriteContext, trade: &OnchainTrade) {
    let base_symbol = trade.symbol.base();

    match crate::dual_write::initialize_position(
        dual_write_context,
        base_symbol,
        dual_write_context.execution_threshold(),
    )
    .await
    {
        Ok(()) => info!(
            "Successfully initialized Position aggregate for symbol={}",
            base_symbol
        ),
        Err(e) => debug!(
            "Position initialization skipped (likely already exists): {e}, symbol={}",
            base_symbol
        ),
    }
}

async fn execute_acknowledge_fill(dual_write_context: &DualWriteContext, trade: &OnchainTrade) {
    match crate::dual_write::acknowledge_onchain_fill(dual_write_context, trade).await {
        Ok(()) => info!(
            "Successfully executed Position::AcknowledgeOnChainFill \
             command: tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
        Err(e) => error!(
            "Failed to execute Position::AcknowledgeOnChainFill \
             command: {e}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn execute_new_execution_dual_write(
    dual_write_context: &DualWriteContext,
    execution: &OffchainExecution,
    base_symbol: &Symbol,
) {
    execute_place_offchain_order(dual_write_context, execution, base_symbol).await;
    execute_place_order(dual_write_context, execution).await;
}

async fn execute_place_offchain_order(
    dual_write_context: &DualWriteContext,
    execution: &OffchainExecution,
    base_symbol: &Symbol,
) {
    match crate::dual_write::place_offchain_order(dual_write_context, execution, base_symbol).await
    {
        Ok(()) => info!(
            "Successfully executed Position::PlaceOffChainOrder \
             command: execution_id={:?}, symbol={}",
            execution.id, base_symbol
        ),
        Err(e) => error!(
            "Failed to execute Position::PlaceOffChainOrder \
             command: {e}, execution_id={:?}, symbol={}",
            execution.id, base_symbol
        ),
    }
}

async fn execute_place_order(dual_write_context: &DualWriteContext, execution: &OffchainExecution) {
    match crate::dual_write::place_order(dual_write_context, execution).await {
        Ok(()) => info!(
            "Successfully executed OffchainOrder::Place \
             command: execution_id={:?}, symbol={}",
            execution.id, execution.symbol
        ),
        Err(e) => error!(
            "Failed to execute OffchainOrder::Place \
             command: {e}, execution_id={:?}, symbol={}",
            execution.id, execution.symbol
        ),
    }
}

async fn execute_stale_execution_cleanup_dual_write(
    dual_write_context: &DualWriteContext,
    cleaned_up_executions: Vec<CleanedUpExecution>,
) {
    for cleaned_up in cleaned_up_executions {
        execute_single_stale_cleanup(dual_write_context, cleaned_up).await;
    }
}

async fn execute_single_stale_cleanup(
    dual_write_context: &DualWriteContext,
    cleaned_up: CleanedUpExecution,
) {
    execute_mark_stale_failed(dual_write_context, &cleaned_up).await;
    execute_fail_stale_offchain(dual_write_context, &cleaned_up).await;
}

async fn execute_mark_stale_failed(
    dual_write_context: &DualWriteContext,
    cleaned_up: &CleanedUpExecution,
) {
    let execution_id = cleaned_up.execution_id;

    match crate::dual_write::mark_failed(
        dual_write_context,
        execution_id,
        cleaned_up.error_reason.clone(),
    )
    .await
    {
        Ok(()) => info!(
            "Successfully executed OffchainOrder::MarkFailed \
             command for stale execution {execution_id}"
        ),
        Err(e) => error!(
            "Failed to execute OffchainOrder::MarkFailed \
             command for stale execution {execution_id}: {e}"
        ),
    }
}

async fn execute_fail_stale_offchain(
    dual_write_context: &DualWriteContext,
    cleaned_up: &CleanedUpExecution,
) {
    let execution_id = cleaned_up.execution_id;
    let symbol = &cleaned_up.symbol;

    match crate::dual_write::fail_offchain_order(
        dual_write_context,
        execution_id,
        symbol,
        cleaned_up.error_reason.clone(),
    )
    .await
    {
        Ok(()) => info!(
            "Successfully executed Position::FailOffChainOrder \
             command for stale execution {execution_id}, symbol {symbol}"
        ),
        Err(e) => error!(
            "Failed to execute Position::FailOffChainOrder \
             command for stale execution {execution_id}, symbol {symbol}: {e}"
        ),
    }
}

async fn process_trade_within_transaction(
    executor_type: SupportedExecutor,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    dual_write_context: &DualWriteContext,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    // Update Position aggregate FIRST so threshold check sees current state
    execute_initialize_position(dual_write_context, &trade).await;
    execute_acknowledge_fill(dual_write_context, &trade).await;

    let mut sql_tx = pool.begin().await.inspect_err(|e| {
        error!(
            "Failed to begin transaction \
             for event processing: {e}"
        );
    })?;

    info!(
        "Started transaction for atomic event processing: \
         event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    let TradeProcessingResult {
        execution,
        cleaned_up_executions,
    } = accumulator::process_onchain_trade(
        &mut sql_tx,
        dual_write_context,
        trade.clone(),
        executor_type,
    )
    .await
    .inspect_err(|e| {
        error!(
            "Failed to process trade through accumulator: {e}, tx_hash={:?}, log_index={}",
            queued_event.tx_hash, queued_event.log_index
        );
    })?;

    mark_event_processed(&mut sql_tx, event_id)
        .await
        .map_err(|e| {
            error!("Failed to mark event {event_id} as processed: {e}");
            EventProcessingError::Queue(e)
        })?;

    sql_tx.commit().await.inspect_err(|e| {
        error!(
            "Failed to commit transaction for event processing: {e}, event_id={}, tx_hash={:?}",
            event_id, queued_event.tx_hash
        );
    })?;

    info!(
        "Successfully committed atomic event processing: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    // Record OnChainTrade event (Position already updated above)
    execute_witness_trade(dual_write_context, &trade, queued_event.block_number).await;

    if let Some(ref exec) = execution {
        let base_symbol = trade.symbol.base();
        execute_new_execution_dual_write(dual_write_context, exec, base_symbol).await;
    }

    execute_stale_execution_cleanup_dual_write(dual_write_context, cleaned_up_executions).await;

    Ok(execution)
}

fn reconstruct_log_from_queued_event(
    evm_ctx: &EvmCtx,
    queued_event: &crate::queue::QueuedEvent,
) -> Log {
    use alloy::primitives::IntoLogData;

    let log_data = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TradeEvent::TakeOrderV3(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = queued_event
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: evm_ctx.orderbook,
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
    pool: &SqlitePool,
    dual_write_context: &DualWriteContext,
) -> Result<(), EventProcessingError>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let executor_type = executor.to_supported_executor();
    let executions =
        check_all_accumulated_positions(pool, dual_write_context, executor_type).await?;

    if executions.is_empty() {
        debug!("No accumulated positions ready for execution");
        return Ok(());
    }

    info!(
        "Found {} accumulated positions ready for execution",
        executions.len()
    );

    for execution in executions {
        let Some(execution_id) = execution.id else {
            error!("Execution returned from check_all_accumulated_positions has None ID");
            continue;
        };

        info!(
            "Executing accumulated position for symbol={}, shares={}, direction={:?}, execution_id={}",
            execution.symbol, execution.shares, execution.direction, execution_id
        );

        // Emit Placed event BEFORE spawning execution task.
        // This ensures the OffchainOrder aggregate is initialized before ConfirmSubmission is attempted.
        execute_new_execution_dual_write(dual_write_context, &execution, &execution.symbol).await;

        let pool_clone = pool.clone();
        let executor_clone = executor.clone();
        let dual_write_clone = dual_write_context.clone();
        tokio::spawn(async move {
            if let Err(e) = execute_pending_offchain_execution(
                &executor_clone,
                &pool_clone,
                &dual_write_clone,
                execution_id,
            )
            .await
            {
                error!(
                    "Failed to execute accumulated position for execution_id {}: {e}",
                    execution_id
                );
            } else {
                info!(
                    "Successfully executed accumulated position for execution_id {}",
                    execution_id
                );
            }
        });
    }

    Ok(())
}

/// Maps database symbols to current executor-recognized tickers.
/// Handles corporate actions like SPLG -> SPYM rename (Oct 31, 2025).
/// Remove once proper tSPYM tokens are issued onchain.
fn to_executor_ticker(symbol: &Symbol) -> Result<Symbol, EmptySymbolError> {
    match symbol.to_string().as_str() {
        "SPLG" => Symbol::new("SPYM"),
        _ => Ok(symbol.clone()),
    }
}

#[tracing::instrument(skip(executor, pool, dual_write_context), level = tracing::Level::INFO)]
async fn execute_pending_offchain_execution<E>(
    executor: &E,
    pool: &SqlitePool,
    dual_write_context: &DualWriteContext,
    execution_id: i64,
) -> Result<(), EventProcessingError>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let execution = find_execution_by_id(pool, execution_id)
        .await?
        .ok_or(EventProcessingError::ExecutionNotFound(execution_id))?;

    info!("Executing offchain order: {execution:?}");

    let market_order = MarketOrder {
        symbol: to_executor_ticker(&execution.symbol)?,
        shares: execution.shares,
        direction: execution.direction,
    };

    let placement = executor.place_market_order(market_order).await?;

    info!("Order placed with ID: {}", placement.order_id);

    let broker_order_id = BrokerOrderId::new(&placement.order_id);

    if let Err(e) = crate::dual_write::confirm_submission(
        dual_write_context,
        execution_id,
        broker_order_id.clone(),
    )
    .await
    {
        error!(
            "Failed to execute OffchainOrder::ConfirmSubmission command: {e}, \
            execution_id={execution_id}, order_id={broker_order_id:?}"
        );
    } else {
        info!(
            "Successfully executed OffchainOrder::ConfirmSubmission command: \
            execution_id={execution_id}, order_id={broker_order_id:?}"
        );
    }

    Ok(())
}

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
        tokio::select! {
            Some(result) = clear_stream.next() => {
                if let Some(first_event) = handle_startup_clear_event(result, &mut events) {
                    return Some(first_event);
                }
            }
            Some(result) = take_stream.next() => {
                if let Some(first_event) = handle_startup_take_event(result, &mut events) {
                    return Some(first_event);
                }
            }
            () = &mut deadline => return None,
        }
    }
}

fn handle_startup_clear_event(
    result: Result<(ClearV3, Log), sol_types::Error>,
    events: &mut Vec<(TradeEvent, Log)>,
) -> Option<(Vec<(TradeEvent, Log)>, u64)> {
    match result {
        Ok((event, log)) => {
            let Some(block_number) = log.block_number else {
                error!("ClearV3 event missing block number");
                return None;
            };
            events.push((TradeEvent::ClearV3(Box::new(event)), log));
            Some((std::mem::take(events), block_number))
        }
        Err(e) => {
            error!("Error in clear event stream during startup: {e}");
            None
        }
    }
}

fn handle_startup_take_event(
    result: Result<(TakeOrderV3, Log), sol_types::Error>,
    events: &mut Vec<(TradeEvent, Log)>,
) -> Option<(Vec<(TradeEvent, Log)>, u64)> {
    match result {
        Ok((event, log)) => {
            let Some(block_number) = log.block_number else {
                error!("TakeOrderV3 event missing block number");
                return None;
            };
            events.push((TradeEvent::TakeOrderV3(Box::new(event)), log));
            Some((std::mem::take(events), block_number))
        }
        Err(e) => {
            error!("Error in take event stream during startup: {e}");
            None
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
                Err(e) => error!("Error in clear event stream during backfill: {e}"),
                _ => {}
            },
            Some(result) = take_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::TakeOrderV3(Box::new(event)), log));
                }
                Err(e) => error!("Error in take event stream during backfill: {e}"),
                _ => {}
            },
            else => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, IntoLogData, U256, address, bytes, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types;
    use futures_util::stream;
    use rust_decimal::Decimal;
    use sqlite_es::sqlite_cqrs;

    use st0x_execution::{
        Direction, FractionalShares, MockExecutorCtx, OrderState, OrderStatus, PersistenceError,
        Positive, SupportedExecutor, Symbol, TryIntoExecutor,
    };

    use super::*;
    use crate::bindings::IOrderBookV5::{ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4};
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::offchain::execution::{
        OffchainExecution, find_executions_by_symbol_status_and_broker,
    };
    use crate::onchain::io::Usdc;
    use crate::onchain::trade::OnchainTrade;
    use crate::test_utils::{OnchainTradeBuilder, get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;
    use crate::tokenized_symbol;

    fn abort_all_conductor_tasks(conductor: Conductor) {
        if let Some(handle) = conductor.executor_maintenance {
            handle.abort();
        }

        if let Some(handle) = conductor.rebalancer {
            handle.abort();
        }

        if let Some(handle) = conductor.inventory_poller {
            handle.abort();
        }

        conductor.order_poller.abort();
        conductor.dex_event_receiver.abort();
        conductor.event_processor.abort();
        conductor.position_checker.abort();
        conductor.queue_processor.abort();
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

        let trade_count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(trade_count, 0);
    }

    #[tokio::test]
    async fn test_onchain_trade_duplicate_handling() {
        let pool = setup_test_db().await;

        let existing_trade = OnchainTradeBuilder::new()
            .with_tx_hash(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ))
            .with_log_index(293)
            .with_symbol("AAPL0x")
            .with_amount(5.0)
            .with_price(20000.0)
            .build();
        let mut sql_tx = pool.begin().await.unwrap();
        existing_trade
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let duplicate_trade = existing_trade.clone();
        let mut sql_tx2 = pool.begin().await.unwrap();
        let err = duplicate_trade
            .save_within_transaction(&mut sql_tx2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Persistence(PersistenceError::Database(ref db_err))
                if matches!(db_err, sqlx::Error::Database(_))
            ),
            "Expected database constraint violation, got: {err:?}"
        );
        sql_tx2.rollback().await.unwrap();

        let count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_duplicate_trade_handling() {
        let pool = setup_test_db().await;

        let existing_trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            log_index: 293,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 5.0,
            direction: Direction::Sell,
            price: Usdc::new(20000.0).unwrap(),
            block_timestamp: None,
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };
        let mut sql_tx = pool.begin().await.unwrap();
        existing_trade
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let duplicate_trade = existing_trade.clone();
        let mut sql_tx2 = pool.begin().await.unwrap();
        let err = duplicate_trade
            .save_within_transaction(&mut sql_tx2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Persistence(PersistenceError::Database(ref db_err))
                if matches!(db_err, sqlx::Error::Database(_))
            ),
            "Expected database constraint violation, got: {err:?}"
        );
        sql_tx2.rollback().await.unwrap();

        let count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_complete_event_processing_flow() {
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
        let log = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();

        if let TradeEvent::ClearV3(boxed_clear_event) = queued_event.event {
            let cache = SymbolCache::default();
            let http_provider =
                ProviderBuilder::new().connect_http("http://localhost:8545".parse().unwrap());

            let feed_id_cache = FeedIdCache::default();
            let order_owner = ctx.order_owner().unwrap();
            if let Ok(Some(trade)) = OnchainTrade::try_from_clear_v3(
                &ctx.evm,
                &cache,
                &http_provider,
                *boxed_clear_event,
                log,
                &feed_id_cache,
                order_owner,
            )
            .await
            {
                let dual_write_context = DualWriteContext::new(pool.clone());

                let mut sql_tx = pool.begin().await.unwrap();
                let TradeProcessingResult { .. } = accumulator::process_onchain_trade(
                    &mut sql_tx,
                    &dual_write_context,
                    trade,
                    SupportedExecutor::DryRun,
                )
                .await
                .unwrap();
                sql_tx.commit().await.unwrap();
            }
        }

        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let remaining_count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(remaining_count, 0);
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
        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
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
        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, next_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
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
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
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
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
        }

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 3);

        let mut processed_count = 0;
        while let Some(event) = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
        {
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
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

        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
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

    #[tokio::test]
    async fn test_clear_v2_event_filtering_without_errors() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut alice_order = get_test_order();
        let mut bob_order = get_test_order();

        alice_order.owner = address!("0x1111111111111111111111111111111111111111");
        bob_order.owner = address!("0x2222222222222222222222222222222222222222");

        let clear_event = ClearV3 {
            sender: address!("0x3333333333333333333333333333333333333333"),
            alice: alice_order,
            bob: bob_order,
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

        let dual_write_context = DualWriteContext::new(pool.clone());
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &vault_registry_cqrs,
        };

        let result = process_next_queued_event(
            SupportedExecutor::DryRun,
            &ctx,
            &pool,
            &provider,
            &dual_write_context,
            &queue_context,
        )
        .await;

        assert!(result.unwrap().is_none());

        let remaining_count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(remaining_count, 0);
    }

    #[tokio::test]
    async fn test_execute_pending_offchain_execution_not_found() {
        let pool = setup_test_db().await;
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let error =
            execute_pending_offchain_execution(&executor, &pool, &dual_write_context, 99999)
                .await
                .unwrap_err();

        assert!(matches!(
            error,
            EventProcessingError::ExecutionNotFound(99999)
        ));
    }

    #[tokio::test]
    async fn test_execute_onchain_trade_dual_write_creates_events() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());

        let mut trade = OnchainTradeBuilder::new()
            .with_symbol("NVDA0x")
            .with_amount(5.5)
            .with_price(450.0)
            .build();
        trade.direction = Direction::Buy;
        trade.block_timestamp = Some(chrono::Utc::now());

        // Test the individual dual-write functions that were split out
        execute_witness_trade(&dual_write_context, &trade, 12345).await;
        execute_initialize_position(&dual_write_context, &trade).await;
        execute_acknowledge_fill(&dual_write_context, &trade).await;

        // Verify OnChainTrade event was created
        let onchain_trade_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            onchain_trade_events.len(),
            1,
            "Expected 1 OnChainTrade event, got {onchain_trade_events:?}"
        );
        assert_eq!(onchain_trade_events[0], "OnChainTradeEvent::Filled");

        let position_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'Position' AND aggregate_id = 'NVDA' \
            ORDER BY sequence"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            position_events.len(),
            2,
            "Expected 2 Position events (Initialized, OnChainOrderFilled), got {position_events:?}"
        );
        assert_eq!(position_events[0], "PositionEvent::Initialized");
        assert_eq!(position_events[1], "PositionEvent::OnChainOrderFilled");
    }

    #[tokio::test]
    async fn test_execute_new_execution_dual_write_creates_events() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("GOOGL").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();

        crate::dual_write::initialize_position(
            &dual_write_context,
            &symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let mut onchain_trade = OnchainTradeBuilder::new()
            .with_symbol("GOOGL0x")
            .with_amount(5.0)
            .with_price(140.0)
            .build();
        onchain_trade.direction = Direction::Buy;
        onchain_trade.block_timestamp = Some(chrono::Utc::now());

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &onchain_trade)
            .await
            .unwrap();

        let execution = OffchainExecution {
            id: Some(42),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        execute_new_execution_dual_write(&dual_write_context, &execution, &symbol).await;

        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = '42' \
            ORDER BY sequence"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_events.len(),
            1,
            "Expected 1 OffchainOrder event (Placed), got {offchain_order_events:?}"
        );
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");

        let position_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'Position' AND aggregate_id = 'GOOGL' \
            ORDER BY sequence"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            position_events.len(),
            3,
            "Expected 3 Position events (Initialized, OnChainOrderFilled, OffChainOrderPlaced), \
            got {position_events:?}"
        );
        assert_eq!(position_events[0], "PositionEvent::Initialized");
        assert_eq!(position_events[1], "PositionEvent::OnChainOrderFilled");
        assert_eq!(position_events[2], "PositionEvent::OffChainOrderPlaced");
    }

    #[tokio::test]
    async fn test_execute_pending_offchain_execution_calls_confirm_submission() {
        let pool = setup_test_db().await;
        let broker = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AMZN").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(10))).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let pending_state = OrderState::Pending;
        let execution_id = pending_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        execute_pending_offchain_execution(&broker, &pool, &dual_write_context, execution_id)
            .await
            .unwrap();

        let aggregate_id = execution_id.to_string();
        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
            ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_events.len(),
            2,
            "Expected 2 OffchainOrder events (Placed, Submitted), got {offchain_order_events:?}"
        );
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
    }

    /// Regression test for the dual-write bug where the legacy `offchain_trades` table
    /// was not updated from PENDING to SUBMITTED after order placement.
    ///
    /// The order poller queries for SUBMITTED orders from the legacy table, but if
    /// `execute_pending_offchain_execution` doesn't update the legacy table, the order
    /// will never be polled and will remain stuck in PENDING state indefinitely.
    ///
    /// This test verifies that after `execute_pending_offchain_execution`:
    /// 1. The event store has the Submitted event (dual-write path)
    /// 2. The legacy `offchain_trades` table has SUBMITTED status with broker order_id
    #[tokio::test]
    async fn test_execute_pending_offchain_execution_updates_legacy_table_to_submitted() {
        let pool = setup_test_db().await;
        let broker = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("BMNR").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(1))).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let pending_state = OrderState::Pending;
        let execution_id = pending_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let legacy_row_before = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(legacy_row_before.status, "PENDING");
        assert!(
            legacy_row_before.order_id.is_none(),
            "order_id should be None before execution"
        );

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        execute_pending_offchain_execution(&broker, &pool, &dual_write_context, execution_id)
            .await
            .unwrap();

        let aggregate_id = execution_id.to_string();
        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
            ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_events.len(),
            2,
            "Expected 2 OffchainOrder events (Placed, Submitted), got {offchain_order_events:?}"
        );
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");

        let legacy_row_after = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            legacy_row_after.status, "SUBMITTED",
            "Legacy table should be updated to SUBMITTED status after order placement. \
            Currently stuck at PENDING which causes the order poller to never find this order."
        );

        assert!(
            legacy_row_after.order_id.is_some(),
            "Legacy table should have broker order_id after order placement. \
            Without this, the order poller cannot query Schwab for order status."
        );
    }

    /// Tests that the order_id stored in the legacy table matches the broker's returned order_id.
    #[tokio::test]
    async fn test_execute_pending_offchain_execution_stores_correct_order_id() {
        let pool = setup_test_db().await;
        let broker = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let pending_state = OrderState::Pending;
        let execution_id = pending_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Sell,
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        execute_pending_offchain_execution(&broker, &pool, &dual_write_context, execution_id)
            .await
            .unwrap();

        let legacy_row = sqlx::query!(
            "SELECT order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let order_id = legacy_row
            .order_id
            .expect("order_id should be set after execution");

        assert!(!order_id.is_empty(), "order_id should not be empty");

        assert!(
            order_id.starts_with("TEST_"),
            "MockBroker order_id should start with 'TEST_', got: {order_id}"
        );
    }

    /// Tests that orders updated to SUBMITTED can be found by the order poller's query.
    /// This verifies the fix enables the order poller to track orders correctly.
    #[tokio::test]
    async fn test_order_poller_can_find_orders_after_execution() {
        let pool = setup_test_db().await;
        let broker = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("NVDA").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(3))).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let pending_state = OrderState::Pending;
        let execution_id = pending_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        let submitted_before = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(symbol.clone()),
            OrderStatus::Submitted,
            Some(SupportedExecutor::DryRun),
        )
        .await
        .unwrap();

        assert!(
            submitted_before.is_empty(),
            "No SUBMITTED orders should exist before execution"
        );

        execute_pending_offchain_execution(&broker, &pool, &dual_write_context, execution_id)
            .await
            .unwrap();

        let submitted_after = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(symbol.clone()),
            OrderStatus::Submitted,
            Some(SupportedExecutor::DryRun),
        )
        .await
        .unwrap();

        assert_eq!(
            submitted_after.len(),
            1,
            "Order poller query should find exactly 1 SUBMITTED order after execution"
        );

        assert_eq!(submitted_after[0].id, Some(execution_id));

        match &submitted_after[0].state {
            OrderState::Submitted { order_id } => {
                assert!(
                    order_id.starts_with("TEST_"),
                    "Order should have broker order_id starting with TEST_, got: {order_id}"
                );
            }
            other => panic!("Expected Submitted state, got: {other:?}"),
        }
    }

    /// Tests that both legacy table and event store are updated atomically.
    /// Verifies the dual-write pattern works correctly.
    #[tokio::test]
    async fn test_execute_pending_offchain_execution_dual_write_consistency() {
        let pool = setup_test_db().await;
        let broker = MockExecutorCtx.try_into_executor().await.unwrap();
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("GOOGL").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(2))).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let pending_state = OrderState::Pending;
        let execution_id = pending_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        execute_pending_offchain_execution(&broker, &pool, &dual_write_context, execution_id)
            .await
            .unwrap();

        let legacy_row = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let legacy_order_id = legacy_row
            .order_id
            .expect("Legacy table should have order_id");

        let aggregate_id = execution_id.to_string();
        let event_payload: String = sqlx::query_scalar!(
            r#"SELECT payload as "payload!: String" FROM events
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ?
            AND event_type = 'OffchainOrderEvent::Submitted'"#,
            aggregate_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(
            event_payload.contains(&legacy_order_id),
            "Event store order_id should match legacy table order_id. \
            Legacy: {legacy_order_id}, Event payload: {event_payload}"
        );

        assert_eq!(legacy_row.status, "SUBMITTED");
    }

    #[tokio::test]
    async fn test_conductor_abort_all() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        assert!(!conductor.order_poller.is_finished());
        assert!(!conductor.event_processor.is_finished());
        assert!(!conductor.position_checker.is_finished());
        assert!(!conductor.queue_processor.is_finished());

        abort_all_conductor_tasks(conductor);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_conductor_individual_abort() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        let order_handle = conductor.order_poller;
        let event_handle = conductor.event_processor;
        let position_handle = conductor.position_checker;
        let queue_handle = conductor.queue_processor;

        assert!(!order_handle.is_finished());
        assert!(!event_handle.is_finished());
        assert!(!position_handle.is_finished());
        assert!(!queue_handle.is_finished());

        order_handle.abort();
        event_handle.abort();
        position_handle.abort();
        queue_handle.abort();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(order_handle.is_finished());
        assert!(event_handle.is_finished());
        assert!(position_handle.is_finished());
        assert!(queue_handle.is_finished());
    }

    #[tokio::test]
    async fn test_conductor_builder_returns_immediately() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let start_time = std::time::Instant::now();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        let elapsed = start_time.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "ConductorBuilder should return quickly, took: {elapsed:?}"
        );

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_without_rebalancer() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        assert!(conductor.rebalancer.is_none());
        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_with_rebalancer() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let rebalancer_handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .with_rebalancer(rebalancer_handle)
            .spawn();

        assert!(conductor.rebalancer.is_some());
        assert!(!conductor.rebalancer.as_ref().unwrap().is_finished());

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_aborted_on_abort_all() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let rebalancer_handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .with_rebalancer(rebalancer_handle)
            .spawn();

        let rebalancer_ref = conductor.rebalancer.as_ref().unwrap();
        assert!(!rebalancer_ref.is_finished());

        abort_all_conductor_tasks(conductor);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_aborted_on_abort_trading_tasks() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let frameworks = CqrsFrameworks {
            dual_write_context: DualWriteContext::new(pool.clone()),
            vault_registry_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
            snapshot_cqrs: sqlite_cqrs(pool.clone(), vec![], ()),
        };

        let rebalancer_handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });

        let conductor = ConductorBuilder::new(ctx, pool, cache, provider, executor, frameworks)
            .with_executor_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .with_rebalancer(rebalancer_handle)
            .spawn();

        assert!(!conductor.rebalancer.as_ref().unwrap().is_finished());

        conductor.abort_trading_tasks();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(conductor.rebalancer.as_ref().unwrap().is_finished());

        abort_all_conductor_tasks(conductor);
    }

    #[test]
    fn test_to_executor_ticker_splg_maps_to_spym() {
        let splg = Symbol::new("SPLG").unwrap();
        assert_eq!(to_executor_ticker(&splg).unwrap().to_string(), "SPYM");
    }

    #[test]
    fn test_to_executor_ticker_other_symbols_unchanged() {
        for ticker in ["AAPL", "NVDA", "MSTR", "IAU", "COIN"] {
            let symbol = Symbol::new(ticker).unwrap();
            assert_eq!(to_executor_ticker(&symbol).unwrap().to_string(), ticker);
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_logs_info_when_event_is_filtered_out() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let dual_write_context = DualWriteContext::new(pool.clone());
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

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

        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &vault_registry_cqrs,
        };

        let result = process_next_queued_event(
            SupportedExecutor::DryRun,
            &ctx,
            &pool,
            &provider,
            &dual_write_context,
            &queue_context,
        )
        .await;

        result.unwrap();
        assert!(
            logs_contain("Event filtered out"),
            "Expected info log when event is filtered"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_logs_event_type_when_processing() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let dual_write_context = DualWriteContext::new(pool.clone());
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

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

        let queue_context = QueueProcessingCtx {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &vault_registry_cqrs,
        };

        process_next_queued_event(
            SupportedExecutor::DryRun,
            &ctx,
            &pool,
            &provider,
            &dual_write_context,
            &queue_context,
        )
        .await
        .unwrap();

        assert!(
            logs_contain("ClearV3"),
            "Expected log to mention event type"
        );
    }

    /// Helper to set up accumulated position state for testing check_and_execute_accumulated_positions.
    /// Returns the symbol string for use in assertions.
    async fn setup_accumulated_position_state(
        pool: &SqlitePool,
        dual_write_context: &DualWriteContext,
        symbol: &Symbol,
    ) -> String {
        crate::dual_write::initialize_position(
            dual_write_context,
            symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let mut trade = OnchainTradeBuilder::new()
            .with_symbol("RKLB0x")
            .with_amount(1.2)
            .with_price(15.0)
            .build();
        trade.direction = Direction::Sell;
        trade.block_timestamp = Some(chrono::Utc::now());

        crate::dual_write::acknowledge_onchain_fill(dual_write_context, &trade)
            .await
            .unwrap();

        let tx_hash_str = trade.tx_hash.to_string();
        let log_index_i64 = i64::try_from(trade.log_index).unwrap();
        let now = chrono::Utc::now();

        sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, \
             price_usdc, block_timestamp, created_at) VALUES (?, ?, 'RKLB0x', 1.2, 'SELL', 15.0, ?, ?)",
            tx_hash_str,
            log_index_i64,
            now,
            now
        )
        .execute(pool)
        .await
        .unwrap();

        let symbol_str = symbol.to_string();
        sqlx::query!(
            "INSERT INTO trade_accumulators (symbol, accumulated_long, accumulated_short, \
             pending_execution_id, last_updated) VALUES (?, 0.0, 1.2, NULL, ?)",
            symbol_str,
            now
        )
        .execute(pool)
        .await
        .unwrap();

        symbol_str
    }

    /// Regression test for #250: check_and_execute_accumulated_positions must emit Placed event
    /// before spawning execution tasks, otherwise ConfirmSubmission fails on uninitialized aggregate.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_accumulated_position_execution_emits_placed_event_before_submission() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let symbol = Symbol::new("RKLB").unwrap();

        let symbol_str =
            setup_accumulated_position_state(&pool, &dual_write_context, &symbol).await;

        check_and_execute_accumulated_positions(&executor, &pool, &dual_write_context)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let execution_id = sqlx::query!(
            "SELECT id FROM offchain_trades WHERE symbol = ?",
            symbol_str
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .id
        .expect("execution_id should exist");

        let aggregate_id = execution_id.to_string();
        let events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OffchainOrder' \
             AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            events.len(),
            2,
            "Expected 2 events (Placed, Submitted), got {events:?}"
        );
        assert_eq!(
            events[0], "OffchainOrderEvent::Placed",
            "First event should be Placed"
        );
        assert_eq!(
            events[1], "OffchainOrderEvent::Submitted",
            "Second event should be Submitted"
        );

        assert!(
            !logs_contain("operation on uninitialized state"),
            "Bug: ConfirmSubmission attempted before Placed event"
        );

        let status: String = sqlx::query_scalar!(
            "SELECT status FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            status, "SUBMITTED",
            "Legacy table should show SUBMITTED status"
        );
    }

    use crate::bindings::IOrderBookV5::TakeOrderConfigV4;
    use crate::cctp::USDC_BASE;
    use crate::queue::QueuedEvent;

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
        let tokenized_symbol = format!("{symbol}0x");
        OnchainTradeBuilder::default()
            .with_symbol(&tokenized_symbol)
            .with_equity_token(TEST_EQUITY_TOKEN)
            .build()
    }

    fn create_vault_discovery_context(
        vault_registry_cqrs: &SqliteCqrs<VaultRegistryAggregate>,
    ) -> VaultDiscoveryCtx<'_> {
        VaultDiscoveryCtx {
            vault_registry_cqrs,
            orderbook: TEST_ORDERBOOK,
            order_owner: ORDER_OWNER,
        }
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_usdc_vault() {
        let pool = setup_test_db().await;
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|e| e == "VaultRegistryEvent::UsdcVaultDiscovered"),
            "Expected UsdcVaultDiscovered event, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_equity_vault() {
        let pool = setup_test_db().await;
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|e| e == "VaultRegistryEvent::EquityVaultDiscovered"),
            "Expected EquityVaultDiscovered event, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_from_take_event() {
        let pool = setup_test_db().await;
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let order = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let queued_event = create_queued_take_event(order);
        let trade = create_test_trade("MSFT");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let events = get_vault_registry_events(&pool).await;

        assert!(
            events
                .iter()
                .any(|e| e == "VaultRegistryEvent::UsdcVaultDiscovered"),
            "Expected UsdcVaultDiscovered event from take order"
        );
        assert!(
            events
                .iter()
                .any(|e| e == "VaultRegistryEvent::EquityVaultDiscovered"),
            "Expected EquityVaultDiscovered event from take order"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_filters_non_owner_vaults() {
        let pool = setup_test_db().await;
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
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
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed");

        let expected_aggregate_id =
            crate::vault_registry::VaultRegistry::aggregate_id(TEST_ORDERBOOK, ORDER_OWNER);

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
        let vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate> =
            sqlite_cqrs(pool.clone(), vec![], ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_queued_clear_event(alice, bob);
        let trade = create_test_trade("GOOG");

        let context = create_vault_discovery_context(&vault_registry_cqrs);
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
}
