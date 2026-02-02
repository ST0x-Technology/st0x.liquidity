mod builder;

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types;
use cqrs_es::Query;
use cqrs_es::persist::GenericQuery;
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

use st0x_execution::{
    EmptySymbolError, Executor, ExecutorOrderId, FractionalShares, MarketOrder, SupportedExecutor,
    Symbol,
};

use crate::bindings::IOrderBookV5::{ClearV3, IOrderBookV5Instance, TakeOrderV3};
use crate::cctp::USDC_BASE;
use crate::config::Config;
use crate::dashboard::ServerMessage;
use crate::equity_redemption::EquityRedemption;
use crate::error::{EventProcessingError, EventQueueError};
use crate::inventory::{
    InventoryPollingService, InventorySnapshotAggregate, InventorySnapshotQuery, InventoryView,
};
use crate::offchain::order_poller::OrderStatusPoller;
use crate::offchain_order::{
    OffchainOrder, OffchainOrderAggregate, OffchainOrderCommand, OffchainOrderCqrs,
    OffchainOrderId, OffchainOrderServices, OrderPlacer,
};
use crate::onchain::accumulator::{
    ExecutionParams, check_all_positions, check_execution_readiness,
};
use crate::onchain::backfill::backfill_events;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::{TradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain::vault::VaultService;
use crate::onchain::{EvmConfig, OnchainTrade};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeCqrs};
use crate::position::{
    Position, PositionAggregate, PositionCommand, PositionCqrs, PositionQuery, TradeId,
};
use crate::queue::{QueuedEvent, enqueue, get_next_unprocessed_event, mark_event_processed};
use crate::rebalancing::{
    RebalancingConfig, RebalancingCqrsFrameworks, RebalancingTrigger, RebalancingTriggerConfig,
    build_rebalancing_queries, spawn_rebalancer,
};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::threshold::ExecutionThreshold;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::vault_registry::{VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand};
pub(crate) use builder::{ConductorBuilder, CqrsFrameworks};

/// Context for vault discovery operations during trade processing.
struct VaultDiscoveryContext<'a> {
    vault_registry_cqrs: &'a SqliteCqrs<VaultRegistryAggregate>,
    orderbook: Address,
    order_owner: Address,
}

/// Bundles CQRS frameworks used throughout the trade processing pipeline.
struct TradeProcessingCqrs {
    onchain_trade_cqrs: Arc<OnChainTradeCqrs>,
    position_cqrs: Arc<PositionCqrs>,
    position_query: Arc<PositionQuery>,
    offchain_order_cqrs: Arc<OffchainOrderCqrs>,
    execution_threshold: ExecutionThreshold,
}

/// Adapter that bridges the generic Executor trait to the OrderPlacer
/// trait used by the OffchainOrder aggregate's Services.
pub(crate) struct ExecutorOrderPlacer<E>(pub E);

#[async_trait::async_trait]
impl<E: Executor> OrderPlacer for ExecutorOrderPlacer<E> {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
        let mapped_order = MarketOrder {
            symbol: to_executor_ticker(&order.symbol)?,
            ..order
        };

        let placement = self.0.place_market_order(mapped_order).await?;
        Ok(ExecutorOrderId::new(&placement.order_id.to_string()))
    }
}

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

pub(crate) async fn run_market_hours_loop<E>(
    executor: E,
    config: Config,
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
        &config,
        &pool,
        executor.clone(),
        executor_maintenance,
        event_sender.clone(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return retry_after_failure(executor, config, pool, e, RERUN_DELAY_SECS, event_sender)
                .await;
        }
    };

    info!("Market opened, conductor running");

    run_conductor_until_market_close(
        &mut conductor,
        executor,
        config,
        pool,
        timeout,
        event_sender,
    )
    .await
}

fn log_market_status(timeout: Duration) {
    let timeout_minutes = timeout.as_secs() / 60;
    if timeout_minutes < 60 * 24 {
        info!("Market is open, starting conductor (will timeout in {timeout_minutes} minutes)");
    } else {
        info!("Starting conductor (no market hours restrictions)");
    }
}

async fn retry_after_failure<E>(
    executor: E,
    config: Config,
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
        config,
        pool,
        new_maintenance,
        event_sender,
    ))
    .await
}

async fn run_conductor_until_market_close<E>(
    conductor: &mut Conductor,
    executor: E,
    config: Config,
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
            handle_market_close(conductor, executor, config, pool, event_sender).await
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
    config: Config,
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
        config,
        pool,
        next_maintenance,
        event_sender,
    ))
    .await
}

impl Conductor {
    pub(crate) async fn start<E>(
        config: &Config,
        pool: &SqlitePool,
        executor: E,
        executor_maintenance: Option<JoinHandle<()>>,
        event_sender: broadcast::Sender<ServerMessage>,
    ) -> anyhow::Result<Self>
    where
        E: Executor + Clone + Send + 'static,
        EventProcessingError: From<E::Error>,
    {
        let ws = WsConnect::new(config.evm.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let cache = SymbolCache::default();
        let orderbook = IOrderBookV5Instance::new(config.evm.orderbook, &provider);

        let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        let cutoff_block =
            get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, pool).await?;

        backfill_events(pool, &provider, &config.evm, cutoff_block - 1).await?;

        let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

        let position_view_repo = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "position_view".to_string(),
        ));
        let position_query = GenericQuery::new(position_view_repo.clone());
        let position_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(position_view_repo))
                as Box<dyn Query<PositionAggregate>>],
            (),
        ));

        let offchain_order_view_repo = Arc::new(SqliteViewRepository::<
            OffchainOrderAggregate,
            OffchainOrderAggregate,
        >::new(
            pool.clone(), "offchain_order_view".to_string()
        ));
        let order_placer: OffchainOrderServices = Arc::new(ExecutorOrderPlacer(executor.clone()));
        let offchain_order_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(offchain_order_view_repo))],
            order_placer,
        ));
        let vault_registry_cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let inventory = Arc::new(RwLock::new(InventoryView::default()));

        let snapshot_query = InventorySnapshotQuery::new(inventory.clone());
        let snapshot_cqrs = sqlite_cqrs(
            pool.clone(),
            vec![Box::new(snapshot_query) as Box<dyn Query<InventorySnapshotAggregate>>],
            (),
        );

        let rebalancer = match config.rebalancing_config() {
            Some(rebalancing_config) => Some(
                spawn_rebalancing_infrastructure(
                    rebalancing_config,
                    pool,
                    config,
                    &inventory,
                    event_sender,
                    &provider,
                )
                .await?,
            ),
            None => None,
        };

        let frameworks = CqrsFrameworks {
            pool: pool.clone(),
            onchain_trade_cqrs,
            position_cqrs,
            position_query: Arc::new(position_query),
            offchain_order_cqrs,
            vault_registry_cqrs,
            snapshot_cqrs,
        };

        let mut builder = ConductorBuilder::new(
            config.clone(),
            pool.clone(),
            cache,
            provider,
            executor,
            config.execution_threshold,
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
        info!("Aborting trading tasks (keeping broker maintenance and DEX event receiver alive)");

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
    rebalancing_config: &RebalancingConfig,
    pool: &SqlitePool,
    config: &Config,
    inventory: &Arc<RwLock<InventoryView>>,
    event_sender: broadcast::Sender<ServerMessage>,
    provider: &P,
) -> anyhow::Result<JoinHandle<()>> {
    info!("Initializing rebalancing infrastructure");

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
    let market_maker_wallet = signer.address();

    const OPERATION_CHANNEL_CAPACITY: usize = 100;
    let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

    let trigger = Arc::new(RebalancingTrigger::new(
        RebalancingTriggerConfig {
            equity_threshold: rebalancing_config.equity_threshold,
            usdc_threshold: rebalancing_config.usdc_threshold,
        },
        pool.clone(),
        config.evm.orderbook,
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
        rebalancing_config,
        provider.clone(),
        config.evm.orderbook,
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
        Err(e) if e.is_cancelled() => info!("{task_name} cancelled (expected during shutdown)"),
        Err(e) => error!("{task_name} task panicked: {e}"),
    }
}

fn log_task_result(result: Result<(), tokio::task::JoinError>, task_name: &str) {
    if let Err(e) = result {
        error!("{task_name} task panicked: {e}");
    }
}

fn spawn_order_poller<E: Executor + Clone + Send + 'static>(
    config: &Config,
    pool: &SqlitePool,
    executor: E,
    offchain_order_cqrs: Arc<OffchainOrderCqrs>,
    position_cqrs: Arc<PositionCqrs>,
) -> JoinHandle<()> {
    let poller_config = config.get_order_poller_config();
    info!(
        "Starting order status poller with interval: {:?}, max jitter: {:?}",
        poller_config.polling_interval, poller_config.max_jitter
    );

    let poller = OrderStatusPoller::new(
        poller_config,
        pool.clone(),
        executor,
        offchain_order_cqrs,
        position_cqrs,
    );
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
    config: &Config,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
    cqrs: TradeProcessingCqrs,
    vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate>,
) -> JoinHandle<()>
where
    P: Provider + Clone + Send + 'static,
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    info!("Starting queue processor service");
    let config_clone = config.clone();
    let pool_clone = pool.clone();
    let cache_clone = cache.clone();

    tokio::spawn(async move {
        run_queue_processor(
            &executor,
            &config_clone,
            &pool_clone,
            &cache_clone,
            provider,
            &cqrs,
            &vault_registry_cqrs,
        )
        .await;
    })
}

fn spawn_periodic_accumulated_position_check<E>(
    executor: E,
    pool: SqlitePool,
    position_cqrs: Arc<PositionCqrs>,
    position_query: Arc<PositionQuery>,
    offchain_order_cqrs: Arc<OffchainOrderCqrs>,
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
            if let Err(e) = check_and_execute_accumulated_positions(
                &executor,
                &pool,
                &position_cqrs,
                &position_query,
                &offchain_order_cqrs,
            )
            .await
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
                "Received blockchain event: tx_hash={:?}, log_index={:?}, block_number={:?}",
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
            "No subscription events within timeout, using current block {current_block} as cutoff"
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
    config: &Config,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
    cqrs: &TradeProcessingCqrs,
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

    let queue_context = QueueProcessingContext {
        cache,
        feed_id_cache: &feed_id_cache,
        vault_registry_cqrs,
    };

    loop {
        let result =
            process_next_queued_event(executor_type, config, pool, &provider, cqrs, &queue_context)
                .await;

        handle_queue_processing_result(result).await;
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

async fn handle_queue_processing_result(
    result: Result<Option<OffchainOrderId>, EventProcessingError>,
) {
    match result {
        Ok(Some(offchain_order_id)) => {
            info!(%offchain_order_id, "Offchain order placed successfully");
        }
        Ok(None) => {
            sleep(Duration::from_millis(100)).await;
        }
        Err(e) => {
            error!("Error processing queued event: {e}");
            sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Context for queue event processing containing caches and CQRS components.
struct QueueProcessingContext<'a> {
    cache: &'a SymbolCache,
    feed_id_cache: &'a FeedIdCache,
    vault_registry_cqrs: &'a SqliteCqrs<VaultRegistryAggregate>,
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn process_next_queued_event<P: Provider + Clone>(
    executor_type: SupportedExecutor,
    config: &Config,
    pool: &SqlitePool,
    provider: &P,
    cqrs: &TradeProcessingCqrs,
    queue_context: &QueueProcessingContext<'_>,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    let queued_event = get_next_unprocessed_event(pool).await?;
    let Some(queued_event) = queued_event else {
        return Ok(None);
    };

    let event_id = extract_event_id(&queued_event)?;

    let onchain_trade = convert_event_to_trade(
        config,
        queue_context.cache,
        provider,
        &queued_event,
        queue_context.feed_id_cache,
    )
    .await?;

    let Some(trade) = onchain_trade else {
        return handle_filtered_event(pool, &queued_event, event_id).await;
    };

    let vault_discovery_context = VaultDiscoveryContext {
        vault_registry_cqrs: queue_context.vault_registry_cqrs,
        orderbook: config.evm.orderbook,
        order_owner: config.order_owner()?,
    };

    process_valid_trade(
        executor_type,
        pool,
        &queued_event,
        event_id,
        trade,
        cqrs,
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
    context: &VaultDiscoveryContext<'_>,
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
    config: &Config,
    cache: &SymbolCache,
    provider: &P,
    queued_event: &QueuedEvent,
    feed_id_cache: &FeedIdCache,
) -> Result<Option<OnchainTrade>, EventProcessingError> {
    let reconstructed_log = reconstruct_log_from_queued_event(&config.evm, queued_event);
    let order_owner = config.order_owner()?;

    let onchain_trade = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => {
            OnchainTrade::try_from_clear_v3(
                &config.evm,
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
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    info!(
        "Event filtered out (no matching owner): event_type={:?}, tx_hash={:?}, log_index={}",
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
    skip(pool, queued_event, trade, cqrs, vault_discovery_context),
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
    vault_discovery_context: &VaultDiscoveryContext<'_>,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    info!(
        "Event successfully converted to trade: event_type={:?}, tx_hash={:?}, log_index={}, symbol={}, amount={}",
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
        "Processing queued trade: symbol={}, amount={}, direction={:?}, tx_hash={:?}, log_index={}",
        trade.symbol, trade.amount, trade.direction, trade.tx_hash, trade.log_index
    );

    process_trade_within_transaction(executor_type, pool, queued_event, event_id, trade, cqrs).await
}

/// Extracts amount and price as Decimals from a trade, logging errors on failure.
fn extract_trade_decimals(trade: &OnchainTrade) -> Option<(Decimal, Decimal)> {
    let amount = match Decimal::try_from(trade.amount) {
        Ok(d) => d,
        Err(e) => {
            error!(
                "Failed to convert trade amount to Decimal: {e}, tx_hash={:?}",
                trade.tx_hash
            );
            return None;
        }
    };

    let price_usdc = match Decimal::try_from(trade.price.value()) {
        Ok(d) => d,
        Err(e) => {
            error!(
                "Failed to convert trade price to Decimal: {e}, tx_hash={:?}",
                trade.tx_hash
            );
            return None;
        }
    };

    Some((amount, price_usdc))
}

async fn execute_witness_trade(
    onchain_trade_cqrs: &OnChainTradeCqrs,
    trade: &OnchainTrade,
    block_number: u64,
) {
    let aggregate_id = OnChainTrade::aggregate_id(trade.tx_hash, trade.log_index);

    let Some((amount, price_usdc)) = extract_trade_decimals(trade) else {
        return;
    };

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

    match onchain_trade_cqrs.execute(&aggregate_id, command).await {
        Ok(()) => info!(
            "Successfully executed OnChainTrade::Witness command: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        ),
        Err(e) => error!(
            "Failed to execute OnChainTrade::Witness command: {e}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn execute_initialize_position(
    position_cqrs: &PositionCqrs,
    trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    let base_symbol = trade.symbol.base();
    let aggregate_id = Position::aggregate_id(base_symbol);

    let command = PositionCommand::Initialize {
        symbol: base_symbol.clone(),
        threshold: execution_threshold,
    };

    match position_cqrs.execute(&aggregate_id, command).await {
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

async fn execute_acknowledge_fill(position_cqrs: &PositionCqrs, trade: &OnchainTrade) {
    let base_symbol = trade.symbol.base();
    let aggregate_id = Position::aggregate_id(base_symbol);

    let Some((amount, price_usdc)) = extract_trade_decimals(trade) else {
        return;
    };

    let Some(block_timestamp) = trade.block_timestamp else {
        error!(
            "Missing block_timestamp for Position::AcknowledgeOnChainFill: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        );
        return;
    };

    let command = PositionCommand::AcknowledgeOnChainFill {
        trade_id: TradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        },
        amount: FractionalShares::new(amount),
        direction: trade.direction,
        price_usdc,
        block_timestamp,
    };

    match position_cqrs.execute(&aggregate_id, command).await {
        Ok(()) => info!(
            "Successfully executed Position::AcknowledgeOnChainFill command: tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
        Err(e) => error!(
            "Failed to execute Position::AcknowledgeOnChainFill command: {e}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

async fn execute_new_execution_cqrs(
    position_cqrs: &PositionCqrs,
    offchain_order_cqrs: &OffchainOrderCqrs,
    offchain_order_id: OffchainOrderId,
    params: &ExecutionParams,
) {
    execute_place_offchain_order_position(position_cqrs, offchain_order_id, params).await;
    execute_place_offchain_order(offchain_order_cqrs, offchain_order_id, params).await;
}

async fn execute_place_offchain_order_position(
    position_cqrs: &PositionCqrs,
    offchain_order_id: OffchainOrderId,
    params: &ExecutionParams,
) {
    let position_agg_id = Position::aggregate_id(&params.symbol);

    let command = PositionCommand::PlaceOffChainOrder {
        offchain_order_id,
        shares: params.shares,
        direction: params.direction,
        executor: params.executor,
    };

    match position_cqrs.execute(&position_agg_id, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %params.symbol,
            "Position::PlaceOffChainOrder succeeded"
        ),
        Err(e) => error!(
            %offchain_order_id,
            symbol = %params.symbol,
            "Position::PlaceOffChainOrder failed: {e}"
        ),
    }
}

async fn execute_place_offchain_order(
    offchain_order_cqrs: &OffchainOrderCqrs,
    offchain_order_id: OffchainOrderId,
    params: &ExecutionParams,
) {
    let offchain_agg_id = offchain_order_id.to_string();

    let command = OffchainOrderCommand::PlaceOrder {
        symbol: params.symbol.clone(),
        shares: params.shares,
        direction: params.direction,
        executor: params.executor,
    };

    match offchain_order_cqrs.execute(&offchain_agg_id, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %params.symbol,
            "OffchainOrder::PlaceOrder succeeded"
        ),
        Err(e) => error!(
            %offchain_order_id,
            symbol = %params.symbol,
            "OffchainOrder::PlaceOrder failed: {e}"
        ),
    }
}

async fn process_trade_within_transaction(
    executor_type: SupportedExecutor,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    // Update Position aggregate FIRST so threshold check sees current state
    execute_initialize_position(&cqrs.position_cqrs, &trade, cqrs.execution_threshold).await;
    execute_acknowledge_fill(&cqrs.position_cqrs, &trade).await;

    let mut sql_tx = pool
        .begin()
        .await
        .inspect_err(|e| error!("Failed to begin transaction for event processing: {e}"))?;

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
        "Successfully committed event processing: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    execute_witness_trade(&cqrs.onchain_trade_cqrs, &trade, queued_event.block_number).await;

    let base_symbol = trade.symbol.base();

    let Some(params) =
        check_execution_readiness(&cqrs.position_query, base_symbol, executor_type).await?
    else {
        return Ok(None);
    };

    let offchain_order_id = OffchainOrder::aggregate_id();
    execute_new_execution_cqrs(
        &cqrs.position_cqrs,
        &cqrs.offchain_order_cqrs,
        offchain_order_id,
        &params,
    )
    .await;

    Ok(Some(offchain_order_id))
}

fn reconstruct_log_from_queued_event(
    config: &EvmConfig,
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
            address: config.orderbook,
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
    position_cqrs: &PositionCqrs,
    position_query: &PositionQuery,
    offchain_order_cqrs: &Arc<OffchainOrderCqrs>,
) -> Result<(), EventProcessingError>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let executor_type = executor.to_supported_executor();
    let ready_positions = check_all_positions(pool, position_query, executor_type).await?;

    if ready_positions.is_empty() {
        debug!("No accumulated positions ready for execution");
        return Ok(());
    }

    info!(
        "Found {} accumulated positions ready for execution",
        ready_positions.len()
    );

    for params in ready_positions {
        let offchain_order_id = OffchainOrder::aggregate_id();

        info!(
            symbol = %params.symbol,
            shares = %params.shares,
            direction = ?params.direction,
            %offchain_order_id,
            "Executing accumulated position"
        );

        execute_new_execution_cqrs(
            position_cqrs,
            offchain_order_cqrs,
            offchain_order_id,
            &params,
        )
        .await;
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
    use std::sync::Arc;

    use alloy::primitives::{B256, IntoLogData, U256, address, bytes, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types;
    use cqrs_es::persist::GenericQuery;
    use futures_util::stream;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use st0x_execution::{MockExecutorConfig, Symbol, TryIntoExecutor};

    use super::*;
    use crate::bindings::IOrderBookV5::{ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4};
    use crate::conductor::builder::CqrsFrameworks;
    use crate::config::tests::create_test_config;
    use crate::onchain::trade::OnchainTrade;
    use crate::test_utils::{OnchainTradeBuilder, get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn trade_processing_cqrs(frameworks: &CqrsFrameworks) -> TradeProcessingCqrs {
        TradeProcessingCqrs {
            onchain_trade_cqrs: frameworks.onchain_trade_cqrs.clone(),
            position_cqrs: frameworks.position_cqrs.clone(),
            position_query: frameworks.position_query.clone(),
            offchain_order_cqrs: frameworks.offchain_order_cqrs.clone(),
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_test_cqrs_frameworks(pool: &SqlitePool) -> CqrsFrameworks {
        let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

        let position_view_repo = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "position_view".to_string(),
        ));
        let position_query = Arc::new(GenericQuery::new(position_view_repo.clone()));
        let position_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(position_view_repo))],
            (),
        ));

        let offchain_order_view_repo = Arc::new(SqliteViewRepository::<
            OffchainOrderAggregate,
            OffchainOrderAggregate,
        >::new(
            pool.clone(), "offchain_order_view".to_string()
        ));
        let offchain_order_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(offchain_order_view_repo))],
            crate::offchain_order::noop_order_placer(),
        ));
        let vault_registry_cqrs = sqlite_cqrs(pool.clone(), vec![], ());
        let snapshot_cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        CqrsFrameworks {
            pool: pool.clone(),
            onchain_trade_cqrs,
            position_cqrs,
            position_query,
            offchain_order_cqrs,
            vault_registry_cqrs,
            snapshot_cqrs,
        }
    }

    fn abort_all_conductor_tasks(conductor: Conductor) {
        conductor.order_poller.abort();
        conductor.event_processor.abort();
        conductor.position_checker.abort();
        conductor.queue_processor.abort();
        conductor.dex_event_receiver.abort();

        if let Some(rebalancer) = conductor.rebalancer {
            rebalancer.abort();
        }

        if let Some(inventory_poller) = conductor.inventory_poller {
            inventory_poller.abort();
        }

        if let Some(executor_maintenance) = conductor.executor_maintenance {
            executor_maintenance.abort();
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
        let config = create_test_config();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let frameworks = create_test_cqrs_frameworks(&pool);

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
        let queue_context = QueueProcessingContext {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &frameworks.vault_registry_cqrs,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        let result = process_next_queued_event(
            SupportedExecutor::DryRun,
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
        let config = create_test_config();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let frameworks = create_test_cqrs_frameworks(&pool);

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
        let queue_context = QueueProcessingContext {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &frameworks.vault_registry_cqrs,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        process_next_queued_event(
            SupportedExecutor::DryRun,
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
        let config = create_test_config();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let frameworks = create_test_cqrs_frameworks(&pool);

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
        let queue_context = QueueProcessingContext {
            cache: &cache,
            feed_id_cache: &feed_id_cache,
            vault_registry_cqrs: &frameworks.vault_registry_cqrs,
        };

        let cqrs = trade_processing_cqrs(&frameworks);

        process_next_queued_event(
            SupportedExecutor::DryRun,
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
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

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

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_individual_abort() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

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

        conductor.order_poller.abort();
        conductor.event_processor.abort();
        conductor.position_checker.abort();
        conductor.queue_processor.abort();
        conductor.dex_event_receiver.abort();
    }

    #[tokio::test]
    async fn test_conductor_builder_returns_immediately() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

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

        assert!(!conductor.order_poller.is_finished());
        assert!(!conductor.event_processor.is_finished());
        assert!(!conductor.position_checker.is_finished());
        assert!(!conductor.queue_processor.is_finished());

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_without_rebalancer() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

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

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_with_rebalancer() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
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

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_aborted_on_abort_all() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
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

        abort_all_conductor_tasks(conductor);
    }

    #[tokio::test]
    async fn test_conductor_rebalancer_aborted_on_abort_trading_tasks() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorConfig.try_into_executor().await.unwrap();
        let frameworks = create_test_cqrs_frameworks(&pool);

        let clear_stream = stream::empty::<Result<(ClearV3, Log), sol_types::Error>>();
        let take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let fake_rebalancer = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
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

        conductor.abort_trading_tasks();

        // abort_trading_tasks aborts the rebalancer
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(conductor.rebalancer.as_ref().unwrap().is_finished());

        // Clean up remaining tasks
        conductor.dex_event_receiver.abort();
        if let Some(executor_maintenance) = conductor.executor_maintenance {
            executor_maintenance.abort();
        }
        if let Some(inventory_poller) = conductor.inventory_poller {
            inventory_poller.abort();
        }
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
        let config = create_test_config();

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

        let reconstructed_log = reconstruct_log_from_queued_event(&config.evm, &queued_event);
        assert_eq!(reconstructed_log.inner.address, config.evm.orderbook);
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

        let result = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_secs(1),
        )
        .await;

        assert!(result.is_some());
        let (events, block_number) = result.unwrap();
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

        let result = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_millis(100),
        )
        .await;

        assert!(result.is_none());
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
        let result =
            process_live_event(&pool, TradeEvent::ClearV3(Box::new(clear_event)), log).await;

        assert!(result.is_ok());

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
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
    ) -> VaultDiscoveryContext<'_> {
        VaultDiscoveryContext {
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
