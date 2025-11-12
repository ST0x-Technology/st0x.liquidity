mod builder;

use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Log;
use alloy::sol_types;
use futures_util::{Stream, StreamExt};
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace};

use st0x_broker::{Broker, MarketOrder, SupportedBroker};

use crate::bindings::IOrderBookV4::{ClearV2, IOrderBookV4Instance, TakeOrderV2};
use crate::env::Config;
use crate::error::EventProcessingError;
use crate::offchain::execution::{OffchainExecution, find_execution_by_id};
use crate::offchain::order_poller::OrderStatusPoller;
use crate::onchain::accumulator::check_all_accumulated_positions;
use crate::onchain::backfill::backfill_events;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::TradeEvent;
use crate::onchain::{EvmEnv, OnchainTrade, accumulator};
use crate::queue::{QueuedEvent, enqueue, get_next_unprocessed_event, mark_event_processed};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;

pub(crate) use builder::ConductorBuilder;

pub(crate) struct Conductor {
    pub(crate) broker_maintenance: Option<JoinHandle<()>>,
    pub(crate) order_poller: JoinHandle<()>,
    pub(crate) dex_event_receiver: JoinHandle<()>,
    pub(crate) event_processor: JoinHandle<()>,
    pub(crate) position_checker: JoinHandle<()>,
    pub(crate) queue_processor: JoinHandle<()>,
}

pub(crate) async fn run_market_hours_loop<B: Broker + Clone + Send + 'static>(
    broker: B,
    config: Config,
    pool: SqlitePool,
    broker_maintenance: Option<JoinHandle<()>>,
) -> anyhow::Result<()> {
    const RERUN_DELAY_SECS: u64 = 10;

    let timeout = broker
        .wait_until_market_open()
        .await
        .map_err(|e| anyhow::anyhow!("Market hours check failed: {e}"))?;

    let timeout_minutes = timeout.as_secs() / 60;
    if timeout_minutes < 60 * 24 {
        info!("Market is open, starting conductor (will timeout in {timeout_minutes} minutes)");
    } else {
        info!("Starting conductor (no market hours restrictions)");
    }

    let mut conductor = match Conductor::start(&config, &pool, broker.clone(), broker_maintenance)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!(
                "Failed to start conductor: {e}, retrying in {} seconds",
                RERUN_DELAY_SECS
            );

            tokio::time::sleep(std::time::Duration::from_secs(RERUN_DELAY_SECS)).await;

            let new_maintenance = broker.run_broker_maintenance().await;

            return Box::pin(run_market_hours_loop(broker, config, pool, new_maintenance)).await;
        }
    };

    info!("Market opened, conductor running");

    tokio::select! {
        result = conductor.wait_for_completion() => {
            info!("Conductor completed");
            conductor.abort_all();
            result?;
            info!("Conductor completed successfully, continuing to next market session");
            Ok(())
        }
        () = tokio::time::sleep(timeout) => {
            info!("Market closed, shutting down trading tasks");
            conductor.abort_trading_tasks();
            let next_maintenance = conductor.broker_maintenance;
            info!("Trading tasks shutdown, DEX events buffering");
            Box::pin(run_market_hours_loop(broker, config, pool, next_maintenance)).await
        }
    }
}

impl Conductor {
    pub(crate) async fn start<B: Broker + Clone + Send + 'static>(
        config: &Config,
        pool: &SqlitePool,
        broker: B,
        broker_maintenance: Option<JoinHandle<()>>,
    ) -> anyhow::Result<Self> {
        let ws = WsConnect::new(config.evm.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let cache = SymbolCache::default();
        let orderbook = IOrderBookV4Instance::new(config.evm.orderbook, &provider);

        let mut clear_stream = orderbook.ClearV2_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV2_filter().watch().await?.into_stream();

        let cutoff_block =
            get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, pool).await?;

        backfill_events(pool, &provider, &config.evm, cutoff_block - 1).await?;

        Ok(
            ConductorBuilder::new(config.clone(), pool.clone(), cache, provider, broker)
                .with_broker_maintenance(broker_maintenance)
                .with_dex_event_streams(clear_stream, take_stream)
                .spawn(),
        )
    }

    pub(crate) async fn wait_for_completion(&mut self) -> Result<(), anyhow::Error> {
        let maintenance_task = async {
            if let Some(handle) = &mut self.broker_maintenance {
                match handle.await {
                    Ok(()) => info!("Broker maintenance completed successfully"),
                    Err(e) if e.is_cancelled() => {
                        info!("Broker maintenance cancelled (expected during shutdown)");
                    }
                    Err(e) => error!("Broker maintenance task panicked: {e}"),
                }
            }
        };

        let (
            (),
            poller_result,
            dex_receiver_result,
            processor_result,
            position_result,
            queue_result,
        ) = tokio::join!(
            maintenance_task,
            &mut self.order_poller,
            &mut self.dex_event_receiver,
            &mut self.event_processor,
            &mut self.position_checker,
            &mut self.queue_processor
        );

        if let Err(e) = poller_result {
            error!("Order poller task panicked: {e}");
        }
        if let Err(e) = dex_receiver_result {
            error!("DEX event receiver task panicked: {e}");
        }
        if let Err(e) = processor_result {
            error!("Event processor task panicked: {e}");
        }
        if let Err(e) = position_result {
            error!("Position checker task panicked: {e}");
        }
        if let Err(e) = queue_result {
            error!("Queue processor task panicked: {e}");
        }

        Ok(())
    }

    pub(crate) fn abort_trading_tasks(&self) {
        info!("Aborting trading tasks (keeping broker maintenance and DEX event receiver alive)");

        self.order_poller.abort();
        self.event_processor.abort();
        self.position_checker.abort();
        self.queue_processor.abort();

        info!("Trading tasks aborted successfully (DEX events will continue buffering)");
    }

    pub(crate) fn abort_all(self) {
        info!("Aborting all background tasks");

        if let Some(handle) = self.broker_maintenance {
            handle.abort();
        }
        self.order_poller.abort();
        self.dex_event_receiver.abort();
        self.event_processor.abort();
        self.position_checker.abort();
        self.queue_processor.abort();

        info!("All background tasks aborted successfully");
    }
}

fn spawn_order_poller<B: Broker + Clone + Send + 'static>(
    config: &Config,
    pool: &SqlitePool,
    broker: B,
) -> JoinHandle<()> {
    let poller_config = config.get_order_poller_config();
    info!(
        "Starting order status poller with interval: {:?}, max jitter: {:?}",
        poller_config.polling_interval, poller_config.max_jitter
    );

    let poller = OrderStatusPoller::new(poller_config, pool.clone(), broker);
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
    clear_stream: impl Stream<Item = Result<(ClearV2, Log), sol_types::Error>> + Unpin + Send + 'static,
    take_stream: impl Stream<Item = Result<(TakeOrderV2, Log), sol_types::Error>>
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

fn spawn_queue_processor<
    P: Provider + Clone + Send + 'static,
    B: Broker + Clone + Send + 'static,
>(
    broker: B,
    config: &Config,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
) -> JoinHandle<()> {
    info!("Starting queue processor service");
    let config_clone = config.clone();
    let pool_clone = pool.clone();
    let cache_clone = cache.clone();

    tokio::spawn(async move {
        run_queue_processor(&broker, &config_clone, &pool_clone, &cache_clone, provider).await;
    })
}

fn spawn_periodic_accumulated_position_check<B: Broker + Clone + Send + 'static>(
    broker: B,
    pool: SqlitePool,
) -> JoinHandle<()> {
    info!("Starting periodic accumulated position checker");

    tokio::spawn(async move {
        const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

        let mut interval = tokio::time::interval(CHECK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running periodic accumulated position check");
            if let Err(e) = check_and_execute_accumulated_positions(&broker, &pool).await {
                error!("Periodic accumulated position check failed: {e}");
            }
        }
    })
}

async fn receive_blockchain_events<S1, S2>(
    mut clear_stream: S1,
    mut take_stream: S2,
    event_sender: UnboundedSender<(TradeEvent, Log)>,
) where
    S1: Stream<Item = Result<(ClearV2, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV2, Log), sol_types::Error>> + Unpin,
{
    loop {
        let event_result = tokio::select! {
            Some(result) = clear_stream.next() => {
                result.map(|(event, log)| (TradeEvent::ClearV2(Box::new(event)), log))
            }
            Some(result) = take_stream.next() => {
                result.map(|(event, log)| (TradeEvent::TakeOrderV2(Box::new(event)), log))
            }
            else => {
                error!("All event streams ended, shutting down event receiver");
                break;
            }
        };

        match event_result {
            Ok((event, log)) => {
                trace!(
                    "Received blockchain event: tx_hash={:?}, log_index={:?}, block_number={:?}",
                    log.transaction_hash, log.log_index, log.block_number
                );
                if event_sender.send((event, log)).is_err() {
                    error!("Event receiver dropped, shutting down");
                    break;
                }
            }
            Err(e) => {
                error!("Error in event stream: {e}");
            }
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
    S1: Stream<Item = Result<(ClearV2, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV2, Log), sol_types::Error>> + Unpin,
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
        TradeEvent::ClearV2(clear_event) => {
            info!(
                "Enqueuing ClearV2 event: tx_hash={:?}, log_index={:?}",
                log.transaction_hash, log.log_index
            );

            enqueue(pool, clear_event.as_ref(), &log)
                .await
                .map_err(EventProcessingError::EnqueueClearV2)?;
        }
        TradeEvent::TakeOrderV2(take_event) => {
            info!(
                "Enqueuing TakeOrderV2 event: tx_hash={:?}, log_index={:?}",
                log.transaction_hash, log.log_index
            );

            enqueue(pool, take_event.as_ref(), &log)
                .await
                .map_err(EventProcessingError::EnqueueTakeOrderV2)?;
        }
    }

    Ok(())
}

async fn run_queue_processor<P: Provider + Clone, B: Broker + Clone>(
    broker: &B,
    config: &Config,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: P,
) {
    info!("Starting queue processor service");

    let feed_id_cache = FeedIdCache::default();

    match crate::queue::count_unprocessed(pool).await {
        Ok(count) if count > 0 => {
            info!("Found {count} unprocessed events from previous sessions to process");
        }
        Ok(_) => {
            info!("No unprocessed events found, starting fresh");
        }
        Err(e) => {
            error!("Failed to count unprocessed events: {e}");
        }
    }

    let broker_type = broker.to_supported_broker();

    loop {
        match process_next_queued_event(broker_type, config, pool, cache, &provider, &feed_id_cache)
            .await
        {
            Ok(Some(execution)) => {
                if let Some(exec_id) = execution.id {
                    if let Err(e) = execute_pending_offchain_execution(broker, pool, exec_id).await
                    {
                        error!("Failed to execute offchain order {exec_id}: {e}");
                    }
                }
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
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn process_next_queued_event<P: Provider + Clone>(
    broker_type: SupportedBroker,
    config: &Config,
    pool: &SqlitePool,
    cache: &SymbolCache,
    provider: &P,
    feed_id_cache: &FeedIdCache,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    let queued_event = get_next_unprocessed_event(pool).await?;
    let Some(queued_event) = queued_event else {
        return Ok(None);
    };

    let event_id = extract_event_id(&queued_event)?;

    let onchain_trade =
        convert_event_to_trade(config, cache, provider, &queued_event, feed_id_cache).await?;

    let Some(trade) = onchain_trade else {
        return handle_filtered_event(pool, &queued_event, event_id).await;
    };

    process_valid_trade(broker_type, pool, &queued_event, event_id, trade).await
}

fn extract_event_id(queued_event: &QueuedEvent) -> Result<i64, EventProcessingError> {
    queued_event.id.ok_or_else(|| {
        EventProcessingError::Queue(crate::error::EventQueueError::Processing(
            "Queued event missing ID".to_string(),
        ))
    })
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

    let onchain_trade = match &queued_event.event {
        TradeEvent::ClearV2(clear_event) => {
            OnchainTrade::try_from_clear_v2(
                &config.evm,
                cache,
                provider,
                *clear_event.clone(),
                reconstructed_log,
                feed_id_cache,
            )
            .await?
        }
        TradeEvent::TakeOrderV2(take_event) => {
            OnchainTrade::try_from_take_order_if_target_owner(
                cache,
                provider,
                *take_event.clone(),
                reconstructed_log,
                config.evm.order_owner,
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
        "Event filtered out (no matching owner): event_type={:?}, tx_hash={:?}, log_index={}",
        match &queued_event.event {
            TradeEvent::ClearV2(_) => "ClearV2",
            TradeEvent::TakeOrderV2(_) => "TakeOrderV2",
        },
        queued_event.tx_hash,
        queued_event.log_index
    );

    let mut sql_tx = pool.begin().await.map_err(|e| {
        error!("Failed to begin transaction for filtered event: {e}");
        EventProcessingError::Queue(crate::error::EventQueueError::Processing(format!(
            "Failed to begin transaction: {e}"
        )))
    })?;

    mark_event_processed(&mut sql_tx, event_id).await?;

    sql_tx.commit().await.map_err(|e| {
        error!("Failed to commit transaction for filtered event: {e}");
        EventProcessingError::Queue(crate::error::EventQueueError::Processing(format!(
            "Failed to commit transaction: {e}"
        )))
    })?;

    Ok(None)
}

#[tracing::instrument(skip(pool, queued_event, trade), fields(event_id, symbol = %trade.symbol), level = tracing::Level::INFO)]
async fn process_valid_trade(
    broker_type: SupportedBroker,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    info!(
        "Event successfully converted to trade: event_type={:?}, tx_hash={:?}, log_index={}, symbol={}, amount={}",
        match &queued_event.event {
            TradeEvent::ClearV2(_) => "ClearV2",
            TradeEvent::TakeOrderV2(_) => "TakeOrderV2",
        },
        trade.tx_hash,
        trade.log_index,
        trade.symbol,
        trade.amount
    );

    let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
    let _guard = symbol_lock.lock().await;

    info!(
        "Processing queued trade: symbol={}, amount={}, direction={:?}, tx_hash={:?}, log_index={}",
        trade.symbol, trade.amount, trade.direction, trade.tx_hash, trade.log_index
    );

    process_trade_within_transaction(broker_type, pool, queued_event, event_id, trade).await
}

async fn process_trade_within_transaction(
    broker_type: SupportedBroker,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
) -> Result<Option<OffchainExecution>, EventProcessingError> {
    let mut sql_tx = pool.begin().await.map_err(|e| {
        error!("Failed to begin transaction for event processing: {e}");
        EventProcessingError::AccumulatorProcessing(format!("Failed to begin transaction: {e}"))
    })?;

    info!(
        "Started transaction for atomic event processing: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    let execution = accumulator::process_onchain_trade(&mut sql_tx, trade, broker_type)
        .await
        .map_err(|e| {
            error!(
                "Failed to process trade through accumulator: {e}, tx_hash={:?}, log_index={}",
                queued_event.tx_hash, queued_event.log_index
            );
            EventProcessingError::AccumulatorProcessing(format!(
                "Failed to process trade through accumulator: {e}"
            ))
        })?;

    mark_event_processed(&mut sql_tx, event_id)
        .await
        .map_err(|e| {
            error!("Failed to mark event {event_id} as processed: {e}");
            EventProcessingError::Queue(e)
        })?;

    sql_tx.commit().await.map_err(|e| {
        error!(
            "Failed to commit transaction for event processing: {e}, event_id={}, tx_hash={:?}",
            event_id, queued_event.tx_hash
        );
        EventProcessingError::AccumulatorProcessing(format!("Failed to commit transaction: {e}"))
    })?;

    info!(
        "Successfully committed atomic event processing: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    Ok(execution)
}

fn reconstruct_log_from_queued_event(
    evm_env: &EvmEnv,
    queued_event: &crate::queue::QueuedEvent,
) -> Log {
    use alloy::primitives::IntoLogData;

    let log_data = match &queued_event.event {
        TradeEvent::ClearV2(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TradeEvent::TakeOrderV2(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = queued_event
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: evm_env.orderbook,
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
async fn check_and_execute_accumulated_positions<B: Broker + Clone + Send + 'static>(
    broker: &B,
    pool: &SqlitePool,
) -> Result<(), EventProcessingError> {
    let broker_type = broker.to_supported_broker();
    let executions = check_all_accumulated_positions(pool, broker_type).await?;

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

        let pool_clone = pool.clone();
        let broker_clone = broker.clone();
        tokio::spawn(async move {
            if let Err(e) =
                execute_pending_offchain_execution(&broker_clone, &pool_clone, execution_id).await
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

#[tracing::instrument(skip(broker, pool), level = tracing::Level::INFO)]
async fn execute_pending_offchain_execution<B: Broker + Clone + Send + 'static>(
    broker: &B,
    pool: &SqlitePool,
    execution_id: i64,
) -> Result<(), EventProcessingError> {
    let execution = find_execution_by_id(pool, execution_id)
        .await?
        .ok_or_else(|| {
            EventProcessingError::AccumulatorProcessing(format!(
                "Execution with ID {execution_id} not found"
            ))
        })?;

    info!("Executing offchain order: {execution:?}");

    let market_order = MarketOrder {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
    };

    let placement = broker.place_market_order(market_order).await.map_err(|e| {
        EventProcessingError::AccumulatorProcessing(format!("Order placement failed: {e}"))
    })?;

    info!("Order placed with ID: {}", placement.order_id);

    Ok(())
}

async fn wait_for_first_event_with_timeout<S1, S2>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    timeout: std::time::Duration,
) -> Option<(Vec<(TradeEvent, Log)>, u64)>
where
    S1: Stream<Item = Result<(ClearV2, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV2, Log), sol_types::Error>> + Unpin,
{
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    let mut events = Vec::new();

    loop {
        tokio::select! {
            Some(result) = clear_stream.next() => {
                match result {
                    Ok((event, log)) => {
                        if let Some(block_number) = log.block_number {
                            events.push((TradeEvent::ClearV2(Box::new(event)), log));
                            return Some((events, block_number));
                        }
                        error!("ClearV2 event missing block number");
                    }
                    Err(e) => {
                        error!("Error in clear event stream during startup: {e}");
                    }
                }
            }
            Some(result) = take_stream.next() => {
                match result {
                    Ok((event, log)) => {
                        if let Some(block_number) = log.block_number {
                            events.push((TradeEvent::TakeOrderV2(Box::new(event)), log));
                            return Some((events, block_number));
                        }
                        error!("TakeOrderV2 event missing block number");
                    }
                    Err(e) => {
                        error!("Error in take event stream during startup: {e}");
                    }
                }
            }
            () = &mut deadline => {
                return None;
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
    S1: Stream<Item = Result<(ClearV2, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV2, Log), sol_types::Error>> + Unpin,
{
    loop {
        tokio::select! {
            Some(result) = clear_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::ClearV2(Box::new(event)), log));
                }
                Err(e) => error!("Error in clear event stream during backfill: {e}"),
                _ => {}
            },
            Some(result) = take_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::TakeOrderV2(Box::new(event)), log));
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
    use super::*;
    use crate::bindings::IOrderBookV4::{ClearConfig, ClearV2};
    use crate::env::tests::create_test_config;
    use crate::onchain::trade::OnchainTrade;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::tokenized_symbol;
    use alloy::primitives::{IntoLogData, address, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types;
    use futures_util::stream;
    use st0x_broker::{Direction, MockBrokerConfig, TryIntoBroker};

    #[tokio::test]
    async fn test_event_enqueued_when_trade_conversion_returns_none() {
        let pool = setup_test_db().await;
        let _config = create_test_config();

        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
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
        let duplicate_result = duplicate_trade.save_within_transaction(&mut sql_tx2).await;
        assert!(duplicate_result.is_err());
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
            amount: 5.0,
            direction: Direction::Sell,
            price_usdc: 20000.0,
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
        let duplicate_result = duplicate_trade.save_within_transaction(&mut sql_tx2).await;
        assert!(duplicate_result.is_err());
        sql_tx2.rollback().await.unwrap();

        let count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_complete_event_processing_flow() {
        let pool = setup_test_db().await;
        let config = create_test_config();

        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
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

        if let TradeEvent::ClearV2(boxed_clear_event) = queued_event.event {
            let cache = SymbolCache::default();
            let http_provider =
                ProviderBuilder::new().connect_http("http://localhost:8545".parse().unwrap());

            let feed_id_cache = FeedIdCache::default();
            if let Ok(Some(trade)) = OnchainTrade::try_from_clear_v2(
                &config.evm,
                &cache,
                &http_provider,
                *boxed_clear_event,
                log,
                &feed_id_cache,
            )
            .await
            {
                let mut sql_tx = pool.begin().await.unwrap();
                accumulator::process_onchain_trade(&mut sql_tx, trade, SupportedBroker::DryRun)
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
        let _config = create_test_config();

        let event1 = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
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
            let event = ClearV2 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: crate::test_utils::get_test_order(),
                bob: crate::test_utils::get_test_order(),
                clearConfig: ClearConfig {
                    aliceInputIOIndex: alloy::primitives::U256::from(0),
                    aliceOutputIOIndex: alloy::primitives::U256::from(1),
                    bobInputIOIndex: alloy::primitives::U256::from(1),
                    bobOutputIOIndex: alloy::primitives::U256::from(0),
                    aliceBountyVaultId: alloy::primitives::U256::ZERO,
                    bobBountyVaultId: alloy::primitives::U256::ZERO,
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
            let event = ClearV2 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: crate::test_utils::get_test_order(),
                bob: crate::test_utils::get_test_order(),
                clearConfig: ClearConfig {
                    aliceInputIOIndex: alloy::primitives::U256::from(0),
                    aliceOutputIOIndex: alloy::primitives::U256::from(1),
                    bobInputIOIndex: alloy::primitives::U256::from(1),
                    bobOutputIOIndex: alloy::primitives::U256::from(0),
                    aliceBountyVaultId: alloy::primitives::U256::ZERO,
                    bobBountyVaultId: alloy::primitives::U256::ZERO,
                },
            };
            let mut log = crate::test_utils::get_test_log();
            log.log_index = Some(i);
            let mut hash_bytes = [0u8; 32];
            hash_bytes[31] = u8::try_from(i).unwrap_or(0);
            log.transaction_hash = Some(alloy::primitives::B256::from(hash_bytes));

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

        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
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

        assert!(matches!(queued_event.event, TradeEvent::ClearV2(_)));

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
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

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
        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
            },
        };
        let mut log = crate::test_utils::get_test_log();
        log.block_number = Some(1000);

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log.clone()))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV2, Log), sol_types::Error>>();

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
        assert!(matches!(events[0].0, TradeEvent::ClearV2(_)));
    }

    #[tokio::test]
    async fn test_wait_for_first_event_missing_block_number() {
        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
            },
        };
        let mut log = crate::test_utils::get_test_log();
        log.block_number = None;

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV2, Log), sol_types::Error>>();

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
        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
            },
        };

        let mut early_log = crate::test_utils::get_test_log();
        early_log.block_number = Some(99);

        let mut late_log = crate::test_utils::get_test_log();
        late_log.block_number = Some(101);

        let events = vec![
            Ok((clear_event.clone(), early_log)),
            Ok((clear_event, late_log)),
        ];

        let mut clear_stream = stream::iter(events);
        let mut take_stream = stream::empty::<Result<(TakeOrderV2, Log), sol_types::Error>>();
        let mut event_buffer = Vec::new();

        buffer_live_events(&mut clear_stream, &mut take_stream, &mut event_buffer, 100).await;

        assert_eq!(event_buffer.len(), 1);
        assert_eq!(event_buffer[0].1.block_number.unwrap(), 101);
    }

    #[tokio::test]
    async fn test_process_live_event_clear_v2() {
        let pool = setup_test_db().await;

        let clear_event = ClearV2 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: crate::test_utils::get_test_order(),
            bob: crate::test_utils::get_test_order(),
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
            },
        };
        let log = crate::test_utils::get_test_log();

        let result =
            process_live_event(&pool, TradeEvent::ClearV2(Box::new(clear_event)), log).await;

        assert!(result.is_ok());

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_clear_v2_event_filtering_without_errors() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut alice_order = crate::test_utils::get_test_order();
        let mut bob_order = crate::test_utils::get_test_order();

        alice_order.owner = address!("0x1111111111111111111111111111111111111111");
        bob_order.owner = address!("0x2222222222222222222222222222222222222222");

        let clear_event = ClearV2 {
            sender: address!("0x3333333333333333333333333333333333333333"),
            alice: alice_order,
            bob: bob_order,
            clearConfig: ClearConfig {
                aliceInputIOIndex: alloy::primitives::U256::from(0),
                aliceOutputIOIndex: alloy::primitives::U256::from(1),
                bobInputIOIndex: alloy::primitives::U256::from(1),
                bobOutputIOIndex: alloy::primitives::U256::from(0),
                aliceBountyVaultId: alloy::primitives::U256::ZERO,
                bobBountyVaultId: alloy::primitives::U256::ZERO,
            },
        };
        let log = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let result = process_next_queued_event(
            SupportedBroker::DryRun,
            &config,
            &pool,
            &cache,
            &provider,
            &feed_id_cache,
        )
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let remaining_count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(remaining_count, 0);
    }

    #[tokio::test]
    async fn test_execute_pending_offchain_execution_not_found() {
        let pool = setup_test_db().await;
        let broker = MockBrokerConfig.try_into_broker().await.unwrap();

        let result = execute_pending_offchain_execution(&broker, &pool, 99999).await;
        assert!(matches!(
            result.unwrap_err(),
            EventProcessingError::AccumulatorProcessing(_)
        ));
    }

    #[tokio::test]
    async fn test_conductor_abort_all() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let broker = MockBrokerConfig.try_into_broker().await.unwrap();

        let conductor = ConductorBuilder::new(config, pool, cache, provider, broker)
            .with_broker_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        assert!(!conductor.order_poller.is_finished());
        assert!(!conductor.event_processor.is_finished());
        assert!(!conductor.position_checker.is_finished());
        assert!(!conductor.queue_processor.is_finished());

        conductor.abort_all();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_conductor_individual_abort() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let broker = MockBrokerConfig.try_into_broker().await.unwrap();

        let conductor = ConductorBuilder::new(config, pool, cache, provider, broker)
            .with_broker_maintenance(None)
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
        let config = create_test_config();
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let clear_stream = stream::empty();
        let take_stream = stream::empty();

        let start_time = std::time::Instant::now();

        let broker = MockBrokerConfig.try_into_broker().await.unwrap();

        let conductor = ConductorBuilder::new(config, pool, cache, provider, broker)
            .with_broker_maintenance(None)
            .with_dex_event_streams(clear_stream, take_stream)
            .spawn();

        let elapsed = start_time.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "ConductorBuilder should return quickly, took: {elapsed:?}"
        );

        conductor.abort_all();
    }
}
