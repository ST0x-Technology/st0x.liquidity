//! Queue processing for trade events.
//!
//! This module handles the continuous processing of queued blockchain events,
//! converting them to trades and executing the necessary CQRS commands.

use alloy::providers::Provider;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use st0x_event_sorcery::Store;
use st0x_execution::Executor;

use super::{
    EventProcessingError, TradeProcessingCqrs, VaultDiscoveryCtx, convert_event_to_trade,
    discover_vaults_for_trade, execute_acknowledge_fill, execute_witness_trade,
    place_offchain_order,
};
use crate::config::Ctx;
use crate::offchain_order::OffchainOrderId;
use crate::onchain::OnchainTrade;
use crate::onchain::accumulator::check_execution_readiness;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::TradeEvent;
use crate::queue::{EventQueueError, QueuedEvent, get_next_unprocessed_event, mark_event_processed};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::vault_registry::VaultRegistry;

/// Context for queue event processing containing caches and CQRS components.
pub(super) struct QueueProcessingContext<'a> {
    pub(super) cache: &'a SymbolCache,
    pub(super) feed_id_cache: &'a FeedIdCache,
    pub(super) vault_registry: &'a Store<VaultRegistry>,
}

/// Main entry point for queue processing. Runs an infinite loop processing
/// queued events and executing the appropriate CQRS commands.
pub(super) async fn run_queue_processor<P, E>(
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
    log_unprocessed_event_count(pool).await;

    let queue_context = QueueProcessingContext {
        cache,
        feed_id_cache: &feed_id_cache,
        vault_registry,
    };

    run_processing_loop(executor, ctx, pool, &provider, cqrs, &queue_context).await;
}

async fn log_unprocessed_event_count(pool: &SqlitePool) {
    match crate::queue::count_unprocessed(pool).await {
        Ok(count) if count > 0 => {
            info!("Found {count} unprocessed events from previous sessions to process");
        }
        Ok(_) => info!("No unprocessed events found, starting fresh"),
        Err(error) => error!("Failed to count unprocessed events: {error}"),
    }
}

async fn run_processing_loop<P, E>(
    executor: &E,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    cqrs: &TradeProcessingCqrs,
    queue_context: &QueueProcessingContext<'_>,
) where
    P: Provider + Clone,
    E: Executor,
    EventProcessingError: From<E::Error>,
{
    loop {
        let result =
            process_next_queued_event(executor, ctx, pool, provider, cqrs, queue_context).await;

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
}

/// Processes the next unprocessed event from the queue.
///
/// Returns `Ok(Some(id))` if an offchain order was placed, `Ok(None)` if no
/// event was available or the event was filtered out, and `Err` on failures.
#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(super) async fn process_next_queued_event<P, E>(
    executor: &E,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    cqrs: &TradeProcessingCqrs,
    queue_context: &QueueProcessingContext<'_>,
) -> Result<Option<OffchainOrderId>, EventProcessingError>
where
    P: Provider + Clone,
    E: Executor,
    EventProcessingError: From<E::Error>,
{
    let queued_event = get_next_unprocessed_event(pool).await?;
    let Some(queued_event) = queued_event else {
        return Ok(None);
    };

    let event_id = queued_event
        .id
        .ok_or(EventProcessingError::Queue(EventQueueError::MissingQueuedEventId))?;

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
            "Event filtered out (no matching owner): event_type={}, tx_hash={:?}, log_index={}",
            event_type_name(&queued_event.event),
            queued_event.tx_hash,
            queued_event.log_index
        );
        mark_event_processed(pool, event_id).await?;
        return Ok(None);
    };

    info!(
        "Event converted to trade: event_type={}, tx_hash={:?}, log_index={}, symbol={}, amount={}",
        event_type_name(&queued_event.event),
        trade.tx_hash,
        trade.log_index,
        trade.symbol,
        trade.amount
    );

    let vault_discovery_context = VaultDiscoveryCtx {
        vault_registry: queue_context.vault_registry,
        orderbook: ctx.evm.orderbook,
        order_owner: ctx.order_owner()?,
    };
    discover_vaults_for_trade(&queued_event, &trade, &vault_discovery_context).await?;

    let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
    let _guard = symbol_lock.lock().await;

    process_queued_trade(executor, pool, &queued_event, event_id, trade, cqrs).await
}

fn event_type_name(event: &TradeEvent) -> &'static str {
    match event {
        TradeEvent::ClearV3(_) => "ClearV3",
        TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
    }
}

/// Processes a queued trade after it has been converted from a blockchain event.
///
/// This is the core trade processing function that:
/// 1. Acknowledges the fill in the position aggregate
/// 2. Marks the event as processed
/// 3. Witnesses the trade in the onchain trade aggregate
/// 4. Checks if execution threshold is met
/// 5. Places an offchain order if threshold is met
pub(super) async fn process_queued_trade<E: Executor>(
    executor: &E,
    pool: &SqlitePool,
    queued_event: &QueuedEvent,
    event_id: i64,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    // Update Position aggregate FIRST so threshold check sees current state
    execute_acknowledge_fill(&cqrs.position, &trade, cqrs.execution_threshold).await;

    mark_event_processed(pool, event_id).await?;

    info!(
        "Successfully marked event as processed: event_id={}, tx_hash={:?}, log_index={}",
        event_id, queued_event.tx_hash, queued_event.log_index
    );

    execute_witness_trade(&cqrs.onchain_trade, &trade, queued_event.block_number).await;

    let base_symbol = trade.symbol.base();
    let executor_type = executor.to_supported_executor();

    let Some(execution) = check_execution_readiness(
        executor,
        &cqrs.position_projection,
        base_symbol,
        executor_type,
    )
    .await?
    else {
        return Ok(None);
    };

    place_offchain_order(&execution, cqrs).await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, TxHash, U256, address};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_event_sorcery::{Projection, StoreBuilder, test_store};
    use st0x_execution::{ExecutorOrderId, MarketOrder, MockExecutor, Symbol};

    use super::*;
    use crate::bindings::IOrderBookV5::{ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4};
    use crate::offchain_order::noop_order_placer;
    use crate::offchain_order::{OffchainOrder, OrderPlacer};
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    struct TestCqrsFrameworks {
        onchain_trade: Arc<Store<OnChainTrade>>,
        position: Arc<Store<Position>>,
        position_projection: Arc<Projection<Position>>,
        offchain_order: Arc<Store<OffchainOrder>>,
        offchain_order_projection: Arc<Projection<OffchainOrder>>,
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

    async fn create_test_cqrs_frameworks(
        pool: &SqlitePool,
        order_placer: Arc<dyn OrderPlacer>,
    ) -> TestCqrsFrameworks {
        let onchain_trade = Arc::new(test_store::<OnChainTrade>(pool.clone(), ()));

        let position_projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position = Arc::new(
            StoreBuilder::<Position>::new(pool.clone())
                .with(position_projection.clone())
                .build(())
                .await
                .unwrap(),
        );
        let position_projection = Arc::new(position_projection);

        let offchain_order_projection = Projection::<OffchainOrder>::sqlite(pool.clone()).unwrap();
        let offchain_order = Arc::new(
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .with(offchain_order_projection.clone())
                .build(order_placer)
                .await
                .unwrap(),
        );
        let offchain_order_projection = Arc::new(offchain_order_projection);

        TestCqrsFrameworks {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            offchain_order_projection,
        }
    }

    fn trade_processing_cqrs(
        frameworks: &TestCqrsFrameworks,
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

    fn test_trade(amount: f64, log_index: u64) -> OnchainTrade {
        OnchainTradeBuilder::default()
            .with_symbol("tAAPL")
            .with_amount(Decimal::try_from(amount).unwrap())
            .with_price(Decimal::try_from(150.0).unwrap())
            .with_log_index(log_index)
            .build()
    }

    async fn enqueue_test_event(pool: &SqlitePool, log_index: u64) -> (QueuedEvent, i64) {
        let event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: OrderV4 {
                owner: address!("0x2222222222222222222222222222222222222222"),
                evaluable: EvaluableV4 {
                    interpreter: address!("0x0000000000000000000000000000000000000000"),
                    store: address!("0x0000000000000000000000000000000000000000"),
                    bytecode: Bytes::default(),
                },
                nonce: B256::ZERO,
                validInputs: vec![IOV2 {
                    token: address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                    vaultId: B256::ZERO,
                }],
                validOutputs: vec![IOV2 {
                    token: address!("0x4444444444444444444444444444444444444444"),
                    vaultId: B256::ZERO,
                }],
            },
            bob: OrderV4 {
                owner: address!("0x3333333333333333333333333333333333333333"),
                evaluable: EvaluableV4 {
                    interpreter: address!("0x0000000000000000000000000000000000000000"),
                    store: address!("0x0000000000000000000000000000000000000000"),
                    bytecode: Bytes::default(),
                },
                nonce: B256::ZERO,
                validInputs: vec![],
                validOutputs: vec![],
            },
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(0),
                bobInputIOIndex: U256::from(0),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = crate::test_utils::get_test_log();
        log.log_index = Some(log_index);
        let mut hash_bytes = [0u8; 32];
        hash_bytes[..8].copy_from_slice(&log_index.to_le_bytes());
        log.transaction_hash = Some(TxHash::from(hash_bytes));

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
        let frameworks = create_test_cqrs_frameworks(&pool, noop_order_placer()).await;
        let cqrs = trade_processing_cqrs(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_test_event(&pool, 10).await;
        let trade = test_trade(0.5, 10);

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
            result.unwrap().is_none(),
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
        let frameworks = create_test_cqrs_frameworks(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_test_event(&pool, 20).await;
        let trade = test_trade(1.5, 20);

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

        let offchain_order = frameworks
            .offchain_order_projection
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
        let frameworks = create_test_cqrs_frameworks(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs(&frameworks, ExecutionThreshold::whole_share());

        // First trade: 0.5 shares - below threshold
        let (queued_event_1, event_id_1) = enqueue_test_event(&pool, 30).await;
        let trade_1 = test_trade(0.5, 30);

        let result_1 = process_queued_trade(
            &MockExecutor::new(),
            &pool,
            &queued_event_1,
            event_id_1,
            trade_1,
            &cqrs,
        )
        .await;

        assert!(
            result_1.unwrap().is_none(),
            "First trade should not trigger execution"
        );

        // Second trade: 0.7 shares - pushes total to 1.2, above threshold
        let (queued_event_2, event_id_2) = enqueue_test_event(&pool, 31).await;
        let trade_2 = test_trade(0.7, 31);

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
            "Second trade should trigger execution (total 1.2 shares)"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.net.inner(),
            dec!(1.2),
            "Position should reflect both trades"
        );
    }

    #[tokio::test]
    async fn event_marked_processed_even_below_threshold() {
        let pool = setup_test_db().await;
        let frameworks = create_test_cqrs_frameworks(&pool, noop_order_placer()).await;
        let cqrs = trade_processing_cqrs(&frameworks, ExecutionThreshold::whole_share());

        let (queued_event, event_id) = enqueue_test_event(&pool, 40).await;
        let trade = test_trade(0.1, 40);

        let unprocessed_before = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(unprocessed_before, 1);

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

        let unprocessed_after = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(
            unprocessed_after, 0,
            "Event should be marked processed regardless of threshold"
        );
    }
}
