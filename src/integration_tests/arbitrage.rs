use std::sync::Arc;

use alloy::primitives::{Address, B256, fixed_bytes};
use chrono::Utc;
use cqrs_es::persist::GenericQuery;
use rust_decimal_macros::dec;
use serde_json::Value;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, FractionalShares, MockExecutor, OrderState, SupportedExecutor, Symbol,
};

use crate::bindings::IOrderBookV5::{TakeOrderConfigV4, TakeOrderV3};
use crate::conductor::{
    ExecutorOrderPlacer, TradeProcessingCqrs, check_and_execute_accumulated_positions,
    process_queued_trade,
};
use crate::offchain::order_poller::{OrderPollerConfig, OrderStatusPoller};
use crate::offchain_order::{OffchainOrderAggregate, OffchainOrderCqrs};
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedEquitySymbol, Usdc};
use crate::onchain::trade::TradeEvent;
use crate::position::{PositionAggregate, PositionCqrs, PositionQuery, load_position};
use crate::queue::QueuedEvent;
use crate::test_utils::setup_test_db;
use crate::threshold::ExecutionThreshold;

#[derive(Debug, sqlx::FromRow)]
struct Event {
    aggregate_type: String,
    aggregate_id: String,
    sequence: i64,
    event_type: String,
    event_version: String,
    payload: Value,
    metadata: Value,
}

async fn fetch_events(pool: &SqlitePool) -> Vec<Event> {
    sqlx::query_as::<_, Event>(
        "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
         FROM events \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

fn make_queued_event(tx_hash: B256, log_index: u64) -> QueuedEvent {
    QueuedEvent {
        id: Some(1),
        tx_hash,
        log_index,
        block_number: 12345,
        event: TradeEvent::TakeOrderV3(Box::new(TakeOrderV3 {
            sender: Address::ZERO,
            config: TakeOrderConfigV4::default(),
            input: B256::ZERO,
            output: B256::ZERO,
        })),
        processed: false,
        created_at: None,
        processed_at: None,
        block_timestamp: Some(Utc::now()),
    }
}

fn make_trade(tx_hash: B256, log_index: u64, amount: f64) -> OnchainTrade {
    OnchainTrade {
        id: None,
        tx_hash,
        log_index,
        symbol: "tAAPL".parse::<TokenizedEquitySymbol>().unwrap(),
        equity_token: Address::ZERO,
        amount,
        direction: Direction::Sell,
        price: Usdc::new(150.25).unwrap(),
        block_timestamp: Some(Utc::now()),
        created_at: None,
        gas_used: Some(21000),
        effective_gas_price: Some(2_000_000_000),
        pyth_price: None,
        pyth_confidence: None,
        pyth_exponent: None,
        pyth_publish_time: None,
    }
}

/// Constructs the CQRS frameworks needed by the integration tests.
///
/// Uses `ExecutorOrderPlacer(MockExecutor::new())` so that the `PlaceOrder`
/// command atomically calls the mock executor and emits `Placed` + `Submitted`.
fn create_test_cqrs(
    pool: &SqlitePool,
) -> (
    TradeProcessingCqrs,
    Arc<PositionCqrs>,
    Arc<PositionQuery>,
    Arc<OffchainOrderCqrs>,
) {
    let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

    let position_view_repo = Arc::new(
        SqliteViewRepository::<PositionAggregate, PositionAggregate>::new(
            pool.clone(),
            "position_view".to_string(),
        ),
    );
    let position_query = Arc::new(GenericQuery::new(position_view_repo.clone()));
    let position_cqrs: Arc<PositionCqrs> = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(GenericQuery::new(position_view_repo))],
        (),
    ));

    let order_placer: crate::offchain_order::OffchainOrderServices =
        Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

    let offchain_order_view_repo = Arc::new(SqliteViewRepository::<
        OffchainOrderAggregate,
        OffchainOrderAggregate,
    >::new(
        pool.clone(), "offchain_order_view".to_string()
    ));
    let offchain_order_cqrs: Arc<OffchainOrderCqrs> = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(GenericQuery::new(offchain_order_view_repo))],
        order_placer,
    ));

    let cqrs = TradeProcessingCqrs {
        onchain_trade_cqrs,
        position_cqrs: position_cqrs.clone(),
        position_query: position_query.clone(),
        offchain_order_cqrs: offchain_order_cqrs.clone(),
        execution_threshold: ExecutionThreshold::whole_share(),
    };

    (cqrs, position_cqrs, position_query, offchain_order_cqrs)
}

#[tokio::test]
async fn happy_path_flow() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);

    let symbol = Symbol::new("AAPL").unwrap();

    // Checkpoint 1: before any trades -- no Position aggregate exists
    let before = load_position(&position_query, &symbol).await?;
    assert!(
        before.is_none(),
        "No position should exist before any trades"
    );

    // Trade 1: 0.5 shares Buy, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let result1 = process_queued_trade(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx1, 1),
        1,
        make_trade(tx1, 1, 0.5),
        &cqrs,
    )
    .await?;

    // Checkpoint 2: after first trade -- below threshold, no execution
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    let position1 = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist after first trade");
    assert_eq!(position1.accumulated_long, FractionalShares::ZERO);
    assert_eq!(
        position1.accumulated_short,
        FractionalShares::new(dec!(0.5))
    );
    assert_eq!(position1.net, FractionalShares::new(dec!(-0.5)));
    assert_eq!(position1.pending_offchain_order_id, None);

    let mut expected_events: Vec<&str> = vec![
        "PositionEvent::OnChainOrderFilled", // trade 1
        "OnChainTradeEvent::Filled",         // trade 1
    ];
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let result2 = process_queued_trade(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx2, 2),
        2,
        make_trade(tx2, 2, 0.7),
        &cqrs,
    )
    .await?;

    // Checkpoint 3: above threshold -- execution created, broker submission is atomic
    let offchain_order_id =
        result2.expect("OffchainOrderId should be returned when threshold is crossed");

    let position2 = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist after second trade");
    assert_eq!(position2.accumulated_long, FractionalShares::ZERO);
    assert_eq!(
        position2.accumulated_short,
        FractionalShares::new(dec!(1.2))
    );
    assert_eq!(position2.net, FractionalShares::new(dec!(-1.2)));
    assert_eq!(
        position2.pending_offchain_order_id,
        Some(offchain_order_id),
        "Position should reference the pending offchain order"
    );

    // PlaceOrder command now emits both Placed and Submitted atomically
    expected_events.extend([
        "PositionEvent::OnChainOrderFilled",  // trade 2
        "OnChainTradeEvent::Filled",          // trade 2
        "PositionEvent::OffChainOrderPlaced", // threshold crossed
        "OffchainOrderEvent::Placed",         // offchain order created
        "OffchainOrderEvent::Submitted",      // broker accepted (atomic with Placed)
    ]);
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    // Fulfillment: order poller detects the filled order and completes the lifecycle
    let mock_executor = MockExecutor::new();
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        mock_executor,
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    );
    poller.poll_pending_orders().await?;

    // Checkpoint 4: after fulfillment -- pending_offchain_order_id should be cleared
    let position3 = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist after fulfillment");
    assert_eq!(
        position3.pending_offchain_order_id, None,
        "pending_offchain_order_id should be cleared after fulfillment",
    );
    assert_eq!(
        position3.net,
        FractionalShares::ZERO,
        "Net should be zero after offchain buy hedges the onchain sells",
    );

    expected_events.extend([
        "OffchainOrderEvent::Filled",         // broker filled
        "PositionEvent::OffChainOrderFilled", // position updated, pending cleared
    ]);
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    Ok(())
}

/// Tests the recovery path: accumulate -> threshold -> execute -> broker fails ->
/// poller handles failure -> position checker picks up unexecuted position -> retry -> fill.
#[tokio::test]
async fn position_checker_recovers_failed_execution() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);

    let symbol = Symbol::new("AAPL").unwrap();

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    process_queued_trade(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx1, 1),
        1,
        make_trade(tx1, 1, 0.5),
        &cqrs,
    )
    .await?;

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let result2 = process_queued_trade(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx2, 2),
        2,
        make_trade(tx2, 2, 0.7),
        &cqrs,
    )
    .await?;

    let _offchain_order_id =
        result2.expect("OffchainOrderId should be returned when threshold is crossed");

    // Poller discovers the broker FAILED the order and handles the failure
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected order".to_string()),
    });
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        failed_executor,
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    );
    poller.poll_pending_orders().await?;

    // Checkpoint: after failure -- pending cleared, position still has net exposure
    let position_after_failure = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist after failure");
    assert_eq!(
        position_after_failure.pending_offchain_order_id, None,
        "pending_offchain_order_id should be cleared after failure",
    );
    assert_eq!(
        position_after_failure.net,
        FractionalShares::new(dec!(-1.2)),
        "Net should still reflect unhedged exposure after failure",
    );

    let mut expected_events: Vec<&str> = vec![
        "PositionEvent::OnChainOrderFilled",  // trade 1
        "OnChainTradeEvent::Filled",          // trade 1
        "PositionEvent::OnChainOrderFilled",  // trade 2
        "OnChainTradeEvent::Filled",          // trade 2
        "PositionEvent::OffChainOrderPlaced", // threshold crossed
        "OffchainOrderEvent::Placed",         // offchain order created
        "OffchainOrderEvent::Submitted",      // broker accepted (atomic with Placed)
        "OffchainOrderEvent::Failed",         // broker reported failure
        "PositionEvent::OffChainOrderFailed", // position cleared
    ];
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    // Position checker finds the unexecuted position and retries
    let threshold = ExecutionThreshold::whole_share();
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        &pool,
        &position_cqrs,
        &position_query,
        &offchain_order_cqrs,
        &threshold,
    )
    .await?;

    // New poller with default MockExecutor (returns Filled) completes the retry lifecycle
    let retry_poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        MockExecutor::new(),
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    );
    retry_poller.poll_pending_orders().await?;

    // Final checkpoint: position fully hedged
    let final_position = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist after recovery");
    assert_eq!(
        final_position.pending_offchain_order_id, None,
        "pending_offchain_order_id should be cleared after successful retry",
    );
    assert_eq!(
        final_position.net,
        FractionalShares::ZERO,
        "Net should be zero after successful retry hedges the onchain sells",
    );

    expected_events.extend([
        "PositionEvent::OffChainOrderPlaced", // position checker retry
        "OffchainOrderEvent::Placed",         // new offchain order
        "OffchainOrderEvent::Submitted",      // broker accepted retry (atomic)
        "OffchainOrderEvent::Filled",         // broker filled
        "PositionEvent::OffChainOrderFilled", // position hedged, net=0
    ]);
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    Ok(())
}
