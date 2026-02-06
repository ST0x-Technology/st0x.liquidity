use alloy::primitives::{Address, B256, fixed_bytes};
use chrono::Utc;
use rust_decimal_macros::dec;
use serde_json::Value;
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, FractionalShares, MockExecutor, OrderState, SupportedExecutor, Symbol,
};

use crate::bindings::IOrderBookV5::{TakeOrderConfigV4, TakeOrderV3};
use crate::conductor::{
    check_and_execute_accumulated_positions, execute_pending_offchain_execution,
    process_trade_within_transaction,
};
use crate::dual_write::{DualWriteContext, load_position};
use crate::offchain::order_poller::{OrderPollerConfig, OrderStatusPoller};
use crate::offchain_order::ExecutionId;
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedEquitySymbol, Usdc};
use crate::onchain::trade::TradeEvent;
use crate::queue::QueuedEvent;
use crate::test_utils::setup_test_db;

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

#[tokio::test]
async fn happy_path_flow() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let dual_write_context = DualWriteContext::new(pool.clone());

    let symbol = Symbol::new("AAPL").unwrap();

    // Checkpoint 1: before any trades — no Position aggregate exists
    let before = load_position(&dual_write_context, &symbol).await?;
    assert!(
        before.is_none(),
        "No position should exist before any trades"
    );

    // Trade 1: 0.5 shares Buy, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let result1 = process_trade_within_transaction(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx1, 1),
        1,
        make_trade(tx1, 1, 0.5),
        &dual_write_context,
    )
    .await?;

    // Checkpoint 2: after first trade — below threshold, no execution
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    let position1 = load_position(&dual_write_context, &symbol)
        .await?
        .expect("Position should exist after first trade");
    assert_eq!(position1.accumulated_long, FractionalShares::ZERO);
    assert_eq!(
        position1.accumulated_short,
        FractionalShares::new(dec!(0.5))
    );
    assert_eq!(position1.net, FractionalShares::new(dec!(-0.5)));
    assert_eq!(position1.pending_execution_id, None);

    let mut expected_events: Vec<&str> = vec![
        "PositionEvent::Initialized",
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
    let result2 = process_trade_within_transaction(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx2, 2),
        2,
        make_trade(tx2, 2, 0.7),
        &dual_write_context,
    )
    .await?;

    // Checkpoint 3: above threshold — execution created
    let execution = result2.expect("Execution should be created when threshold is crossed");
    assert_eq!(
        execution.direction,
        Direction::Buy,
        "Onchain Sell -> offchain Buy to hedge"
    );
    assert_eq!(execution.executor, SupportedExecutor::DryRun);

    let position2 = load_position(&dual_write_context, &symbol)
        .await?
        .expect("Position should exist after second trade");
    assert_eq!(position2.accumulated_long, FractionalShares::ZERO);
    assert_eq!(
        position2.accumulated_short,
        FractionalShares::new(dec!(1.2))
    );
    assert_eq!(position2.net, FractionalShares::new(dec!(-1.2)));
    assert_eq!(
        position2.pending_execution_id,
        Some(ExecutionId(execution.id.unwrap())),
        "Position should reference the pending execution"
    );

    expected_events.extend([
        "PositionEvent::OnChainOrderFilled",  // trade 2
        "OnChainTradeEvent::Filled",          // trade 2
        "PositionEvent::OffChainOrderPlaced", // threshold crossed
        "OffchainOrderEvent::Placed",         // offchain order created
    ]);
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    // Fulfillment: exercise the real conductor and poller orchestration
    let execution_id = execution.id.unwrap();
    let mock_executor = MockExecutor::new();

    // Conductor places the order with the broker and confirms submission
    execute_pending_offchain_execution(&mock_executor, &pool, &dual_write_context, execution_id)
        .await?;

    // Order poller detects the filled order and completes the lifecycle
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        mock_executor,
        dual_write_context.clone(),
    );
    poller.poll_pending_orders().await?;

    // Checkpoint 4: after fulfillment — pending_execution_id should be cleared
    let position3 = load_position(&dual_write_context, &symbol)
        .await?
        .expect("Position should exist after fulfillment");
    assert_eq!(
        position3.pending_execution_id, None,
        "pending_execution_id should be cleared after fulfillment",
    );
    assert_eq!(
        position3.net,
        FractionalShares::ZERO,
        "Net should be zero after offchain buy hedges the onchain sells",
    );

    expected_events.extend([
        "OffchainOrderEvent::Submitted",      // broker accepted
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
    let dual_write_context = DualWriteContext::new(pool.clone());

    let symbol = Symbol::new("AAPL").unwrap();

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    process_trade_within_transaction(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx1, 1),
        1,
        make_trade(tx1, 1, 0.5),
        &dual_write_context,
    )
    .await?;

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let result2 = process_trade_within_transaction(
        SupportedExecutor::DryRun,
        &pool,
        &make_queued_event(tx2, 2),
        2,
        make_trade(tx2, 2, 0.7),
        &dual_write_context,
    )
    .await?;

    let execution = result2.expect("Execution should be created when threshold is crossed");
    let execution_id = execution.id.unwrap();

    // Conductor places the order with the broker and confirms submission
    execute_pending_offchain_execution(
        &MockExecutor::new(),
        &pool,
        &dual_write_context,
        execution_id,
    )
    .await?;

    // Poller discovers the broker FAILED the order and handles the failure
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected order".to_string()),
    });
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        failed_executor,
        dual_write_context.clone(),
    );
    poller.poll_pending_orders().await?;

    // Checkpoint: after failure — pending_execution_id cleared, position still has net exposure
    let position_after_failure = load_position(&dual_write_context, &symbol)
        .await?
        .expect("Position should exist after failure");
    assert_eq!(
        position_after_failure.pending_execution_id, None,
        "pending_execution_id should be cleared after failure",
    );
    assert_eq!(
        position_after_failure.net,
        FractionalShares::new(dec!(-1.2)),
        "Net should still reflect unhedged exposure after failure",
    );

    let mut expected_events: Vec<&str> = vec![
        "PositionEvent::Initialized",
        "PositionEvent::OnChainOrderFilled",  // trade 1
        "OnChainTradeEvent::Filled",          // trade 1
        "PositionEvent::OnChainOrderFilled",  // trade 2
        "OnChainTradeEvent::Filled",          // trade 2
        "PositionEvent::OffChainOrderPlaced", // threshold crossed
        "OffchainOrderEvent::Placed",         // offchain order created
        "OffchainOrderEvent::Submitted",      // broker accepted
        "OffchainOrderEvent::Failed",         // broker reported failure
        "PositionEvent::OffChainOrderFailed", // position cleared
    ];
    let actual_events = fetch_events(&pool).await;
    let actual_types: Vec<&str> = actual_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual_types, expected_events);

    // Position checker finds the unexecuted position and retries.
    // This spawns execute_pending_offchain_execution via tokio::spawn internally.
    check_and_execute_accumulated_positions(&MockExecutor::new(), &pool, &dual_write_context)
        .await?;

    // Let the spawned execution task complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // New poller with default MockExecutor (returns Filled) completes the retry lifecycle
    let retry_poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        MockExecutor::new(),
        dual_write_context.clone(),
    );
    retry_poller.poll_pending_orders().await?;

    // Final checkpoint: position fully hedged
    let final_position = load_position(&dual_write_context, &symbol)
        .await?
        .expect("Position should exist after recovery");
    assert_eq!(
        final_position.pending_execution_id, None,
        "pending_execution_id should be cleared after successful retry",
    );
    assert_eq!(
        final_position.net,
        FractionalShares::ZERO,
        "Net should be zero after successful retry hedges the onchain sells",
    );

    expected_events.extend([
        "PositionEvent::OffChainOrderPlaced", // position checker retry
        "OffchainOrderEvent::Placed",         // new offchain order
        "OffchainOrderEvent::Submitted",      // broker accepted retry
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
