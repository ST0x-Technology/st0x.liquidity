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
use crate::error::EventProcessingError;
use crate::offchain::order_poller::{OrderPollerConfig, OrderStatusPoller};
use crate::offchain_order::{OffchainOrderAggregate, OffchainOrderCqrs, OffchainOrderId};
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedEquitySymbol, Usdc};
use crate::onchain::trade::TradeEvent;
use crate::position::{PositionAggregate, PositionCqrs, PositionQuery, load_position};
use crate::queue::QueuedEvent;
use crate::test_utils::setup_test_db;
use crate::threshold::ExecutionThreshold;

#[derive(Debug, sqlx::FromRow)]
struct StoredEvent {
    aggregate_type: String,
    aggregate_id: String,
    #[allow(dead_code)]
    sequence: i64,
    event_type: String,
    #[allow(dead_code)]
    event_version: String,
    payload: Value,
    #[allow(dead_code)]
    metadata: Value,
}

async fn fetch_events(pool: &SqlitePool) -> Vec<StoredEvent> {
    sqlx::query_as::<_, StoredEvent>(
        "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
         FROM events \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Submits a trade through the pipeline and returns the optional OffchainOrderId.
async fn submit_trade(
    pool: &SqlitePool,
    tx_hash: B256,
    log_index: u64,
    event_id: i64,
    amount: f64,
    symbol: &str,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    process_queued_trade(
        SupportedExecutor::DryRun,
        pool,
        &make_queued_event(tx_hash, log_index),
        event_id,
        make_trade(tx_hash, log_index, amount, symbol),
        cqrs,
    )
    .await
}

/// Creates a poller with the default MockExecutor (returns Filled) and polls pending orders.
async fn poll_and_fill(
    pool: &SqlitePool,
    offchain_order_cqrs: &Arc<OffchainOrderCqrs>,
    position_cqrs: &Arc<PositionCqrs>,
) -> Result<(), Box<dyn std::error::Error>> {
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        MockExecutor::new(),
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    );
    poller.poll_pending_orders().await?;
    Ok(())
}

/// Asserts that all PositionEvent entries in the events list route to the given aggregate_id.
fn assert_position_events_route_to(
    events: &[StoredEvent],
    expected_types: &[&str],
    aggregate_id: &str,
) {
    for (i, event_type) in expected_types.iter().enumerate() {
        if event_type.starts_with("PositionEvent::") {
            assert_eq!(
                events[i].aggregate_id, aggregate_id,
                "Position event at index {i} should route to {aggregate_id} aggregate",
            );
        }
    }
}

/// Fetches events and asserts their event_type sequence matches expected.
async fn assert_event_types(pool: &SqlitePool, expected: &[&str]) -> Vec<StoredEvent> {
    let events = fetch_events(pool).await;
    let actual: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(actual, expected);
    events
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

fn make_trade(tx_hash: B256, log_index: u64, amount: f64, symbol: &str) -> OnchainTrade {
    OnchainTrade {
        id: None,
        tx_hash,
        log_index,
        symbol: symbol.parse::<TokenizedEquitySymbol>().unwrap(),
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
    assert!(load_position(&position_query, &symbol).await?.is_none());

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let result1 = submit_trade(&pool, tx1, 1, 1, 0.5, "tAAPL", &cqrs).await?;
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    let pos1 = load_position(&position_query, &symbol)
        .await?
        .expect("Position should exist");
    assert_eq!(pos1.accumulated_short, FractionalShares::new(dec!(0.5)));
    assert_eq!(pos1.net, FractionalShares::new(dec!(-0.5)));
    assert_eq!(pos1.pending_offchain_order_id, None);

    let mut expected: Vec<&str> = vec![
        "PositionEvent::OnChainOrderFilled",
        "OnChainTradeEvent::Filled",
    ];
    let events = assert_event_types(&pool, &expected).await;
    assert_eq!(events[0].aggregate_id, "AAPL");
    assert_eq!(events[1].aggregate_id, format!("{tx1}:{}", 1));

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let order_id = submit_trade(&pool, tx2, 2, 2, 0.7, "tAAPL", &cqrs)
        .await?
        .expect("Threshold crossed, should return OffchainOrderId");

    let pos2 = load_position(&position_query, &symbol).await?.unwrap();
    assert_eq!(pos2.accumulated_short, FractionalShares::new(dec!(1.2)));
    assert_eq!(pos2.net, FractionalShares::new(dec!(-1.2)));
    assert_eq!(pos2.pending_offchain_order_id, Some(order_id));

    expected.extend([
        "PositionEvent::OnChainOrderFilled",
        "OnChainTradeEvent::Filled",
        "PositionEvent::OffChainOrderPlaced",
        "OffchainOrderEvent::Placed",
        "OffchainOrderEvent::Submitted",
    ]);
    let events = assert_event_types(&pool, &expected).await;

    // Verify aggregate_id routing and payload for post-threshold events
    assert_eq!(events[2].aggregate_id, "AAPL");
    assert_eq!(events[3].aggregate_id, format!("{tx2}:{}", 2));
    assert_eq!(events[4].aggregate_id, "AAPL");

    let placed_pos = &events[4].payload["OffChainOrderPlaced"];
    assert_eq!(
        placed_pos["offchain_order_id"].as_str().unwrap(),
        order_id.to_string()
    );
    assert_eq!(placed_pos["direction"].as_str().unwrap(), "Buy");

    let offchain_placed = &events[5].payload["Placed"];
    assert_eq!(offchain_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(offchain_placed["direction"].as_str().unwrap(), "Buy");
    assert_eq!(events[5].aggregate_id, order_id.to_string());
    assert_eq!(events[6].aggregate_id, order_id.to_string());

    // Fulfillment: order poller detects the filled order and completes the lifecycle
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    let pos3 = load_position(&position_query, &symbol).await?.unwrap();
    assert_eq!(pos3.pending_offchain_order_id, None);
    assert_eq!(pos3.net, FractionalShares::ZERO);

    expected.extend([
        "OffchainOrderEvent::Filled",
        "PositionEvent::OffChainOrderFilled",
    ]);
    let events = assert_event_types(&pool, &expected).await;
    assert_eq!(events[7].aggregate_id, order_id.to_string());
    assert_eq!(events[8].aggregate_id, "AAPL");
    assert_eq!(
        events[8].payload["OffChainOrderFilled"]["offchain_order_id"]
            .as_str()
            .unwrap(),
        order_id.to_string(),
    );

    Ok(())
}

/// Tests the recovery path: accumulate -> threshold -> execute -> broker fails ->
/// poller handles failure -> position checker picks up unexecuted position -> retry -> fill.
#[tokio::test]
async fn position_checker_recovers_failed_execution() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // Two trades: 0.5 + 0.7 = 1.2 shares, crosses threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    submit_trade(&pool, tx1, 1, 1, 0.5, "tAAPL", &cqrs).await?;
    let order_id = submit_trade(&pool, tx2, 2, 2, 0.7, "tAAPL", &cqrs)
        .await?
        .expect("Threshold crossed");

    // Poller discovers the broker FAILED the order
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected order".to_string()),
    });
    OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        failed_executor,
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    )
    .poll_pending_orders()
    .await?;

    // After failure: pending cleared, position still has net exposure
    let pos = load_position(&position_query, &symbol).await?.unwrap();
    assert_eq!(pos.pending_offchain_order_id, None);
    assert_eq!(pos.net, FractionalShares::new(dec!(-1.2)));

    let mut expected: Vec<&str> = vec![
        "PositionEvent::OnChainOrderFilled",
        "OnChainTradeEvent::Filled",
        "PositionEvent::OnChainOrderFilled",
        "OnChainTradeEvent::Filled",
        "PositionEvent::OffChainOrderPlaced",
        "OffchainOrderEvent::Placed",
        "OffchainOrderEvent::Submitted",
        "OffchainOrderEvent::Failed",
        "PositionEvent::OffChainOrderFailed",
    ];
    let events = assert_event_types(&pool, &expected).await;

    // All position events route to AAPL, offchain order events to the order aggregate
    assert_position_events_route_to(&events, &expected, "AAPL");
    assert_eq!(events[5].aggregate_id, order_id.to_string());
    assert_eq!(events[6].aggregate_id, order_id.to_string());
    assert_eq!(events[7].aggregate_id, order_id.to_string());
    assert_eq!(
        events[5].payload["Placed"]["symbol"].as_str().unwrap(),
        "AAPL"
    );
    assert_eq!(
        events[5].payload["Placed"]["direction"].as_str().unwrap(),
        "Buy"
    );

    // Position checker finds the unexecuted position and retries
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        &pool,
        &position_cqrs,
        &position_query,
        &offchain_order_cqrs,
        &ExecutionThreshold::whole_share(),
    )
    .await?;

    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    // Final: position fully hedged
    let final_pos = load_position(&position_query, &symbol).await?.unwrap();
    assert_eq!(final_pos.pending_offchain_order_id, None);
    assert_eq!(final_pos.net, FractionalShares::ZERO);

    expected.extend([
        "PositionEvent::OffChainOrderPlaced",
        "OffchainOrderEvent::Placed",
        "OffchainOrderEvent::Submitted",
        "OffchainOrderEvent::Filled",
        "PositionEvent::OffChainOrderFilled",
    ]);
    let events = assert_event_types(&pool, &expected).await;

    // Retry created a new offchain order
    let retry_id = &events[10].aggregate_id;
    assert_ne!(
        retry_id,
        &order_id.to_string(),
        "Retry should create a new offchain order"
    );
    assert_eq!(events[11].aggregate_id, *retry_id);
    assert_eq!(events[12].aggregate_id, *retry_id);
    assert_position_events_route_to(&events, &expected, "AAPL");

    Ok(())
}

/// Tests that two symbols processed through the pipeline don't contaminate each other's
/// Position state or event streams.
#[tokio::test]
async fn multi_symbol_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let aapl = Symbol::new("AAPL").unwrap();
    let msft = Symbol::new("MSFT").unwrap();

    // Phase 1: interleaved trades -- tAAPL sell 0.6, tMSFT sell 0.4, tAAPL sell 0.6
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let tx3 = fixed_bytes!("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");

    assert!(
        submit_trade(&pool, tx1, 1, 1, 0.6, "tAAPL", &cqrs)
            .await?
            .is_none()
    );
    assert!(
        submit_trade(&pool, tx2, 2, 2, 0.4, "tMSFT", &cqrs)
            .await?
            .is_none()
    );
    let aapl_order_id = submit_trade(&pool, tx3, 3, 3, 0.6, "tAAPL", &cqrs)
        .await?
        .expect("AAPL 1.2 crosses threshold");

    // Phase 1 assertions: position state
    let aapl_pos = load_position(&position_query, &aapl).await?.unwrap();
    assert_eq!(aapl_pos.accumulated_short, FractionalShares::new(dec!(1.2)));
    assert_eq!(aapl_pos.net, FractionalShares::new(dec!(-1.2)));
    assert_eq!(aapl_pos.pending_offchain_order_id, Some(aapl_order_id));

    let msft_pos = load_position(&position_query, &msft).await?.unwrap();
    assert_eq!(msft_pos.accumulated_short, FractionalShares::new(dec!(0.4)));
    assert_eq!(msft_pos.net, FractionalShares::new(dec!(-0.4)));
    assert_eq!(msft_pos.pending_offchain_order_id, None);

    // Phase 1 assertions: event sequence and isolation
    let phase1_expected = vec![
        "PositionEvent::OnChainOrderFilled",  // AAPL trade 1
        "OnChainTradeEvent::Filled",          // AAPL trade 1
        "PositionEvent::OnChainOrderFilled",  // MSFT trade 1
        "OnChainTradeEvent::Filled",          // MSFT trade 1
        "PositionEvent::OnChainOrderFilled",  // AAPL trade 2
        "OnChainTradeEvent::Filled",          // AAPL trade 2
        "PositionEvent::OffChainOrderPlaced", // AAPL crosses threshold
        "OffchainOrderEvent::Placed",         // AAPL offchain order
        "OffchainOrderEvent::Submitted",      // AAPL broker accepted
    ];
    let events = assert_event_types(&pool, &phase1_expected).await;
    assert_phase1_isolation(&events, &aapl_order_id);

    // Phase 2: poll and fill AAPL, verify MSFT unchanged
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;
    assert_eq!(
        load_position(&position_query, &aapl).await?.unwrap().net,
        FractionalShares::ZERO
    );

    let msft_after_aapl = load_position(&position_query, &msft).await?.unwrap();
    assert_eq!(msft_after_aapl.net, FractionalShares::new(dec!(-0.4)));
    assert_eq!(msft_after_aapl.pending_offchain_order_id, None);

    // Phase 3: MSFT crosses threshold (0.4 + 0.6 = 1.0, exactly at threshold)
    let tx4 = fixed_bytes!("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    let msft_order_id = submit_trade(&pool, tx4, 4, 4, 0.6, "tMSFT", &cqrs)
        .await?
        .expect("MSFT 1.0 hits threshold");

    let msft_pos3 = load_position(&position_query, &msft).await?.unwrap();
    assert_eq!(
        msft_pos3.accumulated_short,
        FractionalShares::new(dec!(1.0))
    );
    assert_eq!(msft_pos3.net, FractionalShares::new(dec!(-1.0)));
    assert_eq!(msft_pos3.pending_offchain_order_id, Some(msft_order_id));
    assert_ne!(
        aapl_order_id, msft_order_id,
        "Separate offchain orders per symbol"
    );

    // Verify MSFT offchain order payload
    let msft_events = fetch_events(&pool).await;
    let msft_placed = msft_events
        .iter()
        .find(|e| {
            e.aggregate_id == msft_order_id.to_string()
                && e.event_type == "OffchainOrderEvent::Placed"
        })
        .expect("MSFT should have OffchainOrderEvent::Placed");
    assert_eq!(
        msft_placed.payload["Placed"]["symbol"].as_str().unwrap(),
        "MSFT"
    );
    assert_eq!(
        msft_placed.payload["Placed"]["direction"].as_str().unwrap(),
        "Buy"
    );

    // Phase 4: poll and fill MSFT
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    // Final: both positions hedged
    assert_eq!(
        load_position(&position_query, &aapl).await?.unwrap().net,
        FractionalShares::ZERO
    );
    assert_eq!(
        load_position(&position_query, &msft).await?.unwrap().net,
        FractionalShares::ZERO
    );

    // Final event counts: no cross-contamination
    let all = fetch_events(&pool).await;
    assert_event_count_for_aggregate(&all, "AAPL", "Position", 4);
    assert_event_count_for_aggregate(&all, "MSFT", "Position", 4);
    assert_event_count_for_aggregate(&all, &aapl_order_id.to_string(), "", 3);
    assert_event_count_for_aggregate(&all, &msft_order_id.to_string(), "", 3);

    Ok(())
}

/// Phase 1 isolation assertions for multi_symbol_isolation: verifies aggregate_id routing
/// and MSFT has only one position event at this stage.
fn assert_phase1_isolation(events: &[StoredEvent], aapl_order_id: &OffchainOrderId) {
    assert_eq!(events[0].aggregate_id, "AAPL");
    assert_eq!(events[2].aggregate_id, "MSFT");
    assert_eq!(events[4].aggregate_id, "AAPL");
    assert_eq!(events[6].aggregate_id, "AAPL");

    // MSFT has only one position event (OnChainOrderFilled) at this point
    let msft_pos_count = events
        .iter()
        .filter(|e| e.aggregate_id == "MSFT" && e.aggregate_type == "Position")
        .count();
    assert_eq!(msft_pos_count, 1);

    // AAPL offchain order payload spot checks
    let aapl_placed = &events[7].payload["Placed"];
    assert_eq!(aapl_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(aapl_placed["direction"].as_str().unwrap(), "Buy");
    assert_eq!(events[7].aggregate_id, aapl_order_id.to_string());
    assert_eq!(events[8].aggregate_id, aapl_order_id.to_string());
}

/// Asserts the number of events matching a given aggregate_id (and optionally aggregate_type).
/// Pass empty string for `aggregate_type` to skip the type filter.
fn assert_event_count_for_aggregate(
    events: &[StoredEvent],
    aggregate_id: &str,
    aggregate_type: &str,
    expected_count: usize,
) {
    let count = events
        .iter()
        .filter(|e| {
            e.aggregate_id == aggregate_id
                && (aggregate_type.is_empty() || e.aggregate_type == aggregate_type)
        })
        .count();
    assert_eq!(
        count, expected_count,
        "Expected {expected_count} events for aggregate {aggregate_id} (type={aggregate_type}), found {count}",
    );
}
