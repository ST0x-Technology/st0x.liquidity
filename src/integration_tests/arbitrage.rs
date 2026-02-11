use std::sync::Arc;

use alloy::primitives::{Address, B256, fixed_bytes};
use chrono::Utc;
use cqrs_es::persist::GenericQuery;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, FractionalShares, MockExecutor, OrderState, SupportedExecutor, Symbol,
};

use super::{ExpectedEvent, assert_events, fetch_events};
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
use crate::position::{Position, PositionAggregate, PositionCqrs, PositionQuery, load_position};
use crate::queue::QueuedEvent;
use crate::test_utils::setup_test_db;
use crate::threshold::ExecutionThreshold;

/// Loads a position and asserts it matches the expected field values.
///
/// The `last_updated` timestamp is non-deterministic and is asserted against the loaded value.
async fn assert_position(
    query: &Arc<PositionQuery>,
    symbol: &Symbol,
    net: FractionalShares,
    accumulated_long: FractionalShares,
    accumulated_short: FractionalShares,
    pending: Option<OffchainOrderId>,
    last_price_usdc: Decimal,
) {
    let pos = load_position(query, symbol)
        .await
        .unwrap()
        .expect("Position should exist");
    assert_eq!(
        pos,
        Position {
            net,
            accumulated_long,
            accumulated_short,
            pending_offchain_order_id: pending,
            last_price_usdc: Some(last_price_usdc),
            last_updated: pos.last_updated,
        }
    );
}

/// Submits a trade through the pipeline and returns the optional OffchainOrderId.
async fn submit_trade(
    pool: &SqlitePool,
    event_id: i64,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError> {
    let queued_event = QueuedEvent {
        id: Some(1),
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
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
    };
    process_queued_trade(
        SupportedExecutor::DryRun,
        pool,
        &queued_event,
        event_id,
        trade,
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

fn onchain_trade(
    tx_hash: B256,
    log_index: u64,
    amount: f64,
    symbol: &str,
    direction: Direction,
    price: f64,
) -> OnchainTrade {
    OnchainTrade {
        id: None,
        tx_hash,
        log_index,
        symbol: symbol.parse::<TokenizedEquitySymbol>().unwrap(),
        equity_token: Address::ZERO,
        amount,
        direction,
        price: Usdc::new(price).unwrap(),
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
async fn onchain_trades_accumulate_and_trigger_offchain_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // Checkpoint 1: before any trades -- no Position aggregate exists
    assert!(load_position(&position_query, &symbol).await?.is_none());

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let result1 = submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 0.5, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?;
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(-0.5)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(0.5)),
        None,
        dec!(150.25),
    )
    .await;

    let tx1_agg = format!("{tx1}:1");
    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
    ];
    assert_events(&pool, &expected).await;

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let order_id = submit_trade(
        &pool,
        2,
        onchain_trade(tx2, 2, 0.7, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("Threshold crossed, should return OffchainOrderId");
    let order_id_str = order_id.to_string();

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(-1.2)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        Some(order_id),
        dec!(150.25),
    )
    .await;

    let tx2_agg = format!("{tx2}:2");
    expected.extend([
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    // Payload spot-checks for post-threshold events
    let placed_pos = &events[4].payload["OffChainOrderPlaced"];
    assert_eq!(
        placed_pos["offchain_order_id"].as_str().unwrap(),
        order_id_str
    );
    assert_eq!(placed_pos["direction"].as_str().unwrap(), "Buy");

    let offchain_placed = &events[5].payload["Placed"];
    assert_eq!(offchain_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(offchain_placed["direction"].as_str().unwrap(), "Buy");

    // Fulfillment: order poller detects the filled order and completes the lifecycle
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        None,
        dec!(150.25),
    )
    .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    let events = assert_events(&pool, &expected).await;
    assert_eq!(
        events[8].payload["OffChainOrderFilled"]["offchain_order_id"]
            .as_str()
            .unwrap(),
        order_id_str,
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
    submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 0.5, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?;
    let order_id = submit_trade(
        &pool,
        2,
        onchain_trade(tx2, 2, 0.7, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("Threshold crossed");
    let order_id_str = order_id.to_string();

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
    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(-1.2)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        None,
        dec!(150.25),
    )
    .await;

    let tx1_agg = format!("{tx1}:1");
    let tx2_agg = format!("{tx2}:2");
    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Failed"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFailed"),
    ];
    let events = assert_events(&pool, &expected).await;
    let placed = &events[5].payload["Placed"];
    assert_eq!(placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(placed["direction"].as_str().unwrap(), "Buy");

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
    assert_position(
        &position_query,
        &symbol,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        None,
        dec!(150.25),
    )
    .await;

    // Extract retry order ID from the new OffchainOrderEvent::Placed event
    let all_events = fetch_events(&pool).await;
    let retry_id = all_events[10].aggregate_id.clone();
    assert_ne!(
        retry_id, order_id_str,
        "Retry should create a new offchain order"
    );

    expected.extend([
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Submitted"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

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
        submit_trade(
            &pool,
            1,
            onchain_trade(tx1, 1, 0.6, "tAAPL", Direction::Sell, 150.25),
            &cqrs
        )
        .await?
        .is_none()
    );
    assert!(
        submit_trade(
            &pool,
            2,
            onchain_trade(tx2, 2, 0.4, "tMSFT", Direction::Sell, 420.50),
            &cqrs
        )
        .await?
        .is_none()
    );
    let aapl_order_id = submit_trade(
        &pool,
        3,
        onchain_trade(tx3, 3, 0.6, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("AAPL 1.2 crosses threshold");
    let aapl_order_str = aapl_order_id.to_string();

    assert_position(
        &position_query,
        &aapl,
        FractionalShares::new(dec!(-1.2)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        Some(aapl_order_id),
        dec!(150.25),
    )
    .await;
    assert_position(
        &position_query,
        &msft,
        FractionalShares::new(dec!(-0.4)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(0.4)),
        None,
        dec!(420.50),
    )
    .await;

    let tx1_agg = format!("{tx1}:1");
    let tx2_agg = format!("{tx2}:2");
    let tx3_agg = format!("{tx3}:3");
    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx3_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    let aapl_placed = &events[7].payload["Placed"];
    assert_eq!(aapl_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(aapl_placed["direction"].as_str().unwrap(), "Buy");

    // Poll and fill AAPL, verify MSFT unchanged
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &aapl,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        None,
        dec!(150.25),
    )
    .await;
    assert_position(
        &position_query,
        &msft,
        FractionalShares::new(dec!(-0.4)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(0.4)),
        None,
        dec!(420.50),
    )
    .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    // MSFT crosses threshold (0.4 + 0.6 = 1.0, exactly at threshold)
    let tx4 = fixed_bytes!("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    let msft_order_id = submit_trade(
        &pool,
        4,
        onchain_trade(tx4, 4, 0.6, "tMSFT", Direction::Sell, 420.50),
        &cqrs,
    )
    .await?
    .expect("MSFT 1.0 hits threshold");
    let msft_order_str = msft_order_id.to_string();

    assert_position(
        &position_query,
        &msft,
        FractionalShares::new(dec!(-1.0)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.0)),
        Some(msft_order_id),
        dec!(420.50),
    )
    .await;
    assert_ne!(
        aapl_order_id, msft_order_id,
        "Separate offchain orders per symbol"
    );

    let tx4_agg = format!("{tx4}:4");
    expected.extend([
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx4_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Submitted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    let msft_placed = &events[14].payload["Placed"];
    assert_eq!(msft_placed["symbol"].as_str().unwrap(), "MSFT");
    assert_eq!(msft_placed["direction"].as_str().unwrap(), "Buy");

    // Poll and fill MSFT, both positions end fully hedged
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &aapl,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        None,
        dec!(150.25),
    )
    .await;
    assert_position(
        &position_query,
        &msft,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.0)),
        None,
        dec!(420.50),
    )
    .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that Buy direction onchain trades accumulate `accumulated_long` and produce a
/// Sell hedge when the position crosses threshold.
#[tokio::test]
async fn buy_direction_accumulates_long() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    // Trade 1: Buy 0.5 shares, below threshold
    let result1 = submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 0.5, "tAAPL", Direction::Buy, 150.25),
        &cqrs,
    )
    .await?;
    assert!(result1.is_none(), "Below threshold");

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(0.5)),
        FractionalShares::new(dec!(0.5)),
        FractionalShares::ZERO,
        None,
        dec!(150.25),
    )
    .await;

    // Trade 2: Buy 0.7 shares, crosses threshold -> hedge is Sell
    let order_id = submit_trade(
        &pool,
        2,
        onchain_trade(tx2, 2, 0.7, "tAAPL", Direction::Buy, 150.25),
        &cqrs,
    )
    .await?
    .expect("Threshold crossed");
    let order_id_str = order_id.to_string();

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(1.2)),
        FractionalShares::new(dec!(1.2)),
        FractionalShares::ZERO,
        Some(order_id),
        dec!(150.25),
    )
    .await;

    // Verify offchain order is Sell direction (hedge for long position)
    let tx1_agg = format!("{tx1}:1");
    let tx2_agg = format!("{tx2}:2");
    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    // Hedge direction should be Sell (opposite of onchain Buy)
    assert_eq!(
        events[5].payload["Placed"]["direction"].as_str().unwrap(),
        "Sell"
    );

    // Fill the hedge order
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.2)),
        FractionalShares::ZERO,
        None,
        dec!(150.25),
    )
    .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that a single trade of exactly 1.0 shares immediately triggers execution.
#[tokio::test]
async fn exact_threshold_triggers_execution() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, _position_cqrs, position_query, _offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let order_id = submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 1.0, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("Exactly 1.0 should cross whole-share threshold");

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(-1.0)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.0)),
        Some(order_id),
        dec!(150.25),
    )
    .await;

    let order_id_str = order_id.to_string();
    let tx1_agg = format!("{tx1}:1");
    let expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that the position checker is a no-op when all positions are already hedged.
#[tokio::test]
async fn position_checker_noop_when_hedged() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);

    // Complete a full hedge cycle so position net=0
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 1.0, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("Threshold crossed");
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    let events_before = fetch_events(&pool).await;

    // Position checker should find nothing to do
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        &pool,
        &position_cqrs,
        &position_query,
        &offchain_order_cqrs,
        &ExecutionThreshold::whole_share(),
    )
    .await?;

    let events_after = fetch_events(&pool).await;
    assert_eq!(
        events_before.len(),
        events_after.len(),
        "Position checker should not emit any new events when positions are hedged"
    );

    Ok(())
}

/// Tests that after completing a full hedge cycle, new trades can accumulate and trigger
/// a second hedge cycle.
#[tokio::test]
async fn second_hedge_after_full_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // First cycle: 1.0 share sell -> hedge -> fill
    let tx1 = fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let order1 = submit_trade(
        &pool,
        1,
        onchain_trade(tx1, 1, 1.0, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("First threshold crossing");
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(1.0)),
        None,
        dec!(150.25),
    )
    .await;

    // Second cycle: another 1.5 share sell -> crosses threshold again
    let tx2 = fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let order2 = submit_trade(
        &pool,
        2,
        onchain_trade(tx2, 2, 1.5, "tAAPL", Direction::Sell, 150.25),
        &cqrs,
    )
    .await?
    .expect("Second threshold crossing");
    assert_ne!(order1, order2, "Second cycle should create a new order");

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::new(dec!(-1.5)),
        FractionalShares::ZERO,
        FractionalShares::new(dec!(2.5)),
        Some(order2),
        dec!(150.25),
    )
    .await;

    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position(
        &position_query,
        &symbol,
        FractionalShares::ZERO,
        FractionalShares::ZERO,
        FractionalShares::new(dec!(2.5)),
        None,
        dec!(150.25),
    )
    .await;

    // Verify the full event sequence across both cycles
    let order1_str = order1.to_string();
    let order2_str = order2.to_string();
    let tx1_agg = format!("{tx1}:1");
    let tx2_agg = format!("{tx2}:2");
    let expected = vec![
        // First cycle
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order1_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
        // Second cycle
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &tx2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order2_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ];
    assert_events(&pool, &expected).await;

    Ok(())
}
