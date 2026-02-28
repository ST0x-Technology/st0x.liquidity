//! Shared helpers for e2e test assertions.
//!
//! Provides the `ExpectedPosition` struct, DB assertion utilities, and bot
//! lifecycle helpers used across `hedging.rs` and `rebalancing.rs`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use st0x_event_sorcery::Projection;
use st0x_execution::{Direction, SupportedExecutor, Symbol};
use st0x_hedge::config::Ctx;
use st0x_hedge::{Dollars, OffchainOrder, Position, launch};

use crate::assert_decimal_eq;
use crate::services::alpaca_broker::{AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot};
use crate::services::base_chain::TakeDirection;

/// Per-symbol expected final state after all trades are processed.
///
/// `amount` is the total expected hedge shares for this symbol across
/// all trades (e.g., 3 sells of 1.0 each → amount = "3.0").
///
/// Two prices are tracked separately:
/// - `onchain_price`: the execution price from the Raindex order (USDC per
///   equity share, derived from the Rain expression ioRatio)
/// - `broker_fill_price`: the offchain hedge fill price from the broker mock
pub struct ExpectedPosition {
    pub symbol: &'static str,
    pub amount: Decimal,
    pub direction: TakeDirection,
    pub onchain_price: Decimal,
    pub broker_fill_price: Decimal,
    pub expected_accumulated_long: Decimal,
    pub expected_accumulated_short: Decimal,
    pub expected_net: Decimal,
}

#[bon::bon]
impl ExpectedPosition {
    #[builder]
    pub fn new(
        symbol: &'static str,
        amount: Decimal,
        direction: TakeDirection,
        onchain_price: Decimal,
        broker_fill_price: Decimal,
        expected_accumulated_long: Decimal,
        expected_accumulated_short: Decimal,
        expected_net: Decimal,
    ) -> Self {
        Self {
            symbol,
            amount,
            direction,
            onchain_price,
            broker_fill_price,
            expected_accumulated_long,
            expected_accumulated_short,
            expected_net,
        }
    }

    /// Whether an offchain hedge is expected (false for NetZero).
    pub fn expects_hedge(&self) -> bool {
        !matches!(self.direction, TakeDirection::NetZero)
    }

    /// The hedge direction is the inverse of the onchain direction.
    pub fn expected_hedge_direction(&self) -> Direction {
        match self.direction {
            TakeDirection::SellEquity => Direction::Buy,
            TakeDirection::BuyEquity => Direction::Sell,
            TakeDirection::NetZero => panic!("NetZero position has no hedge direction"),
        }
    }

    pub fn expected_broker_side(&self) -> &'static str {
        match self.direction {
            TakeDirection::SellEquity => "buy",
            TakeDirection::BuyEquity => "sell",
            TakeDirection::NetZero => panic!("NetZero position has no broker side"),
        }
    }
}

/// Minimal event record for querying the CQRS events table.
#[derive(Debug, sqlx::FromRow)]
pub struct StoredEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
}

/// Spawns the full bot as a background task.
pub fn spawn_bot(ctx: Ctx) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(launch(ctx))
}

/// Sleeps for `seconds`, then panics if the bot task has already finished
/// (indicating it crashed during processing).
pub async fn wait_for_processing(bot: &mut JoinHandle<anyhow::Result<()>>, seconds: u64) {
    tokio::select! {
        result = &mut *bot => {
            match result {
                Ok(Ok(())) => panic!("Bot exited cleanly before processing completed ({seconds}s)"),
                Ok(Err(error)) => panic!("Bot crashed during wait_for_processing: {error:#}"),
                Err(join_error) => panic!("Bot task panicked: {join_error}"),
            }
        }
        () = tokio::time::sleep(Duration::from_secs(seconds)) => {}

    }
}

pub const POLL_INTERVAL: Duration = Duration::from_millis(200);
pub const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

/// Sleeps for [`POLL_INTERVAL`], panicking immediately if the bot task
/// exits (crash or clean shutdown) during the sleep.
pub async fn sleep_or_crash(bot: &mut JoinHandle<anyhow::Result<()>>, context: &str) {
    tokio::select! {
        result = &mut *bot => {
            match result {
                Ok(Ok(())) => panic!("Bot exited cleanly while polling for: {context}"),
                Ok(Err(error)) => panic!("Bot crashed while polling for: {context}: {error:#}"),
                Err(join_error) => panic!("Bot panicked while polling for: {context}: {join_error}"),
            }
        }
        () = tokio::time::sleep(POLL_INTERVAL) => {}
    }
}

/// Polls the CQRS events table until at least `expected_count` events of
/// the given `event_type` exist, using the default 30s timeout.
pub async fn poll_for_events(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    event_type: &str,
    expected_count: i64,
) {
    poll_for_events_with_timeout(
        bot,
        db_path,
        event_type,
        expected_count,
        Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS),
    )
    .await;
}

/// Polls the CQRS events table until at least `expected_count` events of
/// the given `event_type` exist, with an explicit timeout.
///
/// Tolerates the database not existing yet (the bot creates it on
/// startup via migrations), retrying the connection each poll cycle.
pub async fn poll_for_events_with_timeout(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    event_type: &str,
    expected_count: i64,
    timeout: Duration,
) {
    let url = format!("sqlite:{}", db_path.display());
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_count}x {event_type}");

    loop {
        sleep_or_crash(bot, &context).await;

        // The DB file may not exist yet if the bot is still starting up.
        let Ok(pool) = SqlitePool::connect(&url).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(&pool)
            .await
            .map(|row| row.0)
            .unwrap_or(0);

        pool.close().await;

        if count >= expected_count {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context} (found {count})",
        );
    }
}

/// Polls for events matching an `aggregate_type` whose `event_type`
/// contains `type_substring` (e.g. "Failed"). Uses the default 30s
/// timeout.
///
/// Tolerates the database not existing yet (see
/// [`poll_for_events_with_timeout`]).
pub async fn poll_for_aggregate_events_containing(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    aggregate_type: &str,
    type_substring: &str,
    expected_count: i64,
) {
    let url = format!("sqlite:{}", db_path.display());
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_count}x {aggregate_type}/*{type_substring}*");

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect(&url).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let count = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = ? AND event_type LIKE '%' || ? || '%'",
        )
        .bind(aggregate_type)
        .bind(type_substring)
        .fetch_one(&pool)
        .await
        .map(|row| row.0)
        .unwrap_or(0);

        pool.close().await;

        if count >= expected_count {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context} (found {count})",
        );
    }
}

/// Opens a read-only SQLite connection to the test database.
pub async fn connect_db(db_path: &std::path::Path) -> anyhow::Result<SqlitePool> {
    let url = format!("sqlite:{}", db_path.display());
    Ok(SqlitePool::connect(&url).await?)
}

/// Counts CQRS events for a specific aggregate type.
pub async fn count_events(pool: &SqlitePool, aggregate_type: &str) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE aggregate_type = ?")
        .bind(aggregate_type)
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Counts all domain events (excludes `SchemaRegistry` bookkeeping events).
pub async fn count_all_domain_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE aggregate_type != 'SchemaRegistry'")
            .fetch_one(pool)
            .await?;

    Ok(row.0)
}

/// Counts hedging-specific domain events, excluding bookkeeping
/// (`SchemaRegistry`) and background polling (`InventorySnapshot`)
/// aggregates whose event counts are non-deterministic due to timing.
pub async fn count_hedging_domain_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events \
         WHERE aggregate_type NOT IN ('SchemaRegistry', 'InventorySnapshot')",
    )
    .fetch_one(pool)
    .await?;

    Ok(row.0)
}

/// Fetches all domain events ordered by insertion.
pub async fn fetch_all_domain_events(pool: &SqlitePool) -> anyhow::Result<Vec<StoredEvent>> {
    let events: Vec<StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type != 'SchemaRegistry' \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(events)
}

/// Asserts broker orders and positions match expected state per symbol.
///
/// For each expected_position, verifies at least one filled order exists with the
/// correct side and fill price, and exactly one position exists.
pub fn assert_broker_state(expected_positions: &[ExpectedPosition], broker: &AlpacaBrokerMock) {
    let orders = broker.orders();
    let positions = broker.positions();

    for expected_position in expected_positions {
        let symbol_orders: Vec<&MockOrderSnapshot> = orders
            .iter()
            .filter(|order| order.symbol == expected_position.symbol)
            .collect();

        if !expected_position.expects_hedge() {
            assert!(
                symbol_orders.is_empty(),
                "Expected no broker orders for {} (net zero), got {}",
                expected_position.symbol,
                symbol_orders.len()
            );

            continue;
        }

        assert!(
            !symbol_orders.is_empty(),
            "Expected at least one broker order for {}",
            expected_position.symbol
        );

        for order in &symbol_orders {
            assert_broker_order(expected_position, order);
        }
        assert_total_broker_order_qty(expected_position, &symbol_orders);

        let symbol_positions: Vec<&MockPositionSnapshot> = positions
            .iter()
            .filter(|pos| pos.symbol == expected_position.symbol)
            .collect();

        assert_eq!(
            symbol_positions.len(),
            1,
            "Expected exactly one broker position for {}",
            expected_position.symbol
        );
        assert_broker_position(expected_position, symbol_positions[0]);
    }
}

fn assert_broker_order(expected_position: &ExpectedPosition, order: &MockOrderSnapshot) {
    assert_eq!(order.symbol, expected_position.symbol);
    assert_eq!(order.side, expected_position.expected_broker_side());
    assert_eq!(order.status, "filled");
    let _parsed_order_qty: Decimal = order.qty.parse().unwrap_or_else(|error| {
        panic!(
            "Broker order qty for {} should be a valid Decimal, got '{}': {error}",
            expected_position.symbol, order.qty
        )
    });
    assert_eq!(
        order.filled_price.as_deref(),
        Some(expected_position.broker_fill_price.to_string()).as_deref(),
        "Broker fill price for {} should match configured mock price",
        expected_position.symbol
    );
}

fn assert_total_broker_order_qty(
    expected_position: &ExpectedPosition,
    symbol_orders: &[&MockOrderSnapshot],
) {
    let total_qty = symbol_orders.iter().fold(Decimal::ZERO, |acc, order| {
        let qty: Decimal = order.qty.parse().unwrap_or_else(|error| {
            panic!(
                "Broker order qty for {} should be a valid Decimal, got '{}': {error}",
                expected_position.symbol, order.qty
            )
        });
        acc + qty
    });

    assert_decimal_eq!(
        total_qty,
        expected_position.amount,
        DEFAULT_EPSILON,
        "Total broker order qty for {} should match expected hedge amount",
        expected_position.symbol
    );
}

fn assert_broker_position(expected_position: &ExpectedPosition, position: &MockPositionSnapshot) {
    assert_eq!(position.symbol, expected_position.symbol);

    // Verify the qty is a valid Decimal. We intentionally don't compare it
    // against `expected_position.amount` because rebalancing operations
    // (equity mint/redeem) modify the broker position qty independently of
    // the original hedge amount. Total order qty is validated separately by
    // `assert_total_broker_order_qty`.
    let _parsed_qty: Decimal = position.qty.parse().unwrap_or_else(|error| {
        panic!(
            "Broker position qty for {} should be a valid Decimal, got '{}': {error}",
            expected_position.symbol, position.qty
        )
    });
}

/// Comprehensive CQRS assertions for one or more expected positions.
///
/// Checks OnChainTrade events, Position events, OffchainOrder events,
/// position projection views, and offchain order projection views.
pub async fn assert_cqrs_state(
    expected_positions: &[ExpectedPosition],
    expected_onchain_trade_count: usize,
    database_url: &str,
) -> anyhow::Result<()> {
    let pool = SqlitePool::connect(&format!("sqlite:{database_url}")).await?;

    let events = fetch_all_domain_events(&pool).await?;
    assert!(!events.is_empty(), "Expected CQRS events to be persisted");

    assert_onchain_trade_events(expected_positions, &events, expected_onchain_trade_count);
    assert_position_events(expected_positions, &events);
    assert_offchain_order_events(expected_positions, &events);

    for expected_position in expected_positions {
        assert_position_view(expected_position, &pool).await?;
    }

    assert_offchain_order_views(expected_positions, &pool).await?;

    pool.close().await;
    Ok(())
}

fn assert_onchain_trade_events(
    expected_positions: &[ExpectedPosition],
    events: &[StoredEvent],
    expected_count: usize,
) {
    let all_trades: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OnChainTrade")
        .collect();

    assert_eq!(
        all_trades.len(),
        expected_count,
        "Expected exactly {expected_count} OnChainTrade events, got {}",
        all_trades.len(),
    );

    for expected_position in expected_positions {
        let symbol_trades: Vec<&&StoredEvent> = all_trades
            .iter()
            .filter(|event| {
                event.payload["Filled"]["symbol"].as_str() == Some(expected_position.symbol)
            })
            .collect();

        assert!(
            !symbol_trades.is_empty(),
            "Expected at least one OnChainTrade for {}",
            expected_position.symbol
        );

        for trade in &symbol_trades {
            assert_eq!(trade.event_type, "OnChainTradeEvent::Filled");

            let recorded_price: Decimal = trade.payload["Filled"]["price_usdc"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "price_usdc should be present in OnChainTrade::Filled payload: {:?}",
                        trade.payload
                    )
                })
                .parse()
                .unwrap_or_else(|parse_error| {
                    panic!("price_usdc should be a valid Decimal: {parse_error}")
                });
            // Round to 2 dp (cents) because buy-side ioRatio (1/price)
            // introduces tiny precision artifacts in the onchain representation.
            assert_eq!(
                recorded_price.round_dp(2),
                expected_position.onchain_price.round_dp(2),
                "OnChainTrade price_usdc for {} should match onchain execution \
                 price (rounded to cents): recorded={recorded_price}, \
                 expected={}",
                expected_position.symbol,
                expected_position.onchain_price
            );
        }
    }
}

fn assert_position_events(expected_positions: &[ExpectedPosition], events: &[StoredEvent]) {
    for expected_position in expected_positions {
        let pos_events: Vec<&StoredEvent> = events
            .iter()
            .filter(|event| {
                event.aggregate_type == "Position" && event.aggregate_id == expected_position.symbol
            })
            .collect();
        assert!(
            !pos_events.is_empty(),
            "Expected Position events for {}",
            expected_position.symbol
        );

        assert_eq!(
            pos_events[0].event_type, "PositionEvent::Initialized",
            "First Position event for {} should be Initialized",
            expected_position.symbol
        );
        assert_eq!(
            pos_events[0].aggregate_id, expected_position.symbol,
            "Position aggregate_id should be the symbol"
        );

        let event_types: Vec<&str> = pos_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();

        assert!(
            event_types.contains(&"PositionEvent::OnChainOrderFilled"),
            "Missing PositionEvent::OnChainOrderFilled for {}",
            expected_position.symbol
        );

        if expected_position.expects_hedge() {
            assert!(
                event_types.contains(&"PositionEvent::OffChainOrderPlaced"),
                "Missing PositionEvent::OffChainOrderPlaced for {}",
                expected_position.symbol
            );
            assert!(
                event_types.contains(&"PositionEvent::OffChainOrderFilled"),
                "Missing PositionEvent::OffChainOrderFilled for {}",
                expected_position.symbol
            );
        } else {
            assert!(
                !event_types.contains(&"PositionEvent::OffChainOrderPlaced"),
                "Unexpected PositionEvent::OffChainOrderPlaced for net-zero {}",
                expected_position.symbol
            );
            assert!(
                !event_types.contains(&"PositionEvent::OffChainOrderFilled"),
                "Unexpected PositionEvent::OffChainOrderFilled for net-zero {}",
                expected_position.symbol
            );
        }
    }
}

fn assert_offchain_order_events(expected_positions: &[ExpectedPosition], events: &[StoredEvent]) {
    let hedged_symbols: HashSet<&str> = expected_positions
        .iter()
        .filter(|s| s.expects_hedge())
        .map(|position| position.symbol)
        .collect();

    let offchain_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OffchainOrder")
        .collect();

    if hedged_symbols.is_empty() {
        assert!(
            offchain_events.is_empty(),
            "Expected no OffchainOrder events for net-zero positions, got {}",
            offchain_events.len()
        );

        return;
    }

    let mut events_by_order: HashMap<&str, Vec<&StoredEvent>> = HashMap::new();
    let mut order_counts_by_symbol: HashMap<&str, usize> = HashMap::new();

    for event in &offchain_events {
        events_by_order
            .entry(event.aggregate_id.as_str())
            .or_default()
            .push(*event);
    }

    for (order_id, order_events) in &events_by_order {
        let event_types: Vec<&str> = order_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            event_types,
            vec![
                "OffchainOrderEvent::Placed",
                "OffchainOrderEvent::Submitted",
                "OffchainOrderEvent::Filled",
            ],
            "OffchainOrder aggregate {order_id} should have exact success event sequence",
        );

        let placed_symbol = order_events[0]
            .payload
            .get("Placed")
            .and_then(|value| value.get("symbol"))
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "OffchainOrderEvent::Placed payload missing symbol for {order_id}: {}",
                    order_events[0].payload
                )
            });
        assert!(
            hedged_symbols.contains(placed_symbol),
            "Unexpected OffchainOrder symbol {placed_symbol} for aggregate {order_id}",
        );
        *order_counts_by_symbol.entry(placed_symbol).or_insert(0) += 1;
    }

    assert_eq!(
        offchain_events.len(),
        events_by_order.len() * 3,
        "OffchainOrder success path should emit exactly 3 events per aggregate",
    );

    let position_filled_events = events
        .iter()
        .filter(|event| {
            event.aggregate_type == "Position"
                && event.event_type == "PositionEvent::OffChainOrderFilled"
        })
        .count();
    assert_eq!(
        events_by_order.len(),
        position_filled_events,
        "OffchainOrder aggregate count should match PositionEvent::OffChainOrderFilled count",
    );

    for expected_position in expected_positions {
        if expected_position.expects_hedge() {
            assert!(
                order_counts_by_symbol.contains_key(expected_position.symbol),
                "Missing OffchainOrder events for hedged symbol {}",
                expected_position.symbol
            );
        } else {
            assert!(
                !order_counts_by_symbol.contains_key(expected_position.symbol),
                "Unexpected OffchainOrder events for net-zero symbol {}",
                expected_position.symbol
            );
        }
    }
}

async fn assert_position_view(
    expected_position: &ExpectedPosition,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let projection = Projection::<Position>::sqlite(pool.clone());
    let symbol = Symbol::new(expected_position.symbol)?;

    let position = projection.load(&symbol).await?.ok_or_else(|| {
        anyhow::anyhow!(
            "Position view should exist for {}",
            expected_position.symbol
        )
    })?;

    let expected_net = expected_position.expected_net;
    let expected_long = expected_position.expected_accumulated_long;
    let expected_short = expected_position.expected_accumulated_short;

    assert_eq!(position.symbol, symbol);
    assert_decimal_eq!(
        position.net.inner(),
        expected_net,
        DEFAULT_EPSILON,
        "net position mismatch for {}",
        expected_position.symbol
    );
    assert_decimal_eq!(
        position.accumulated_long.inner(),
        expected_long,
        DEFAULT_EPSILON,
        "accumulated_long mismatch for {}",
        expected_position.symbol
    );
    assert_decimal_eq!(
        position.accumulated_short.inner(),
        expected_short,
        DEFAULT_EPSILON,
        "accumulated_short mismatch for {}",
        expected_position.symbol
    );

    assert!(
        position.pending_offchain_order_id.is_none(),
        "pending_offchain_order_id should be None after fill completes for {}",
        expected_position.symbol
    );
    assert_eq!(
        position.last_price_usdc.map(|price| price.round_dp(2)),
        Some(expected_position.onchain_price.round_dp(2)),
        "last_price_usdc for {} should match onchain execution price",
        expected_position.symbol
    );
    assert!(
        position.last_updated.is_some(),
        "last_updated should be set for {}",
        expected_position.symbol
    );

    Ok(())
}

/// Asserts offchain order views per symbol: all orders are Filled with
/// correct direction, executor, and fill price. Total hedge shares per
/// symbol must equal `expected_position.amount`.
async fn assert_offchain_order_views(
    expected_positions: &[ExpectedPosition],
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let projection = Projection::<OffchainOrder>::sqlite(pool.clone());
    let all_orders = projection.load_all().await?;

    for expected_position in expected_positions {
        if !expected_position.expects_hedge() {
            let expected_symbol = Symbol::new(expected_position.symbol)?;
            let has_orders = all_orders
                .iter()
                .any(|(_, order)| matches!(order, OffchainOrder::Filled { symbol, .. } if *symbol == expected_symbol));

            assert!(
                !has_orders,
                "Expected no offchain orders for net-zero {}, but found some",
                expected_position.symbol
            );

            continue;
        }

        let expected_symbol = Symbol::new(expected_position.symbol)?;
        let expected_amount = expected_position.amount;
        let expected_price = expected_position.broker_fill_price;

        let mut total_shares = Decimal::ZERO;
        let mut found_any = false;

        for (order_id, order) in &all_orders {
            let OffchainOrder::Filled {
                symbol,
                shares,
                direction,
                executor,
                price,
                ..
            } = order
            else {
                panic!("Offchain order {order_id} should be Filled, got {order:?}");
            };

            if symbol != &expected_symbol {
                continue;
            }

            found_any = true;
            total_shares += shares.inner().inner();

            assert_eq!(
                direction,
                &expected_position.expected_hedge_direction(),
                "Direction mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
            assert_eq!(
                executor,
                &SupportedExecutor::AlpacaBrokerApi,
                "Executor mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
            assert_eq!(
                price,
                &Dollars(expected_price),
                "Fill price mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
        }

        assert!(
            found_any,
            "Expected at least one offchain order for {}",
            expected_position.symbol
        );
        assert_decimal_eq!(
            total_shares,
            expected_amount,
            dec!(0.000_001),
            "Total hedge shares for {} should match expected amount",
            expected_position.symbol
        );
    }

    Ok(())
}

/// Fetches events for a specific aggregate type, ordered by insertion.
pub async fn fetch_events_by_type(
    pool: &SqlitePool,
    aggregate_type: &str,
) -> anyhow::Result<Vec<StoredEvent>> {
    let events: Vec<StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type = ? \
         ORDER BY rowid ASC",
    )
    .bind(aggregate_type)
    .fetch_all(pool)
    .await?;

    Ok(events)
}

/// Asserts that events contain the expected event types as an ordered
/// subsequence (types appear in order, but gaps are allowed between them).
pub fn assert_event_subsequence(events: &[StoredEvent], expected_types: &[&str]) {
    let mut expected_iter = expected_types.iter().peekable();

    for event in events {
        if let Some(expected) = expected_iter.peek()
            && event.event_type == **expected
        {
            expected_iter.next();
        }
    }

    let remaining: Vec<&&str> = expected_iter.collect();
    assert!(
        remaining.is_empty(),
        "Event subsequence not found.\n  Expected types: {expected_types:?}\n  \
         Actual event types: {:?}\n  Missing: {remaining:?}",
        events
            .iter()
            .map(|event| &event.event_type)
            .collect::<Vec<_>>(),
    );
}

/// Asserts that exactly one rebalancing aggregate exists for the given type
/// and that no error events were emitted.
///
/// This catches two classes of bugs:
/// - **Duplicate triggers**: If the trigger fires twice, two aggregates are
///   created. The subsequence check on combined events could hide the second
///   aggregate's failure.
/// - **Silent errors**: If the pipeline produces error events alongside
///   success events (e.g. a retry that fails then succeeds), the subsequence
///   check passes but the error goes unnoticed.
///
/// `error_substrings` are patterns that indicate error event types
/// (e.g. "Failed", "Rejected").
pub fn assert_single_clean_aggregate(events: &[StoredEvent], error_substrings: &[&str]) {
    let aggregate_ids: std::collections::HashSet<&str> = events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert!(
        !aggregate_ids.is_empty(),
        "Expected at least 1 rebalancing aggregate, got 0",
    );
    assert_eq!(
        aggregate_ids.len(),
        1,
        "Expected exactly 1 rebalancing aggregate, got {}: {:?}",
        aggregate_ids.len(),
        aggregate_ids
    );

    let error_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| {
            error_substrings
                .iter()
                .any(|sub| event.event_type.contains(sub))
        })
        .collect();
    assert!(
        error_events.is_empty(),
        "Expected no error events, found: {:?}",
        error_events
            .iter()
            .map(|event| &event.event_type)
            .collect::<Vec<_>>(),
    );
}

#[macro_export]
macro_rules! assert_decimal_eq {
    ($left:expr, $right:expr, $epsilon:expr $(,)?) => {
        let (l, r, eps) = ($left, $right, $epsilon);
        let diff = (l - r).abs();
        if diff > eps {
            panic!(
                "assertion failed: `(left == right)` (within epsilon {})\n  left: `{:?}`\n right: `{:?}`\n delta: `{:?}`",
                eps, l, r, diff
            );
        }
    };
    ($left:expr, $right:expr, $epsilon:expr, $($arg:tt)+) => {
        let (l, r, eps) = ($left, $right, $epsilon);
        let diff = (l - r).abs();
        if diff > eps {
            panic!(
                "assertion failed: `(left == right)` (within epsilon {})\n  left: `{:?}`\n right: `{:?}`\n delta: `{:?}`\n{}",
                eps, l, r, diff, format_args!($($arg)+)
            );
        }
    };
}
const DEFAULT_EPSILON: Decimal = dec!(0.000000000000001);

#[cfg(test)]
mod tests {
    use serde_json::json;

    use rust_decimal_macros::dec;

    use super::{
        ExpectedPosition, MockOrderSnapshot, MockPositionSnapshot, StoredEvent,
        assert_broker_position, assert_single_clean_aggregate, assert_total_broker_order_qty,
    };
    use crate::services::base_chain::TakeDirection;

    fn expected_position(
        direction: TakeDirection,
        amount: rust_decimal::Decimal,
    ) -> ExpectedPosition {
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(amount)
            .direction(direction)
            .onchain_price(dec!(100))
            .broker_fill_price(dec!(99))
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(dec!(0))
            .expected_net(dec!(0))
            .build()
    }

    fn stored_event(aggregate_id: &str, event_type: &str) -> StoredEvent {
        StoredEvent {
            aggregate_type: "UsdcRebalance".to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            payload: json!({}),
        }
    }

    #[test]
    fn assert_single_clean_aggregate_accepts_single_aggregate_without_errors() {
        let events = vec![
            stored_event("agg-1", "UsdcRebalanceEvent::Initiated"),
            stored_event("agg-1", "UsdcRebalanceEvent::DepositConfirmed"),
        ];

        assert_single_clean_aggregate(&events, &["Failed"]);
    }

    #[test]
    #[should_panic(expected = "Expected exactly 1 rebalancing aggregate")]
    fn assert_single_clean_aggregate_rejects_multiple_aggregates() {
        let events = vec![
            stored_event("agg-1", "UsdcRebalanceEvent::Initiated"),
            stored_event("agg-2", "UsdcRebalanceEvent::DepositConfirmed"),
        ];

        assert_single_clean_aggregate(&events, &["Failed"]);
    }

    #[test]
    #[should_panic(expected = "Expected no error events")]
    fn assert_single_clean_aggregate_rejects_error_events() {
        let events = vec![
            stored_event("agg-1", "UsdcRebalanceEvent::Initiated"),
            stored_event("agg-1", "UsdcRebalanceEvent::Failed"),
        ];

        assert_single_clean_aggregate(&events, &["Failed"]);
    }

    #[test]
    fn assert_broker_position_parses_qty() {
        let buy_hedge = expected_position(TakeDirection::SellEquity, dec!(7.5));
        let long_position = MockPositionSnapshot {
            symbol: "AAPL".to_string(),
            qty: "7.5".to_string(),
            market_value: "742.5".to_string(),
        };
        assert_broker_position(&buy_hedge, &long_position);

        let any_position = expected_position(TakeDirection::BuyEquity, dec!(7.5));
        let signed_position = MockPositionSnapshot {
            symbol: "AAPL".to_string(),
            qty: "-7.5".to_string(),
            market_value: "-742.5".to_string(),
        };
        assert_broker_position(&any_position, &signed_position);
    }

    #[test]
    fn assert_total_broker_order_qty_sums_multiple_orders() {
        let expected = expected_position(TakeDirection::SellEquity, dec!(10.75));
        let first = MockOrderSnapshot {
            order_id: "1".to_string(),
            symbol: "AAPL".to_string(),
            qty: "5.25".to_string(),
            side: "buy".to_string(),
            status: "filled".to_string(),
            poll_count: 1,
            filled_price: Some("99".to_string()),
        };
        let second = MockOrderSnapshot {
            order_id: "2".to_string(),
            symbol: "AAPL".to_string(),
            qty: "5.5".to_string(),
            side: "buy".to_string(),
            status: "filled".to_string(),
            poll_count: 1,
            filled_price: Some("99".to_string()),
        };

        assert_total_broker_order_qty(&expected, &[&first, &second]);
    }

    #[test]
    #[should_panic(expected = "Total broker order qty for AAPL should match expected hedge amount")]
    fn assert_total_broker_order_qty_rejects_wrong_total() {
        let expected = expected_position(TakeDirection::SellEquity, dec!(10.75));
        let only = MockOrderSnapshot {
            order_id: "1".to_string(),
            symbol: "AAPL".to_string(),
            qty: "10.5".to_string(),
            side: "buy".to_string(),
            status: "filled".to_string(),
            poll_count: 1,
            filled_price: Some("99".to_string()),
        };

        assert_total_broker_order_qty(&expected, &[&only]);
    }
}
