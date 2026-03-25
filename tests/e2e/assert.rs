//! Shared assertion helpers for e2e tests.
//!
//! Provides `ExpectedPosition`, DB assertion utilities, and the
//! `assert_decimal_eq!` macro used across hedging and rebalancing tests.

use std::collections::{HashMap, HashSet};

use rain_math_float::Float;
use serde_json::Value;
use sqlx::SqlitePool;

use st0x_event_sorcery::Projection;
use st0x_execution::alpaca_broker_api::{
    AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot, OrderSide, OrderStatus,
};
use st0x_execution::{Direction, SupportedExecutor, Symbol};
use st0x_finance::Usd;
use st0x_hedge::{OffchainOrder, Position};

use crate::assert_decimal_eq;
use crate::base_chain::TakeDirection;
use crate::poll::fetch_all_domain_events;
use st0x_float_macro::float;

/// Rounds a Float to `decimals` decimal places by converting to fixed-point
/// and back. Only used in test code for approximate comparisons.
pub(crate) fn round_float(value: Float, decimals: u8) -> anyhow::Result<Float> {
    let (fixed, _) = value
        .to_fixed_decimal_lossy(decimals)
        .map_err(|err| anyhow::anyhow!("to_fixed_decimal_lossy failed: {err:?}"))?;

    let (rounded, _) = Float::from_fixed_decimal_lossy(fixed, decimals)
        .map_err(|err| anyhow::anyhow!("from_fixed_decimal_lossy failed: {err:?}"))?;

    Ok(rounded)
}

/// Per-symbol expected final state after all trades are processed.
///
/// `amount` is the total expected hedge shares for this symbol across
/// all trades (e.g., 3 sells of 1.0 each -> amount = "3.0").
///
/// Two prices are tracked separately:
/// - `onchain_price`: the execution price from the Raindex order (USDC per
///   equity share, derived from the Rain expression ioRatio)
/// - `broker_fill_price`: the offchain hedge fill price from the broker mock
pub struct ExpectedPosition {
    pub symbol: &'static str,
    pub amount: Float,
    pub direction: TakeDirection,
    pub onchain_price: Float,
    pub broker_fill_price: Float,
    pub expected_accumulated_long: Float,
    pub expected_accumulated_short: Float,
    pub expected_net: Float,
}

#[bon::bon]
impl ExpectedPosition {
    #[builder]
    pub fn new(
        symbol: &'static str,
        amount: Float,
        direction: TakeDirection,
        onchain_price: Float,
        broker_fill_price: Float,
        expected_accumulated_long: Float,
        expected_accumulated_short: Float,
        expected_net: Float,
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

    pub fn expected_broker_side(&self) -> OrderSide {
        match self.direction {
            TakeDirection::SellEquity => OrderSide::Buy,
            TakeDirection::BuyEquity => OrderSide::Sell,
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
    assert_eq!(order.status, OrderStatus::Filled);

    let filled = order.filled_price.unwrap_or_else(|| {
        panic!(
            "Broker order for {} should have a fill price",
            expected_position.symbol
        )
    });
    assert!(
        filled.eq(expected_position.broker_fill_price).unwrap(),
        "Broker fill price for {} should match configured mock price",
        expected_position.symbol
    );
}

fn assert_total_broker_order_qty(
    expected_position: &ExpectedPosition,
    symbol_orders: &[&MockOrderSnapshot],
) {
    let total_quantity = symbol_orders
        .iter()
        .fold(Float::zero().unwrap(), |acc, order| {
            (acc + order.quantity).unwrap()
        });

    let epsilon = truncation_epsilon(symbol_orders.len());

    assert_decimal_eq!(
        total_quantity,
        expected_position.amount,
        epsilon,
        "Total broker order quantity for {} should match expected hedge amount",
        expected_position.symbol
    );
}

fn assert_broker_position(expected_position: &ExpectedPosition, position: &MockPositionSnapshot) {
    assert_eq!(position.symbol, expected_position.symbol);
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
        assert_position_view(expected_position, &pool, &events).await?;
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

            let recorded_price = Float::parse(
                trade.payload["Filled"]["price_usdc"]
                    .as_str()
                    .unwrap_or_else(|| {
                        panic!(
                            "price_usdc should be present in OnChainTrade::Filled payload: {:?}",
                            trade.payload
                        )
                    })
                    .to_string(),
            )
            .unwrap_or_else(|parse_error| {
                panic!("price_usdc should be a valid Float: {parse_error:?}")
            });

            // Round to 2 dp (cents) because buy-side ioRatio (1/price)
            // introduces tiny precision artifacts in the onchain representation.
            let recorded_rounded = round_float(recorded_price, 2).unwrap();
            let expected_rounded = round_float(expected_position.onchain_price, 2).unwrap();
            assert!(
                recorded_rounded.eq(expected_rounded).unwrap(),
                "OnChainTrade price_usdc for {} should match onchain execution \
                 price (rounded to cents): recorded={recorded_price:?}, \
                 expected={:?}",
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

/// Computes a truncation-aware epsilon scaled by the number of broker orders.
/// Each order can lose up to 1e-9 of precision from Alpaca's 9-decimal-place
/// truncation; cumulative fields need epsilon proportional to order count.
fn truncation_epsilon(order_count: usize) -> Float {
    let per_order = Float::parse("0.000000001".to_string()).expect("per_order_epsilon parse");
    let count = Float::parse(order_count.to_string()).expect("order_count parse");
    (per_order * count).expect("epsilon mul")
}

/// Counts the number of OffchainOrderEvent::Placed events for a given symbol.
fn count_offchain_orders_for_symbol(events: &[StoredEvent], symbol: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            event.event_type == "OffchainOrderEvent::Placed"
                && event
                    .payload
                    .get("Placed")
                    .and_then(|placed| placed.get("symbol"))
                    .and_then(|sym| sym.as_str())
                    == Some(symbol)
        })
        .count()
}

async fn assert_position_view(
    expected_position: &ExpectedPosition,
    pool: &SqlitePool,
    events: &[StoredEvent],
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
    let order_count = count_offchain_orders_for_symbol(events, expected_position.symbol).max(1);
    let epsilon = truncation_epsilon(order_count);

    assert_eq!(position.symbol, symbol);
    assert_decimal_eq!(
        position.net.inner(),
        expected_net,
        epsilon,
        "net position mismatch for {}",
        expected_position.symbol
    );
    assert_decimal_eq!(
        position.accumulated_long.inner(),
        expected_long,
        epsilon,
        "accumulated_long mismatch for {}",
        expected_position.symbol
    );
    assert_decimal_eq!(
        position.accumulated_short.inner(),
        expected_short,
        epsilon,
        "accumulated_short mismatch for {}",
        expected_position.symbol
    );

    assert!(
        position.pending_offchain_order_id.is_none(),
        "pending_offchain_order_id should be None after fill completes for {}",
        expected_position.symbol
    );

    let price = position.last_price_usdc.unwrap_or_else(|| {
        panic!(
            "last_price_usdc should be set for {}",
            expected_position.symbol
        )
    });
    let rounded_price = round_float(price, 2)?;
    let rounded_expected = round_float(expected_position.onchain_price, 2)?;
    assert!(
        rounded_price.eq(rounded_expected).unwrap(),
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
        let expected_price = expected_position.broker_fill_price;

        let mut total_shares = Float::zero().unwrap();
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
            total_shares = (total_shares + shares.inner().inner())
                .unwrap_or_else(|error| panic!("Float addition failed: {error:?}"));

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
                &Usd::new(expected_price),
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
            expected_position.amount,
            float!(0.000001),
            "Total hedge shares for {} should match expected amount",
            expected_position.symbol
        );
    }

    Ok(())
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
        let fmt = st0x_float_serde::format_float_with_fallback;
        let diff = match (l - r) {
            Ok(sub) => sub,
            Err(error) => panic!("Float subtraction failed: {error:?}"),
        };
        let zero = match rain_math_float::Float::zero() {
            Ok(z) => z,
            Err(error) => panic!("Float::zero() failed: {error:?}"),
        };
        let abs_diff = if diff.lt(zero).unwrap_or(false) {
            match (zero - diff) {
                Ok(neg) => neg,
                Err(error) => panic!("Float negation failed: {error:?}"),
            }
        } else {
            diff
        };
        if abs_diff.gt(eps).unwrap_or(false) {
            panic!(
                "assertion failed: `(left == right)` (within epsilon {})\n  \
                 left: `{}`\n right: `{}`\n delta: `{}`",
                fmt(&eps), fmt(&l), fmt(&r), fmt(&abs_diff)
            );
        }
    };
    ($left:expr, $right:expr, $epsilon:expr, $($arg:tt)+) => {
        let (l, r, eps) = ($left, $right, $epsilon);
        let fmt = st0x_float_serde::format_float_with_fallback;
        let diff = match (l - r) {
            Ok(sub) => sub,
            Err(error) => panic!("Float subtraction failed: {error:?}"),
        };
        let zero = match rain_math_float::Float::zero() {
            Ok(z) => z,
            Err(error) => panic!("Float::zero() failed: {error:?}"),
        };
        let abs_diff = if diff.lt(zero).unwrap_or(false) {
            match (zero - diff) {
                Ok(neg) => neg,
                Err(error) => panic!("Float negation failed: {error:?}"),
            }
        } else {
            diff
        };
        if abs_diff.gt(eps).unwrap_or(false) {
            panic!(
                "assertion failed: `(left == right)` (within epsilon {})\n  \
                 left: `{}`\n right: `{}`\n delta: `{}`\n{}",
                fmt(&eps), fmt(&l), fmt(&r), fmt(&abs_diff), format_args!($($arg)+)
            );
        }
    };
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use rain_math_float::Float;
    use st0x_execution::alpaca_broker_api::{OrderSide, OrderStatus};

    use super::{
        ExpectedPosition, MockOrderSnapshot, MockPositionSnapshot, StoredEvent,
        assert_broker_position, assert_single_clean_aggregate, assert_total_broker_order_qty,
    };
    use crate::base_chain::TakeDirection;
    use st0x_float_macro::float;

    fn expected_position(direction: TakeDirection, amount: Float) -> ExpectedPosition {
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(amount)
            .direction(direction)
            .onchain_price(float!(100))
            .broker_fill_price(float!(99))
            .expected_accumulated_long(float!(0))
            .expected_accumulated_short(float!(0))
            .expected_net(float!(0))
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
        let buy_hedge = expected_position(TakeDirection::SellEquity, float!(7.5));
        let long_position = MockPositionSnapshot {
            symbol: "AAPL".to_string(),
            quantity: float!(7.5),
            market_value: float!(742.5),
        };
        assert_broker_position(&buy_hedge, &long_position);

        let any_position = expected_position(TakeDirection::BuyEquity, float!(7.5));
        let signed_position = MockPositionSnapshot {
            symbol: "AAPL".to_string(),
            quantity: float!(-7.5),
            market_value: float!(-742.5),
        };
        assert_broker_position(&any_position, &signed_position);
    }

    #[test]
    fn assert_total_broker_order_qty_sums_multiple_orders() {
        let expected = expected_position(TakeDirection::SellEquity, float!(10.75));
        let first = MockOrderSnapshot {
            order_id: "1".to_string(),
            symbol: "AAPL".to_string(),
            quantity: float!(5.25),
            side: OrderSide::Buy,
            status: OrderStatus::Filled,
            poll_count: 1,
            filled_price: Some(float!(99)),
        };
        let second = MockOrderSnapshot {
            order_id: "2".to_string(),
            symbol: "AAPL".to_string(),
            quantity: float!(5.5),
            side: OrderSide::Buy,
            status: OrderStatus::Filled,
            poll_count: 1,
            filled_price: Some(float!(99)),
        };

        assert_total_broker_order_qty(&expected, &[&first, &second]);
    }

    #[test]
    #[should_panic(
        expected = "Total broker order quantity for AAPL should match expected hedge amount"
    )]
    fn assert_total_broker_order_qty_rejects_wrong_total() {
        let expected = expected_position(TakeDirection::SellEquity, float!(10.75));
        let only = MockOrderSnapshot {
            order_id: "1".to_string(),
            symbol: "AAPL".to_string(),
            quantity: float!(10.5),
            side: OrderSide::Buy,
            status: OrderStatus::Filled,
            poll_count: 1,
            filled_price: Some(float!(99)),
        };

        assert_total_broker_order_qty(&expected, &[&only]);
    }
}
