//! Shared helpers for e2e test scenarios.
//!
//! Provides the `E2eScenario` struct, DB assertion utilities, and bot
//! lifecycle helpers used across `scenarios.rs` and `main.rs`.

use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use st0x_event_sorcery::Projection;
use st0x_execution::{
    AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, FractionalShares, SupportedExecutor,
    Symbol, TimeInForce,
};
use st0x_hedge::bindings::IOrderBookV5;
use st0x_hedge::config::{BrokerCtx, Ctx, EvmCtx, LogLevel};
use st0x_hedge::{Dollars, OffchainOrder, Position, launch};

use super::services::alpaca_broker::{
    self, AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot,
};
use super::services::base_chain::{self, TakeDirection, TakeOrderResult};

// ── Types ────────────────────────────────────────────────────────────

/// Per-symbol expected final state after all trades are processed.
///
/// `amount` is the total expected hedge shares for this symbol across
/// all trades (e.g., 3 sells of 1.0 each → amount = "3.0").
pub struct E2eScenario {
    pub symbol: &'static str,
    pub amount: &'static str,
    pub direction: TakeDirection,
    pub fill_price: &'static str,
    pub expected_accumulated_long: &'static str,
    pub expected_accumulated_short: &'static str,
    pub expected_net: &'static str,
}

impl E2eScenario {
    /// The hedge direction is the inverse of the onchain direction.
    pub fn expected_hedge_direction(&self) -> Direction {
        match self.direction {
            TakeDirection::SellEquity => Direction::Buy,
            TakeDirection::BuyEquity => Direction::Sell,
        }
    }

    pub fn expected_broker_side(&self) -> &'static str {
        match self.direction {
            TakeDirection::SellEquity => "buy",
            TakeDirection::BuyEquity => "sell",
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

// ── Bot lifecycle helpers ────────────────────────────────────────────

/// Builds a `Ctx` pointing at the given chain, broker, and database path.
pub fn build_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
) -> anyhow::Result<Ctx> {
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: alpaca_broker::TEST_ACCOUNT_ID.to_owned(),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    });
    let execution_threshold = broker_ctx.execution_threshold()?;

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            order_owner: Some(chain.owner),
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: None,
        execution_threshold,
    })
}

/// Spawns the full bot as a background task.
pub fn spawn_bot(ctx: Ctx) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(launch(ctx))
}

/// Sleeps for `seconds`, then panics if the bot task has already finished
/// (indicating it crashed during processing).
pub async fn wait_for_processing(bot: &JoinHandle<anyhow::Result<()>>, seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;

    assert!(
        !bot.is_finished(),
        "Bot task finished unexpectedly during wait_for_processing"
    );
}

// ── Database helpers ─────────────────────────────────────────────────

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

/// Counts rows in the `event_queue` table.
pub async fn count_queued_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_queue")
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Counts processed rows in the `event_queue` table.
pub async fn count_processed_queue_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_queue WHERE processed = 1")
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

// ── Full pipeline assertions ─────────────────────────────────────────

/// Comprehensive end-to-end assertions covering broker state, onchain
/// vaults, and CQRS events/views for one or more symbols.
///
/// This is the primary assertion entry point. Every scenario test should
/// call this after the bot has finished processing.
pub async fn assert_full_pipeline<P: Provider>(
    scenarios: &[E2eScenario],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    database_url: &str,
) -> anyhow::Result<()> {
    assert_broker_state(scenarios, broker);

    for take_result in take_results {
        assert_onchain_vaults(provider, orderbook_addr, owner, take_result).await?;
    }

    assert_cqrs_state(scenarios, take_results.len(), database_url).await?;

    Ok(())
}

// ── Broker assertions ────────────────────────────────────────────────

/// Asserts broker orders and positions match expected state per symbol.
///
/// For each scenario, verifies at least one filled order exists with the
/// correct side and fill price, and exactly one position exists.
pub fn assert_broker_state(scenarios: &[E2eScenario], broker: &AlpacaBrokerMock) {
    let orders = broker.orders();
    let positions = broker.positions();

    for scenario in scenarios {
        let symbol_orders: Vec<&MockOrderSnapshot> = orders
            .iter()
            .filter(|order| order.symbol == scenario.symbol)
            .collect();

        assert!(
            !symbol_orders.is_empty(),
            "Expected at least one broker order for {}",
            scenario.symbol
        );

        for order in &symbol_orders {
            assert_broker_order(scenario, order);
        }

        let symbol_positions: Vec<&MockPositionSnapshot> = positions
            .iter()
            .filter(|pos| pos.symbol == scenario.symbol)
            .collect();

        assert_eq!(
            symbol_positions.len(),
            1,
            "Expected exactly one broker position for {}",
            scenario.symbol
        );
        assert_broker_position(scenario, symbol_positions[0]);
    }
}

fn assert_broker_order(scenario: &E2eScenario, order: &MockOrderSnapshot) {
    assert_eq!(order.symbol, scenario.symbol);
    assert_eq!(order.side, scenario.expected_broker_side());
    assert_eq!(order.status, "filled");
    assert!(
        order.poll_count >= 1,
        "Order for {} should have been polled at least once, got {}",
        scenario.symbol,
        order.poll_count
    );
    assert_eq!(
        order.filled_price.as_deref(),
        Some(scenario.fill_price),
        "Fill price for {} should match configured mock price",
        scenario.symbol
    );
}

fn assert_broker_position(scenario: &E2eScenario, position: &MockPositionSnapshot) {
    assert_eq!(position.symbol, scenario.symbol);
}

// ── Onchain vault assertions ─────────────────────────────────────────

async fn assert_onchain_vaults<P: Provider>(
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    take_result: &TakeOrderResult,
) -> anyhow::Result<()> {
    let orderbook = IOrderBookV5::IOrderBookV5Instance::new(orderbook_addr, provider);

    // Output vault should be depleted after the taker consumed it
    let output_balance = orderbook
        .vaultBalance2(owner, take_result.output_token, take_result.output_vault_id)
        .call()
        .await?;
    assert_eq!(
        output_balance,
        B256::ZERO,
        "Output vault should be fully consumed"
    );

    // Input vault should have received payment
    let input_balance = orderbook
        .vaultBalance2(owner, take_result.input_token, take_result.input_vault_id)
        .call()
        .await?;
    assert_ne!(input_balance, B256::ZERO, "Input vault should hold payment");

    Ok(())
}

// ── CQRS assertions ─────────────────────────────────────────────────

/// Comprehensive CQRS assertions for one or more scenarios.
///
/// Checks OnChainTrade events, Position events, OffchainOrder events,
/// position projection views, and offchain order projection views.
async fn assert_cqrs_state(
    scenarios: &[E2eScenario],
    expected_onchain_trade_count: usize,
    database_url: &str,
) -> anyhow::Result<()> {
    let pool = SqlitePool::connect(&format!("sqlite:{database_url}")).await?;

    let events = fetch_all_domain_events(&pool).await?;
    assert!(!events.is_empty(), "Expected CQRS events to be persisted");

    assert_onchain_trade_events(scenarios, &events, expected_onchain_trade_count);
    assert_position_events(scenarios, &events);
    assert_offchain_order_events(scenarios, &events);

    for scenario in scenarios {
        assert_position_view(scenario, &pool).await?;
    }

    assert_offchain_order_views(scenarios, &pool).await?;

    pool.close().await;
    Ok(())
}

fn assert_onchain_trade_events(
    scenarios: &[E2eScenario],
    events: &[StoredEvent],
    expected_count: usize,
) {
    let all_trades: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OnChainTrade")
        .collect();

    assert!(
        all_trades.len() >= expected_count,
        "Expected at least {expected_count} OnChainTrade events, got {}",
        all_trades.len()
    );

    for scenario in scenarios {
        let symbol_trades: Vec<&&StoredEvent> = all_trades
            .iter()
            .filter(|event| event.payload["Filled"]["symbol"].as_str() == Some(scenario.symbol))
            .collect();

        assert!(
            !symbol_trades.is_empty(),
            "Expected at least one OnChainTrade for {}",
            scenario.symbol
        );

        for trade in &symbol_trades {
            assert_eq!(trade.event_type, "OnChainTradeEvent::Filled");
        }
    }
}

fn assert_position_events(scenarios: &[E2eScenario], events: &[StoredEvent]) {
    for scenario in scenarios {
        let pos_events: Vec<&StoredEvent> = events
            .iter()
            .filter(|event| {
                event.aggregate_type == "Position" && event.aggregate_id == scenario.symbol
            })
            .collect();

        assert!(
            pos_events.len() >= 3,
            "Expected at least 3 Position events for {}, got {}",
            scenario.symbol,
            pos_events.len()
        );

        // First event is always Initialized
        assert_eq!(
            pos_events[0].event_type, "PositionEvent::Initialized",
            "First Position event for {} should be Initialized",
            scenario.symbol
        );
        assert_eq!(
            pos_events[0].aggregate_id, scenario.symbol,
            "Position aggregate_id should be the symbol"
        );

        // Must have at least one of each key event type
        let event_types: Vec<&str> = pos_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();

        assert!(
            event_types.contains(&"PositionEvent::OnChainOrderFilled"),
            "Missing PositionEvent::OnChainOrderFilled for {}",
            scenario.symbol
        );
        assert!(
            event_types.contains(&"PositionEvent::OffChainOrderPlaced"),
            "Missing PositionEvent::OffChainOrderPlaced for {}",
            scenario.symbol
        );
        assert!(
            event_types.contains(&"PositionEvent::OffChainOrderFilled"),
            "Missing PositionEvent::OffChainOrderFilled for {}",
            scenario.symbol
        );
    }
}

fn assert_offchain_order_events(scenarios: &[E2eScenario], events: &[StoredEvent]) {
    let offchain_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OffchainOrder")
        .collect();

    // At least 3 events per symbol (Placed, Submitted, Filled for at
    // least one offchain order aggregate per symbol)
    assert!(
        offchain_events.len() >= 3 * scenarios.len(),
        "Expected at least {} OffchainOrder events, got {}",
        3 * scenarios.len(),
        offchain_events.len()
    );

    let event_types: Vec<&str> = offchain_events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();

    assert!(
        event_types.contains(&"OffchainOrderEvent::Placed"),
        "Missing OffchainOrderEvent::Placed"
    );
    assert!(
        event_types.contains(&"OffchainOrderEvent::Submitted"),
        "Missing OffchainOrderEvent::Submitted"
    );
    assert!(
        event_types.contains(&"OffchainOrderEvent::Filled"),
        "Missing OffchainOrderEvent::Filled"
    );
}

async fn assert_position_view(scenario: &E2eScenario, pool: &SqlitePool) -> anyhow::Result<()> {
    let projection = Projection::<Position>::sqlite(pool.clone())?;
    let symbol = Symbol::new(scenario.symbol)?;

    let position = projection
        .load(&symbol)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Position view should exist for {}", scenario.symbol))?;

    let expected_net: Decimal = scenario.expected_net.parse()?;
    let expected_long: Decimal = scenario.expected_accumulated_long.parse()?;
    let expected_short: Decimal = scenario.expected_accumulated_short.parse()?;

    assert_eq!(position.symbol, symbol);
    assert_eq!(
        position.net,
        FractionalShares::new(expected_net),
        "net position mismatch for {}",
        scenario.symbol
    );
    assert_eq!(
        position.accumulated_long,
        FractionalShares::new(expected_long),
        "accumulated_long mismatch for {}",
        scenario.symbol
    );
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(expected_short),
        "accumulated_short mismatch for {}",
        scenario.symbol
    );

    assert!(
        position.pending_offchain_order_id.is_none(),
        "pending_offchain_order_id should be None after fill completes for {}",
        scenario.symbol
    );
    assert!(
        position.last_price_usdc.is_some(),
        "last_price_usdc should be set after onchain fill for {}",
        scenario.symbol
    );
    assert!(
        position.last_updated.is_some(),
        "last_updated should be set for {}",
        scenario.symbol
    );

    Ok(())
}

/// Asserts offchain order views per symbol: all orders are Filled with
/// correct direction, executor, and fill price. Total hedge shares per
/// symbol must equal `scenario.amount`.
async fn assert_offchain_order_views(
    scenarios: &[E2eScenario],
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let projection = Projection::<OffchainOrder>::sqlite(pool.clone())?;
    let all_orders = projection.load_all().await?;

    for scenario in scenarios {
        let expected_symbol = Symbol::new(scenario.symbol)?;
        let expected_amount: Decimal = scenario.amount.parse()?;
        let expected_price: Decimal = scenario.fill_price.parse()?;

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

            if *symbol != expected_symbol {
                continue;
            }

            found_any = true;
            total_shares += shares.inner().inner();

            assert_eq!(
                *direction,
                scenario.expected_hedge_direction(),
                "Direction mismatch for {} offchain order {order_id}",
                scenario.symbol
            );
            assert_eq!(
                *executor,
                SupportedExecutor::AlpacaBrokerApi,
                "Executor mismatch for {} offchain order {order_id}",
                scenario.symbol
            );
            assert_eq!(
                *price,
                Dollars(expected_price),
                "Fill price mismatch for {} offchain order {order_id}",
                scenario.symbol
            );
        }

        assert!(
            found_any,
            "Expected at least one offchain order for {}",
            scenario.symbol
        );
        assert_eq!(
            FractionalShares::new(total_shares),
            FractionalShares::new(expected_amount),
            "Total hedge shares for {} should be {}",
            scenario.symbol,
            scenario.amount
        );
    }

    Ok(())
}
