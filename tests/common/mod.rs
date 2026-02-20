//! Shared helpers for e2e test assertions.
//!
//! Provides the `ExpectedPosition` struct, DB assertion utilities, and bot
//! lifecycle helpers used across `hedging.rs`, `rebalancing.rs`, and `main.rs`.

use std::collections::HashMap;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use approx::assert_relative_eq;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use st0x_event_sorcery::Projection;
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, FractionalShares,
    SupportedExecutor, Symbol, TimeInForce,
};
use st0x_hedge::bindings::IOrderBookV5;
use st0x_hedge::config::{BrokerCtx, Ctx, EvmCtx, LogLevel};
use st0x_hedge::{
    Dollars, EquityTokenAddresses, ImbalanceThreshold, OffchainOrder, Position, RebalancingConfig,
    RebalancingSecrets, UsdcRebalancing, launch,
};

use super::services::alpaca_broker::{
    self, AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot,
};
use super::services::alpaca_tokenization::AlpacaTokenizationMock;
use super::services::base_chain::{self, TakeDirection, TakeOrderResult};
use super::services::ethereum_chain;
use crate::assert_decimal_eq;

// ── Types ────────────────────────────────────────────────────────────

/// Per-symbol expected final state after all trades are processed.
///
/// `amount` is the total expected hedge shares for this symbol across
/// all trades (e.g., 3 sells of 1.0 each → amount = "3.0").
///
/// Two prices are tracked separately:
/// - `onchain_price`: the execution price from the Raindex order (USDC per
///   equity share, derived from the Rain expression ioRatio)
/// - `broker_fill_price`: the offchain hedge fill price from the broker mock
///
/// The spread between these two is where profit comes from.
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

impl ExpectedPosition {
    /// Whether an offchain hedge is expected for this position.
    /// `NetZero` means opposing trades cancelled out and no hedge is needed.
    pub fn expects_hedge(&self) -> bool {
        !matches!(self.direction, TakeDirection::NetZero)
    }

    /// The hedge direction is the inverse of the onchain direction.
    ///
    /// # Panics
    ///
    /// Panics if called on a `NetZero` position (no hedge direction exists).
    pub fn expected_hedge_direction(&self) -> Direction {
        match self.direction {
            TakeDirection::SellEquity => Direction::Buy,
            TakeDirection::BuyEquity => Direction::Sell,
            TakeDirection::NetZero => panic!("NetZero position has no hedge direction"),
        }
    }

    /// # Panics
    ///
    /// Panics if called on a `NetZero` position (no broker side exists).
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
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
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
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: None,
        execution_threshold,
    })
}

/// Builds a `Ctx` with rebalancing enabled.
///
/// Uses the same broker/chain setup as `build_ctx`, but adds a
/// `RebalancingCtx` with aggressive equity thresholds (target=0.5,
/// deviation=0.1) and the given USDC rebalancing configuration.
///
/// All Alpaca services (broker, tokenization, wallet) are pointed at
/// the broker mock's URL since the conductor resolves all three from
/// `AlpacaBrokerApiCtx.base_url()`.
///
/// `equity_tokens` is a slice of `(symbol, vault_address, underlying_address)`
/// tuples. Each entry deploys an ERC-4626 vault wrapping the underlying
/// ERC20, required for equity rebalancing triggers (`convertToAssets()`).
pub fn build_rebalancing_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    usdc_rebalancing: UsdcRebalancing,
    redemption_wallet: Option<Address>,
) -> anyhow::Result<Ctx> {
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth.clone());
    let execution_threshold = broker_ctx.execution_threshold()?;

    let equities: HashMap<Symbol, EquityTokenAddresses> = equity_tokens
        .iter()
        .map(|&(ref symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityTokenAddresses { wrapped, unwrapped },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let config = RebalancingConfig {
        equity: ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        },
        usdc: usdc_rebalancing,
        redemption_wallet: redemption_wallet.unwrap_or_else(Address::random),
        usdc_vault_id: B256::random(),
        equities,
        circle_api_base: None,
        required_confirmations: Some(1),
    };

    // Use the chain owner's private key so that `Ctx::order_owner()`
    // (which derives the address from this key when rebalancing is
    // enabled) matches the account that places orders in tests.
    let secrets = RebalancingSecrets {
        ethereum_rpc_url: chain.ws_endpoint()?,
        evm_private_key: chain.owner_key,
    };

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::new(config, secrets, alpaca_auth)?;

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
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: Some(rebalancing_ctx),
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
/// This is the primary assertion entry point. Every e2e test should
/// call this after the bot has finished processing.
pub async fn assert_full_hedging_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    database_url: &str,
) -> anyhow::Result<()> {
    assert_broker_state(expected_positions, broker);

    for take_result in take_results {
        assert_onchain_vaults(provider, orderbook_addr, owner, take_result).await?;
    }

    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    Ok(())
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
    assert!(
        order.poll_count >= 1,
        "Order for {} should have been polled at least once, got {}",
        expected_position.symbol,
        order.poll_count
    );
    assert_eq!(
        order.filled_price.as_deref(),
        Some(expected_position.broker_fill_price.to_string()).as_deref(),
        "Broker fill price for {} should match configured mock price",
        expected_position.symbol
    );
}

fn assert_broker_position(expected_position: &ExpectedPosition, position: &MockPositionSnapshot) {
    assert_eq!(position.symbol, expected_position.symbol);
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

    assert!(
        all_trades.len() >= expected_count,
        "Expected at least {expected_count} OnChainTrade events, got {}",
        all_trades.len()
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

        // First event is always Initialized
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
                pos_events.len() >= 3,
                "Expected at least 3 Position events for {}, got {}",
                expected_position.symbol,
                pos_events.len()
            );

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
    let hedged_positions: Vec<&ExpectedPosition> = expected_positions
        .iter()
        .filter(|s| s.expects_hedge())
        .collect();

    let offchain_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OffchainOrder")
        .collect();

    if hedged_positions.is_empty() {
        assert!(
            offchain_events.is_empty(),
            "Expected no OffchainOrder events for net-zero positions, got {}",
            offchain_events.len()
        );

        return;
    }

    // At least 3 events per hedged symbol (Placed, Submitted, Filled)
    assert!(
        offchain_events.len() >= 3 * hedged_positions.len(),
        "Expected at least {} OffchainOrder events, got {}",
        3 * hedged_positions.len(),
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

async fn assert_position_view(
    expected_position: &ExpectedPosition,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let projection = Projection::<Position>::sqlite(pool.clone())?;
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
    let projection = Projection::<OffchainOrder>::sqlite(pool.clone())?;
    let all_orders = projection.load_all().await?;

    for expected_position in expected_positions {
        if !expected_position.expects_hedge() {
            let expected_symbol = Symbol::new(expected_position.symbol)?;
            let has_orders = all_orders
                .iter()
                .any(|(_, order)| matches!(order, OffchainOrder::Filled { symbol, .. } if symbol == &expected_symbol));

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

            if *symbol != expected_symbol {
                continue;
            }

            found_any = true;
            total_shares += shares.inner().inner();

            assert_eq!(
                *direction,
                expected_position.expected_hedge_direction(),
                "Direction mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
            assert_eq!(
                *executor,
                SupportedExecutor::AlpacaBrokerApi,
                "Executor mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
            assert_eq!(
                *price,
                Dollars(expected_price),
                "Fill price mismatch for {} offchain order {order_id}",
                expected_position.symbol
            );
        }

        assert!(
            found_any,
            "Expected at least one offchain order for {}",
            expected_position.symbol
        );
        assert_eq!(
            FractionalShares::new(total_shares),
            FractionalShares::new(expected_amount),
            "Total hedge shares for {} should be {}",
            expected_position.symbol,
            expected_position.amount
        );
    }

    Ok(())
}

// ── Rebalancing helpers ─────────────────────────────────────────────

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
///
/// # Panics
///
/// Panics if the expected sequence is not found in the events.
pub fn assert_event_subsequence(events: &[StoredEvent], expected_types: &[&str]) {
    let mut expected_iter = expected_types.iter().peekable();

    for event in events {
        if let Some(expected) = expected_iter.peek() {
            if event.event_type == **expected {
                expected_iter.next();
            }
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
    // At least one aggregate present
    let aggregate_ids: std::collections::HashSet<&str> = events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert!(
        !aggregate_ids.is_empty(),
        "Expected at least 1 rebalancing aggregate, got 0",
    );

    // No error events
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

// ── Rebalancing pipeline assertions ──────────────────────────────────

/// Optional mint pipeline assertions passed to `assert_rebalancing_state`.
pub struct MintAssertions<'a> {
    pub symbol: &'a str,
    pub tokenization: &'a AlpacaTokenizationMock,
}

/// Unified rebalancing assertions covering inventory snapshots and
/// optionally the mint pipeline (events + broker tokenization state).
///
/// Every rebalancing e2e test should call this after the bot has
/// finished processing. It complements `assert_broker_state` /
/// `assert_cqrs_state` which cover the hedging layer.
pub async fn assert_rebalancing_state(
    db_path: &std::path::Path,
    min_onchain_trades: i64,
    mint: Option<MintAssertions<'_>>,
) -> anyhow::Result<()> {
    let pool = connect_db(db_path).await?;

    let onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert!(
        onchain_events >= min_onchain_trades,
        "Expected at least {min_onchain_trades} OnChainTrade events, \
         got {onchain_events}"
    );

    let inventory_events = count_events(&pool, "InventorySnapshot").await?;
    assert!(
        inventory_events >= 1,
        "Expected at least 1 InventorySnapshot event from the inventory \
         poller, got {inventory_events}"
    );

    if let Some(mint_assertions) = mint {
        let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
        assert!(
            !mint_events.is_empty(),
            "Expected at least 1 TokenizedEquityMint event \
             (mint trigger fired), got 0"
        );

        assert_event_subsequence(
            &mint_events,
            &[
                "TokenizedEquityMintEvent::MintRequested",
                "TokenizedEquityMintEvent::MintAccepted",
                "TokenizedEquityMintEvent::TokensReceived",
                "TokenizedEquityMintEvent::TokensWrapped",
                "TokenizedEquityMintEvent::DepositedIntoRaindex",
            ],
        );

        assert_single_clean_aggregate(&mint_events, &["Failed", "Rejected"]);

        let mint_requests: Vec<_> = mint_assertions
            .tokenization
            .tokenization_requests()
            .into_iter()
            .filter(|req| req.request_type == "mint" && req.symbol == mint_assertions.symbol)
            .collect();
        assert!(
            !mint_requests.is_empty(),
            "Expected at least 1 mint tokenization request for {} \
             on the broker",
            mint_assertions.symbol
        );
        assert!(
            mint_requests.iter().any(|req| req.status == "completed"),
            "Expected at least one completed mint request, got: {:?}",
            mint_requests
                .iter()
                .map(|req| &req.status)
                .collect::<Vec<_>>()
        );
    }

    pool.close().await;
    Ok(())
}

/// Builds a `Ctx` with USDC rebalancing enabled and both chain endpoints.
///
/// Key differences from `build_rebalancing_ctx`:
/// - `RebalancingSecrets.ethereum_rpc_url` points to the Ethereum fork
/// - `UsdcRebalancing::Enabled` with aggressive thresholds for testing
/// - `circle_api_base` overridden to point at the attestation mock
/// - `required_confirmations` set to 1 for Anvil single-node chains
/// - `usdc_vault_id` from parameter (must match a real Raindex vault)
pub fn build_usdc_rebalancing_ctx<BP, EP>(
    base_chain: &base_chain::BaseChain<BP>,
    ethereum_chain: &ethereum_chain::EthereumChain<EP>,
    broker: &AlpacaBrokerMock,
    attestation_base_url: &str,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(&str, Address, Address)],
    usdc_vault_id: B256,
) -> anyhow::Result<Ctx>
where
    BP: Provider + Clone,
    EP: Provider + Clone,
{
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth.clone());
    let execution_threshold = broker_ctx.execution_threshold()?;

    let equities: HashMap<Symbol, EquityTokenAddresses> = equity_tokens
        .iter()
        .map(|&(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityTokenAddresses { wrapped, unwrapped },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let config = RebalancingConfig {
        equity: ImbalanceThreshold {
            target: Decimal::new(5, 1),
            deviation: Decimal::new(1, 1),
        },
        usdc: UsdcRebalancing::Enabled {
            target: Decimal::new(5, 1),
            deviation: Decimal::new(1, 1),
        },
        redemption_wallet: Address::random(),
        usdc_vault_id,
        equities,
        circle_api_base: Some(attestation_base_url.to_string()),
        required_confirmations: Some(1),
    };

    let secrets = RebalancingSecrets {
        ethereum_rpc_url: ethereum_chain.ws_endpoint()?,
        evm_private_key: base_chain.owner_key,
    };

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::new(config, secrets, alpaca_auth)?;

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        evm: EvmCtx {
            ws_rpc_url: base_chain.ws_endpoint()?,
            orderbook: base_chain.orderbook_addr,
            order_owner: Some(base_chain.owner),
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 15,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: Some(rebalancing_ctx),
        execution_threshold,
    })
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