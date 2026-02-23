//! Shared helpers for e2e test assertions.
//!
//! Provides the `ExpectedPosition` struct, DB assertion utilities, and bot
//! lifecycle helpers used across `hedging.rs`, `rebalancing.rs`, and `main.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::signers::local::PrivateKeySigner;
use rain_math_float::Float;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use async_trait::async_trait;
use st0x_event_sorcery::Projection;
use st0x_evm::{Evm, EvmError, Wallet};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, SupportedExecutor, Symbol,
    TimeInForce,
};
use st0x_hedge::bindings::IOrderBookV5;
use st0x_hedge::config::{BrokerCtx, Ctx, LogLevel, OperationalLimits};
use st0x_hedge::{
    Dollars, EquityTokenAddresses, EvmCtx, ImbalanceThreshold, OffchainOrder, Position,
    TradingMode, UsdcRebalancing, launch,
};

use crate::assert_decimal_eq;
use crate::services::alpaca_broker::{
    self, AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot,
};
use crate::services::alpaca_tokenization::AlpacaTokenizationMock;
use crate::services::base_chain::{self, TakeDirection, TakeOrderResult};
use crate::services::cctp_attestation::CctpAttestationMock;
use alloy::providers::Identity;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};

// ── Test wallet ──────────────────────────────────────────────────────

/// Local signing wallet for e2e tests that exposes `Provider = RootProvider`.
///
/// Matches the `FireblocksWallet` pattern: keeps a bare `RootProvider`
/// for the `Evm::provider()` trait method (used by view calls and event
/// queries) and a filler-equipped signing provider internally for
/// `send()`.
struct TestWallet {
    address: Address,
    read_provider: RootProvider,
    signing_provider: FillProvider<
        JoinFill<
            JoinFill<
                Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        RootProvider,
        alloy::network::Ethereum,
    >,
    required_confirmations: u64,
}

impl TestWallet {
    fn new(private_key: &B256, rpc_url: url::Url, required_confirmations: u64) -> Self {
        use alloy::rpc::client::RpcClient;

        let signer = PrivateKeySigner::from_bytes(private_key).unwrap();
        let address = signer.address();
        let eth_wallet = EthereumWallet::from(signer);

        let read_provider = RootProvider::new(RpcClient::builder().http(rpc_url.clone()));
        let signing_provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect_http(rpc_url);

        Self {
            address,
            read_provider,
            signing_provider,
            required_confirmations,
        }
    }
}

#[async_trait]
impl Evm for TestWallet {
    type Provider = RootProvider;

    fn provider(&self) -> &RootProvider {
        &self.read_provider
    }
}

#[async_trait]
impl Wallet for TestWallet {
    fn address(&self) -> Address {
        self.address
    }

    async fn send(
        &self,
        contract: Address,
        calldata: alloy::primitives::Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        tracing::info!(%contract, note, "Submitting local test wallet call");

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.signing_provider.send_transaction(tx).await?;
        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        Ok(receipt)
    }
}

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
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Standalone {
            order_owner: chain.owner,
        },
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

    let wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(TestWallet::new(
        &chain.owner_key,
        chain.endpoint().parse()?,
        1,
    ));

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets(
        ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        },
        usdc_rebalancing,
        redemption_wallet.unwrap_or_else(Address::random),
        B256::random(),
        alpaca_auth,
        equities,
        wallet.clone(),
        wallet,
    );

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Rebalancing(Box::new(rebalancing_ctx)),
        execution_threshold,
    })
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
        () = tokio::time::sleep(Duration::from_secs(seconds)) => {
            // Bot is still running — expected
        }
    }
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

/// Counts CQRS events for a specific aggregate type AND aggregate ID.
///
/// More precise than `count_events` when multiple aggregates of the same
/// type exist in the database (e.g. multiple `OffchainOrder` aggregates).
pub async fn count_events_for_aggregate(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
) -> anyhow::Result<i64> {
    let row: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE aggregate_type = ? AND aggregate_id = ?")
            .bind(aggregate_type)
            .bind(aggregate_id)
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
        "Output vault should be fully consumed, got {}",
        float_to_decimal(output_balance).unwrap_or_default()
    );

    // Input vault should have received payment
    let input_balance = orderbook
        .vaultBalance2(owner, take_result.input_token, take_result.input_vault_id)
        .call()
        .await?;
    assert_ne!(input_balance, B256::ZERO, "Input vault should hold payment");

    Ok(())
}

fn float_to_decimal(float: B256) -> anyhow::Result<Decimal> {
    let float = Float::from_raw(float);
    let formatted = float.format_with_scientific(false)?;
    Ok(formatted.parse::<Decimal>()?)
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
        let diff = (total_shares - expected_amount).abs();
        assert!(
            diff < dec!(0.000_001),
            "Total hedge shares for {} should be ~{}, got {} (diff: {})",
            expected_position.symbol,
            expected_position.amount,
            total_shares,
            diff
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

/// Verifies inventory snapshot events have correct aggregate_id, expected
/// event types, and meaningful content (nonzero balances for tracked symbols).
///
/// `expected_symbols` lists the equity symbols that should appear in both
/// onchain and offchain equity snapshots. Pass `assert_cash` = true for USDC
/// rebalancing tests where OnchainCash/OffchainCash events are expected.
/// Pass `assert_nonzero_equity` = true to verify OnchainEquity balances are
/// nonzero (only valid when the discovered equity vault retains funds after
/// takes — e.g. BuyEquity fills the equity input vault, SellEquity depletes
/// the equity output vault).
/// Pass `assert_nonzero_cash` = true to also verify the OnchainCash USDC
/// balance is nonzero (only valid when the discovered USDC vault retains
/// funds after takes — e.g. BaseToAlpaca where SellEquity fills the vault).
async fn assert_inventory_snapshots(
    pool: &SqlitePool,
    orderbook_addr: Address,
    owner: Address,
    expected_symbols: &[&str],
    assert_cash: bool,
    assert_nonzero_equity: bool,
    assert_nonzero_cash: bool,
) -> anyhow::Result<()> {
    let events = fetch_events_by_type(pool, "InventorySnapshot").await?;
    assert!(
        !events.is_empty(),
        "Expected InventorySnapshot events, got 0"
    );

    // All events should share the same aggregate_id: "{orderbook}:{owner}"
    let expected_aggregate_id = format!(
        "{}:{}",
        orderbook_addr.to_checksum(None),
        owner.to_checksum(None),
    );
    for event in &events {
        assert_eq!(
            event.aggregate_id, expected_aggregate_id,
            "InventorySnapshot aggregate_id mismatch: expected {expected_aggregate_id}, \
             got {}",
            event.aggregate_id
        );
    }

    // Verify the expected event types were emitted
    let event_types: Vec<&str> = events.iter().map(|ev| ev.event_type.as_str()).collect();

    assert!(
        event_types.contains(&"InventorySnapshotEvent::OnchainEquity"),
        "Missing OnchainEquity event, got types: {event_types:?}"
    );
    assert!(
        event_types.contains(&"InventorySnapshotEvent::OffchainEquity"),
        "Missing OffchainEquity event, got types: {event_types:?}"
    );

    if assert_cash {
        assert!(
            event_types.contains(&"InventorySnapshotEvent::OnchainCash"),
            "Missing OnchainCash event, got types: {event_types:?}"
        );
        assert!(
            event_types.contains(&"InventorySnapshotEvent::OffchainCash"),
            "Missing OffchainCash event, got types: {event_types:?}"
        );
    }

    // Parse the latest OnchainEquity event and verify symbols with nonzero
    // balances. "Latest" = last in rowid order.
    let last_onchain_equity = events
        .iter()
        .rev()
        .find(|ev| ev.event_type == "InventorySnapshotEvent::OnchainEquity")
        .expect("Missing OnchainEquity event");
    let onchain_balances = last_onchain_equity
        .payload
        .get("OnchainEquity")
        .and_then(|val| val.get("balances"))
        .expect("OnchainEquity payload missing balances");

    for symbol in expected_symbols {
        let balance_str = onchain_balances
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("OnchainEquity missing symbol {symbol}, got: {onchain_balances}")
            });

        if assert_nonzero_equity {
            let balance: Decimal = balance_str.parse().unwrap_or_else(|err| {
                panic!("Failed to parse onchain balance for {symbol}: {err}")
            });
            assert_ne!(
                balance,
                Decimal::ZERO,
                "OnchainEquity balance for {symbol} should be nonzero, got {balance}"
            );
        }
    }

    // Parse the latest OffchainEquity event and verify symbols are present
    let last_offchain_equity = events
        .iter()
        .rev()
        .find(|ev| ev.event_type == "InventorySnapshotEvent::OffchainEquity")
        .expect("Missing OffchainEquity event");
    let offchain_positions = last_offchain_equity
        .payload
        .get("OffchainEquity")
        .and_then(|val| val.get("positions"))
        .expect("OffchainEquity payload missing positions");

    for symbol in expected_symbols {
        let position_str = offchain_positions
            .get(*symbol)
            .and_then(|val| val.as_str())
            .unwrap_or_else(|| {
                panic!("OffchainEquity missing symbol {symbol}, got: {offchain_positions}")
            });
        let position: Decimal = position_str
            .parse()
            .unwrap_or_else(|err| panic!("Failed to parse offchain position for {symbol}: {err}"));
        assert_ne!(
            position,
            Decimal::ZERO,
            "OffchainEquity position for {symbol} should be nonzero, got {position}"
        );
    }

    if assert_cash && assert_nonzero_cash {
        // Verify OnchainCash has a nonzero USDC balance.
        // Only meaningful when the discovered USDC vault retains funds
        // after takes (e.g. SellEquity fills the USDC input vault).
        // For BuyEquity the order's USDC output vault is depleted by takes
        // so the balance is legitimately zero.
        let last_onchain_cash = events
            .iter()
            .rev()
            .find(|ev| ev.event_type == "InventorySnapshotEvent::OnchainCash")
            .expect("Missing OnchainCash event");
        let usdc_balance_str = last_onchain_cash
            .payload
            .get("OnchainCash")
            .and_then(|val| val.get("usdc_balance"))
            .and_then(|val| val.as_str())
            .expect("OnchainCash payload missing usdc_balance");
        let usdc_balance: Decimal = usdc_balance_str
            .parse()
            .unwrap_or_else(|err| panic!("Failed to parse onchain USDC balance: {err}"));
        assert_ne!(
            usdc_balance,
            Decimal::ZERO,
            "OnchainCash USDC balance should be nonzero, got {usdc_balance}"
        );
    }

    Ok(())
}

pub enum EquityRebalanceType<'a> {
    Mint {
        symbol: &'a str,
        tokenization: &'a AlpacaTokenizationMock,
    },
    Redeem {
        symbol: &'a str,
        tokenization: &'a AlpacaTokenizationMock,
        /// Pre-fetched balance of the redemption wallet for the underlying
        /// token. Caller queries this onchain before calling.
        redemption_wallet_balance: U256,
    },
}

/// Comprehensive end-to-end assertions for the equity rebalancing pipeline.
///
/// This function centralizes assertions for both mint and redeem operations,
/// verifying the entire flow from hedging to final onchain and offchain state.
///
/// It checks:
/// 1.  **Hedging**: Correct broker orders and positions via `assert_broker_state`.
/// 2.  **CQRS**: `OnChainTrade`, `Position`, and `OffchainOrder` events and
///     views via `assert_cqrs_state`.
/// 3.  **Inventory**: At least one `InventorySnapshot` event was emitted.
/// 4.  **Rebalancing Events**: The correct sequence of mint or redeem events,
///     ensuring no duplicates or errors.
/// 5.  **Tokenization State**: Correct mint/redeem requests were made to the
///     mock tokenization service and completed.
/// 6.  **Onchain State**: For redemptions, verifies that the redemption wallet
///     received the underlying tokens.
pub async fn assert_equity_rebalancing_flow(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    rebalance_type: EquityRebalanceType<'_>,
) -> anyhow::Result<()> {
    let database_url = &db_path.display().to_string();

    // Layer 1: Hedging + Core CQRS
    assert_broker_state(expected_positions, broker);
    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    // Note: we don't assert onchain vault depletion here because rebalancing
    // specifically refills these vaults asynchronously.

    let pool = connect_db(db_path).await?;

    // Layer 2: Inventory snapshots — verify actual events and content
    let equity_symbol = match &rebalance_type {
        EquityRebalanceType::Mint { symbol, .. } | EquityRebalanceType::Redeem { symbol, .. } => {
            *symbol
        }
    };
    assert_inventory_snapshots(
        &pool,
        orderbook_addr,
        owner,
        &[equity_symbol],
        false,
        false,
        false,
    )
    .await?;

    match rebalance_type {
        EquityRebalanceType::Mint {
            symbol,
            tokenization,
        } => {
            // Layer 3: Mint pipeline (events)
            let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
            assert!(!mint_events.is_empty(), "Expected mint events, got 0");
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

            // Layer 3: Mint pipeline (tokenization state)
            let mint_requests: Vec<_> = tokenization
                .tokenization_requests()
                .into_iter()
                .filter(|req| req.request_type == "mint" && req.symbol == symbol)
                .collect();
            assert!(
                !mint_requests.is_empty(),
                "Expected mint requests for {symbol}"
            );

            let completed_mint = mint_requests
                .iter()
                .find(|req| req.status == "completed")
                .unwrap_or_else(|| {
                    panic!(
                        "Expected a completed mint request for {symbol}, got: {:?}",
                        mint_requests.iter().map(|r| &r.status).collect::<Vec<_>>()
                    )
                });
            let mint_qty: Decimal = completed_mint.qty.parse().unwrap_or_else(|err| {
                panic!("Failed to parse mint qty '{}': {err}", completed_mint.qty)
            });
            assert!(
                mint_qty > Decimal::ZERO,
                "Completed mint qty should be positive, got {mint_qty}"
            );
        }
        EquityRebalanceType::Redeem {
            symbol,
            tokenization,
            redemption_wallet_balance,
        } => {
            // Layer 3: Redemption pipeline (events)
            let redeem_events = fetch_events_by_type(&pool, "EquityRedemption").await?;
            assert!(!redeem_events.is_empty(), "Expected redeem events, got 0");
            assert_event_subsequence(
                &redeem_events,
                &[
                    "EquityRedemptionEvent::WithdrawnFromRaindex",
                    "EquityRedemptionEvent::TokensUnwrapped",
                    "EquityRedemptionEvent::TokensSent",
                    "EquityRedemptionEvent::Detected",
                    "EquityRedemptionEvent::Completed",
                ],
            );
            assert_single_clean_aggregate(&redeem_events, &["Failed", "Rejected"]);

            // Layer 3: Redemption pipeline (tokenization state)
            let redeem_requests: Vec<_> = tokenization
                .tokenization_requests()
                .into_iter()
                .filter(|req| req.request_type == "redeem" && req.symbol == symbol)
                .collect();
            assert!(
                !redeem_requests.is_empty(),
                "Expected redeem requests for {symbol}"
            );

            let completed_redeem = redeem_requests
                .iter()
                .find(|req| req.status == "completed")
                .unwrap_or_else(|| {
                    panic!(
                        "Expected a completed redeem request for {symbol}, got: {:?}",
                        redeem_requests
                            .iter()
                            .map(|r| &r.status)
                            .collect::<Vec<_>>()
                    )
                });
            let redeem_qty: Decimal = completed_redeem.qty.parse().unwrap_or_else(|err| {
                panic!(
                    "Failed to parse redeem qty '{}': {err}",
                    completed_redeem.qty
                )
            });
            assert!(
                redeem_qty > Decimal::ZERO,
                "Completed redeem qty should be positive, got {redeem_qty}"
            );

            // Layer 3: Redemption pipeline (onchain)
            assert!(
                redemption_wallet_balance > U256::ZERO,
                "Redemption wallet should hold tokens after redemption"
            );

            // Layer 3: Redemption pipeline (CQRS projection)
            let (payload_json,): (String,) =
                sqlx::query_as("SELECT payload FROM equity_redemption_view LIMIT 1")
                    .fetch_one(&pool)
                    .await?;
            let payload: Value = serde_json::from_str(&payload_json)?;
            let live = payload.get("Live").unwrap_or_else(|| {
                panic!("equity_redemption_view payload should be Live, got: {payload}")
            });
            let completed = live.get("Completed").unwrap_or_else(|| {
                panic!("equity_redemption_view should be in Completed state, got: {live}")
            });
            assert_eq!(
                completed.get("symbol").and_then(|val| val.as_str()),
                Some(symbol),
                "Completed redemption should be for {symbol}, got: {completed}"
            );
        }
    }

    pool.close().await;
    Ok(())
}

pub enum UsdcRebalanceType {
    AlpacaToBase,
    BaseToAlpaca,
}

/// Comprehensive end-to-end assertions for the USDC CCTP rebalancing pipeline.
///
/// This function centralizes assertions for both `AlpacaToBase` and `BaseToAlpaca`
/// flows, verifying the entire process from hedging to final onchain and
/// offchain state.
///
/// It checks:
/// 1.  **Hedging**: Correct broker orders and positions via `assert_broker_state`.
/// 2.  **CQRS**: `OnChainTrade`, `Position`, and `OffchainOrder` events and
///     views via `assert_cqrs_state`.
/// 3.  **Inventory**: At least one `InventorySnapshot` event was emitted.
/// 4.  **Rebalancing Events**: The correct sequence of USDC rebalancing events
///     for the specified direction.
/// 5.  **Broker State**:
///     - `USDCUSD` conversion order (buy for AlpacaToBase, sell for BaseToAlpaca).
///     - Wallet transfers (outgoing for AlpacaToBase, incoming for BaseToAlpaca).
/// 6.  **Onchain State**: The final balance of the Raindex USDC vault.
pub async fn assert_usdc_rebalancing_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    attestation: &CctpAttestationMock,
    db_path: &std::path::Path,
    usdc_vault_id: B256,
    rebalance_type: UsdcRebalanceType,
) -> anyhow::Result<()> {
    let database_url = &db_path.display().to_string();

    // Layer 1: Hedging + Core CQRS
    assert_broker_state(expected_positions, broker);
    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    // Note: we don't assert onchain vault depletion here because rebalancing
    // specifically refills/drains these vaults asynchronously.

    let (event_sequence, direction_str, expected_broker_side, expected_transfer_direction) =
        match rebalance_type {
            UsdcRebalanceType::AlpacaToBase => (
                vec![
                    "UsdcRebalanceEvent::ConversionInitiated",
                    "UsdcRebalanceEvent::ConversionConfirmed",
                    "UsdcRebalanceEvent::Initiated",
                    "UsdcRebalanceEvent::WithdrawalConfirmed",
                    "UsdcRebalanceEvent::BridgingInitiated",
                    "UsdcRebalanceEvent::BridgeAttestationReceived",
                    "UsdcRebalanceEvent::Bridged",
                    "UsdcRebalanceEvent::DepositInitiated",
                    "UsdcRebalanceEvent::DepositConfirmed",
                ],
                "AlpacaToBase",
                "buy",
                "OUTGOING",
            ),
            UsdcRebalanceType::BaseToAlpaca => (
                vec![
                    "UsdcRebalanceEvent::Initiated",
                    "UsdcRebalanceEvent::WithdrawalConfirmed",
                    "UsdcRebalanceEvent::BridgingInitiated",
                    "UsdcRebalanceEvent::BridgeAttestationReceived",
                    "UsdcRebalanceEvent::Bridged",
                    "UsdcRebalanceEvent::DepositInitiated",
                    "UsdcRebalanceEvent::DepositConfirmed",
                    "UsdcRebalanceEvent::ConversionInitiated",
                    "UsdcRebalanceEvent::ConversionConfirmed",
                ],
                "BaseToAlpaca",
                "sell",
                "INCOMING",
            ),
        };

    let pool = connect_db(db_path).await?;

    // Layer 2: Inventory snapshots — verify actual events and content
    // USDC tests trade AAPL, so equity snapshots should have AAPL.
    // Cash snapshots should be present since USDC vaults are tracked.
    //
    // For BaseToAlpaca (SellEquity), the discovered USDC vault receives
    // USDC from takes, so OnchainCash balance is non-zero.
    // For AlpacaToBase (BuyEquity), the discovered USDC output vault is
    // depleted by takes (taker receives the USDC), so balance is zero.
    // For BuyEquity (AlpacaToBase): the equity input vault receives tokens
    // from takes, so OnchainEquity is nonzero. The USDC output vault is
    // depleted by takes, so OnchainCash is zero.
    // For SellEquity (BaseToAlpaca): the equity output vault is depleted by
    // takes, so OnchainEquity is zero. The USDC input vault receives payment
    // from takes (plus pre-funded balance), so OnchainCash is nonzero.
    let is_base_to_alpaca = matches!(rebalance_type, UsdcRebalanceType::BaseToAlpaca);
    let assert_nonzero_equity = !is_base_to_alpaca;
    let assert_nonzero_cash = is_base_to_alpaca;
    let equity_symbols: Vec<&str> = expected_positions.iter().map(|pos| pos.symbol).collect();
    assert_inventory_snapshots(
        &pool,
        orderbook_addr,
        owner,
        &equity_symbols,
        true,
        assert_nonzero_equity,
        assert_nonzero_cash,
    )
    .await?;

    let usdc_events = fetch_events_by_type(&pool, "UsdcRebalance").await?;
    assert!(
        !usdc_events.is_empty(),
        "Expected at least 1 UsdcRebalance event (USDC trigger fired), got 0"
    );

    assert_event_subsequence(&usdc_events, &event_sequence);
    assert_single_clean_aggregate(&usdc_events, &["Failed"]);

    // Verify the Initiated event payload records the expected direction.
    let initiated_event = usdc_events
        .iter()
        .find(|event| event.event_type == "UsdcRebalanceEvent::Initiated")
        .expect("Missing Initiated event");
    let initiated_payload = initiated_event
        .payload
        .get("Initiated")
        .expect("Initiated event payload missing Initiated wrapper");
    assert_eq!(
        initiated_payload
            .get("direction")
            .and_then(|val| val.as_str()),
        Some(direction_str),
        "Initiated event should record {direction_str} direction, \
         got: {initiated_payload}"
    );

    // Verify the Bridged event records a nonzero amount received.
    let bridged_event = usdc_events
        .iter()
        .find(|event| event.event_type == "UsdcRebalanceEvent::Bridged")
        .expect("Missing Bridged event");
    let bridged_payload = bridged_event
        .payload
        .get("Bridged")
        .expect("Bridged event payload missing Bridged wrapper");
    let amount_received = bridged_payload
        .get("amount_received")
        .and_then(|val| val.as_str())
        .expect("Bridged event missing amount_received");
    assert_ne!(
        amount_received, "0",
        "Bridged amount_received should be nonzero"
    );

    pool.close().await;

    // Layer 3: Broker state
    let usdcusd_orders: Vec<_> = broker
        .orders()
        .into_iter()
        .filter(|order| order.symbol == "USDCUSD")
        .collect();
    assert!(!usdcusd_orders.is_empty(), "Expected USDCUSD order");
    let matched_order = usdcusd_orders
        .iter()
        .find(|o| o.side == expected_broker_side)
        .unwrap_or_else(|| {
            panic!(
                "Expected a {expected_broker_side} USDCUSD order, got sides: {:?}",
                usdcusd_orders.iter().map(|o| &o.side).collect::<Vec<_>>()
            )
        });
    let order_qty: Decimal = matched_order.qty.parse().unwrap_or_else(|err| {
        panic!(
            "Failed to parse USDCUSD order qty '{}': {err}",
            matched_order.qty
        )
    });
    assert!(
        order_qty > Decimal::ZERO,
        "USDCUSD order qty should be positive, got {order_qty}"
    );

    let transfers: Vec<_> = broker
        .wallet_transfers()
        .into_iter()
        .filter(|t| t.direction == expected_transfer_direction)
        .collect();
    assert!(
        !transfers.is_empty(),
        "Expected {} wallet transfer",
        expected_transfer_direction
    );
    for transfer in &transfers {
        assert_eq!(
            transfer.status, "COMPLETE",
            "{expected_transfer_direction} transfer {} should be COMPLETE, got {}",
            transfer.transfer_id, transfer.status
        );
        let amount: Decimal = transfer.amount.parse().unwrap_or_else(|err| {
            panic!(
                "Failed to parse transfer amount '{}': {err}",
                transfer.amount
            )
        });
        assert!(
            amount > Decimal::ZERO,
            "Transfer amount should be positive, got {amount}"
        );
    }

    // Layer 3: CCTP attestation mock — at least 1 attestation was processed
    let attestation_count = attestation.processed_attestation_count();
    assert!(
        attestation_count >= 1,
        "Expected at least 1 CCTP attestation processed, got {attestation_count}"
    );

    // Layer 3: Onchain vault balance
    let orderbook = IOrderBookV5::IOrderBookV5Instance::new(orderbook_addr, provider);
    let vault_balance = orderbook
        .vaultBalance2(owner, base_chain::USDC_BASE, usdc_vault_id)
        .call()
        .await?;
    assert_ne!(
        vault_balance,
        B256::ZERO,
        "USDC vault should have non-zero balance"
    );

    Ok(())
}

/// CCTP contract addresses for USDC rebalancing e2e tests.
pub struct CctpOverrides {
    pub attestation_base_url: String,
    pub token_messenger: Address,
    pub message_transmitter: Address,
}

/// Builds a `Ctx` with USDC rebalancing enabled and both chain endpoints.
///
/// Key differences from `build_rebalancing_ctx`:
/// - `RebalancingSecrets.ethereum_rpc_url` points to the Ethereum chain
/// - `UsdcRebalancing::Enabled` with aggressive thresholds for testing
/// - `circle_api_base` overridden to point at the attestation mock
/// - CCTP contract addresses overridden to local deployments
/// - `required_confirmations` set to 1 for Anvil single-node chains
/// - `usdc_vault_id` from parameter (must match a real Raindex vault)
pub fn build_usdc_rebalancing_ctx<BP>(
    base_chain: &base_chain::BaseChain<BP>,
    ethereum_endpoint: &str,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    usdc_vault_id: B256,
    cctp: CctpOverrides,
) -> anyhow::Result<Ctx>
where
    BP: Provider + Clone,
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
        .map(|(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityTokenAddresses {
                    wrapped: *wrapped,
                    unwrapped: *unwrapped,
                },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let base_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(
        TestWallet::new(&base_chain.owner_key, base_chain.endpoint().parse()?, 1),
    );

    let ethereum_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> = Arc::new(
        TestWallet::new(&base_chain.owner_key, ethereum_endpoint.parse()?, 1),
    );

    // Equity threshold uses an extremely wide deviation so it never triggers.
    // These tests focus on USDC rebalancing; equity rebalancing would
    // start an equity redemption flow that blocks the USDC pipeline.
    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets(
        ImbalanceThreshold {
            target: Decimal::new(5, 1),
            deviation: Decimal::from(100),
        },
        UsdcRebalancing::Enabled {
            target: Decimal::new(5, 1),
            deviation: Decimal::new(1, 1),
        },
        Address::random(),
        usdc_vault_id,
        alpaca_auth,
        equities,
        base_wallet,
        ethereum_wallet,
    )
    .with_circle_api_base(cctp.attestation_base_url)
    .with_cctp_addresses(cctp.token_messenger, cctp.message_transmitter);

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: base_chain.ws_endpoint()?,
            orderbook: base_chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 15,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Rebalancing(Box::new(rebalancing_ctx)),
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
