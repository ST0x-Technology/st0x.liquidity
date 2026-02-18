mod services;

use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::providers::Provider;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::SqlitePool;

use st0x_event_sorcery::Projection;
use st0x_execution::{
    AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, FractionalShares, SupportedExecutor,
    Symbol, TimeInForce,
};
use st0x_hedge::bindings::IOrderBookV5;
use st0x_hedge::config::{BrokerCtx, Ctx, EvmCtx, LogLevel};
use st0x_hedge::{Dollars, OffchainOrder, Position, launch};

use services::alpaca_broker::{self, AlpacaBrokerMock, MockOrderSnapshot, MockPositionSnapshot};
use services::alpaca_tokenization::AlpacaTokenizationMock;
use services::base_chain::{self, TakeDirection, TakeOrderResult};
use services::cctp_attestation::CctpAttestationMock;

/// Test scenario configuration for parameterized e2e assertions.
struct E2eScenario {
    symbol: &'static str,
    amount: &'static str,
    direction: TakeDirection,
    fill_price: &'static str,
    expected_accumulated_long: &'static str,
    expected_accumulated_short: &'static str,
    expected_net: &'static str,
}

impl E2eScenario {
    /// The hedge direction is the inverse of the onchain direction.
    fn expected_hedge_direction(&self) -> Direction {
        match self.direction {
            TakeDirection::SellEquity => Direction::Buy,
            TakeDirection::BuyEquity => Direction::Sell,
        }
    }

    fn expected_broker_side(&self) -> &'static str {
        match self.direction {
            TakeDirection::SellEquity => "buy",
            TakeDirection::BuyEquity => "sell",
        }
    }
}

#[tokio::test]
async fn smoke_test_base_fork() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start().await?;

    let orderbook_code = chain.provider.get_code_at(chain.orderbook_addr).await?;
    assert!(
        !orderbook_code.is_empty(),
        "OrderBook should have bytecode at {}",
        chain.orderbook_addr
    );

    let equity_addr = chain.deploy_equity_token("AAPL").await?;
    let equity_code = chain.provider.get_code_at(equity_addr).await?;
    assert!(
        !equity_code.is_empty(),
        "Equity token should have bytecode at {equity_addr}"
    );

    let usdc = base_chain::IERC20::new(base_chain::USDC_BASE, &chain.provider);
    let balance = usdc.balanceOf(chain.owner).call().await?;
    assert!(
        balance > U256::ZERO,
        "Owner should have USDC balance after minting"
    );

    Ok(())
}

#[tokio::test]
async fn smoke_test_alpaca_broker_mock() -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let broker = AlpacaBrokerMock::start().build().await;

    let account_url = format!(
        "{}/v1/trading/accounts/{}/account",
        broker.base_url(),
        alpaca_broker::TEST_ACCOUNT_ID
    );
    let account_resp: serde_json::Value = http.get(&account_url).send().await?.json().await?;
    assert_eq!(account_resp["status"], "ACTIVE");

    Ok(())
}

#[tokio::test]
async fn smoke_test_cctp_attestation_mock() -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let cctp = CctpAttestationMock::start().await;

    let fee_url = format!("{}/v2/burn/USDC/fees/0/6", cctp.base_url());
    let fee_resp: serde_json::Value = http.get(&fee_url).send().await?.json().await?;
    let fee_array = fee_resp
        .as_array()
        .expect("fee response should be an array");
    assert!(!fee_array.is_empty(), "fee array should not be empty");

    Ok(())
}

#[tokio::test]
async fn smoke_test_alpaca_tokenization_mock() -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let tokenization = AlpacaTokenizationMock::start().await;

    let requests_url = format!(
        "{}/v1/accounts/{}/tokenization/requests",
        tokenization.base_url(),
        alpaca_broker::TEST_ACCOUNT_ID
    );
    let requests_resp: serde_json::Value = http.get(&requests_url).send().await?.json().await?;
    let requests_array = requests_resp
        .as_array()
        .expect("requests response should be an array");
    assert!(
        requests_array.is_empty(),
        "default request list should be empty"
    );

    Ok(())
}

#[tokio::test]
async fn e2e_hedging_via_launch() -> anyhow::Result<()> {
    let scenario = E2eScenario {
        symbol: "AAPL",
        amount: "1.0",
        direction: TakeDirection::SellEquity,
        fill_price: "150.25",
        // Owner sold 1.0 onchain -> accumulated_short = 1.0
        // Bot hedged by buying 1.0 offchain -> net back to 0.0
        expected_accumulated_long: "0",
        expected_accumulated_short: "1.0",
        expected_net: "0",
    };

    // 1. Deploy Anvil fork + Rain stack + equity token
    let mut chain = base_chain::BaseChain::start().await?;
    let _equity_addr = chain.deploy_equity_token(scenario.symbol).await?;

    // 2. Start mock broker (autonomous mode — no per-request setup needed)
    //    Owner sells equity onchain -> bot hedges by BUYING on broker
    let broker = AlpacaBrokerMock::start()
        .with_fill_price(scenario.fill_price)
        .build()
        .await;

    // 3. Use current block as deployment_block so backfill skips history
    let current_block = chain.provider.get_block_number().await?;

    // 4. Create a shared temp DB file so the test can query the bot's state
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();

    // 5. Construct Ctx pointing to Anvil WS + mock broker + shared DB
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: alpaca_broker::TEST_ACCOUNT_ID.to_owned(),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    });
    let execution_threshold = broker_ctx.execution_threshold()?;

    let ctx = Ctx {
        database_url: database_url.clone(),
        log_level: LogLevel::Debug,
        server_port: 0,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            order_owner: Some(chain.owner),
            deployment_block: current_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: None,
        execution_threshold,
    };

    // 6. Spawn the full bot as background task
    let bot = tokio::spawn(launch(ctx));

    // 7. Wait for bot to connect and subscribe to events
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 8. Create a take-order on the OrderBook (emits TakeOrderV3)
    //    Owner's order sells tAAPL for USDC, taker buys tAAPL.
    //    Bot sees owner's tAAPL was sold -> hedges by buying AAPL on broker.
    let take_result = chain
        .take_order(scenario.symbol, scenario.amount, scenario.direction)
        .await?;

    // 9. Wait for bot to process event + place order + poll fill
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Check if the bot task panicked during processing
    if bot.is_finished() {
        let result = bot.await;
        panic!("Bot task finished early: {result:?}");
    }

    // 10. Assert the full pipeline
    assert_broker_state(&scenario, &broker);
    assert_onchain_vaults(
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &take_result,
    )
    .await?;
    assert_cqrs_state(&scenario, &database_url).await?;

    bot.abort();
    Ok(())
}

fn assert_broker_state(scenario: &E2eScenario, broker: &AlpacaBrokerMock) {
    let orders = broker.orders();
    assert_eq!(orders.len(), 1, "Expected exactly one order placed");
    assert_broker_order(scenario, &orders[0]);

    let positions = broker.positions();
    assert_eq!(positions.len(), 1, "Expected one position after fill");
    assert_broker_position(scenario, &positions[0]);
}

fn assert_broker_order(scenario: &E2eScenario, order: &MockOrderSnapshot) {
    assert_eq!(order.symbol, scenario.symbol);
    assert_eq!(order.side, scenario.expected_broker_side());
    assert_eq!(order.status, "filled");
    assert!(
        order.poll_count >= 1,
        "Order should have been polled at least once, got {}",
        order.poll_count
    );
    assert_eq!(
        order.filled_price.as_deref(),
        Some(scenario.fill_price),
        "Fill price should match configured default"
    );
}

fn assert_broker_position(scenario: &E2eScenario, position: &MockPositionSnapshot) {
    assert_eq!(position.symbol, scenario.symbol);
}

async fn assert_onchain_vaults<P: Provider>(
    provider: &P,
    orderbook_addr: alloy::primitives::Address,
    owner: alloy::primitives::Address,
    take_result: &TakeOrderResult,
) -> anyhow::Result<()> {
    let orderbook = IOrderBookV5::IOrderBookV5Instance::new(orderbook_addr, provider);

    // Output vault (tAAPL) should be depleted after the taker consumed it
    let output_balance = orderbook
        .vaultBalance2(owner, take_result.output_token, take_result.output_vault_id)
        .call()
        .await?;
    assert_eq!(
        output_balance,
        B256::ZERO,
        "Output vault should be fully consumed"
    );

    // Input vault (USDC) should have received payment
    let input_balance = orderbook
        .vaultBalance2(owner, take_result.input_token, take_result.input_vault_id)
        .call()
        .await?;
    assert_ne!(input_balance, B256::ZERO, "Input vault should hold payment");

    Ok(())
}

async fn assert_cqrs_state(scenario: &E2eScenario, database_url: &str) -> anyhow::Result<()> {
    let pool = SqlitePool::connect(&format!("sqlite:{database_url}")).await?;

    let events: Vec<StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type != 'SchemaRegistry' \
         ORDER BY rowid ASC",
    )
    .fetch_all(&pool)
    .await?;

    assert!(!events.is_empty(), "Expected CQRS events to be persisted");

    assert_onchain_trade_events(scenario, &events);
    assert_position_events(scenario, &events);
    assert_offchain_order_events(&events);
    assert_position_view(scenario, &pool).await?;
    assert_offchain_order_view(scenario, &pool).await?;

    pool.close().await;
    Ok(())
}

fn assert_onchain_trade_events(scenario: &E2eScenario, events: &[StoredEvent]) {
    let trades: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OnChainTrade")
        .collect();

    assert_eq!(trades.len(), 1, "Expected exactly one OnChainTrade event");
    assert_eq!(trades[0].event_type, "OnChainTradeEvent::Filled");

    let symbol = trades[0].payload["Filled"]["symbol"].as_str().unwrap_or("");
    assert_eq!(
        symbol, scenario.symbol,
        "OnChainTrade should be for {}",
        scenario.symbol
    );
}

fn assert_position_events(scenario: &E2eScenario, events: &[StoredEvent]) {
    let position_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "Position")
        .collect();

    assert!(
        position_events.len() >= 3,
        "Expected at least 3 Position events (Initialized, OnChainOrderFilled, \
         OffChainOrderPlaced), got {}",
        position_events.len()
    );

    assert_eq!(position_events[0].event_type, "PositionEvent::Initialized");
    assert_eq!(
        position_events[0].aggregate_id, scenario.symbol,
        "Position aggregate_id should be the symbol"
    );
    assert_eq!(
        position_events[1].event_type,
        "PositionEvent::OnChainOrderFilled"
    );
    assert_eq!(
        position_events[2].event_type,
        "PositionEvent::OffChainOrderPlaced"
    );

    let has_offchain_filled = position_events
        .iter()
        .any(|event| event.event_type == "PositionEvent::OffChainOrderFilled");
    assert!(
        has_offchain_filled,
        "Expected PositionEvent::OffChainOrderFilled after broker fill"
    );
}

fn assert_offchain_order_events(events: &[StoredEvent]) {
    let offchain_events: Vec<&StoredEvent> = events
        .iter()
        .filter(|event| event.aggregate_type == "OffchainOrder")
        .collect();

    assert!(
        offchain_events.len() >= 3,
        "Expected at least 3 OffchainOrder events (Placed, Submitted, Filled), got {}",
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
        "net position mismatch"
    );
    assert_eq!(
        position.accumulated_long,
        FractionalShares::new(expected_long),
        "accumulated_long mismatch"
    );
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(expected_short),
        "accumulated_short mismatch"
    );

    assert!(
        position.pending_offchain_order_id.is_none(),
        "pending_offchain_order_id should be None after fill completes"
    );
    assert!(
        position.last_price_usdc.is_some(),
        "last_price_usdc should be set after onchain fill"
    );
    assert!(
        position.last_updated.is_some(),
        "last_updated should be set"
    );

    Ok(())
}

async fn assert_offchain_order_view(
    scenario: &E2eScenario,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let projection = Projection::<OffchainOrder>::sqlite(pool.clone())?;
    let orders = projection.load_all().await?;

    assert_eq!(orders.len(), 1, "Expected exactly one offchain order");

    let (_, order) = &orders[0];

    let OffchainOrder::Filled {
        symbol,
        shares,
        direction,
        executor,
        price,
        ..
    } = order
    else {
        panic!("Expected Filled variant, got {order:?}");
    };

    let expected_symbol = Symbol::new(scenario.symbol)?;
    let amount: Decimal = scenario.amount.parse()?;
    let fill_price: Decimal = scenario.fill_price.parse()?;

    assert_eq!(*symbol, expected_symbol);
    assert_eq!(*direction, scenario.expected_hedge_direction());
    assert_eq!(*executor, SupportedExecutor::AlpacaBrokerApi);
    assert_eq!(
        shares.inner(),
        FractionalShares::new(amount),
        "Hedge shares should match onchain amount"
    );
    assert_eq!(
        *price,
        Dollars(fill_price),
        "Fill price should match mock broker price"
    );

    Ok(())
}

/// Minimal event record for querying the CQRS events table.
#[derive(Debug, sqlx::FromRow)]
struct StoredEvent {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Value,
}
