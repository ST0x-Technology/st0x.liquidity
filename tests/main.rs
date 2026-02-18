mod services;

use std::time::Duration;

use alloy::primitives::U256;
use alloy::providers::Provider;

use st0x_execution::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
use st0x_hedge::config::{BrokerCtx, Ctx, EvmCtx, LogLevel};
use st0x_hedge::launch;

use services::alpaca_broker::{self, AlpacaBrokerMock};
use services::alpaca_tokenization::AlpacaTokenizationMock;
use services::base_chain::{self, TakeDirection};
use services::cctp_attestation::CctpAttestationMock;

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
    let broker = AlpacaBrokerMock::start().await;

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
    // 1. Deploy Anvil fork + Rain stack + tAAPL token
    let mut chain = base_chain::BaseChain::start().await?;
    let _equity_addr = chain.deploy_equity_token("AAPL").await?;

    // 2. Start mock broker with order placement + fill endpoints
    //    Owner sells equity onchain -> bot hedges by BUYING on broker
    let broker = AlpacaBrokerMock::start().await;
    broker.mock_place_equity_order("e2e-order-1", "AAPL", "buy");
    broker.mock_order_filled("e2e-order-1", "AAPL", "150.25");

    // 3. Construct Ctx pointing to Anvil WS + mock broker
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
        database_url: ":memory:".to_owned(),
        log_level: LogLevel::Debug,
        server_port: 0,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            order_owner: Some(chain.owner),
            deployment_block: 1,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        broker: broker_ctx,
        telemetry: None,
        rebalancing: None,
        execution_threshold,
    };

    // 4. Spawn the full bot as background task
    let bot = tokio::spawn(launch(ctx));

    // 5. Wait for bot to connect and subscribe to events
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 6. Create a take-order on the OrderBook (emits TakeOrderV3)
    //    Owner's order sells tAAPL for USDC, taker buys tAAPL.
    //    Bot sees owner's tAAPL was sold -> hedges by buying AAPL on broker.
    let _tx_hash = chain
        .take_order("AAPL", "1.0", TakeDirection::SellEquity)
        .await?;

    // 7. Wait for bot to process event + place order + poll fill
    //    order_polling_interval=1s, so within a few seconds the full cycle
    //    should complete.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // 8. Assert the bot placed an order and polled until filled
    broker.assert_order_placed("AAPL", "buy");
    broker.assert_order_filled("e2e-order-1");

    // 9. Cleanup
    bot.abort();
    Ok(())
}
