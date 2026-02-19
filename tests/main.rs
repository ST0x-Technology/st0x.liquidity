mod services;

use alloy::primitives::U256;
use alloy::providers::Provider;
use serde_json;

use services::alpaca_broker::{self, AlpacaBrokerMock};
use services::alpaca_tokenization::AlpacaTokenizationMock;
use services::base_chain;
use services::cctp_attestation::CctpAttestationMock;

#[tokio::test]
async fn smoke_test_base_fork() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start("https://mainnet.base.org").await?;

    let orderbook_code = chain.provider.get_code_at(chain.orderbook_addr).await?;
    assert!(
        !orderbook_code.is_empty(),
        "OrderBook should have bytecode at {}",
        chain.orderbook_addr
    );

    let (equity_addr, _) = chain.deploy_equity_vault("AAPL").await?;
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
