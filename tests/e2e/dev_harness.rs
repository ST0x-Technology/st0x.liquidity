//! Long-running dev harness for iterating on the dashboard.
//!
//! Starts the bot with a mock executor, generates periodic trade events,
//! and keeps the server alive so you can connect a dashboard client.
//!
//! Run via bacon: `bacon dev-harness`
//! Or directly: `cargo test --test e2e dev_harness -- --ignored --nocapture`

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, U256, utils::parse_units};
use alloy::providers::{Provider, RootProvider};
use st0x_float_macro::float;
use tokio::sync::broadcast;

use st0x_dto::Statement;
use st0x_evm::Wallet;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, TEST_API_KEY, TEST_API_SECRET};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Symbol, TimeInForce,
};
use st0x_hedge::config::{BrokerCtx, Ctx};
use st0x_hedge::mock_api::REDEMPTION_WALLET;
use st0x_hedge::{
    AssetsConfig, EquitiesConfig, EquityAssetConfig, ImbalanceThreshold, OperationMode,
    RebalancingCtx, TradingMode, UsdcRebalancing,
};

use tracing::{info, warn};

use crate::base_chain::TakeDirection;
use crate::poll::{poll_for_ready, spawn_bot_with_event_channel};
use crate::rebalancing::assertions::TestWallet;
use crate::test_infra::TestInfra;

fn build_dev_ctx<P: Provider + Clone>(
    chain: &crate::base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
) -> anyhow::Result<Ctx> {
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: TEST_API_KEY.to_owned(),
        api_secret: TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };

    let equities: HashMap<Symbol, EquityAssetConfig> = equity_tokens
        .iter()
        .map(|(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityAssetConfig {
                    tokenized_equity: *unwrapped,
                    tokenized_equity_derivative: *wrapped,
                    vault_id: None,
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    operational_limit: None,
                },
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    let base_wallet: Arc<dyn Wallet<Provider = RootProvider>> = Arc::new(TestWallet::new(
        &chain.owner_key,
        chain.endpoint().parse()?,
        1,
    )?);

    let rebalancing_ctx = RebalancingCtx::with_wallets()
        .equity(ImbalanceThreshold::new(float!(0.5), float!(0.1))?)
        .usdc(UsdcRebalancing::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .alpaca_broker_auth(alpaca_auth.clone())
        .base_wallet(base_wallet.clone())
        .ethereum_wallet(base_wallet)
        .call();

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .ws_rpc_url(chain.ws_endpoint()?)
        .orderbook(chain.orderbook)
        .deployment_block(deployment_block)
        .broker(BrokerCtx::AlpacaBrokerApi(alpaca_auth))
        .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing_ctx)))
        .assets(AssetsConfig {
            equities: EquitiesConfig {
                symbols: equities,
                operational_limit: None,
            },
            cash: None,
        })
        .inventory_poll_interval(15)
        .server_port(8001)
        .call()
        .map_err(Into::into)
}

#[tokio::test]
#[ignore = "long-running dev harness, not a CI test"]
async fn dev_harness() -> anyhow::Result<()> {
    crate::test_infra::init_tracing();

    let infra = TestInfra::start(
        vec![("AAPL", float!(150.25)), ("TSLA", float!(245.00))],
        vec![],
    )
    .await?;

    let usdc_amount: U256 = parse_units("100000", 6)?.into();
    let _usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // Prepare a sell order so the bot has something to process
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!(5.0))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    let (event_sender, _) = broadcast::channel::<Statement>(256);
    let dashboard_sender = event_sender.clone();

    let ctx = build_dev_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
    )?;

    let mut bot = spawn_bot_with_event_channel(ctx, event_sender);

    poll_for_ready(&mut bot, 8001).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    info!("Dev harness ready on http://localhost:8001");
    info!("Taking initial AAPL sell order");

    infra.base_chain.take_prepared_order(&prepared).await?;

    // Generate periodic sell orders to keep events flowing
    let chain = &infra.base_chain;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Check if bot is still alive
        if bot.is_finished() {
            warn!("Bot exited, shutting down dev harness");
            break;
        }

        // Check if any dashboard client was ever connected and now disconnected
        if dashboard_sender.receiver_count() == 0 {
            info!("No dashboard clients, continuing to generate events");
        }

        let prepared = chain
            .setup_order()
            .symbol("AAPL")
            .amount(float!(2.0))
            .price(float!(155.00))
            .direction(TakeDirection::SellEquity)
            .call()
            .await?;

        info!("Taking another AAPL sell order");
        chain.take_prepared_order(&prepared).await?;
    }

    Ok(())
}
