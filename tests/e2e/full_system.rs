//! Mega e2e test exercising hedging, equity rebalancing, and USDC rebalancing
//! together in a single bot instance with shared state.
//!
//! Phases run sequentially, each building on accumulated state from prior
//! phases. This validates that the systems compose correctly under realistic
//! multi-asset, multi-feature conditions.
//!
//! When a dashboard WebSocket client is connected, the test stays alive after
//! assertions complete so the dashboard can visualize the full event stream.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::{Provider, RootProvider};
use st0x_float_macro::float;
use tokio::sync::broadcast;

use st0x_dto::ServerMessage;
use st0x_event_sorcery::Projection;
use st0x_evm::Wallet;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, TEST_API_KEY, TEST_API_SECRET};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Symbol, TimeInForce,
};
use st0x_hedge::config::{BrokerCtx, Ctx};
use st0x_hedge::mock_api::REDEMPTION_WALLET;
use st0x_hedge::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, ImbalanceThreshold,
    OperationMode, Position, RebalancingCtx, TradingMode, UsdcRebalancing,
};

use crate::assert::ExpectedPosition;
use crate::base_chain::{self, TakeDirection};
use crate::cctp::{CctpInfra, CctpOverrides};
use crate::hedging::assertions::assert_full_hedging_flow;
use crate::poll::{
    await_dashboard_disconnect, connect_db, count_events, poll_for_events,
    poll_for_events_with_timeout, poll_for_ready, spawn_bot_with_event_channel,
};
use crate::rebalancing::assertions::TestWallet;
use crate::test_infra::TestInfra;

/// Builds a `Ctx` with ALL features enabled: hedging, equity rebalancing,
/// and USDC rebalancing. This is the superset context for the mega test.
#[bon::builder]
fn build_full_system_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    ethereum_endpoint: &str,
    broker: &AlpacaBrokerMock,
    db_path: &Path,
    deployment_block: u64,
    equity_tokens: &[(String, Address, Address)],
    equity_vault_ids: &HashMap<String, B256>,
    cash_vault_id: B256,
    cctp: CctpOverrides,
) -> anyhow::Result<Ctx> {
    let alpaca_auth = AlpacaBrokerApiCtx {
        api_key: TEST_API_KEY.to_owned(),
        api_secret: TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth.clone());

    let equities: HashMap<Symbol, EquityAssetConfig> = equity_tokens
        .iter()
        .map(|(symbol, wrapped, unwrapped)| {
            Ok((
                Symbol::new(symbol)?,
                EquityAssetConfig {
                    tokenized_equity: *unwrapped,
                    tokenized_equity_derivative: *wrapped,
                    vault_id: equity_vault_ids.get(symbol).copied(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Enabled,
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

    let ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>> = Arc::new(TestWallet::new(
        &chain.owner_key,
        ethereum_endpoint.parse()?,
        1,
    )?);

    let rebalancing_ctx = RebalancingCtx::with_wallets()
        .equity(ImbalanceThreshold::new(float!(0.5), float!(0.1))?)
        .usdc(UsdcRebalancing::Enabled {
            target: float!(0.5),
            deviation: float!(0.1),
        })
        .redemption_wallet(REDEMPTION_WALLET)
        .alpaca_broker_auth(alpaca_auth)
        .base_wallet(base_wallet)
        .ethereum_wallet(ethereum_wallet)
        .call()
        .with_circle_api_base(cctp.attestation_base_url)
        .with_cctp_addresses(cctp.token_messenger, cctp.message_transmitter);

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .ws_rpc_url(chain.ws_endpoint()?)
        .orderbook(chain.orderbook)
        .deployment_block(deployment_block)
        .broker(broker_ctx)
        .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing_ctx)))
        .assets(AssetsConfig {
            equities: EquitiesConfig {
                symbols: equities,
                operational_limit: None,
            },
            cash: Some(CashAssetConfig {
                vault_id: Some(cash_vault_id),
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            }),
        })
        .inventory_poll_interval(15)
        .server_port(8001)
        .call()
        .map_err(Into::into)
}

#[tokio::test]
async fn full_system() -> anyhow::Result<()> {
    crate::test_infra::init_tracing();
    // -- Infrastructure (superset of all scenarios) --

    let aapl_broker_price = float!(150.25);
    let tsla_broker_price = float!(245.00);

    let infra = TestInfra::start(
        vec![("AAPL", aapl_broker_price), ("TSLA", tsla_broker_price)],
        vec![],
    )
    .await?;
    let cctp = CctpInfra::start(&infra).await?;

    // Pre-fund USDC vault for the combined hedging+rebalancing scenario.
    // Sized so the ratio is initially balanced but drifts after trades.
    let usdc_amount: U256 = parse_units("100000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // -- Phase 1: Hedging (AAPL sell) --
    // Set up a SellEquity order for AAPL. Uses setup_order + take_prepared_order
    // so the order is created before the bot starts (nonce safety).

    let aapl_onchain_price = float!(155.00);
    let aapl_sell_amount = float!(10.75);

    let aapl_sell_prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(aapl_sell_amount)
        .price(aapl_onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let aapl_equity_vault_id = aapl_sell_prepared.output_vault_id;

    // -- Phase 2 prep: TSLA hedging (buy) --

    let tsla_onchain_price = float!(250.00);
    let tsla_buy_amount = float!(5.0);

    let tsla_buy_prepared = infra
        .base_chain
        .setup_order()
        .symbol("TSLA")
        .amount(tsla_buy_amount)
        .price(tsla_onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    // -- Phase 3 prep: Equity mint (3 AAPL sells to trigger imbalance) --

    let aapl_mint_price = float!(150.00);
    let aapl_mint_amount = float!(7.5);
    let mut mint_prepared_orders = Vec::new();
    for _ in 0..3 {
        mint_prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(aapl_mint_amount)
                .price(aapl_mint_price)
                .direction(TakeDirection::SellEquity)
                .call()
                .await?,
        );
    }

    // Build equity vault ID map from the prepared orders
    let tsla_equity_vault_id = tsla_buy_prepared.input_vault_id;
    let equity_vault_ids = HashMap::from([
        ("AAPL".to_owned(), aapl_equity_vault_id),
        ("TSLA".to_owned(), tsla_equity_vault_id),
    ]);

    // Capture block after all setup
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let (event_sender, _) = broadcast::channel::<ServerMessage>(256);
    let dashboard_sender = event_sender.clone();

    let ctx = build_full_system_ctx()
        .chain(&infra.base_chain)
        .ethereum_endpoint(&cctp.ethereum_endpoint)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(usdc_vault_id)
        .cctp(cctp.cctp_overrides())
        .call()?;

    let mut bot = spawn_bot_with_event_channel(ctx, event_sender);

    // Wait for Rocket to be serving, then allow conductor to finish
    // initialization (WebSocket subscription timeout + backfill + vault seeding).
    poll_for_ready(&mut bot, 8001).await;
    tokio::time::sleep(Duration::from_secs(6)).await;

    // === Phase 1: AAPL sell hedge ===

    let aapl_sell_result = infra
        .base_chain
        .take_prepared_order(&aapl_sell_prepared)
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let pool = connect_db(&infra.db_path).await?;
    let aapl_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("AAPL position should exist after sell hedge");
    assert_eq!(
        aapl_position.net,
        FractionalShares::ZERO,
        "AAPL should be fully hedged after phase 1",
    );
    pool.close().await;

    // === Phase 2: TSLA buy hedge ===

    tokio::time::sleep(Duration::from_secs(3)).await;

    let tsla_buy_result = infra
        .base_chain
        .take_prepared_order(&tsla_buy_prepared)
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 2).await;

    let pool = connect_db(&infra.db_path).await?;
    let tsla_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("TSLA")?)
        .await?
        .expect("TSLA position should exist after buy hedge");
    assert_eq!(
        tsla_position.net,
        FractionalShares::ZERO,
        "TSLA should be fully hedged after phase 2",
    );
    pool.close().await;

    // Checkpoint: both hedges complete
    let hedging_expected_positions = [
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(aapl_sell_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(aapl_onchain_price)
            .broker_fill_price(aapl_broker_price)
            .expected_accumulated_long(float!(0))
            .expected_accumulated_short(aapl_sell_amount)
            .expected_net(float!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("TSLA")
            .amount(tsla_buy_amount)
            .direction(TakeDirection::BuyEquity)
            .onchain_price(tsla_onchain_price)
            .broker_fill_price(tsla_broker_price)
            .expected_accumulated_long(tsla_buy_amount)
            .expected_accumulated_short(float!(0))
            .expected_net(float!(0))
            .build(),
    ];

    assert_full_hedging_flow(
        &hedging_expected_positions,
        &[aapl_sell_result, tsla_buy_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    // === Phase 3: Equity mint (accumulated sell imbalance) ===
    // Take all 3 AAPL sell orders to push equity imbalance past the
    // threshold. The inventory poller should detect TooMuchOffchain and
    // trigger a mint cycle.

    for prepared in &mint_prepared_orders {
        infra.base_chain.take_prepared_order(prepared).await?;
    }

    // Wait for all Phase 3 hedges to complete (2 from earlier + 3 new = 5 total)
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        5,
        Duration::from_secs(120),
    )
    .await;

    // Now wait for the mint rebalancing to complete
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    // Verify mint completed: AAPL position should reflect all trades
    let pool = connect_db(&infra.db_path).await?;
    let aapl_final = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("AAPL position should exist after mint");
    assert_eq!(
        aapl_final.net,
        FractionalShares::ZERO,
        "AAPL should be fully hedged after mint phase",
    );
    // Total AAPL short: initial 10.75 + 3 * 7.5 = 33.25
    assert_eq!(
        aapl_final.accumulated_short,
        FractionalShares::new(float!(33.25)),
        "AAPL accumulated short should reflect all sell trades",
    );
    pool.close().await;

    // Verify mint events exist
    let pool = connect_db(&infra.db_path).await?;
    let mint_events = count_events(&pool, "TokenizedEquityMint").await?;
    assert!(
        mint_events >= 5,
        "Mint should emit at least MintRequested + MintAccepted + \
         TokensReceived + TokensWrapped + DepositedIntoRaindex, got {mint_events}",
    );
    pool.close().await;

    // === Dashboard auto-detect ===
    await_dashboard_disconnect(&dashboard_sender, Duration::from_secs(3)).await;

    bot.abort();
    Ok(())
}
