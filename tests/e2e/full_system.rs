//! Full-system e2e tests: hedging, equity rebalancing, and USDC rebalancing
//! running together in a single bot instance with shared state.
//!
//! - [`full_system`]: Sequential phases with assertions after each step.
//!   Validates correctness of the complete hedging -> mint -> USDC bridge
//!   pipeline. Finishes when all assertions pass.
//!
//! - [`simulate`]: Long-running market simulation. Sets up Raindex liquidity
//!   orders once (buy + sell per symbol, shared vaults) and continuously
//!   takes them to simulate users buying and selling tokenized equities.
//!   The bot counter-trades, mints/redeems, and bridges USDC to maintain
//!   liquidity — if it works correctly, the vaults never drain permanently.
//!   Run via `nix run .#simulate` (mprocs with dashboard). Ctrl-C to stop.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::{Provider, RootProvider};
use rain_math_float::Float;
use rand::Rng;
use st0x_float_macro::float;
use tokio::sync::broadcast;
use tracing::{debug, info};

use st0x_dto::Statement;
use st0x_event_sorcery::Projection;
use st0x_evm::Wallet;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, TEST_API_KEY, TEST_API_SECRET};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, FractionalShares, Symbol, TimeInForce,
};
use st0x_hedge::config::{BrokerCtx, Ctx};
use st0x_hedge::mock_api::REDEMPTION_WALLET;
use st0x_hedge::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, ImbalanceThreshold,
    OperationMode, Position, RebalancingCtx, TradingMode, UsdcRebalancing,
};

use crate::assert::ExpectedPosition;
use crate::base_chain::{self, TakeDirection};
use crate::cctp::{CctpInfra, CctpOverrides, USDC_ETHEREUM};
use crate::hedging::assertions::assert_full_hedging_flow;
use crate::poll::{
    connect_db, count_events, poll_for_broker_fills, poll_for_events, poll_for_events_with_timeout,
    poll_for_ready, spawn_bot_with_event_channel,
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
        counter_trade_slippage_bps: DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
    };
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(alpaca_auth);

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
        .call()
        .with_circle_api_base(cctp.attestation_base_url)
        .with_cctp_addresses(cctp.token_messenger, cctp.message_transmitter);

    let wallet_ctx =
        st0x_hedge::wallet::OnchainWalletCtx::from_wallets(base_wallet, ethereum_wallet);

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .ws_rpc_url(chain.ws_endpoint()?)
        .orderbook(chain.orderbook)
        .deployment_block(deployment_block)
        .broker(broker_ctx)
        .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing_ctx)))
        .wallet(wallet_ctx)
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

/// Asserts correctness of the full hedging + rebalancing pipeline across
/// multiple assets. Runs sequential phases — AAPL sell hedge, TSLA buy
/// hedge, equity mint from accumulated imbalance, USDC bridge rebalance —
/// with assertions after each. Exits when all phases pass.
#[tokio::test]
#[ignore = "long-running system test -- run explicitly with --run-ignored"]
#[allow(clippy::too_many_lines)]
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
    // The mock broker starts with 100k USD cash. With 300k onchain and
    // ~100k offchain, the ratio is ~0.75 (above 0.6 threshold), so
    // BaseToAlpaca USDC rebalancing triggers after inventory polling.
    let usdc_amount: U256 = parse_units("300000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // -- Phase 1: Hedging (AAPL sell) --
    // Set up a SellEquity order for AAPL. Uses setup_order + take_prepared_order
    // so the order is created before the bot starts (nonce safety).
    //
    // All orders share the pre-funded USDC vault so the VaultRegistry
    // discovers the same vault via TakeOrderV3 events. Without this,
    // each order creates a random USDC vault and the registry overwrites
    // the seeded vault, causing the inventory poller to read the wrong balance.
    let aapl_onchain_price = float!(155.00);
    let aapl_sell_amount = float!(10.75);

    let aapl_sell_prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(aapl_sell_amount)
        .price(aapl_onchain_price)
        .direction(TakeDirection::SellEquity)
        .usdc_vault_id(usdc_vault_id)
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
        .usdc_vault_id(usdc_vault_id)
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
                .usdc_vault_id(usdc_vault_id)
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

    // Start deposit watcher on Ethereum for BaseToAlpaca USDC rebalancing.
    // The CCTP bridge mints USDC on Ethereum; the watcher detects the
    // Transfer event and registers an incoming Alpaca wallet transfer so
    // the bot's deposit polling succeeds.
    let eth_deposit_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let _deposit_watcher = infra
        .broker_service
        .start_deposit_watcher(eth_deposit_provider, USDC_ETHEREUM, infra.base_chain.owner)
        .await?;

    // Capture block after all setup
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let (event_sender, _) = broadcast::channel::<Statement>(256);

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

    // Wait for all AAPL hedges by polling the broker for total filled buy
    // quantity. Onchain sells: 10.75 + 3 * 7.5 = 33.25 shares total.
    // With concurrent processing, trades may batch into fewer orders.
    poll_for_broker_fills(
        &mut bot,
        &infra.broker_service,
        "AAPL",
        st0x_execution::alpaca_broker_api::OrderSide::Buy,
        FractionalShares::new(float!(33.25)),
        Duration::from_secs(120),
    )
    .await;

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

    // === Phase 4: USDC rebalancing (BaseToAlpaca) ===
    // The vault starts with 100k USDC onchain and 0 offchain (ratio = 1.0).
    // With target 0.5 and deviation 0.1, ratio > 0.6 triggers BaseToAlpaca
    // rebalancing: withdraw from Rain vault -> CCTP bridge -> Alpaca deposit
    // -> conversion. ConversionConfirmed is the terminal event.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "UsdcRebalanceEvent::ConversionConfirmed",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let usdc_events = count_events(&pool, "UsdcRebalance").await?;
    assert!(
        usdc_events >= 9,
        "USDC rebalance should emit at least Initiated + WithdrawalConfirmed + \
         BridgingInitiated + BridgeAttestationReceived + Bridged + DepositInitiated + \
         DepositConfirmed + ConversionInitiated + ConversionConfirmed, got {usdc_events}",
    );
    pool.close().await;

    bot.abort();
    Ok(())
}

/// Long-running simulation: sets up mock infrastructure and Raindex
/// liquidity orders once, starts the bot, then simulates users buying and
/// selling indefinitely. Each symbol has a SellEquity order (user buys
/// tokenized equity from us, paying USDC) and a BuyEquity order (user
/// sells tokenized equity to us, receiving USDC).
///
/// If the system works correctly this runs forever: the bot counter-trades
/// each fill on the offchain broker, mints/redeems to rebalance equity
/// supply, and bridges USDC to keep cash balanced. The Rain vaults stay
/// funded because the bot continuously cycles liquidity back into them.
///
/// Run via `nix run .#simulate` which pairs this with the dashboard dev
/// server in mprocs so you can observe the system in real time.
#[tokio::test]
#[ignore = "infinite simulation -- run via nix run .#simulate"]
#[allow(clippy::too_many_lines)]
async fn simulate() -> anyhow::Result<()> {
    crate::test_infra::init_tracing();

    let aapl_broker_price = float!(150.25);
    let tsla_broker_price = float!(245.00);

    let db_path = std::path::PathBuf::from("/tmp/st0x-liquidity-simulate.sqlite");
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(db_path.with_extension("sqlite-shm"));
    let _ = std::fs::remove_file(db_path.with_extension("sqlite-wal"));

    let infra = TestInfra::start_with_cash(
        vec![("AAPL", aapl_broker_price), ("TSLA", tsla_broker_price)],
        vec![("TSLA", float!(400))],
        Some(float!(20000)),
        Some(db_path),
    )
    .await?;
    debug!("Starting CCTP mock infrastructure");
    let cctp = CctpInfra::start(&infra).await?;

    debug!("Creating USDC vault");
    let usdc_amount: U256 = parse_units("80000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    debug!("Setting up Raindex orders");
    let aapl_sell = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!(67))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .usdc_vault_id(usdc_vault_id)
        .call()
        .await?;

    let aapl_equity_vault_id = aapl_sell.output_vault_id;

    let aapl_buy = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!(67))
        .price(float!(155.00))
        .direction(TakeDirection::BuyEquity)
        .usdc_vault_id(usdc_vault_id)
        .equity_vault_id(aapl_equity_vault_id)
        .call()
        .await?;

    let tsla_sell = infra
        .base_chain
        .setup_order()
        .symbol("TSLA")
        .amount(float!(20))
        .price(float!(250.00))
        .direction(TakeDirection::SellEquity)
        .usdc_vault_id(usdc_vault_id)
        .call()
        .await?;

    let tsla_equity_vault_id = tsla_sell.output_vault_id;

    let tsla_buy = infra
        .base_chain
        .setup_order()
        .symbol("TSLA")
        .amount(float!(20))
        .price(float!(250.00))
        .direction(TakeDirection::BuyEquity)
        .usdc_vault_id(usdc_vault_id)
        .equity_vault_id(tsla_equity_vault_id)
        .call()
        .await?;

    let equity_vault_ids = HashMap::from([
        ("AAPL".to_owned(), aapl_equity_vault_id),
        ("TSLA".to_owned(), tsla_equity_vault_id),
    ]);

    debug!("Starting deposit watcher");
    let eth_deposit_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let _deposit_watcher = infra
        .broker_service
        .start_deposit_watcher(eth_deposit_provider, USDC_ETHEREUM, infra.base_chain.owner)
        .await?;

    debug!("Building bot context");
    let current_block = infra.base_chain.provider.get_block_number().await?;
    let (event_sender, _) = broadcast::channel::<Statement>(256);

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

    debug!("Starting bot");
    let mut bot = spawn_bot_with_event_channel(ctx, event_sender);

    poll_for_ready(&mut bot, 8001).await;
    info!("Bot ready. Starting continuous trade simulation.");

    let orders = [
        (&aapl_sell, "AAPL", "SellEquity"),
        (&aapl_buy, "AAPL", "BuyEquity"),
        (&tsla_sell, "TSLA", "SellEquity"),
        (&tsla_buy, "TSLA", "BuyEquity"),
    ];

    let trade_onchain_minutes = 10;
    let trade_duration = Duration::from_secs(trade_onchain_minutes * 60);
    let started = tokio::time::Instant::now();
    let mut round = 0u64;

    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;

        if bot.is_finished() {
            let result = (&mut bot).await;
            panic!("Bot exited during simulation: {result:?}");
        }

        if started.elapsed() >= trade_duration && round > 0 {
            // Only log once when transitioning to idle
            info!("Trade phase complete. Bot still running — observe the system settling.");
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if bot.is_finished() {
                    let result = (&mut bot).await;
                    panic!("Bot exited during idle phase: {result:?}");
                }
            }
        }

        let (order, symbol, direction) = orders[usize::try_from(round).unwrap() % orders.len()];
        round += 1;

        let mut rng = rand::thread_rng();
        let amount: f64 = rng.gen_range(1.0..10.0);
        let amount_str = format!("{amount:.3}");
        let max_amount = Float::parse(amount_str.clone()).ok();

        info!(round, symbol, direction, amount = %amount_str, "Simulating user trade");

        match infra
            .base_chain
            .take_prepared_order_with_max(order, max_amount)
            .await
        {
            Ok(_) => info!(round, symbol, direction, amount = %amount_str, "Trade executed"),
            Err(error) => {
                info!(
                    round, symbol, direction, %error,
                    "Trade reverted (vault drained, waiting for rebalance)",
                );
            }
        }
    }
}
