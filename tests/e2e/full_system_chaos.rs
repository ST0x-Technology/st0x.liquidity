//! Chaos helpers for [`super::full_system::full_system_concurrent`].
//!
//! Exercises restarts, broker NAV bumps, and config changes that add or remove
//! assets while trades fire concurrently with staggered submission.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::Provider;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::{JoinHandle, JoinSet};
use tracing::info;

use rain_math_float::Float;
use st0x_dto::Statement;
use st0x_event_sorcery::Projection;
use st0x_execution::FractionalShares;
use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, OrderSide};
use st0x_float_macro::float;
use st0x_hedge::Position;

use crate::base_chain::{PreparedOrder, TakeDirection};
use crate::cctp::{CctpInfra, USDC_ETHEREUM};
use crate::full_system::build_full_system_ctx;
use crate::poll::{
    connect_db, count_events, count_events_of_type, poll_for_broker_fills,
    poll_for_events_with_timeout, poll_for_ready, simulation_ports, sleep_or_crash,
    spawn_bot_with_event_channel,
};
use crate::test_infra::TestInfra;

const CHAOS_ACTION_COUNT: usize = 5;

/// One mint/rebalance lifecycle per AAPL sell imbalance cycle.
const EXPECTED_TOKENIZED_EQUITY_MINT_EVENTS: i64 = 5;
/// Full BaseToAlpaca happy-path lifecycle (see `full_system` phase 4).
const EXPECTED_USDC_REBALANCE_EVENTS: i64 = 12;
/// Five core onchain trades plus the deferred MSFT sell after `AddAsset`.
const EXPECTED_OFFCHAIN_FILLS: i64 = 6;

#[derive(Debug, Clone, Copy)]
enum ChaosAction {
    NavBump,
    Restart,
    AddAsset,
    RemoveAsset,
    BrokerLatency,
}

#[derive(Debug, Clone, Copy)]
enum CoreTrade {
    AaplSell,
    TslaBuy,
    AaplMint(usize),
    MsftSell,
}

struct ChaosConfig {
    msft_enabled: bool,
    tsla_removed: bool,
    /// Set after the core TSLA buy is onchain and its broker hedge has converged.
    /// `RemoveAsset` must not run before this, or the bot could drop TSLA while
    /// exposure is still open.
    tsla_hedge_confirmed: bool,
}

struct ActiveSymbols<'symbol> {
    symbols: Vec<&'symbol str>,
}

impl ActiveSymbols<'_> {
    fn includes(&self, symbol: &str) -> bool {
        self.symbols.contains(&symbol)
    }
}

struct ChaosBootstrap<P> {
    infra: Arc<TestInfra<P>>,
    take_lock: Arc<Mutex<()>>,
    shared_trades: Arc<SharedTradeOrders>,
    cctp: CctpInfra,
    usdc_vault_id: B256,
    equity_vault_ids: HashMap<String, B256>,
    deployment_block: u64,
    server_port: u16,
    board_port: u16,
    event_sender: broadcast::Sender<Statement>,
    aapl_broker_price: Float,
    tsla_broker_price: Float,
    msft_broker_price: Float,
}

struct SharedTradeOrders {
    aapl_sell: PreparedOrder,
    tsla_buy: PreparedOrder,
    aapl_mints: Vec<PreparedOrder>,
    msft_sell: PreparedOrder,
}

impl SharedTradeOrders {
    fn prepared(&self, trade: CoreTrade) -> &PreparedOrder {
        match trade {
            CoreTrade::AaplSell => &self.aapl_sell,
            CoreTrade::TslaBuy => &self.tsla_buy,
            CoreTrade::AaplMint(index) => self
                .aapl_mints
                .get(index)
                .unwrap_or_else(|| panic!("invalid AAPL mint index {index}")),
            CoreTrade::MsftSell => &self.msft_sell,
        }
    }
}

struct ChaosSession<'bootstrap, P: Provider + Clone> {
    bot: JoinHandle<anyhow::Result<()>>,
    bootstrap: &'bootstrap ChaosBootstrap<P>,
    chaos_config: ChaosConfig,
}

pub(crate) async fn run_concurrent_chaos_simulation() -> anyhow::Result<()> {
    crate::test_infra::init_tracing();

    let bootstrap = Box::pin(bootstrap_chaos_world()).await?;
    let mut session = start_chaos_session(&bootstrap).await?;
    let mut nav_baselines = HashMap::from([
        ("AAPL".to_owned(), bootstrap.aapl_broker_price),
        ("TSLA".to_owned(), bootstrap.tsla_broker_price),
        ("MSFT".to_owned(), bootstrap.msft_broker_price),
    ]);

    let mut rng = StdRng::from_entropy();
    fire_core_trades_concurrently(&bootstrap, &mut rng).await?;
    wait_for_tsla_hedge(&mut session, &bootstrap).await?;

    let action_plan = build_chaos_action_plan(&mut rng);
    run_chaos_rounds(&mut session, &mut nav_baselines, &action_plan, &mut rng).await?;
    take_msft_after_asset_add(&bootstrap, &session.chaos_config).await?;
    wait_for_chaos_convergence(&mut session.bot, &bootstrap).await?;
    assert_final_state(&bootstrap.infra).await?;

    if std::env::var_os("SIMULATE_EXIT_AFTER_CHAOS").is_some() {
        session.bot.abort();
        return Ok(());
    }

    info!("Chaos scenario converged - bot staying up for dashboard observation (Ctrl-C to stop)");
    idle_for_dashboard(&mut session.bot).await
}

async fn bootstrap_chaos_world() -> anyhow::Result<ChaosBootstrap<impl Provider + Clone + 'static>>
{
    let aapl_broker_price = float!(150.25);
    let tsla_broker_price = float!(245.00);
    let msft_broker_price = float!(380.00);

    let infra = TestInfra::start_with_cash(
        vec![
            ("AAPL", aapl_broker_price),
            ("TSLA", tsla_broker_price),
            ("MSFT", msft_broker_price),
        ],
        vec![("TSLA", float!(400))],
        Some(float!(500_000)),
        None,
    )
    .await?;
    let cctp = CctpInfra::start(&infra).await?;

    let usdc_amount: U256 = parse_units("300000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    let aapl_sell_prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!(10.75))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .usdc_vault_id(usdc_vault_id)
        .call()
        .await?;

    let aapl_equity_vault_id = aapl_sell_prepared.output_vault_id;

    let tsla_buy_prepared = infra
        .base_chain
        .setup_order()
        .symbol("TSLA")
        .amount(float!(5.0))
        .price(float!(250.00))
        .direction(TakeDirection::BuyEquity)
        .usdc_vault_id(usdc_vault_id)
        .call()
        .await?;

    let mut mint_prepared_orders = Vec::new();
    for _ in 0..3 {
        mint_prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(float!(7.5))
                .price(float!(150.00))
                .direction(TakeDirection::SellEquity)
                .usdc_vault_id(usdc_vault_id)
                .call()
                .await?,
        );
    }

    let msft_sell_prepared = infra
        .base_chain
        .setup_order()
        .symbol("MSFT")
        .amount(float!(4.0))
        .price(float!(390.00))
        .direction(TakeDirection::SellEquity)
        .usdc_vault_id(usdc_vault_id)
        .call()
        .await?;

    let equity_vault_ids = HashMap::from([
        ("AAPL".to_owned(), aapl_equity_vault_id),
        ("TSLA".to_owned(), tsla_buy_prepared.input_vault_id),
        ("MSFT".to_owned(), msft_sell_prepared.output_vault_id),
    ]);

    let eth_deposit_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let _deposit_watcher = infra
        .broker_service
        .start_deposit_watcher(eth_deposit_provider, USDC_ETHEREUM, infra.base_chain.owner)
        .await?;

    let deployment_block = infra.base_chain.provider.get_block_number().await?;
    let (event_sender, _) = broadcast::channel::<Statement>(256);
    let (server_port, board_port) = simulation_ports();

    Ok(ChaosBootstrap {
        infra: Arc::new(infra),
        take_lock: Arc::new(Mutex::new(())),
        shared_trades: Arc::new(SharedTradeOrders {
            aapl_sell: aapl_sell_prepared,
            tsla_buy: tsla_buy_prepared,
            aapl_mints: mint_prepared_orders,
            msft_sell: msft_sell_prepared,
        }),
        cctp,
        usdc_vault_id,
        equity_vault_ids,
        deployment_block,
        server_port,
        board_port,
        event_sender,
        aapl_broker_price,
        tsla_broker_price,
        msft_broker_price,
    })
}

async fn start_chaos_session<P: Provider + Clone + 'static>(
    bootstrap: &ChaosBootstrap<P>,
) -> anyhow::Result<ChaosSession<'_, P>> {
    let chaos_config = ChaosConfig {
        msft_enabled: false,
        tsla_removed: false,
        tsla_hedge_confirmed: false,
    };
    let active_symbols = active_symbols(&chaos_config);
    let mut bot = spawn_bot_for_symbols(bootstrap, &active_symbols)?;

    poll_for_ready(&mut bot, bootstrap.server_port).await;
    tokio::time::sleep(Duration::from_secs(6)).await;

    bootstrap
        .infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new("TSLA")?, 3);

    Ok(ChaosSession {
        bot,
        bootstrap,
        chaos_config,
    })
}

async fn fire_core_trades_concurrently<P: Provider + Clone + 'static>(
    bootstrap: &ChaosBootstrap<P>,
    rng: &mut StdRng,
) -> anyhow::Result<()> {
    let mut trades = vec![
        CoreTrade::AaplSell,
        CoreTrade::TslaBuy,
        CoreTrade::AaplMint(0),
        CoreTrade::AaplMint(1),
        CoreTrade::AaplMint(2),
    ];
    trades.shuffle(rng);

    let delays_ms: Vec<u64> = (0..trades.len()).map(|_| rng.gen_range(0..500)).collect();

    info!(
        trade_count = trades.len(),
        "Submitting core trades concurrently with staggered delays",
    );

    let mut join_set = JoinSet::new();
    for (trade, delay_ms) in trades.into_iter().zip(delays_ms) {
        let infra = bootstrap.infra.clone();
        let take_lock = bootstrap.take_lock.clone();
        let shared_trades = bootstrap.shared_trades.clone();
        join_set.spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            let _take_guard = take_lock.lock().await;
            let prepared = shared_trades.prepared(trade);
            infra.base_chain.take_prepared_order(prepared).await
        });
    }

    while let Some(result) = join_set.join_next().await {
        result??;
    }

    Ok(())
}

async fn wait_for_tsla_hedge<P: Provider + Clone>(
    session: &mut ChaosSession<'_, P>,
    bootstrap: &ChaosBootstrap<P>,
) -> anyhow::Result<()> {
    poll_for_broker_fills(
        &mut session.bot,
        &bootstrap.infra.broker_service,
        "TSLA",
        OrderSide::Sell,
        FractionalShares::new(float!(5)),
        Duration::from_secs(180),
    )
    .await;
    session.chaos_config.tsla_hedge_confirmed = true;
    Ok(())
}

fn build_chaos_action_plan(rng: &mut StdRng) -> Vec<ChaosAction> {
    let mut plan = vec![
        ChaosAction::Restart,
        ChaosAction::AddAsset,
        ChaosAction::RemoveAsset,
        ChaosAction::NavBump,
        ChaosAction::BrokerLatency,
    ];
    assert_eq!(plan.len(), CHAOS_ACTION_COUNT);
    plan.shuffle(rng);
    plan
}

async fn run_chaos_rounds<P: Provider + Clone + 'static>(
    session: &mut ChaosSession<'_, P>,
    nav_baselines: &mut HashMap<String, Float>,
    action_plan: &[ChaosAction],
    rng: &mut StdRng,
) -> anyhow::Result<()> {
    info!(
        rounds = action_plan.len(),
        "Running chaos rounds with guaranteed action coverage",
    );

    for action in action_plan {
        apply_chaos_action(session, nav_baselines, *action, rng).await?;
    }

    Ok(())
}

async fn take_msft_after_asset_add<P: Provider + Clone + 'static>(
    bootstrap: &ChaosBootstrap<P>,
    chaos_config: &ChaosConfig,
) -> anyhow::Result<()> {
    if !chaos_config.msft_enabled {
        return Ok(());
    }

    info!("Taking deferred MSFT sell after asset was added to config");
    let _take_guard = bootstrap.take_lock.lock().await;
    bootstrap
        .infra
        .base_chain
        .take_prepared_order(bootstrap.shared_trades.prepared(CoreTrade::MsftSell))
        .await?;

    Ok(())
}

async fn wait_for_chaos_convergence<P: Provider + Clone>(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    bootstrap: &ChaosBootstrap<P>,
) -> anyhow::Result<()> {
    poll_for_broker_fills(
        bot,
        &bootstrap.infra.broker_service,
        "AAPL",
        OrderSide::Buy,
        FractionalShares::new(float!(33.25)),
        Duration::from_secs(180),
    )
    .await;

    poll_for_broker_fills(
        bot,
        &bootstrap.infra.broker_service,
        "MSFT",
        OrderSide::Buy,
        FractionalShares::new(float!(4)),
        Duration::from_secs(180),
    )
    .await;

    poll_for_events_with_timeout(
        bot,
        &bootstrap.infra.db_path,
        "OffchainOrderEvent::Filled",
        EXPECTED_OFFCHAIN_FILLS,
        Duration::from_secs(180),
    )
    .await;

    poll_for_events_with_timeout(
        bot,
        &bootstrap.infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(180),
    )
    .await;

    poll_for_events_with_timeout(
        bot,
        &bootstrap.infra.db_path,
        "UsdcRebalanceEvent::ConversionConfirmed",
        1,
        Duration::from_secs(180),
    )
    .await;

    Ok(())
}

async fn idle_for_dashboard(bot: &mut JoinHandle<anyhow::Result<()>>) -> anyhow::Result<()> {
    loop {
        sleep_or_crash(bot, "dashboard idle").await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

async fn apply_chaos_action<P: Provider + Clone + 'static>(
    session: &mut ChaosSession<'_, P>,
    nav_baselines: &mut HashMap<String, Float>,
    action: ChaosAction,
    rng: &mut StdRng,
) -> anyhow::Result<()> {
    info!(?action, "Applying chaos action");

    match action {
        ChaosAction::NavBump => {
            for symbol in ["AAPL", "TSLA", "MSFT"] {
                let baseline = *nav_baselines
                    .get(symbol)
                    .ok_or_else(|| anyhow::anyhow!("missing NAV baseline for {symbol}"))?;
                let bumped = bump_symbol_nav(
                    &session.bootstrap.infra.broker_service,
                    symbol,
                    baseline,
                    rng,
                )?;
                nav_baselines.insert(symbol.to_owned(), bumped);
            }
        }
        ChaosAction::Restart => restart_bot(session).await?,
        ChaosAction::AddAsset => {
            session.chaos_config.msft_enabled = true;
            restart_bot(session).await?;
        }
        ChaosAction::RemoveAsset => {
            anyhow::ensure!(
                session.chaos_config.tsla_hedge_confirmed,
                "RemoveAsset blocked: TSLA core trade must be taken and hedged before removal",
            );
            session.chaos_config.tsla_removed = true;
            restart_bot(session).await?;
        }
        ChaosAction::BrokerLatency => {
            let delay_polls = rng.gen_range(1..=5);
            session
                .bootstrap
                .infra
                .broker_service
                .set_symbol_fill_delay(Symbol::new("TSLA")?, delay_polls);
            info!(delay_polls, "Adjusted TSLA broker fill latency");
        }
    }

    Ok(())
}

fn bump_symbol_nav(
    broker: &AlpacaBrokerMock,
    symbol: &str,
    baseline: Float,
    rng: &mut StdRng,
) -> anyhow::Result<Float> {
    let bump_factors = [float!(1.06), float!(0.94), float!(1.03), float!(0.97)];
    let factor = *bump_factors
        .choose(rng)
        .expect("bump factors must be non-empty");
    let bumped =
        (baseline * factor).map_err(|error| anyhow::anyhow!("NAV bump failed: {error:?}"))?;
    broker.set_symbol_fill_price(Symbol::new(symbol)?, bumped);
    info!(%symbol, ?baseline, ?bumped, "Applied broker NAV bump");
    Ok(bumped)
}

async fn restart_bot<P: Provider + Clone + 'static>(
    session: &mut ChaosSession<'_, P>,
) -> anyhow::Result<()> {
    let bootstrap = session.bootstrap;
    info!("Restarting bot against same database (crash-recovery path)");
    let previous = std::mem::replace(&mut session.bot, tokio::spawn(async { Ok(()) }));
    previous.abort();
    let _ = previous.await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    session.bot = spawn_bot_for_symbols(bootstrap, &active_symbols(&session.chaos_config))?;
    poll_for_ready(&mut session.bot, bootstrap.server_port).await;
    Ok(())
}

fn spawn_bot_for_symbols<P: Provider + Clone>(
    bootstrap: &ChaosBootstrap<P>,
    active: &ActiveSymbols<'_>,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let equity_tokens = equity_tokens_for(&bootstrap.infra, &active.symbols);
    let active_vault_ids: HashMap<String, B256> = bootstrap
        .equity_vault_ids
        .iter()
        .filter(|(symbol, _)| active.includes(symbol))
        .map(|(symbol, vault_id)| (symbol.clone(), *vault_id))
        .collect();

    let ctx = build_full_system_ctx()
        .chain(&bootstrap.infra.base_chain)
        .ethereum_endpoint(&bootstrap.cctp.ethereum_endpoint)
        .broker(&bootstrap.infra.broker_service)
        .db_path(&bootstrap.infra.db_path)
        .deployment_block(bootstrap.deployment_block)
        .equity_tokens(&equity_tokens)
        .equity_vault_ids(&active_vault_ids)
        .cash_vault_id(bootstrap.usdc_vault_id)
        .cctp(bootstrap.cctp.cctp_overrides())
        .server_port(bootstrap.server_port)
        .board_port(bootstrap.board_port)
        .issuance_base_url(bootstrap.infra.issuance_base_url.clone())
        .call()?;

    Ok(spawn_bot_with_event_channel(
        ctx,
        bootstrap.event_sender.clone(),
    ))
}

fn active_symbols(chaos_config: &ChaosConfig) -> ActiveSymbols<'static> {
    let mut symbols = vec!["AAPL"];
    if !chaos_config.tsla_removed {
        symbols.push("TSLA");
    }
    if chaos_config.msft_enabled {
        symbols.push("MSFT");
    }
    ActiveSymbols { symbols }
}

fn equity_tokens_for(
    infra: &TestInfra<impl Provider + Clone>,
    symbols: &[&str],
) -> Vec<(String, Address, Address)> {
    infra
        .equity_addresses
        .iter()
        .filter(|(symbol, _, _)| symbols.contains(&symbol.as_str()))
        .cloned()
        .collect()
}

async fn assert_final_state(infra: &TestInfra<impl Provider + Clone>) -> anyhow::Result<()> {
    let pool = connect_db(&infra.db_path).await?;

    let aapl_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("AAPL position should exist");
    assert_eq!(
        aapl_position.net,
        FractionalShares::ZERO,
        "AAPL should be fully hedged (net zero)",
    );
    assert_eq!(
        aapl_position.accumulated_short,
        FractionalShares::new(float!(33.25)),
        "AAPL accumulated short should reflect all 4 sell trades",
    );

    let tsla_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("TSLA")?)
        .await?
        .expect("TSLA position should exist");
    assert_eq!(
        tsla_position.net,
        FractionalShares::ZERO,
        "TSLA should be fully hedged (net zero) even after config removal",
    );
    assert_eq!(
        tsla_position.accumulated_long,
        FractionalShares::new(float!(5)),
        "TSLA accumulated long should reflect buy trade",
    );

    let msft_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("MSFT")?)
        .await?
        .expect("MSFT position should exist after asset was added");
    assert_eq!(
        msft_position.net,
        FractionalShares::ZERO,
        "MSFT should be fully hedged (net zero)",
    );
    assert_eq!(
        msft_position.accumulated_short,
        FractionalShares::new(float!(4)),
        "MSFT accumulated short should reflect sell trade",
    );

    let mint_events = count_events(&pool, "TokenizedEquityMint").await?;
    assert_eq!(
        mint_events, EXPECTED_TOKENIZED_EQUITY_MINT_EVENTS,
        "TokenizedEquityMint lifecycle should emit exactly one event per stage",
    );

    let usdc_events = count_events(&pool, "UsdcRebalance").await?;
    assert_eq!(
        usdc_events, EXPECTED_USDC_REBALANCE_EVENTS,
        "USDC rebalance should emit the full happy-path lifecycle exactly once",
    );

    let pending_burn_recorded =
        count_events_of_type(&pool, "UsdcRebalanceEvent::PendingBurnRecorded").await?;
    assert_eq!(
        pending_burn_recorded, 1,
        "USDC rebalance must record PendingBurnRecorded exactly once, got {pending_burn_recorded}",
    );

    pool.close().await;
    Ok(())
}
