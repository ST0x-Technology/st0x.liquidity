//! Composite test harness for e2e tests.
//!
//! Provides `TestInfra` which wires together `AlpacaBrokerMock`,
//! `AlpacaTokenizationMock`, `BaseChain`, `DeployableERC20`, and
//! `CctpAttestationMock` into a single startup call.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Once};

use alloy::primitives::{Address, U256, utils::parse_units};
use alloy::providers::Provider;
use rain_math_float::Float;
use tempfile::TempDir;
use tracing::{debug, info};

use st0x_bridge::cctp::CctpAttestationMock;
use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, MockPosition};
use st0x_hedge::config::{AssetsConfig, EquitiesConfig, EquityAssetConfig, OperationMode};
use st0x_hedge::mock_api::{AlpacaTokenizationMock, REDEMPTION_WALLET};
use st0x_hedge::telemetry::mk_env_filter;

use crate::base_chain::{BaseChain, DeployableERC20};

static TRACING_INIT: Once = Once::new();

/// Initializes a compact tracing subscriber filtered to st0x crates only.
/// Safe to call multiple times -- only the first call takes effect.
pub fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let base_filter = mk_env_filter(tracing::Level::DEBUG);
        let trace = tracing::Level::TRACE;
        let directive = format!("e2e={trace}").parse().unwrap();
        let filter = base_filter.add_directive(directive);

        tracing_subscriber::fmt()
            .compact()
            .with_env_filter(filter)
            .with_target(true)
            .with_line_number(true)
            .with_writer(std::io::stderr)
            .try_init()
            .expect("Failed to initialize tracing subscriber");
    });
}

pub struct TestInfra<P> {
    /// Kept alive so the temp directory isn't deleted while the test runs.
    _db_dir: TempDir,
    pub db_path: PathBuf,
    pub base_chain: BaseChain<P>,
    pub broker_service: Arc<AlpacaBrokerMock>,
    pub tokenization_service: AlpacaTokenizationMock,
    pub attestation_service: CctpAttestationMock,
    /// `(symbol, vault_address, underlying_address)` per deployed equity vault.
    pub equity_addresses: Vec<(String, Address, Address)>,
}

impl<P> TestInfra<P> {
    /// Builds an `AssetsConfig` with trading enabled for all deployed
    /// equities. Use this when constructing `Ctx::for_test()` in hedging
    /// tests so the conductor treats the symbols as active.
    pub fn assets_config(&self) -> AssetsConfig {
        let symbols = self
            .equity_addresses
            .iter()
            .map(|(symbol, vault_addr, underlying_addr)| {
                let config = EquityAssetConfig {
                    tokenized_equity: *underlying_addr,
                    tokenized_equity_derivative: *vault_addr,
                    vault_id: None,
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    operational_limit: None,
                };
                (Symbol::new(symbol.clone()).unwrap(), config)
            })
            .collect();

        AssetsConfig {
            equities: EquitiesConfig {
                symbols,
                operational_limit: None,
            },
            cash: None,
        }
    }
}

impl TestInfra<()> {
    pub async fn start(
        equity_prices: Vec<(&str, Float)>,
        equity_positions: Vec<(&str, Float)>,
    ) -> anyhow::Result<TestInfra<impl Provider + Clone>> {
        Self::start_with_cash(equity_prices, equity_positions, None, None).await
    }

    pub async fn start_with_cash(
        equity_prices: Vec<(&str, Float)>,
        equity_positions: Vec<(&str, Float)>,
        initial_cash: Option<Float>,
        db_path_override: Option<std::path::PathBuf>,
    ) -> anyhow::Result<TestInfra<impl Provider + Clone>> {
        let db_dir = tempfile::tempdir()?;
        let db_path = db_path_override.unwrap_or_else(|| db_dir.path().join("e2e.sqlite"));

        let (base_chain, equity_addresses) = deploy_chain_and_vaults(&equity_prices).await?;

        let broker_service =
            start_broker_mock(&equity_prices, &equity_positions, initial_cash).await?;

        let tokenization_service =
            start_tokenization_mock(&broker_service, &equity_addresses, &base_chain).await?;

        let attestation_service = CctpAttestationMock::start().await;
        debug!("CCTP attestation mock started");
        info!("Test infrastructure ready");

        Ok(TestInfra {
            _db_dir: db_dir,
            db_path,
            base_chain,
            broker_service,
            tokenization_service,
            attestation_service,
            equity_addresses,
        })
    }
}

async fn deploy_chain_and_vaults(
    equity_prices: &[(&str, Float)],
) -> anyhow::Result<(
    BaseChain<impl Provider + Clone + use<>>,
    Vec<(String, Address, Address)>,
)> {
    info!("Starting Anvil base chain");
    let mut base_chain = BaseChain::start().await?;
    info!("Anvil started, deploying equity vaults");

    let equity_addresses = deploy_equity_vaults(&mut base_chain, equity_prices).await?;
    fund_taker_with_equity(&base_chain, &equity_addresses).await?;

    Ok((base_chain, equity_addresses))
}

async fn deploy_equity_vaults<P: Provider + Clone>(
    base_chain: &mut BaseChain<P>,
    equity_prices: &[(&str, Float)],
) -> anyhow::Result<Vec<(String, Address, Address)>> {
    let mut equity_addresses = Vec::new();

    for (symbol, _price) in equity_prices {
        let (vault_addr, underlying_addr) = base_chain.deploy_equity_vault(symbol).await?;
        debug!(%symbol, %vault_addr, %underlying_addr, "Deployed equity vault");
        equity_addresses.push(((*symbol).to_owned(), vault_addr, underlying_addr));
    }

    Ok(equity_addresses)
}

async fn fund_taker_with_equity<P: Provider + Clone>(
    base_chain: &BaseChain<P>,
    equity_addresses: &[(String, Address, Address)],
) -> anyhow::Result<()> {
    debug!("Funding taker with equity vault shares");
    let taker_equity: U256 = parse_units("100000", 18)?.into();

    for (symbol, vault_addr, _underlying_addr) in equity_addresses {
        DeployableERC20::new(*vault_addr, &base_chain.provider)
            .transfer(base_chain.taker, taker_equity)
            .send()
            .await?
            .get_receipt()
            .await?;
        debug!(%symbol, "Funded taker");
    }

    Ok(())
}

async fn start_broker_mock(
    equity_prices: &[(&str, Float)],
    equity_positions: &[(&str, Float)],
    initial_cash: Option<Float>,
) -> anyhow::Result<Arc<AlpacaBrokerMock>> {
    let (symbol_prices, symbol_positions) = build_mock_positions(equity_prices, equity_positions)?;

    info!("Starting mock services");
    let broker_service = Arc::new(
        AlpacaBrokerMock::start()
            .symbol_fill_prices(symbol_prices)
            .symbol_positions(symbol_positions)
            .maybe_initial_cash(initial_cash)
            .call()
            .await,
    );
    debug!(broker_url = %broker_service.base_url(), "Broker mock started");

    Ok(broker_service)
}

type MockBrokerState = (Vec<(Symbol, Float)>, Vec<MockPosition>);

fn build_mock_positions(
    equity_prices: &[(&str, Float)],
    equity_positions: &[(&str, Float)],
) -> anyhow::Result<MockBrokerState> {
    let symbol_prices: Vec<(Symbol, Float)> = equity_prices
        .iter()
        .map(|(symbol, price)| Ok((Symbol::new(*symbol)?, *price)))
        .collect::<anyhow::Result<_>>()?;

    let price_lookup: HashMap<Symbol, Float> = equity_prices
        .iter()
        .map(|(symbol, price)| Ok((Symbol::new(*symbol)?, *price)))
        .collect::<anyhow::Result<_>>()?;

    let symbol_positions: Vec<MockPosition> = equity_positions
        .iter()
        .map(|(symbol, quantity)| {
            let sym = Symbol::new(*symbol)?;
            let price = price_lookup.get(&sym).ok_or_else(|| {
                anyhow::anyhow!("no price configured for position symbol {symbol}")
            })?;
            let market_value =
                (*quantity * *price).map_err(|err| anyhow::anyhow!("Float mul failed: {err:?}"))?;
            Ok(MockPosition {
                symbol: sym,
                quantity: *quantity,
                market_value,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    Ok((symbol_prices, symbol_positions))
}

async fn start_tokenization_mock<P: Provider + Clone + 'static>(
    broker_service: &Arc<AlpacaBrokerMock>,
    equity_addresses: &[(String, Address, Address)],
    base_chain: &BaseChain<P>,
) -> anyhow::Result<AlpacaTokenizationMock> {
    let mut tokenization_service =
        AlpacaTokenizationMock::start(broker_service.server(), broker_service.clone());

    // Map both vault and underlying token addresses to symbol so the
    // redemption watcher can resolve the symbol regardless of which
    // ERC-20 contract emits the Transfer event.
    let token_symbols: HashMap<Address, String> = equity_addresses
        .iter()
        .flat_map(|(symbol, vault_addr, underlying_addr)| {
            [
                (*vault_addr, symbol.clone()),
                (*underlying_addr, symbol.clone()),
            ]
        })
        .collect();
    tokenization_service
        .start_redemption_watcher(
            base_chain.provider.clone(),
            REDEMPTION_WALLET,
            token_symbols,
        )
        .await?;
    debug!("Redemption watcher started");

    let mint_token_addresses: HashMap<String, Address> = equity_addresses
        .iter()
        .map(|(symbol, _vault_addr, underlying_addr)| (symbol.clone(), *underlying_addr))
        .collect();
    tokenization_service
        .start_mint_executor(base_chain.minter_provider.clone(), mint_token_addresses);
    debug!("Mint executor started");

    Ok(tokenization_service)
}
