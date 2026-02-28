//! Mocks and helpers for end-to-end tests.
//!
//! Provides `TestInfra` -- the composite test harness that wires together
//! `AlpacaBrokerMock`, `AlpacaTokenizationMock`, `BaseChain`,
//! `DeployableERC20`, and `CctpAttestationMock` into a single startup call.

use std::collections::HashMap;
use std::path::PathBuf;

use alloy::primitives::{Address, U256, utils::parse_units};
use alloy::providers::Provider;
use rust_decimal::Decimal;
use st0x_execution::Symbol;
use tempfile::TempDir;

use crate::services::alpaca_broker::{AlpacaBrokerMock, MockPosition};
use crate::services::alpaca_tokenization::{AlpacaTokenizationMock, REDEMPTION_WALLET};
use crate::services::base_chain::{BaseChain, DeployableERC20};
use crate::services::cctp::attestation::CctpAttestationMock;

pub mod alpaca_broker;
pub mod alpaca_tokenization;
pub mod base_chain;
pub mod cctp;

pub struct TestInfra<P> {
    /// Kept alive so the temp directory isn't deleted while the test runs.
    _db_dir: TempDir,
    pub db_path: PathBuf,
    pub base_chain: BaseChain<P>,
    pub broker_service: AlpacaBrokerMock,
    pub tokenization_service: AlpacaTokenizationMock,
    pub attestation_service: CctpAttestationMock,
    /// `(symbol, vault_address, underlying_address)` per deployed equity vault.
    pub equity_addresses: Vec<(String, Address, Address)>,
}

impl TestInfra<()> {
    pub async fn start(
        equity_prices: Vec<(&str, Decimal)>,
        equity_positions: Vec<(&str, Decimal)>,
    ) -> anyhow::Result<TestInfra<impl Provider + Clone>> {
        let db_dir = tempfile::tempdir()?;
        let db_path = db_dir.path().join("e2e.sqlite");

        let mut base_chain = BaseChain::start().await?;
        let mut equity_addresses = Vec::new();
        for (symbol, _price) in &equity_prices {
            let (vault_addr, underlying_addr) = base_chain.deploy_equity_vault(symbol).await?;
            equity_addresses.push(((*symbol).to_owned(), vault_addr, underlying_addr));
        }

        // Fund taker with equity vault shares so it can take BuyEquity
        // orders (where the taker pays equity tokens to the order).
        let taker_equity: U256 = parse_units("100000", 18)?.into();
        for (_symbol, vault_addr, _underlying_addr) in &equity_addresses {
            DeployableERC20::new(*vault_addr, &base_chain.provider)
                .transfer(base_chain.taker, taker_equity)
                .send()
                .await?
                .get_receipt()
                .await?;
        }

        let symbol_prices: Vec<(Symbol, Decimal)> = equity_prices
            .iter()
            .map(|(symbol, price)| Ok((Symbol::new(*symbol)?, *price)))
            .collect::<anyhow::Result<_>>()?;

        let price_lookup: HashMap<Symbol, Decimal> = symbol_prices.iter().cloned().collect();

        let symbol_positions: Vec<MockPosition> = equity_positions
            .iter()
            .map(|(symbol, qty)| {
                let sym = Symbol::new(*symbol)?;
                let price = price_lookup.get(&sym).ok_or_else(|| {
                    anyhow::anyhow!("no price configured for position symbol {symbol}")
                })?;
                Ok(MockPosition {
                    symbol: sym,
                    qty: *qty,
                    market_value: qty * price,
                })
            })
            .collect::<anyhow::Result<_>>()?;

        let broker_service = AlpacaBrokerMock::start()
            .symbol_fill_prices(symbol_prices)
            .symbol_positions(symbol_positions)
            .call()
            .await;
        let mut tokenization_service = AlpacaTokenizationMock::start(broker_service.server());
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
        tokenization_service.start_redemption_watcher(
            base_chain.provider.clone(),
            REDEMPTION_WALLET,
            token_symbols,
        )?;
        let attestation_service = CctpAttestationMock::start().await;

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
