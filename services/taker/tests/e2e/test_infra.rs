//! Test infrastructure for taker e2e tests.
//!
//! Provides [`TakerTestInfra`] which wires together an Anvil-based
//! `RainOrderBook` and a temporary SQLite database for the taker bot.

use std::path::PathBuf;

use alloy::primitives::{Address, utils::parse_units};
use alloy::providers::Provider;
use tempfile::TempDir;

use st0x_execution::Symbol;
use st0x_shared::test_support::RainOrderBook;

/// Assembled test environment for taker e2e tests.
pub struct TakerTestInfra<P> {
    /// Kept alive so the temp directory isn't deleted during the test.
    _db_dir: TempDir,
    pub db_path: PathBuf,
    pub rain: RainOrderBook<P>,
    pub usdc_addr: Address,
    pub wt_aapl_addr: Address,
}

impl TakerTestInfra<()> {
    pub async fn start() -> anyhow::Result<TakerTestInfra<impl Provider + Clone>> {
        let db_dir = tempfile::tempdir()?;
        let db_path = db_dir.path().join("e2e.sqlite");

        let rain = RainOrderBook::start().await?;

        let usdc_addr = rain
            .deploy_erc20("USD Coin", "USDC", 6, parse_units("1000000", 6)?.into())
            .await?;

        let wt_aapl_addr = rain
            .deploy_erc20(
                "Wrapped AAPL",
                "wtAAPL",
                18,
                parse_units("1000000", 18)?.into(),
            )
            .await?;

        Ok(TakerTestInfra {
            _db_dir: db_dir,
            db_path,
            rain,
            usdc_addr,
            wt_aapl_addr,
        })
    }
}

#[allow(dead_code)]
impl<P> TakerTestInfra<P> {
    pub fn aapl_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    pub fn database_url(&self) -> String {
        format!("sqlite:{}?mode=rwc", self.db_path.display())
    }
}
