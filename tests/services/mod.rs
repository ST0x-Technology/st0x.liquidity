use alloy::primitives::Address;
use alloy::providers::Provider;
use rust_decimal::Decimal;
use st0x_execution::Symbol;
use std::path::PathBuf;
use tempfile::TempDir;

use crate::services::alpaca_broker::AlpacaBrokerMock;
use crate::services::alpaca_tokenization::AlpacaTokenizationMock;
use crate::services::base_chain::BaseChain;
use crate::services::cctp_attestation::CctpAttestationMock;
use crate::services::ethereum_chain::EthereumChain;

pub mod alpaca_broker;
pub mod alpaca_tokenization;
pub mod base_chain;
pub mod cctp_attestation;
pub mod cctp_attester;
pub mod ethereum_chain;

const BASE_RPC_URL: &str =
    "https://api.developer.coinbase.com/rpc/v1/base/DD1T1ZCY6bZ09lHv19TjBHi1saRZCHFF";
const ETHEREUM_RPC_URL: &str = "https://eth-mainnet.g.alchemy.com/v2/X6rDkPx_2PTbeNkRSXd2r";

pub struct TestInfra<P1, P2> {
    /// Kept alive so the temp directory isn't deleted while the test runs.
    _db_dir: TempDir,
    pub db_path: PathBuf,
    pub base_chain: BaseChain<P1>,
    pub ethereum_chain: EthereumChain<P2>,
    pub broker_service: AlpacaBrokerMock,
    pub tokenization_service: AlpacaTokenizationMock,
    pub attestation_service: CctpAttestationMock,
    /// `(symbol, vault_address, underlying_address)` per deployed equity vault.
    pub equity_addresses: Vec<(String, Address, Address)>,
}

impl TestInfra<(), ()> {
    pub async fn start(
        equities: Vec<(&str, Decimal)>,
    ) -> anyhow::Result<TestInfra<impl Provider + Clone, impl Provider + Clone>> {
        let db_dir = tempfile::tempdir()?;
        let db_path = db_dir.path().join("e2e.sqlite");

        let mut base_chain = BaseChain::start(BASE_RPC_URL).await?;
        let mut equity_addresses = Vec::new();
        for (symbol, _price) in &equities {
            let (vault_addr, underlying_addr) = base_chain.deploy_equity_vault(symbol).await?;
            equity_addresses.push(((*symbol).to_owned(), vault_addr, underlying_addr));
        }

        let ethereum_chain = EthereumChain::start(ETHEREUM_RPC_URL).await?;

        let symbol_prices: Vec<(Symbol, Decimal)> = equities
            .iter()
            .map(|(symbol, price)| Ok((Symbol::new(*symbol)?, *price)))
            .collect::<anyhow::Result<_>>()?;

        let broker_service = AlpacaBrokerMock::start()
            .symbol_fill_prices(symbol_prices)
            .call()
            .await;
        let tokenization_service = AlpacaTokenizationMock::start(broker_service.server());
        let attestation_service = CctpAttestationMock::start().await;

        Ok(TestInfra {
            _db_dir: db_dir,
            db_path,
            base_chain,
            ethereum_chain,
            broker_service,
            tokenization_service,
            attestation_service,
            equity_addresses,
        })
    }
}
