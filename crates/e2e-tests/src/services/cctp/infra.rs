//! CCTP infrastructure for e2e USDC rebalancing tests.
//!
//! Deploys CCTP V2 contracts on a second (Ethereum) Anvil chain and on the
//! existing Base chain from [`TestInfra`], links them, replaces USDC bytecode
//! with mint/burn tokens, mints USDC on both chains, and starts the
//! attestation watcher.

use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use st0x_exact_decimal::ExactDecimal;
use st0x_execution::Symbol;
use tokio::task::JoinHandle;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestMintBurnToken, "src/services/cctp/TestMintBurnToken.json"
);

use super::contracts;
use crate::services::TestInfra;
use crate::services::base_chain::USDC_BASE;

/// USDC on Ethereum mainnet.
pub const USDC_ETHEREUM: Address =
    alloy::primitives::address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

/// CCTP contract addresses for USDC rebalancing e2e tests.
pub struct CctpOverrides {
    pub attestation_base_url: String,
    pub token_messenger: Address,
    pub message_transmitter: Address,
}

/// CCTP layer that sits on top of [`TestInfra`].
///
/// Deploys CCTP V2 contracts on a second (Ethereum) Anvil chain and on
/// the existing Base chain from `TestInfra`, links them, replaces Base
/// USDC with a mint/burn token, mints USDC on both chains, registers
/// broker wallet endpoints, and starts the attestation watcher.
pub struct CctpInfra {
    pub ethereum_endpoint: String,
    attestation_base_url: String,
    token_messenger: Address,
    message_transmitter: Address,

    // Kept alive so resources aren't dropped while the test runs.
    _ethereum_anvil: AnvilInstance,
    _attestation_watcher: JoinHandle<()>,
}

impl CctpInfra {
    /// Deploys CCTP infrastructure on top of an existing [`TestInfra`].
    pub async fn start<BP: Provider + Clone>(infra: &TestInfra<BP>) -> anyhow::Result<Self> {
        let ethereum_anvil = Anvil::new().chain_id(1u64).spawn();
        let ethereum_endpoint = ethereum_anvil.endpoint();

        // Use Anvil account #2 as CCTP deployer to avoid nonce collisions
        // with account #0 (owner), whose provider is already used by
        // BaseChain for contract deployments and vault operations. Each
        // contracts function creates a fresh provider from key+endpoint,
        // so sharing the owner key causes "nonce too low" when Base's
        // block_time(1) hasn't yet mined the owner's pending transactions.
        let cctp_deployer_key = B256::from_slice(&ethereum_anvil.keys()[2].to_bytes());
        let cctp_deployer = PrivateKeySigner::from_bytes(&cctp_deployer_key)?.address();

        // Fund the CCTP deployer on Base (Ethereum Anvil pre-funds all
        // default accounts, but Base's Anvil instance is separate).
        let hundred_eth: U256 = parse_units("100", 18)?.into();
        infra
            .base_chain
            .provider
            .anvil_set_balance(cctp_deployer, hundred_eth)
            .await?;

        let attester_key = B256::random();
        let attester_address = PrivateKeySigner::from_bytes(&attester_key)?.address();

        let mut ethereum_cctp = contracts::deploy_cctp_on_chain(
            &ethereum_endpoint,
            &cctp_deployer_key,
            0, // Ethereum domain
            attester_address,
        )
        .await?;

        let mut base_cctp = contracts::deploy_cctp_on_chain(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            6, // Base domain
            attester_address,
        )
        .await?;

        let eth_provider = alloy::providers::ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await?;

        // The bot uses USDC at canonical addresses, not the freshly deployed
        // MockMintBurnToken. Replace bytecode at those addresses with
        // TestMintBurnToken so CCTP can mint/burn there.

        eth_provider
            .anvil_set_code(USDC_ETHEREUM, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await?;

        contracts::set_max_burn_amount(
            &ethereum_endpoint,
            &cctp_deployer_key,
            ethereum_cctp.token_minter,
            USDC_ETHEREUM,
            U256::from(1_000_000_000_000u64),
        )
        .await?;

        ethereum_cctp.usdc = USDC_ETHEREUM;

        // Base: place TestMintBurnToken at USDC_BASE. Storage slots
        // (name, symbol, decimals, balances) were already set by
        // deploy_usdc_at_base in TestInfra::start and persist across
        // anvil_set_code calls.
        infra
            .base_chain
            .provider
            .anvil_set_code(USDC_BASE, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await?;

        contracts::set_max_burn_amount(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            base_cctp.token_minter,
            USDC_BASE,
            U256::from(1_000_000_000_000u64),
        )
        .await?;

        base_cctp.usdc = USDC_BASE;

        contracts::link_chains(
            &ethereum_endpoint,
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            &ethereum_cctp,
            &base_cctp,
        )
        .await?;

        // MockMintBurnToken.mint() has no access control, so any key works.
        let mint_amount = U256::from(1_000_000_000_000u64); // 1M USDC

        contracts::mint_usdc(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            USDC_BASE,
            infra.base_chain.owner,
            mint_amount,
        )
        .await?;

        contracts::mint_usdc(
            &ethereum_endpoint,
            &cctp_deployer_key,
            USDC_ETHEREUM,
            infra.base_chain.owner,
            mint_amount,
        )
        .await?;

        infra
            .broker_service
            .register_wallet_endpoints(infra.base_chain.owner);

        // USDC/USD conversion is a 1:1 stablecoin pair on Alpaca
        infra.broker_service.set_symbol_fill_price(
            Symbol::force_new("USDCUSD".to_string()),
            ExactDecimal::parse("1")?,
        );

        let eth_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await?;
        let base_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&infra.base_chain.endpoint())
            .await?;
        let attestation_watcher = infra.attestation_service.start_watcher(
            eth_watcher_provider,
            base_watcher_provider,
            attester_key,
        );

        Ok(Self {
            ethereum_endpoint,
            attestation_base_url: infra.attestation_service.base_url(),
            token_messenger: base_cctp.token_messenger,
            message_transmitter: base_cctp.message_transmitter,
            _ethereum_anvil: ethereum_anvil,
            _attestation_watcher: attestation_watcher,
        })
    }

    pub fn cctp_overrides(&self) -> CctpOverrides {
        CctpOverrides {
            attestation_base_url: self.attestation_base_url.clone(),
            token_messenger: self.token_messenger,
            message_transmitter: self.message_transmitter,
        }
    }
}
