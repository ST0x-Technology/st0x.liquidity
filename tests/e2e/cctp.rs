//! CCTP infrastructure for e2e USDC rebalancing tests.
//!
//! Deploys CCTP V2 contracts on a second (Ethereum) Anvil chain and on the
//! existing Base chain from [`TestInfra`], links them, replaces USDC bytecode
//! with mint/burn tokens, and starts the attestation watcher.
//!
//! The bot's wallets start with zero USDC. Inbound USDC arrives only through
//! the rebalance flow itself: CCTP mints (BaseToAlpaca) or the Alpaca
//! withdrawal executor that mints on Ethereum when the broker mock records
//! an OUTGOING transfer (AlpacaToBase). This mirrors production semantics
//! where wallet balances are non-zero only mid-transfer.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;
use alloy::signers::local::PrivateKeySigner;
use futures_util::{StreamExt, stream};
use rain_math_float::Float;
use task_supervisor::{SupervisedTask, SupervisorBuilder, SupervisorHandle, TaskResult};
use tokio::task::JoinHandle;
use tracing::warn;

use st0x_bridge::cctp::{
    TestMintBurnToken, deploy_cctp_on_chain, link_chains, mint_usdc, set_max_burn_amount,
};
use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, TransferFlow};
use st0x_finance::Usdc;

use crate::base_chain::USDC_BASE;
use crate::test_infra::TestInfra;
use st0x_float_macro::float;

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
/// USDC with a mint/burn token, registers broker wallet endpoints, starts
/// the attestation watcher, and starts the Alpaca-withdrawal executor that
/// mints USDC on Ethereum when the broker mock records an OUTGOING wallet
/// transfer (simulating Alpaca actually sending USDC on completion).
///
/// The bot's wallets begin with zero USDC; balances arise only mid-transfer
/// (Alpaca withdrawal -> Ethereum -> CCTP burn / CCTP mint -> Base wallet
/// or Ethereum wallet -> Alpaca deposit), matching production semantics.
pub struct CctpInfra {
    pub ethereum_endpoint: String,
    attestation_base_url: String,
    token_messenger: Address,
    message_transmitter: Address,

    // Kept alive so resources aren't dropped while the test runs.
    _ethereum_anvil: AnvilInstance,
    _attestation_watcher: JoinHandle<()>,
    _supervisor: SupervisorHandle,
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

        let mut ethereum_cctp = deploy_cctp_on_chain(
            &ethereum_endpoint,
            &cctp_deployer_key,
            0, // Ethereum domain
            attester_address,
        )
        .await?;

        let mut base_cctp = deploy_cctp_on_chain(
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

        set_max_burn_amount(
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

        set_max_burn_amount(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            base_cctp.token_minter,
            USDC_BASE,
            U256::from(1_000_000_000_000u64),
        )
        .await?;

        base_cctp.usdc = USDC_BASE;

        link_chains(
            &ethereum_endpoint,
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            &ethereum_cctp,
            &base_cctp,
        )
        .await?;

        infra
            .broker_service
            .register_wallet_endpoints(infra.base_chain.owner);

        // Simulate Alpaca's withdrawal-side leg: when the broker mock
        // records an OUTGOING wallet transfer, mint USDC on Ethereum to
        // the recipient. Without this, AlpacaToBase tests would have
        // nothing to burn during the CCTP step.
        let supervisor = SupervisorBuilder::default()
            .with_task(
                "alpaca-withdrawal-executor",
                AlpacaWithdrawalExecutor {
                    broker: Arc::clone(&infra.broker_service),
                    ethereum_endpoint: ethereum_endpoint.clone(),
                    deployer_key: cctp_deployer_key,
                },
            )
            .build()
            .run();

        // USDC/USD conversion is a 1:1 stablecoin pair on Alpaca
        infra
            .broker_service
            .set_symbol_fill_price(Symbol::new("USDCUSD").unwrap(), float!(1));

        let eth_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await?;
        let base_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&infra.base_chain.endpoint())
            .await?;
        let attestation_watcher = infra
            .attestation_service
            .start_watcher(eth_watcher_provider, base_watcher_provider, attester_key)
            .await?;

        Ok(Self {
            ethereum_endpoint,
            attestation_base_url: infra.attestation_service.base_url(),
            token_messenger: base_cctp.token_messenger,
            message_transmitter: base_cctp.message_transmitter,
            _ethereum_anvil: ethereum_anvil,
            _attestation_watcher: attestation_watcher,
            _supervisor: supervisor,
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

/// Simulates Alpaca's withdrawal-side leg by polling the broker mock for
/// OUTGOING wallet transfers and minting USDC on Ethereum to the recipient.
///
/// Mints on first observation so the bot's CCTP burn finds USDC available
/// before the mock flips the transfer's status to `Complete`. The
/// per-transfer dedupe set is task-local; if `task-supervisor` restarts the
/// task after a panic, we re-walk all transfers but the on-chain mint
/// behaves idempotently per `transfer_id` only within a single run -- a
/// restart could double-mint, which is acceptable for tests.
#[derive(Clone)]
struct AlpacaWithdrawalExecutor {
    broker: Arc<AlpacaBrokerMock>,
    ethereum_endpoint: String,
    deployer_key: B256,
}

/// Outcome of a single mint attempt for an outgoing transfer.
enum MintAttempt {
    /// Terminal outcome (success or permanent validation failure). The
    /// transfer_id should be recorded so we don't re-attempt next tick.
    Done,
    /// Transient failure -- leave the transfer_id off the handled set so
    /// the outer loop re-attempts on the next tick.
    Retry,
}

impl AlpacaWithdrawalExecutor {
    async fn try_mint(&self, transfer_id: &str, recipient: Address, amount: Float) -> MintAttempt {
        let raw_amount = match Usdc::new(amount).to_u256_6_decimals() {
            Ok(raw) => raw,
            Err(error) => {
                warn!(
                    %transfer_id,
                    ?error,
                    "withdrawal executor: invalid USDC amount, skipping"
                );
                return MintAttempt::Done;
            }
        };

        match mint_usdc(
            &self.ethereum_endpoint,
            &self.deployer_key,
            USDC_ETHEREUM,
            recipient,
            raw_amount,
        )
        .await
        {
            Ok(()) => MintAttempt::Done,
            Err(error) => {
                warn!(
                    %transfer_id,
                    ?error,
                    "withdrawal executor: mint_usdc failed, will retry"
                );
                MintAttempt::Retry
            }
        }
    }
}

impl SupervisedTask for AlpacaWithdrawalExecutor {
    async fn run(&mut self) -> TaskResult {
        let this = &*self;
        let mut minted: HashSet<String> = HashSet::new();

        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let seen = &minted;
            let newly_handled: HashSet<String> = stream::iter(this.broker.wallet_transfers())
                .filter_map(|transfer| async move {
                    let TransferFlow::Outgoing { recipient } = transfer.flow else {
                        return None;
                    };

                    (!seen.contains(&transfer.transfer_id)).then_some((
                        transfer.transfer_id,
                        recipient,
                        transfer.amount,
                    ))
                })
                .filter_map(|(transfer_id, recipient, amount)| async move {
                    match this.try_mint(&transfer_id, recipient, amount).await {
                        MintAttempt::Done => Some(transfer_id),
                        MintAttempt::Retry => None,
                    }
                })
                .collect()
                .await;

            minted.extend(newly_handled);
        }
    }
}
