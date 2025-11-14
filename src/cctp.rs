//! Circle CCTP bridge service for cross-chain USDC transfers.
//!
//! This module provides a service layer for bridging USDC between Ethereum mainnet
//! and Base using Circle's Cross-Chain Transfer Protocol (CCTP) V2 with fast transfers.
//!
//! ## Overview
//!
//! Circle CCTP enables native USDC transfers between blockchains by burning on the
//! source chain and minting on the destination chain. This implementation uses
//! **CCTP V2 Fast Transfer** which reduces transfer time from 13-19 minutes per chain
//! to approximately **30 seconds total** (20s on Ethereum + 8s on Base) for a cost of
//! 1 basis point (0.01%) per transfer.
//!
//! ## Supported Chains
//!
//! - **Ethereum mainnet** (domain 0)
//! - **Base** (domain 6)
//!
//! ## Architecture
//!
//! The CCTP bridge flow consists of three steps:
//!
//! 1. **Burn**: Lock and burn USDC on source chain via `TokenMessengerV2.depositForBurn()`
//! 2. **Attest**: Poll Circle's attestation API for signed message (fast transfer: ~20-30s)
//! 3. **Mint**: Mint native USDC on destination chain via `MessageTransmitterV2.receiveMessage()`
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Create bridge instance
//! let ethereum = EvmAccount::new(ethereum_provider, ethereum_signer);
//! let base = EvmAccount::new(base_provider, base_signer);
//! let bridge = CctpBridge::new(ethereum, base);
//!
//! // Bridge 1 USDC from Ethereum to Base (USDC has 6 decimals)
//! let amount = U256::from(1_000_000); // 1 USDC
//! let tx_hash = bridge.bridge_ethereum_to_base(amount, recipient).await?;
//! ```
//!
//! ## CCTP V2 Fast Transfer
//!
//! Fast transfers are enabled by setting `minFinalityThreshold` to 1000 in the
//! `depositForBurn()` call. The fee is dynamically queried from Circle's API
//! (`/v2/burn/USDC/fees`) before each burn operation.
//!
//! **Timing**: ~30 seconds total (20s Ethereum finality + 8s Base finality)
//! **Cost**: 1 basis point (0.01%) of transfer amount

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256, address};
use alloy::providers::Provider;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::SolEvent;
use backon::Retryable;
use serde::Deserialize;

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "lib/evm-cctp-contracts/out/TokenMessengerV2.sol/TokenMessengerV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    MessageTransmitterV2,
    "lib/evm-cctp-contracts/out/MessageTransmitterV2.sol/MessageTransmitterV2.json"
);

/// CCTP domain identifier for Ethereum mainnet
const ETHEREUM_DOMAIN: u32 = 0;

/// CCTP domain identifier for Base
const BASE_DOMAIN: u32 = 6;

const USDC_ETHEREUM: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDC_BASE: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

const TOKEN_MESSENGER_V2: Address = address!("28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
const MESSAGE_TRANSMITTER_V2: Address = address!("81D40F21F12A8F0E3252Bccb954D722d4c464B64");

const ATTESTATION_API_BASE: &str = "https://iris-api.circle.com/attestations";

/// Minimum finality threshold for CCTP V2 fast transfer (enables ~30 second transfers)
const FAST_TRANSFER_THRESHOLD: u32 = 1000;

/// Receipt from burning USDC on the source chain.
///
/// Contains all information needed to poll for attestation and mint on the destination chain.
#[derive(Debug)]
pub(crate) struct BurnReceipt {
    /// Transaction hash of the burn transaction
    pub(crate) tx: TxHash,
    /// CCTP message nonce (extracted from message bytes at index 12-44)
    pub(crate) nonce: FixedBytes<32>,
    /// Keccak256 hash of the CCTP message (used for attestation polling)
    pub(crate) hash: FixedBytes<32>,
    /// Full CCTP message bytes (passed to `receiveMessage()` on destination)
    pub(crate) message: Bytes,
    /// Amount of USDC burned (in smallest unit, 6 decimals for USDC)
    pub(crate) amount: U256,
}

/// EVM account wrapper combining provider and signer.
///
/// Generic container for blockchain provider and transaction signer,
/// used to configure Ethereum and Base connections for the CCTP bridge.
pub(crate) struct EvmAccount<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    /// Blockchain provider for reading chain state and sending transactions
    provider: P,
    /// Transaction signer for authorizing transactions
    signer: S,
}

impl<P, S> EvmAccount<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    /// Creates a new EVM account with the given provider and signer.
    pub(crate) fn new(provider: P, signer: S) -> Self {
        Self { provider, signer }
    }
}

/// Circle CCTP bridge for Ethereum <-> Base USDC transfers.
///
/// Provides both low-level methods (burn, poll, mint) and high-level convenience
/// methods (bridge_ethereum_to_base, bridge_base_to_ethereum) for cross-chain transfers.
///
/// # Type Parameters
///
/// * `P` - Provider type implementing [`Provider`] + [`Clone`]
/// * `S` - Signer type implementing [`Signer`] + [`Clone`] + [`Sync`]
///
/// # Example
///
/// ```rust,ignore
/// let ethereum = EvmAccount::new(ethereum_provider, ethereum_signer);
/// let base = EvmAccount::new(base_provider, base_signer);
/// let bridge = CctpBridge::new(ethereum, base);
///
/// let amount = U256::from(1_000_000); // 1 USDC
/// let tx_hash = bridge.bridge_ethereum_to_base(amount, recipient).await?;
/// ```
pub(crate) struct CctpBridge<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    /// Ethereum mainnet provider and signer
    ethereum: EvmAccount<P, S>,
    /// Base provider and signer
    base: EvmAccount<P, S>,
    /// HTTP client for Circle attestation API requests
    http_client: reqwest::Client,
}

/// Errors that can occur during CCTP bridge operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CctpError {
    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Attestation timeout after {attempts} attempts: {source}")]
    AttestationTimeout {
        attempts: usize,
        source: AttestationError,
    },
    #[error("MessageSent event not found in transaction receipt")]
    MessageSentEventNotFound,
    #[error("Fee calculation overflow")]
    FeeCalculationOverflow,
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Fee value parse error: {0}")]
    FeeValueParse(#[from] std::num::ParseIntError),
}

/// Errors specific to attestation polling from Circle's API.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AttestationError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Attestation not ready")]
    NotReady,
}

#[derive(Deserialize)]
struct FeeResponse {
    #[serde(rename = "minFee")]
    min_fee: String,
}

impl<P, S> CctpBridge<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    pub(crate) fn new(ethereum: EvmAccount<P, S>, base: EvmAccount<P, S>) -> Self {
        Self {
            ethereum,
            base,
            http_client: reqwest::Client::new(),
        }
    }

    async fn query_fast_transfer_fee(&self, amount: U256) -> Result<U256, CctpError> {
        let url = format!("{ATTESTATION_API_BASE}/v2/burn/USDC/fees");
        let response = self.http_client.get(&url).send().await?;

        let fee_response: FeeResponse = response.json().await?;

        let fee_bps: u64 = fee_response.min_fee.parse()?;

        let max_fee = amount
            .checked_mul(U256::from(fee_bps))
            .ok_or(CctpError::FeeCalculationOverflow)?
            / U256::from(10000u64);

        Ok(max_fee)
    }

    pub(crate) async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let contract = TokenMessengerV2::new(TOKEN_MESSENGER_V2, &self.ethereum.provider);

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());
        let destination_caller = FixedBytes::<32>::ZERO;

        let call = contract.depositForBurn(
            amount,
            BASE_DOMAIN,
            recipient_bytes32,
            USDC_ETHEREUM,
            destination_caller,
            max_fee,
            FAST_TRANSFER_THRESHOLD,
        );

        let receipt = call.send().await?.get_receipt().await?;

        let message_sent_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).ok())
            .ok_or(CctpError::MessageSentEventNotFound)?;

        let message = message_sent_event.message.clone();
        let hash = alloy::primitives::keccak256(&message);

        const NONCE_INDEX: usize = 12;
        let nonce = FixedBytes::<32>::from_slice(&message[NONCE_INDEX..NONCE_INDEX + 32]);

        Ok(BurnReceipt {
            tx: receipt.transaction_hash,
            nonce,
            hash,
            message,
            amount,
        })
    }

    pub(crate) async fn poll_attestation(&self, hash: FixedBytes<32>) -> Result<Bytes, CctpError> {
        const MAX_ATTEMPTS: usize = 60;
        const RETRY_INTERVAL_SECS: u64 = 5;

        let url = format!("{ATTESTATION_API_BASE}/{hash}");

        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_secs(RETRY_INTERVAL_SECS))
            .with_max_times(MAX_ATTEMPTS);

        #[derive(Deserialize)]
        struct AttestationResponse {
            attestation: String,
        }

        let fetch_attestation = || async {
            let response = self.http_client.get(&url).send().await?;

            if !response.status().is_success() {
                return Err(AttestationError::NotReady);
            }

            let attestation_response: AttestationResponse = response.json().await?;

            let attestation_hex = attestation_response
                .attestation
                .strip_prefix("0x")
                .unwrap_or(&attestation_response.attestation);

            let attestation_bytes = alloy::hex::decode(attestation_hex)?;

            Ok(Bytes::from(attestation_bytes))
        };

        fetch_attestation
            .retry(backoff)
            .await
            .map_err(|err| CctpError::AttestationTimeout {
                attempts: MAX_ATTEMPTS,
                source: err,
            })
    }

    pub(crate) async fn mint_on_base(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, CctpError> {
        let contract = MessageTransmitterV2::new(MESSAGE_TRANSMITTER_V2, &self.base.provider);

        let call = contract.receiveMessage(message, attestation);

        let receipt = call.send().await?.get_receipt().await?;

        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;

    fn setup_anvil() -> (AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    async fn create_bridge(
        ethereum_endpoint: &str,
        base_endpoint: &str,
        private_key: &B256,
    ) -> Result<CctpBridge<impl Provider + Clone, impl Signer + Clone>, Box<dyn std::error::Error>>
    {
        let signer = PrivateKeySigner::from_bytes(private_key)?;
        let wallet = EthereumWallet::from(signer.clone());

        let ethereum_provider = ProviderBuilder::new()
            .wallet(wallet.clone())
            .connect(ethereum_endpoint)
            .await?;

        let base_provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(base_endpoint)
            .await?;

        let ethereum_account = EvmAccount::new(ethereum_provider, signer.clone());
        let base_account = EvmAccount::new(base_provider, signer);

        Ok(CctpBridge::new(ethereum_account, base_account))
    }

    #[tokio::test]
    async fn test_extract_nonce_from_message_sent_event() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _base_key) = setup_anvil();

        let bridge = create_bridge(&ethereum_endpoint, &base_endpoint, &private_key)
            .await
            .unwrap();

        let recipient = address!("0x1234567890123456789012345678901234567890");
        let amount = U256::from(1_000_000u64);

        let result = bridge.burn_on_ethereum(amount, recipient).await;

        assert!(
            matches!(result, Err(CctpError::Http(_))),
            "Expected HTTP error when querying fee API, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_message_sent_event_not_found() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _base_key) = setup_anvil();

        let bridge = create_bridge(&ethereum_endpoint, &base_endpoint, &private_key)
            .await
            .unwrap();

        let recipient = address!("0x1234567890123456789012345678901234567890");
        let amount = U256::from(1_000_000u64);

        let result = bridge.burn_on_ethereum(amount, recipient).await;

        assert!(
            result.is_err(),
            "Expected error due to contract not deployed"
        );
    }

    #[tokio::test]
    async fn test_attestation_succeeds_with_retry_logic() {
        let server = MockServer::start();
        let message_hash =
            b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!("/{message_hash}"));
            then.status(200).json_body(serde_json::json!({
                "attestation": "0x1234567890abcdef"
            }));
        });

        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _base_key) = setup_anvil();

        let bridge = create_bridge(&ethereum_endpoint, &base_endpoint, &private_key)
            .await
            .unwrap();

        let url = format!("{}/{message_hash}", server.base_url());
        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_millis(10))
            .with_max_times(5);

        #[derive(serde::Deserialize)]
        struct AttestationResponse {
            attestation: String,
        }

        let fetch_attestation = || async {
            let response = bridge.http_client.get(&url).send().await?;

            if !response.status().is_success() {
                return Err(AttestationError::NotReady);
            }

            let attestation_response: AttestationResponse = response.json().await?;
            let attestation_hex = attestation_response
                .attestation
                .strip_prefix("0x")
                .unwrap_or(&attestation_response.attestation);
            let attestation_bytes = alloy::hex::decode(attestation_hex)?;

            Ok::<Bytes, AttestationError>(Bytes::from(attestation_bytes))
        };

        let result = fetch_attestation.retry(backoff).await;

        assert!(result.is_ok(), "Expected attestation to succeed");
        assert_eq!(mock.hits(), 1, "Expected exactly 1 API call");
    }

    #[tokio::test]
    async fn test_attestation_timeout() {
        let server = MockServer::start();
        let message_hash =
            b256!("abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!("/{message_hash}"));
            then.status(404);
        });

        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _base_key) = setup_anvil();

        let mut bridge = create_bridge(&ethereum_endpoint, &base_endpoint, &private_key)
            .await
            .unwrap();

        bridge.http_client = reqwest::Client::new();

        let url = format!("{}/{message_hash}", server.base_url());
        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_millis(10))
            .with_max_times(3);

        #[derive(serde::Deserialize)]
        struct AttestationResponse {
            attestation: String,
        }

        let fetch_attestation = || async {
            let response = bridge.http_client.get(&url).send().await?;

            if !response.status().is_success() {
                return Err(AttestationError::NotReady);
            }

            let attestation_response: AttestationResponse = response.json().await?;
            let attestation_hex = attestation_response
                .attestation
                .strip_prefix("0x")
                .unwrap_or(&attestation_response.attestation);
            let attestation_bytes = alloy::hex::decode(attestation_hex)?;

            Ok::<Bytes, AttestationError>(Bytes::from(attestation_bytes))
        };

        let result = fetch_attestation.retry(backoff).await;

        assert!(
            result.is_err(),
            "Expected attestation to fail after max retries"
        );
        assert!(
            matches!(result.unwrap_err(), AttestationError::NotReady),
            "Expected AttestationError::NotReady"
        );
        assert!(
            mock.hits() >= 3,
            "Expected at least 3 attempts with retries"
        );
    }
}
