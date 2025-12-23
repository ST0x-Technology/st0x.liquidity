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

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256, address, keccak256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use backon::Retryable;
use serde::Deserialize;

use crate::bindings::IERC20;

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

pub(crate) const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub(crate) const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

pub(crate) const TOKEN_MESSENGER_V2: Address =
    address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
pub(crate) const MESSAGE_TRANSMITTER_V2: Address =
    address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");

const CIRCLE_API_BASE: &str = "https://iris-api.circle.com";

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

/// EVM chain connection with contract instances for CCTP operations.
pub(crate) struct Evm<P>
where
    P: Provider + Clone,
{
    /// Address of the account that owns tokens and signs transactions
    owner: Address,
    /// USDC token contract instance
    usdc: IERC20::IERC20Instance<P>,
    /// TokenMessengerV2 contract instance for CCTP burns
    token_messenger: TokenMessengerV2::TokenMessengerV2Instance<P>,
    /// MessageTransmitterV2 contract instance for CCTP mints
    message_transmitter: MessageTransmitterV2::MessageTransmitterV2Instance<P>,
}

impl<P> Evm<P>
where
    P: Provider + Clone,
{
    /// Creates a new EVM chain connection with the given provider and contract addresses.
    ///
    /// The `owner` address should be the account that will sign transactions
    /// (typically obtained from a signer via `.address()`).
    pub(crate) fn new(
        provider: P,
        owner: Address,
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
    ) -> Self {
        Self {
            owner,
            usdc: IERC20::new(usdc, provider.clone()),
            token_messenger: TokenMessengerV2::new(token_messenger, provider.clone()),
            message_transmitter: MessageTransmitterV2::new(message_transmitter, provider),
        }
    }
}

/// Circle CCTP bridge for Ethereum <-> Base USDC transfers.
///
/// Provides both low-level methods (burn, poll, mint) and high-level convenience
/// methods (bridge_ethereum_to_base, bridge_base_to_ethereum) for cross-chain transfers.
///
/// # Type Parameters
///
/// * `EP` - Ethereum provider type implementing [`Provider`] + [`Clone`]
/// * `BP` - Base provider type implementing [`Provider`] + [`Clone`]
///
/// # Example
///
/// ```rust,ignore
/// let bridge = CctpBridge::new(ethereum_evm, base_evm);
/// let tx_hash = bridge.bridge_ethereum_to_base(amount, recipient).await?;
/// ```
pub(crate) struct CctpBridge<EP, BP>
where
    EP: Provider + Clone,
    BP: Provider + Clone,
{
    ethereum: Evm<EP>,
    base: Evm<BP>,
    http_client: reqwest::Client,
    circle_api_base: String,
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

impl<EP, BP> CctpBridge<EP, BP>
where
    EP: Provider + Clone,
    BP: Provider + Clone,
{
    pub(crate) fn new(ethereum: Evm<EP>, base: Evm<BP>) -> Self {
        Self {
            ethereum,
            base,
            http_client: reqwest::Client::new(),
            circle_api_base: CIRCLE_API_BASE.to_string(),
        }
    }

    #[cfg(test)]
    fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }

    async fn query_fast_transfer_fee(&self, amount: U256) -> Result<U256, CctpError> {
        let url = format!("{}/v2/burn/USDC/fees", self.circle_api_base);
        let response = self.http_client.get(&url).send().await?;

        let fee_response: FeeResponse = response.json().await?;

        let fee_bps: u64 = fee_response.min_fee.parse()?;

        let max_fee = amount
            .checked_mul(U256::from(fee_bps))
            .ok_or(CctpError::FeeCalculationOverflow)?
            / U256::from(10000u64);

        Ok(max_fee)
    }

    async fn ensure_usdc_approval_ethereum(&self, amount: U256) -> Result<(), CctpError> {
        let owner = self.ethereum.owner;
        let spender = *self.ethereum.token_messenger.address();

        let allowance = self.ethereum.usdc.allowance(owner, spender).call().await?;

        if allowance < amount {
            self.ethereum
                .usdc
                .approve(spender, amount)
                .send()
                .await?
                .get_receipt()
                .await?;
        }

        Ok(())
    }

    async fn ensure_usdc_approval_base(&self, amount: U256) -> Result<(), CctpError> {
        let owner = self.base.owner;
        let spender = *self.base.token_messenger.address();

        let allowance = self.base.usdc.allowance(owner, spender).call().await?;

        if allowance < amount {
            self.base
                .usdc
                .approve(spender, amount)
                .send()
                .await?
                .get_receipt()
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        self.ensure_usdc_approval_ethereum(amount).await?;

        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let call = self.ethereum.token_messenger.depositForBurn(
            amount,
            BASE_DOMAIN,
            recipient_bytes32,
            *self.ethereum.usdc.address(),
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
        let hash = keccak256(&message);

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

        let url = format!("{CIRCLE_API_BASE}/attestations/{hash}");

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
        let call = self
            .base
            .message_transmitter
            .receiveMessage(message, attestation);

        let receipt = call.send().await?.get_receipt().await?;

        Ok(receipt.transaction_hash)
    }

    /// Burns USDC on Base chain for cross-chain transfer to Ethereum.
    ///
    /// This is the reverse direction of `burn_on_ethereum`.
    pub(crate) async fn burn_on_base(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        self.ensure_usdc_approval_base(amount).await?;

        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let call = self.base.token_messenger.depositForBurn(
            amount,
            ETHEREUM_DOMAIN,
            recipient_bytes32,
            *self.base.usdc.address(),
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
        let hash = keccak256(&message);

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

    /// Mints USDC on Ethereum chain after cross-chain transfer from Base.
    ///
    /// This is the reverse direction of `mint_on_base`.
    pub(crate) async fn mint_on_ethereum(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, CctpError> {
        let call = self
            .ethereum
            .message_transmitter
            .receiveMessage(message, attestation);

        let receipt = call.send().await?.get_receipt().await?;

        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::Signer;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolCall;
    use httpmock::prelude::*;
    use rand::Rng;

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
        usdc_address: Address,
    ) -> Result<CctpBridge<impl Provider + Clone, impl Provider + Clone>, Box<dyn std::error::Error>>
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

        let owner = signer.address();

        let ethereum = Evm::new(
            ethereum_provider,
            owner,
            usdc_address,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let base = Evm::new(
            base_provider,
            owner,
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        Ok(CctpBridge::new(ethereum, base))
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

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            USDC_ETHEREUM,
        )
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

        let mut bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            USDC_ETHEREUM,
        )
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

    sol!(
        #![sol(all_derives = true, rpc)]
        MockMintBurnToken,
        "lib/evm-cctp-contracts/out/MockMintBurnToken.sol/MockMintBurnToken.json"
    );

    async fn deploy_mock_usdc(
        anvil_endpoint: &str,
        deployer_key: &B256,
    ) -> Result<Address, Box<dyn std::error::Error>> {
        let signer = PrivateKeySigner::from_bytes(deployer_key)?;
        let wallet = EthereumWallet::from(signer.clone());

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(anvil_endpoint)
            .await?;

        let mock = MockMintBurnToken::deploy(&provider).await?;
        let mock_address = *mock.address();

        let mint_amount = U256::from(1_000_000_000_000u64); // 1M USDC (6 decimals)
        mock.mint(signer.address(), mint_amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(mock_address)
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_ethereum_with_zero_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .expect("Failed to deploy mock USDC");

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let owner = bridge.ethereum.owner;
        let spender = *bridge.ethereum.token_messenger.address();

        let initial_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::ZERO,
            "Initial allowance should be zero"
        );

        let result = bridge.ensure_usdc_approval_ethereum(amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_ethereum should succeed: {result:?}"
        );

        let final_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, amount,
            "Allowance should equal requested amount"
        );
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_ethereum_with_sufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .expect("Failed to deploy mock USDC");

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let higher_amount = U256::from(2_000_000u64);
        let owner = bridge.ethereum.owner;
        let spender = *bridge.ethereum.token_messenger.address();

        bridge
            .ethereum
            .usdc
            .approve(spender, higher_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance, higher_amount,
            "Initial allowance should be set"
        );

        let result = bridge.ensure_usdc_approval_ethereum(amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_ethereum should succeed: {result:?}"
        );

        let final_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, higher_amount,
            "Allowance should remain unchanged when sufficient"
        );
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_ethereum_with_insufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .expect("Failed to deploy mock USDC");

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap();

        let initial_allowance_amount = U256::from(500_000u64);
        let required_amount = U256::from(1_000_000u64);
        let owner = bridge.ethereum.owner;
        let spender = *bridge.ethereum.token_messenger.address();

        bridge
            .ethereum
            .usdc
            .approve(spender, initial_allowance_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance, initial_allowance_amount,
            "Initial allowance should be set"
        );

        let result = bridge.ensure_usdc_approval_ethereum(required_amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_ethereum should succeed: {result:?}"
        );

        let final_allowance = bridge
            .ethereum
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, required_amount,
            "Allowance should be updated to required amount"
        );
    }

    async fn create_bridge_with_base_usdc(
        ethereum_endpoint: &str,
        base_endpoint: &str,
        private_key: &B256,
        base_usdc_address: Address,
    ) -> Result<CctpBridge<impl Provider + Clone, impl Provider + Clone>, Box<dyn std::error::Error>>
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

        let owner = signer.address();

        let ethereum = Evm::new(
            ethereum_provider,
            owner,
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let base = Evm::new(
            base_provider,
            owner,
            base_usdc_address,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        Ok(CctpBridge::new(ethereum, base))
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_zero_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key)
            .await
            .expect("Failed to deploy mock USDC on Base");

        let bridge = create_bridge_with_base_usdc(
            &ethereum_endpoint,
            &base_endpoint,
            &base_key,
            base_usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let owner = bridge.base.owner;
        let spender = *bridge.base.token_messenger.address();

        let initial_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::ZERO,
            "Initial allowance should be zero"
        );

        let result = bridge.ensure_usdc_approval_base(amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_base should succeed: {result:?}"
        );

        let final_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, amount,
            "Allowance should equal requested amount"
        );
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_sufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key)
            .await
            .expect("Failed to deploy mock USDC on Base");

        let bridge = create_bridge_with_base_usdc(
            &ethereum_endpoint,
            &base_endpoint,
            &base_key,
            base_usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let higher_amount = U256::from(2_000_000u64);
        let owner = bridge.base.owner;
        let spender = *bridge.base.token_messenger.address();

        bridge
            .base
            .usdc
            .approve(spender, higher_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance, higher_amount,
            "Initial allowance should be set"
        );

        let result = bridge.ensure_usdc_approval_base(amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_base should succeed: {result:?}"
        );

        let final_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, higher_amount,
            "Allowance should remain unchanged when sufficient"
        );
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_insufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key)
            .await
            .expect("Failed to deploy mock USDC on Base");

        let bridge = create_bridge_with_base_usdc(
            &ethereum_endpoint,
            &base_endpoint,
            &base_key,
            base_usdc_address,
        )
        .await
        .unwrap();

        let initial_allowance_amount = U256::from(500_000u64);
        let required_amount = U256::from(1_000_000u64);
        let owner = bridge.base.owner;
        let spender = *bridge.base.token_messenger.address();

        bridge
            .base
            .usdc
            .approve(spender, initial_allowance_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance, initial_allowance_amount,
            "Initial allowance should be set"
        );

        let result = bridge.ensure_usdc_approval_base(required_amount).await;
        assert!(
            result.is_ok(),
            "ensure_usdc_approval_base should succeed: {result:?}"
        );

        let final_allowance = bridge
            .base
            .usdc
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance, required_amount,
            "Allowance should be updated to required amount"
        );
    }

    sol!(
        #![sol(all_derives = true, rpc)]
        TokenMinterV2,
        "lib/evm-cctp-contracts/out/TokenMinterV2.sol/TokenMinterV2.json"
    );

    sol!(
        #![sol(all_derives = true, rpc)]
        AdminUpgradableProxy,
        "lib/evm-cctp-contracts/out/AdminUpgradableProxy.sol/AdminUpgradableProxy.json"
    );

    /// CCTP domain for test "Ethereum" chain
    const TEST_ETHEREUM_DOMAIN: u32 = 0;
    /// CCTP domain for test "Base" chain
    const TEST_BASE_DOMAIN: u32 = 6;
    /// Message version for CCTP V2
    const MESSAGE_VERSION: u32 = 1;
    /// Message body version for CCTP V2
    const MESSAGE_BODY_VERSION: u32 = 1;
    /// Max message body size
    const MAX_MESSAGE_BODY_SIZE: U256 = U256::from_limbs([8192, 0, 0, 0]);

    /// Deployed CCTP chain with all contracts configured
    struct DeployedCctpChain {
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
        token_minter: Address,
    }

    /// Full CCTP test infrastructure with two chains (simulating Ethereum and Base)
    struct LocalCctp {
        _ethereum_anvil: AnvilInstance,
        _base_anvil: AnvilInstance,
        ethereum_endpoint: String,
        base_endpoint: String,
        ethereum: DeployedCctpChain,
        base: DeployedCctpChain,
        deployer_key: B256,
        attester_key: B256,
        fee_mock_server: MockServer,
    }

    impl LocalCctp {
        async fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let ethereum_anvil = Anvil::new().spawn();
            let base_anvil = Anvil::new().spawn();

            let ethereum_endpoint = ethereum_anvil.endpoint();
            let base_endpoint = base_anvil.endpoint();

            let deployer_key = B256::from_slice(&ethereum_anvil.keys()[0].to_bytes());
            // Use second key as attester for signing attestations
            let attester_key = B256::from_slice(&ethereum_anvil.keys()[1].to_bytes());
            let attester_signer = PrivateKeySigner::from_bytes(&attester_key)?;
            let attester_address = attester_signer.address();

            let deployer_signer = PrivateKeySigner::from_bytes(&deployer_key)?;
            let deployer_address = deployer_signer.address();

            // Deploy on Ethereum
            let ethereum = Self::deploy_cctp_chain(
                &ethereum_endpoint,
                &deployer_key,
                TEST_ETHEREUM_DOMAIN,
                attester_address,
            )
            .await?;

            // Deploy on Base
            let base = Self::deploy_cctp_chain(
                &base_endpoint,
                &deployer_key,
                TEST_BASE_DOMAIN,
                attester_address,
            )
            .await?;

            // Link remote token messengers
            Self::link_remote_token_messenger(
                &ethereum_endpoint,
                &deployer_key,
                ethereum.token_messenger,
                TEST_BASE_DOMAIN,
                base.token_messenger,
            )
            .await?;

            Self::link_remote_token_messenger(
                &base_endpoint,
                &deployer_key,
                base.token_messenger,
                TEST_ETHEREUM_DOMAIN,
                ethereum.token_messenger,
            )
            .await?;

            // Link token pairs
            Self::link_token_pair(
                &ethereum_endpoint,
                &deployer_key,
                ethereum.token_minter,
                ethereum.usdc,
                TEST_BASE_DOMAIN,
                base.usdc,
            )
            .await?;

            Self::link_token_pair(
                &base_endpoint,
                &deployer_key,
                base.token_minter,
                base.usdc,
                TEST_ETHEREUM_DOMAIN,
                ethereum.usdc,
            )
            .await?;

            Self::mint_usdc(
                &ethereum_endpoint,
                &deployer_key,
                ethereum.usdc,
                deployer_address,
            )
            .await?;
            Self::mint_usdc(&base_endpoint, &deployer_key, base.usdc, deployer_address).await?;

            let fee_mock_server = MockServer::start();
            fee_mock_server.mock(|when, then| {
                when.method(GET).path("/v2/burn/USDC/fees");
                then.status(200).json_body(serde_json::json!({
                    "minFee": "1"
                }));
            });

            Ok(Self {
                _ethereum_anvil: ethereum_anvil,
                _base_anvil: base_anvil,
                ethereum_endpoint,
                base_endpoint,
                ethereum,
                base,
                deployer_key,
                attester_key,
                fee_mock_server,
            })
        }

        async fn deploy_cctp_chain(
            endpoint: &str,
            deployer_key: &B256,
            domain: u32,
            attester: Address,
        ) -> Result<DeployedCctpChain, Box<dyn std::error::Error>> {
            let signer = PrivateKeySigner::from_bytes(deployer_key)?;
            let deployer = signer.address();
            let wallet = EthereumWallet::from(signer);
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect(endpoint)
                .await?;

            // Deploy MockMintBurnToken (USDC)
            let usdc = MockMintBurnToken::deploy(&provider).await?;
            let usdc_address = *usdc.address();

            // Deploy TokenMinterV2
            let token_minter = TokenMinterV2::deploy(&provider, deployer).await?;
            let token_minter_address = *token_minter.address();

            // Deploy MessageTransmitterV2 implementation
            let msg_transmitter_impl =
                MessageTransmitterV2::deploy(&provider, domain, MESSAGE_VERSION).await?;

            let attesters = vec![attester];
            let init_data = MessageTransmitterV2::initializeCall {
                owner_: deployer,
                attesterManager_: deployer,
                pauser_: deployer,
                rescuer_: deployer,
                attesters_: attesters,
                signatureThreshold_: U256::from(1),
                maxMessageBodySize_: MAX_MESSAGE_BODY_SIZE,
            }
            .abi_encode();

            // Deploy proxy for MessageTransmitterV2
            let msg_transmitter_proxy = AdminUpgradableProxy::deploy(
                &provider,
                *msg_transmitter_impl.address(),
                deployer,
                Bytes::from(init_data),
            )
            .await?;
            let message_transmitter_address = *msg_transmitter_proxy.address();

            // Deploy TokenMessengerV2 implementation
            let token_messenger_impl = TokenMessengerV2::deploy(
                &provider,
                message_transmitter_address,
                MESSAGE_BODY_VERSION,
            )
            .await?;

            let roles = TokenMessengerV2::TokenMessengerV2Roles {
                owner: deployer,
                rescuer: deployer,
                feeRecipient: deployer,
                denylister: deployer,
                tokenMinter: token_minter_address,
                minFeeController: deployer,
            };
            let init_data = TokenMessengerV2::initializeCall {
                roles,
                minFee_: U256::from(1),
                remoteDomains_: vec![],
                remoteTokenMessengers_: vec![],
            }
            .abi_encode();

            // Deploy proxy for TokenMessengerV2
            let token_messenger_proxy = AdminUpgradableProxy::deploy(
                &provider,
                *token_messenger_impl.address(),
                deployer,
                Bytes::from(init_data),
            )
            .await?;
            let token_messenger_address = *token_messenger_proxy.address();

            // Add token messenger to minter
            token_minter
                .addLocalTokenMessenger(token_messenger_address)
                .send()
                .await?
                .get_receipt()
                .await?;

            // Set max burn amount
            token_minter
                .setMaxBurnAmountPerMessage(usdc_address, U256::from(1_000_000_000_000u64))
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(DeployedCctpChain {
                usdc: usdc_address,
                token_messenger: token_messenger_address,
                message_transmitter: message_transmitter_address,
                token_minter: token_minter_address,
            })
        }

        async fn link_remote_token_messenger(
            endpoint: &str,
            deployer_key: &B256,
            local_token_messenger: Address,
            remote_domain: u32,
            remote_token_messenger: Address,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let signer = PrivateKeySigner::from_bytes(deployer_key)?;
            let wallet = EthereumWallet::from(signer);
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect(endpoint)
                .await?;

            let token_messenger = TokenMessengerV2::new(local_token_messenger, &provider);
            let remote_bytes32 =
                FixedBytes::<32>::left_padding_from(remote_token_messenger.as_slice());

            token_messenger
                .addRemoteTokenMessenger(remote_domain, remote_bytes32)
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(())
        }

        async fn link_token_pair(
            endpoint: &str,
            deployer_key: &B256,
            token_minter: Address,
            local_token: Address,
            remote_domain: u32,
            remote_token: Address,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let signer = PrivateKeySigner::from_bytes(deployer_key)?;
            let wallet = EthereumWallet::from(signer);
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect(endpoint)
                .await?;

            let minter = TokenMinterV2::new(token_minter, &provider);
            let remote_bytes32 = FixedBytes::<32>::left_padding_from(remote_token.as_slice());

            minter
                .linkTokenPair(local_token, remote_domain, remote_bytes32)
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(())
        }

        async fn mint_usdc(
            endpoint: &str,
            deployer_key: &B256,
            usdc: Address,
            to: Address,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let signer = PrivateKeySigner::from_bytes(deployer_key)?;
            let wallet = EthereumWallet::from(signer);
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect(endpoint)
                .await?;

            let token = MockMintBurnToken::new(usdc, &provider);
            token
                .mint(to, U256::from(1_000_000_000_000u64)) // 1M USDC
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(())
        }

        async fn create_bridge(
            &self,
        ) -> Result<
            CctpBridge<impl Provider + Clone, impl Provider + Clone>,
            Box<dyn std::error::Error>,
        > {
            let signer = PrivateKeySigner::from_bytes(&self.deployer_key)?;
            let wallet = EthereumWallet::from(signer.clone());

            let ethereum_provider = ProviderBuilder::new()
                .wallet(wallet.clone())
                .connect(&self.ethereum_endpoint)
                .await?;

            let base_provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect(&self.base_endpoint)
                .await?;

            let owner = signer.address();

            let ethereum = Evm::new(
                ethereum_provider,
                owner,
                self.ethereum.usdc,
                self.ethereum.token_messenger,
                self.ethereum.message_transmitter,
            );

            let base = Evm::new(
                base_provider,
                owner,
                self.base.usdc,
                self.base.token_messenger,
                self.base.message_transmitter,
            );

            Ok(CctpBridge::new(ethereum, base)
                .with_circle_api_base(self.fee_mock_server.base_url()))
        }

        /// Signs a CCTP message as the attester.
        ///
        /// In CCTP V2, the MessageSent event contains a message with EMPTY_NONCE (bytes32(0))
        /// and EMPTY_FINALITY_THRESHOLD_EXECUTED (0). The attester service fills in:
        /// 1. A unique nonce at position 12-44
        /// 2. The finality threshold achieved at position 144-148
        ///
        /// Returns both the signature and the modified message.
        async fn sign_message(
            &self,
            message: &[u8],
        ) -> Result<(Bytes, Bytes), Box<dyn std::error::Error>> {
            // Generate a random nonce (simulating Circle's attester)
            let mut nonce = [0u8; 32];
            rand::thread_rng().fill(&mut nonce);
            // Ensure nonce is not zero (reserved)
            nonce[0] |= 1;

            let mut modified_message = message.to_vec();

            // Insert nonce at position 12-44
            const NONCE_INDEX: usize = 12;
            modified_message[NONCE_INDEX..NONCE_INDEX + 32].copy_from_slice(&nonce);

            // Set finalityThresholdExecuted to FINALITY_THRESHOLD_FINALIZED (2000)
            // This indicates the message has been fully finalized on source chain.
            // Position 144-148 (4 bytes, big-endian uint32)
            const FINALITY_THRESHOLD_EXECUTED_INDEX: usize = 144;
            const FINALITY_THRESHOLD_FINALIZED: u32 = 2000;
            modified_message
                [FINALITY_THRESHOLD_EXECUTED_INDEX..FINALITY_THRESHOLD_EXECUTED_INDEX + 4]
                .copy_from_slice(&FINALITY_THRESHOLD_FINALIZED.to_be_bytes());

            let signer = PrivateKeySigner::from_bytes(&self.attester_key)?;
            let message_hash = keccak256(&modified_message);
            let signature = signer.sign_hash(&message_hash).await?;

            Ok((
                Bytes::from(signature.as_bytes().to_vec()),
                Bytes::from(modified_message),
            ))
        }
    }

    #[tokio::test]
    async fn test_burn_on_ethereum_with_deployed_contracts() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner;
        let amount = U256::from(1_000_000u64); // 1 USDC

        let result = bridge.burn_on_ethereum(amount, recipient).await;

        assert!(
            result.is_ok(),
            "burn_on_ethereum should succeed: {result:?}"
        );

        let receipt = result.unwrap();
        assert!(!receipt.tx.is_zero(), "Transaction hash should be set");
        assert!(!receipt.message.is_empty(), "Message should not be empty");
        assert_eq!(receipt.amount, amount, "Amount should match");
    }

    #[tokio::test]
    async fn test_burn_on_base_with_deployed_contracts() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner;
        let amount = U256::from(1_000_000u64); // 1 USDC

        let result = bridge.burn_on_base(amount, recipient).await;

        assert!(result.is_ok(), "burn_on_base should succeed: {result:?}");

        let receipt = result.unwrap();
        assert!(!receipt.tx.is_zero(), "Transaction hash should be set");
        assert!(!receipt.message.is_empty(), "Message should not be empty");
        assert_eq!(receipt.amount, amount, "Amount should match");
    }

    #[tokio::test]
    async fn test_full_bridge_ethereum_to_base() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner;
        let amount = U256::from(1_000_000u64); // 1 USDC

        // Step 1: Burn on Ethereum
        let burn_receipt = bridge
            .burn_on_ethereum(amount, recipient)
            .await
            .expect("burn_on_ethereum failed");

        // sign_message returns (attestation, modified_message_with_nonce)
        let (attestation, message_with_nonce) = cctp
            .sign_message(&burn_receipt.message)
            .await
            .expect("Failed to sign message");

        let mint_result = bridge.mint_on_base(message_with_nonce, attestation).await;

        assert!(
            mint_result.is_ok(),
            "mint_on_base should succeed: {mint_result:?}"
        );
    }

    #[tokio::test]
    async fn test_full_bridge_base_to_ethereum() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner;
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_on_base(amount, recipient)
            .await
            .expect("burn_on_base failed");

        // sign_message returns (attestation, modified_message_with_nonce)
        let (attestation, message_with_nonce) = cctp
            .sign_message(&burn_receipt.message)
            .await
            .expect("Failed to sign message");

        let mint_result = bridge
            .mint_on_ethereum(message_with_nonce, attestation)
            .await;

        assert!(
            mint_result.is_ok(),
            "mint_on_ethereum should succeed: {mint_result:?}"
        );
    }

    #[tokio::test]
    async fn test_mint_on_ethereum_with_invalid_attestation() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner;
        let amount = U256::from(1_000_000u64);

        // Burn on Base to get a valid message
        let burn_receipt = bridge
            .burn_on_base(amount, recipient)
            .await
            .expect("burn_on_base failed");

        // Get the message with a proper nonce, but use an invalid attestation
        let (_, message_with_nonce) = cctp
            .sign_message(&burn_receipt.message)
            .await
            .expect("Failed to sign message");

        // Use invalid attestation (wrong signature)
        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint_on_ethereum(message_with_nonce, invalid_attestation)
            .await
            .expect_err("Expected error for invalid attestation");

        let err_msg = err.to_string();
        assert!(
            matches!(err, CctpError::Contract(_)),
            "Expected Contract error, got: {err:?}"
        );
        assert!(
            err_msg.contains("ECDSA: invalid signature"),
            "Expected ECDSA invalid signature error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_mint_on_base_with_invalid_attestation() {
        let cctp = LocalCctp::new()
            .await
            .expect("Failed to deploy CCTP infrastructure");
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner;
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_on_ethereum(amount, recipient)
            .await
            .expect("burn_on_ethereum failed");

        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint_on_base(burn_receipt.message.clone(), invalid_attestation)
            .await
            .expect_err("Expected error for invalid attestation");

        let err_msg = err.to_string();
        assert!(
            matches!(err, CctpError::Contract(_)),
            "Expected Contract error, got: {err:?}"
        );
        assert!(
            err_msg.contains("ECDSA: invalid signature"),
            "Expected ECDSA invalid signature error, got: {err_msg}"
        );
    }
}
