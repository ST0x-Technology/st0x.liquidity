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

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256, address, keccak256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use backon::Retryable;
use serde::Deserialize;

use crate::{Bridge, BurnReceipt};

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20,
    "../../lib/forge-std/out/IERC20.sol/IERC20.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "cctp-abis/TokenMessengerV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    MessageTransmitterV2,
    "cctp-abis/MessageTransmitterV2.json"
);

/// CCTP domain identifier for Ethereum mainnet
const ETHEREUM_DOMAIN: u32 = 0;

/// CCTP domain identifier for Base
const BASE_DOMAIN: u32 = 6;

/// USDC contract address on Ethereum mainnet
pub const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

/// USDC contract address on Base
pub const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

/// TokenMessengerV2 contract address (same on both chains)
pub const TOKEN_MESSENGER_V2: Address = address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");

/// MessageTransmitterV2 contract address (same on both chains)
pub const MESSAGE_TRANSMITTER_V2: Address = address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");

const CIRCLE_API_BASE: &str = "https://iris-api.circle.com";

/// Minimum finality threshold for CCTP V2 fast transfer (enables ~30 second transfers)
const FAST_TRANSFER_THRESHOLD: u32 = 1000;

/// EVM chain connection with contract instances for CCTP operations.
pub struct Evm<P>
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
    pub fn new(
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
/// methods for cross-chain transfers.
pub struct CctpBridge<EP, BP>
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
pub enum CctpError {
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
pub enum AttestationError {
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
    pub fn new(ethereum: Evm<EP>, base: Evm<BP>) -> Self {
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

    /// Burns USDC on Ethereum chain for cross-chain transfer to Base.
    pub async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        self.ensure_usdc_approval_ethereum(amount).await?;

        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
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

    /// Polls Circle's attestation API until the attestation is ready.
    pub async fn poll_attestation(&self, hash: FixedBytes<32>) -> Result<Bytes, CctpError> {
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

    /// Mints USDC on Base chain after receiving attestation.
    pub async fn mint_on_base(
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
    pub async fn burn_on_base(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        self.ensure_usdc_approval_base(amount).await?;

        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
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

    /// Mints USDC on Ethereum chain after receiving attestation from Base burn.
    pub async fn mint_on_ethereum(
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

#[async_trait]
impl<EP, BP> Bridge for CctpBridge<EP, BP>
where
    EP: Provider + Clone + Send + Sync + 'static,
    BP: Provider + Clone + Send + Sync + 'static,
{
    type Error = CctpError;

    async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error> {
        Self::burn_on_ethereum(self, amount, recipient).await
    }

    async fn burn_on_base(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error> {
        Self::burn_on_base(self, amount, recipient).await
    }

    async fn poll_attestation(&self, hash: FixedBytes<32>) -> Result<Bytes, Self::Error> {
        Self::poll_attestation(self, hash).await
    }

    async fn mint_on_base(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, Self::Error> {
        Self::mint_on_base(self, message, attestation).await
    }

    async fn mint_on_ethereum(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, Self::Error> {
        Self::mint_on_ethereum(self, message, attestation).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address};
    use httpmock::MockServer;

    use super::*;

    #[test]
    fn usdc_ethereum_address_is_correct() {
        assert_eq!(
            USDC_ETHEREUM,
            address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        );
    }

    #[test]
    fn usdc_base_address_is_correct() {
        assert_eq!(
            USDC_BASE,
            address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        );
    }

    #[test]
    fn token_messenger_v2_address_is_correct() {
        assert_eq!(
            TOKEN_MESSENGER_V2,
            address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d")
        );
    }

    #[test]
    fn message_transmitter_v2_address_is_correct() {
        assert_eq!(
            MESSAGE_TRANSMITTER_V2,
            address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64")
        );
    }

    #[test]
    fn ethereum_domain_is_zero() {
        assert_eq!(ETHEREUM_DOMAIN, 0);
    }

    #[test]
    fn base_domain_is_six() {
        assert_eq!(BASE_DOMAIN, 6);
    }

    fn create_test_bridge(
        server: &MockServer,
    ) -> CctpBridge<
        alloy::providers::RootProvider<alloy::network::Ethereum>,
        alloy::providers::RootProvider<alloy::network::Ethereum>,
    > {
        let eth_provider = alloy::providers::ProviderBuilder::new()
            .connect_http("http://localhost:8545".parse().unwrap());
        let base_provider = alloy::providers::ProviderBuilder::new()
            .connect_http("http://localhost:8546".parse().unwrap());

        let owner = Address::ZERO;

        let ethereum = Evm::new(
            eth_provider,
            owner,
            USDC_ETHEREUM,
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

        CctpBridge::new(ethereum, base).with_circle_api_base(server.base_url())
    }

    #[tokio::test]
    async fn query_fast_transfer_fee_calculates_correctly() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("GET").path("/v2/burn/USDC/fees");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"minFee": "1"}"#);
        });

        let bridge = create_test_bridge(&server);

        let amount = U256::from(1_000_000u64);
        let fee = bridge.query_fast_transfer_fee(amount).await.unwrap();

        // 1 basis point = 0.01% = amount * 1 / 10000
        let expected_fee = amount / U256::from(10000u64);
        assert_eq!(fee, expected_fee);
    }

    #[tokio::test]
    async fn query_fast_transfer_fee_with_higher_fee_rate() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("GET").path("/v2/burn/USDC/fees");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"minFee": "10"}"#);
        });

        let bridge = create_test_bridge(&server);

        let amount = U256::from(1_000_000u64);
        let fee = bridge.query_fast_transfer_fee(amount).await.unwrap();

        // 10 basis points = 0.1% = amount * 10 / 10000
        let expected_fee = amount * U256::from(10u64) / U256::from(10000u64);
        assert_eq!(fee, expected_fee);
    }

    #[tokio::test]
    async fn query_fast_transfer_fee_handles_large_amounts() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("GET").path("/v2/burn/USDC/fees");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"minFee": "1"}"#);
        });

        let bridge = create_test_bridge(&server);

        // 1 billion USDC (with 6 decimals)
        let amount = U256::from(1_000_000_000_000_000u64);
        let fee = bridge.query_fast_transfer_fee(amount).await.unwrap();

        let expected_fee = amount / U256::from(10000u64);
        assert_eq!(fee, expected_fee);
    }

    #[tokio::test]
    async fn query_fast_transfer_fee_returns_error_on_invalid_fee_format() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("GET").path("/v2/burn/USDC/fees");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"minFee": "not_a_number"}"#);
        });

        let bridge = create_test_bridge(&server);

        let result = bridge.query_fast_transfer_fee(U256::from(1000u64)).await;

        assert!(matches!(result.unwrap_err(), CctpError::FeeValueParse(_)));
    }

    #[tokio::test]
    async fn query_fast_transfer_fee_returns_error_on_http_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("GET").path("/v2/burn/USDC/fees");
            then.status(500);
        });

        let bridge = create_test_bridge(&server);

        let result = bridge.query_fast_transfer_fee(U256::from(1000u64)).await;

        assert!(matches!(result.unwrap_err(), CctpError::Http(_)));
    }

    #[test]
    fn recipient_address_converts_to_bytes32_with_left_padding() {
        let recipient = address!("0x1234567890123456789012345678901234567890");
        let bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // First 12 bytes should be zero (padding)
        assert_eq!(&bytes32[..12], &[0u8; 12]);
        // Last 20 bytes should be the address
        assert_eq!(&bytes32[12..], recipient.as_slice());
    }

    #[test]
    fn fee_response_deserializes_correctly() {
        let json = r#"{"minFee": "1"}"#;
        let response: FeeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.min_fee, "1");
    }

    #[test]
    fn fee_response_handles_string_numbers() {
        let json = r#"{"minFee": "100"}"#;
        let response: FeeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.min_fee, "100");
    }
}
