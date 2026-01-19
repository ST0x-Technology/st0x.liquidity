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
//! to ~40-70 seconds for a cost of 1 basis point (0.01%) per transfer.
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
//! let ethereum = Evm::new(ethereum_provider, owner, usdc, token_messenger, message_transmitter);
//! let base = Evm::new(base_provider, owner, usdc, token_messenger, message_transmitter);
//! let bridge = CctpBridge::new(ethereum, base);
//!
//! // Bridge 1 USDC from Ethereum to Base (USDC has 6 decimals)
//! let amount = U256::from(1_000_000); // 1 USDC
//! let tx_hash = bridge.burn(BridgeDirection::EthereumToBase, amount, recipient).await?;
//! ```
//!
//! ## CCTP V2 Fast Transfer
//!
//! Fast transfers are enabled by setting `minFinalityThreshold` to 1000 in the
//! `depositForBurn()` call. The fee is dynamically queried from Circle's API
//! (`/v2/burn/USDC/fees`) before each burn operation.
//!
//! **Timing**: ~40s Base->Ethereum, ~70s Ethereum->Base (measured)
//! **Cost**: 1 basis point (0.01%) of transfer amount

mod evm;

pub(crate) use evm::Evm;

use std::mem::size_of;
use std::time::Duration;

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256, address};
use alloy::providers::Provider;
use alloy::sol;
use backon::Retryable;
use rain_error_decoding::AbiDecodedErrorType;
use serde::Deserialize;
use tracing::{debug, info, warn};

// Committed ABI: CCTP contracts use solc 0.7.6 which solc.nix doesn't have for aarch64-darwin
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "cctp-abis/TokenMessengerV2.json"
);

// Committed ABI: CCTP contracts use solc 0.7.6 which solc.nix doesn't have for aarch64-darwin
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

/// Direction of a CCTP bridge transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BridgeDirection {
    /// Bridge USDC from Ethereum to Base
    EthereumToBase,
    /// Bridge USDC from Base to Ethereum
    BaseToEthereum,
}

impl BridgeDirection {
    /// Returns the source CCTP domain for this bridge direction.
    const fn source_domain(self) -> u32 {
        match self {
            Self::EthereumToBase => ETHEREUM_DOMAIN,
            Self::BaseToEthereum => BASE_DOMAIN,
        }
    }

    /// Returns the destination CCTP domain for this bridge direction.
    const fn dest_domain(self) -> u32 {
        match self {
            Self::EthereumToBase => BASE_DOMAIN,
            Self::BaseToEthereum => ETHEREUM_DOMAIN,
        }
    }
}

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
/// Note: The nonce is NOT included here because in CCTP V2, the MessageSent event
/// contains a placeholder nonce (bytes32(0)). The real nonce is only available after
/// receiving the attestation from Circle's API, which fills in the actual value.
#[derive(Debug)]
pub(crate) struct BurnReceipt {
    /// Transaction hash of the burn transaction
    pub(crate) tx: TxHash,
    /// Amount of USDC burned (in smallest unit, 6 decimals for USDC)
    pub(crate) amount: U256,
}

/// Response from Circle's attestation API for CCTP V2.
///
/// Contains both the CCTP message bytes and the Circle attestation signature,
/// which together are required to call `receiveMessage()` on the destination chain.
///
/// The nonce is extracted from the attested message, NOT from the original MessageSent
/// event (which contains a placeholder bytes32(0) in CCTP V2).
#[derive(Debug)]
pub(crate) struct AttestationResponse {
    /// CCTP message bytes from the attestation API.
    /// Unlike the MessageSent event message, this contains the real nonce
    /// filled in by Circle's attestation service.
    pub(crate) message: Bytes,
    /// Circle's attestation signature for the message.
    /// Required to prove the burn happened and authorize minting.
    pub(crate) attestation: Bytes,
    /// The real CCTP nonce extracted from the attested message.
    /// This is the authoritative nonce value, not the placeholder from MessageSent.
    pub(crate) nonce: FixedBytes<32>,
}

impl AttestationResponse {
    /// Returns the nonce as a u64 by taking the last 8 bytes.
    ///
    /// CCTP nonces are 32 bytes but the significant portion fits in u64 for our use case.
    /// Returns an error if the first 24 bytes are non-zero, indicating the nonce exceeds u64.
    pub(crate) fn nonce_as_u64(&self) -> Result<u64, CctpError> {
        let bytes: &[u8; 32] = self.nonce.as_ref();
        let (padding, value) = bytes.split_at(bytes.len() - size_of::<u64>());

        if padding.iter().any(|&b| b != 0) {
            return Err(CctpError::NonceOverflow { nonce: self.nonce });
        }

        Ok(u64::from_be_bytes(value.try_into()?))
    }
}

// CCTP V2 message layout (see lib/evm-cctp-contracts/src/messages/v2/MessageV2.sol):
// - Bytes 0-3: version (4 bytes)
// - Bytes 4-7: source domain (4 bytes)
// - Bytes 8-11: destination domain (4 bytes)
// - Bytes 12-43: nonce (32 bytes) <- we extract this
// - Bytes 44+: remaining message data
// Minimum length required: 44 bytes (to include the full nonce)
const NONCE_INDEX: usize = 12;
const NONCE_SIZE: usize = size_of::<FixedBytes<32>>();
const MIN_MESSAGE_LENGTH: usize = NONCE_INDEX + NONCE_SIZE;

/// Extracts the 32-byte nonce from a CCTP V2 message.
///
/// Used to extract the real nonce from the attested message returned by Circle's API.
/// The nonce in the original MessageSent event is always bytes32(0) in CCTP V2.
fn extract_nonce_from_message(message: &[u8]) -> Result<FixedBytes<32>, CctpError> {
    if message.len() < MIN_MESSAGE_LENGTH {
        return Err(CctpError::MessageTooShort {
            length: message.len(),
        });
    }

    Ok(FixedBytes::<32>::from_slice(
        &message[NONCE_INDEX..NONCE_INDEX + 32],
    ))
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
    #[error("Contract reverted: {0}")]
    Revert(#[from] AbiDecodedErrorType),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Attestation timeout after {attempts} attempts: {source}")]
    AttestationTimeout {
        attempts: usize,
        source: AttestationError,
    },
    #[error("MessageSent event not found in transaction receipt")]
    MessageSentEventNotFound,
    #[error("Message too short for nonce extraction: got {length} bytes, need at least 44")]
    MessageTooShort { length: usize },
    #[error("Fee calculation overflow")]
    FeeCalculationOverflow,
    #[error("Fast transfer fee not available for {direction:?}")]
    FastTransferFeeNotAvailable { direction: BridgeDirection },
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Fee value parse error: {0}")]
    FeeValueParse(#[from] std::num::ParseIntError),
    #[error("Nonce exceeds u64: upper 24 bytes are non-zero")]
    NonceOverflow { nonce: FixedBytes<32> },
    #[error("Slice conversion error: {0}")]
    SliceConversion(#[from] std::array::TryFromSliceError),
}

/// Errors specific to attestation polling from Circle's API.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AttestationError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Failed to parse attestation response: {0}")]
    JsonParse(#[from] serde_json::Error),
    #[error("Attestation pending: {status}")]
    Pending { status: String },
    #[error("No messages in attestation response")]
    NoMessages,
    #[error("Attestation response missing required field: {field}")]
    MissingField { field: &'static str },
    #[error("Attestation not yet available (HTTP {status})")]
    NotYetAvailable { status: u16 },
}

/// Fee entry from Circle's `/v2/burn/USDC/fees/{source}/{dest}` API.
///
/// The API returns an array of these entries, one per finality threshold level.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FeeEntry {
    /// Finality threshold: 1000 = fast transfer, 2000 = standard transfer
    finality_threshold: u32,
    /// Minimum fee in basis points (1 = 0.01%)
    minimum_fee: u64,
}

impl<EP, BP> CctpBridge<EP, BP>
where
    EP: Provider + Clone,
    BP: Provider + Clone,
{
    pub(crate) fn new(ethereum: Evm<EP>, base: Evm<BP>) -> Result<Self, CctpError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            ethereum,
            base,
            http_client,
            circle_api_base: CIRCLE_API_BASE.to_string(),
        })
    }

    async fn query_fast_transfer_fee(
        &self,
        amount: U256,
        direction: BridgeDirection,
    ) -> Result<U256, CctpError> {
        let url = format!(
            "{}/v2/burn/USDC/fees/{}/{}",
            self.circle_api_base,
            direction.source_domain(),
            direction.dest_domain()
        );
        let response = self.http_client.get(&url).send().await?;

        if !response.status().is_success() {
            warn!(
                url,
                status = response.status().as_u16(),
                "Fee endpoint failed"
            );
            return Err(CctpError::FastTransferFeeNotAvailable { direction });
        }

        let fee_entries: Vec<FeeEntry> = response.json().await?;

        // Find the fast transfer fee (threshold 1000)
        let fast_fee = fee_entries
            .iter()
            .find(|e| e.finality_threshold == FAST_TRANSFER_THRESHOLD)
            .ok_or(CctpError::FastTransferFeeNotAvailable { direction })?
            .minimum_fee;

        debug!(
            ?direction,
            fast_fee_bps = fast_fee,
            "Retrieved fast transfer fee"
        );

        // Calculate maxFee: amount * fee_bps / 10000
        let max_fee = amount
            .checked_mul(U256::from(fast_fee))
            .ok_or(CctpError::FeeCalculationOverflow)?
            / U256::from(10000u64);

        Ok(max_fee)
    }

    /// Polls for attestation using CCTP V2 API.
    pub(crate) async fn poll_attestation(
        &self,
        direction: BridgeDirection,
        tx_hash: TxHash,
    ) -> Result<AttestationResponse, CctpError> {
        const MAX_ATTEMPTS: usize = 60;
        const RETRY_INTERVAL_SECS: u64 = 5;

        let url = format!(
            "{}/v2/messages/{}?transactionHash={tx_hash}",
            self.circle_api_base,
            direction.source_domain()
        );

        info!(%url, "Polling attestation API");

        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_secs(RETRY_INTERVAL_SECS))
            .with_max_times(MAX_ATTEMPTS);

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct MessageEntry {
            attestation: Option<String>,
            message: Option<String>,
            status: String,
        }

        #[derive(Deserialize, Debug)]
        struct V2Response {
            messages: Vec<MessageEntry>,
        }

        let fetch_attestation = || async {
            let response = self.http_client.get(&url).send().await?;

            if !response.status().is_success() {
                return Err(AttestationError::NotYetAvailable {
                    status: response.status().as_u16(),
                });
            }

            let v2_response: V2Response = response.json().await?;

            let entry = v2_response
                .messages
                .first()
                .ok_or(AttestationError::NoMessages)?;

            if entry.status != "complete" {
                return Err(AttestationError::Pending {
                    status: entry.status.clone(),
                });
            }

            let attestation_hex =
                entry
                    .attestation
                    .as_ref()
                    .ok_or(AttestationError::MissingField {
                        field: "attestation",
                    })?;
            let message_hex = entry
                .message
                .as_ref()
                .ok_or(AttestationError::MissingField { field: "message" })?;

            let message = Bytes::from(alloy::hex::decode(message_hex)?);
            let attestation = Bytes::from(alloy::hex::decode(attestation_hex)?);

            Ok((message, attestation))
        };

        let (message, attestation) = fetch_attestation
            .retry(backoff)
            .notify(|err, dur| match err {
                AttestationError::Pending { status } => {
                    info!(%status, ?dur, "Attestation pending, retrying");
                }
                AttestationError::NotYetAvailable { status } => {
                    debug!(status, ?dur, "API non-success, retrying");
                }
                err => warn!(?err, ?dur, "Attestation error, retrying"),
            })
            .await
            .map_err(|err| CctpError::AttestationTimeout {
                attempts: MAX_ATTEMPTS,
                source: err,
            })?;

        let nonce = extract_nonce_from_message(&message)?;

        Ok(AttestationResponse {
            message,
            attestation,
            nonce,
        })
    }

    /// Burns USDC on the source chain for the given bridge direction.
    pub(crate) async fn burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        let max_fee = self.query_fast_transfer_fee(amount, direction).await?;

        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum.ensure_usdc_approval(amount).await?;
                self.ethereum
                    .deposit_for_burn(amount, recipient, direction, max_fee)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base.ensure_usdc_approval(amount).await?;
                self.base
                    .deposit_for_burn(amount, recipient, direction, max_fee)
                    .await
            }
        }
    }

    /// Mints USDC on the destination chain for the given bridge direction.
    pub(crate) async fn mint(
        &self,
        direction: BridgeDirection,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, CctpError> {
        match direction {
            BridgeDirection::EthereumToBase => self.base.claim(message, attestation).await,
            BridgeDirection::BaseToEthereum => self.ethereum.claim(message, attestation).await,
        }
    }

    #[cfg(test)]
    fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, b256, keccak256};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::Signer;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::{SolCall, SolEvent};
    use httpmock::prelude::*;
    use proptest::prelude::*;
    use rand::Rng;

    use super::*;

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

        Ok(CctpBridge::new(ethereum, base)?)
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
                return Err(AttestationError::NotYetAvailable {
                    status: response.status().as_u16(),
                });
            }

            let attestation_response: AttestationResponse = response.json().await?;
            let attestation_bytes = alloy::hex::decode(&attestation_response.attestation)?;

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
                return Err(AttestationError::NotYetAvailable {
                    status: response.status().as_u16(),
                });
            }

            let attestation_response: AttestationResponse = response.json().await?;
            let attestation_bytes = alloy::hex::decode(&attestation_response.attestation)?;

            Ok::<Bytes, AttestationError>(Bytes::from(attestation_bytes))
        };

        let result = fetch_attestation.retry(backoff).await;

        assert!(
            result.is_err(),
            "Expected attestation to fail after max retries"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                AttestationError::NotYetAvailable { status: 404 }
            ),
            "Expected AttestationError::NotYetAvailable with status 404"
        );
        assert!(
            mock.hits() >= 3,
            "Expected at least 3 attempts with retries"
        );
    }

    // Committed ABI: CCTP contracts use solc 0.7.6 which solc.nix doesn't have for aarch64-darwin
    sol!(
        #![sol(all_derives = true, rpc)]
        MockMintBurnToken,
        "cctp-abis/MockMintBurnToken.json"
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
            .unwrap();

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.ethereum.token_messenger().address();

        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, U256::ZERO);

        bridge.ethereum.ensure_usdc_approval(amount).await.unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, amount);
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_ethereum_with_sufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .unwrap();

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
        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.ethereum.token_messenger().address();

        bridge
            .ethereum
            .usdc()
            .approve(spender, higher_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, higher_amount);

        bridge.ethereum.ensure_usdc_approval(amount).await.unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, higher_amount);
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_ethereum_with_insufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .unwrap();

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
        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.ethereum.token_messenger().address();

        bridge
            .ethereum
            .usdc()
            .approve(spender, initial_allowance_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, initial_allowance_amount);

        bridge
            .ethereum
            .ensure_usdc_approval(required_amount)
            .await
            .unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, required_amount);
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

        Ok(CctpBridge::new(ethereum, base)?)
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_zero_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key).await.unwrap();

        let bridge = create_bridge_with_base_usdc(
            &ethereum_endpoint,
            &base_endpoint,
            &base_key,
            base_usdc_address,
        )
        .await
        .unwrap();

        let amount = U256::from(1_000_000u64);
        let signer = PrivateKeySigner::from_bytes(&base_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.base.token_messenger().address();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, U256::ZERO);

        bridge.base.ensure_usdc_approval(amount).await.unwrap();

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, amount);
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_sufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key).await.unwrap();

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
        let signer = PrivateKeySigner::from_bytes(&base_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.base.token_messenger().address();

        bridge
            .base
            .usdc()
            .approve(spender, higher_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, higher_amount);

        bridge.base.ensure_usdc_approval(amount).await.unwrap();

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, higher_amount);
    }

    #[tokio::test]
    async fn test_ensure_usdc_approval_base_with_insufficient_allowance() {
        let (_ethereum_anvil, ethereum_endpoint, _) = setup_anvil();
        let (_base_anvil, base_endpoint, base_key) = setup_anvil();

        let base_usdc_address = deploy_mock_usdc(&base_endpoint, &base_key).await.unwrap();

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
        let signer = PrivateKeySigner::from_bytes(&base_key).unwrap();
        let owner = signer.address();
        let spender = *bridge.base.token_messenger().address();

        bridge
            .base
            .usdc()
            .approve(spender, initial_allowance_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, initial_allowance_amount);

        bridge
            .base
            .ensure_usdc_approval(required_amount)
            .await
            .unwrap();

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(final_allowance, required_amount);
    }

    // Committed ABI: CCTP contracts use solc 0.7.6 which solc.nix doesn't have for aarch64-darwin
    sol!(
        #![sol(all_derives = true, rpc)]
        TokenMinterV2,
        "cctp-abis/TokenMinterV2.json"
    );

    // Committed ABI: CCTP contracts use solc 0.7.6 which solc.nix doesn't have for aarch64-darwin
    sol!(
        #![sol(all_derives = true, rpc)]
        AdminUpgradableProxy,
        "cctp-abis/AdminUpgradableProxy.json"
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
            // Mock fee endpoint for any domain pair - returns fast (1000) and standard (2000) fees
            fee_mock_server.mock(|when, then| {
                when.method(GET).path_contains("/v2/burn/USDC/fees/");
                then.status(200).json_body(serde_json::json!([
                    {"finalityThreshold": 1000, "minimumFee": 1},
                    {"finalityThreshold": 2000, "minimumFee": 0}
                ]));
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

            Ok(CctpBridge::new(ethereum, base)?
                .with_circle_api_base(self.fee_mock_server.base_url()))
        }

        /// Extracts the CCTP message from a burn transaction receipt.
        /// In production, this message comes from Circle's attestation API.
        async fn extract_message_from_burn_tx(
            &self,
            tx_hash: TxHash,
            is_ethereum: bool,
        ) -> Result<Bytes, Box<dyn std::error::Error>> {
            let endpoint = if is_ethereum {
                &self.ethereum_endpoint
            } else {
                &self.base_endpoint
            };

            let provider = ProviderBuilder::new().connect(endpoint).await?;
            let receipt = provider
                .get_transaction_receipt(tx_hash)
                .await?
                .ok_or("Transaction receipt not found")?;

            let message_sent_event = receipt
                .inner
                .logs()
                .iter()
                .find_map(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).ok())
                .ok_or("MessageSent event not found")?;

            Ok(message_sent_event.message.clone())
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
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let receipt = bridge
            .burn(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        assert!(!receipt.tx.is_zero(), "Transaction hash should be set");
        assert_eq!(receipt.amount, amount, "Amount should match");

        let message = cctp
            .extract_message_from_burn_tx(receipt.tx, true)
            .await
            .unwrap();
        assert!(!message.is_empty(), "Message should not be empty");
    }

    #[tokio::test]
    async fn test_burn_on_base_with_deployed_contracts() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let receipt = bridge
            .burn(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        assert!(!receipt.tx.is_zero(), "Transaction hash should be set");
        assert_eq!(receipt.amount, amount, "Amount should match");

        let message = cctp
            .extract_message_from_burn_tx(receipt.tx, false)
            .await
            .unwrap();
        assert!(!message.is_empty(), "Message should not be empty");
    }

    #[tokio::test]
    async fn test_full_bridge_ethereum_to_base() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let burn_receipt = bridge
            .burn(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        bridge
            .mint(
                BridgeDirection::EthereumToBase,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_full_bridge_base_to_ethereum() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        bridge
            .mint(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_mint_on_ethereum_with_invalid_attestation() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (_, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                invalid_attestation,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, CctpError::Revert(_)), "got: {err:?}");
        assert!(
            err.to_string().contains("ECDSA: invalid signature"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn test_mint_on_base_with_invalid_attestation() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&cctp.deployer_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint(
                BridgeDirection::EthereumToBase,
                message,
                invalid_attestation,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, CctpError::Revert(_)), "got: {err:?}");
        assert!(
            err.to_string().contains("ECDSA: invalid signature"),
            "got: {err}"
        );
    }

    fn build_nonce_message(header: &[u8], nonce: [u8; 32], trailer: &[u8]) -> Vec<u8> {
        use itertools::Itertools;

        header
            .iter()
            .copied()
            .chain(nonce)
            .chain(trailer.iter().copied())
            .collect_vec()
    }

    #[test]
    fn extract_nonce_from_empty_message_returns_message_too_short() {
        let err = extract_nonce_from_message(&[]).unwrap_err();

        assert!(
            matches!(err, CctpError::MessageTooShort { length: 0 }),
            "Expected MessageTooShort with length 0, got: {err:?}"
        );
    }

    #[test]
    fn extract_nonce_from_short_message_returns_message_too_short() {
        let message = [0u8; 43]; // One byte short of minimum

        let err = extract_nonce_from_message(&message).unwrap_err();

        assert!(
            matches!(err, CctpError::MessageTooShort { length: 43 }),
            "Expected MessageTooShort with length 43, got: {err:?}"
        );
    }

    #[test]
    fn extract_nonce_from_minimum_length_message_succeeds() {
        let expected_nonce: [u8; 32] = core::array::from_fn(|i| {
            u8::try_from(i + 1).expect("index 0..31 + 1 always fits in u8")
        });
        let message = build_nonce_message(&[0u8; NONCE_INDEX], expected_nonce, &[]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    #[test]
    fn extract_nonce_from_longer_message_succeeds() {
        let expected_nonce = [0xFF; 32];
        let message = build_nonce_message(&[0u8; NONCE_INDEX], expected_nonce, &[0u8; 56]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    #[test]
    fn extract_nonce_ignores_bytes_before_nonce_index() {
        let expected_nonce = [0x00; 32];
        let message = build_nonce_message(&[0xAB; NONCE_INDEX], expected_nonce, &[]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    proptest! {
        #[test]
        fn short_messages_always_fail(len in 0..MIN_MESSAGE_LENGTH) {
            let message = vec![0u8; len];

            let err = extract_nonce_from_message(&message).unwrap_err();

            match err {
                CctpError::MessageTooShort { length } => prop_assert_eq!(length, len),
                other => prop_assert!(false, "Expected MessageTooShort, got: {:?}", other),
            }
        }

        #[test]
        fn valid_messages_always_extract_correct_nonce(
            header in prop::collection::vec(any::<u8>(), NONCE_INDEX),
            nonce in any::<[u8; 32]>(),
            trailer_len in 0usize..100,
        ) {
            let trailer = vec![0u8; trailer_len];
            let message = build_nonce_message(&header, nonce, &trailer);

            let extracted = extract_nonce_from_message(&message).unwrap();

            prop_assert_eq!(extracted, FixedBytes::from(nonce));
        }

        #[test]
        fn nonce_extraction_is_independent_of_surrounding_bytes(
            header in prop::collection::vec(any::<u8>(), NONCE_INDEX),
            nonce in any::<[u8; 32]>(),
            trailer in prop::collection::vec(any::<u8>(), 0..100),
        ) {
            let message = build_nonce_message(&header, nonce, &trailer);

            let extracted = extract_nonce_from_message(&message).unwrap();

            prop_assert_eq!(extracted, FixedBytes::from(nonce));
        }
    }
}
