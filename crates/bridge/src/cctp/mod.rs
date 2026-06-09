//! Circle CCTP bridge service for cross-chain USDC transfers.
//!
//! This module provides a service layer for bridging USDC between
//! Ethereum mainnet and Base using Circle's Cross-Chain Transfer
//! Protocol (CCTP) V2 with fast transfers.
//!
//! ## Overview
//!
//! Circle CCTP enables native USDC transfers between blockchains by
//! burning on the source chain and minting on the destination chain.
//! This implementation uses **CCTP V2 Fast Transfer** which reduces
//! transfer time from 13-19 minutes per chain to ~40-70 seconds for a
//! cost of 1 basis point (0.01%) per transfer.
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
//! 1. **Burn**: Lock and burn USDC on source chain via
//!    `TokenMessengerV2.depositForBurn()`
//! 2. **Attest**: Poll Circle's attestation API for signed message
//!    (fast transfer: ~20-30s)
//! 3. **Mint**: Mint native USDC on destination chain via
//!    `MessageTransmitterV2.receiveMessage()`
//!
//! ## Usage
//!
//! ```rust,ignore
//! let bridge = CctpBridge::try_from_ctx(CctpCtx {
//!     usdc_ethereum,
//!     usdc_base,
//!     ethereum_wallet,
//!     base_wallet,
//! })?;
//!
//! // Bridge 1 USDC from Ethereum to Base (USDC has 6 decimals)
//! let amount = U256::from(1_000_000); // 1 USDC
//! let receipt = bridge.burn(
//!     BridgeDirection::EthereumToBase, amount, recipient,
//! ).await?;
//! ```
//!
//! ## CCTP V2 Fast Transfer
//!
//! Fast transfers are enabled by setting `minFinalityThreshold` to
//! 1000 in the `depositForBurn()` call. The fee is dynamically
//! queried from Circle's API (`/v2/burn/USDC/fees`) before each burn
//! operation.
//!
//! **Timing**: ~40s Base->Ethereum, ~70s Ethereum->Base (measured)
//! **Cost**: 1 basis point (0.01%) of transfer amount

mod evm;
#[cfg(feature = "mock")]
mod mock_attestation;
#[cfg(feature = "mock")]
pub use mock_attestation::CctpAttestationMock;
#[cfg(feature = "mock")]
mod test_contracts;
#[cfg(feature = "mock")]
pub use test_contracts::{
    DeployedCctpChain, TestMintBurnToken, deploy_cctp_on_chain, link_chains, mint_usdc,
    set_max_burn_amount,
};

use std::mem::size_of;
use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, FixedBytes, TxHash, U256, address};
use alloy::sol;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use backon::Retryable;
use rain_math_float::Float;
use serde::Deserialize;
use st0x_float_macro::float;
use tracing::{debug, info, warn};

use st0x_evm::{EvmError, IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_float_serde::{deserialize_float_from_number_or_string, format_float_with_fallback};

use crate::BridgeDirection;
use evm::CctpEndpoint;

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

impl BridgeDirection {
    /// Returns the source CCTP domain for this bridge direction.
    pub(crate) const fn source_domain(self) -> u32 {
        match self {
            Self::EthereumToBase => ETHEREUM_DOMAIN,
            Self::BaseToEthereum => BASE_DOMAIN,
        }
    }

    /// Returns the destination CCTP domain for this bridge direction.
    pub(crate) const fn dest_domain(self) -> u32 {
        match self {
            Self::EthereumToBase => BASE_DOMAIN,
            Self::BaseToEthereum => ETHEREUM_DOMAIN,
        }
    }
}

/// CCTP TokenMessengerV2 contract address (same on all supported chains).
#[cfg(any(test, feature = "test-support"))]
pub const TOKEN_MESSENGER_V2: Address = address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
#[cfg(not(any(test, feature = "test-support")))]
const TOKEN_MESSENGER_V2: Address = address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");

/// CCTP MessageTransmitterV2 contract address (same on all supported chains).
#[cfg(any(test, feature = "test-support"))]
pub const MESSAGE_TRANSMITTER_V2: Address = address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");
#[cfg(not(any(test, feature = "test-support")))]
const MESSAGE_TRANSMITTER_V2: Address = address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");

#[cfg(any(test, feature = "test-support"))]
pub const CIRCLE_API_BASE: &str = "https://iris-api.circle.com";
#[cfg(not(any(test, feature = "test-support")))]
const CIRCLE_API_BASE: &str = "https://iris-api.circle.com";

/// Minimum finality threshold for CCTP V2 fast transfer (enables ~30 second transfers)
const FAST_TRANSFER_THRESHOLD: u32 = 1000;

/// Internal receipt from minting USDC on the destination chain.
///
/// Contains the actual amounts from the `MintAndWithdraw` event, which is the
/// source of truth for what was actually received after fee deduction.
#[derive(Debug)]
struct MintReceipt {
    /// Transaction hash of the mint transaction
    tx: TxHash,
    /// Actual USDC minted to recipient (NET of fees).
    amount: U256,
    /// Actual fee collected by Circle for this transfer.
    fee_collected: U256,
}

/// Response from Circle's attestation API for CCTP V2.
///
/// Contains both the CCTP message bytes and the Circle attestation signature,
/// which together are required to call `receiveMessage()` on the destination chain.
///
/// The nonce is extracted from the attested message, NOT from the original MessageSent
/// event (which contains a placeholder bytes32(0) in CCTP V2).
#[derive(Debug)]
pub struct AttestationResponse {
    /// CCTP message bytes from the attestation API.
    /// Unlike the MessageSent event message, this contains the real nonce
    /// filled in by Circle's attestation service.
    message: Bytes,
    /// Circle's attestation signature for the message.
    /// Required to prove the burn happened and authorize minting.
    attestation: Bytes,
    /// The real 32-byte CCTP V2 nonce extracted from the attested message.
    nonce: B256,
}

impl crate::Attestation for AttestationResponse {
    fn nonce(&self) -> B256 {
        self.nonce
    }

    fn as_bytes(&self) -> &[u8] {
        &self.attestation
    }

    fn message_bytes(&self) -> &[u8] {
        &self.message
    }
}

impl AttestationResponse {
    /// Reconstructs a response from a persisted message envelope and signature,
    /// re-deriving the nonce from the message so a stored attestation is
    /// self-validating (an all-zero placeholder nonce is rejected, mirroring a
    /// fresh poll).
    ///
    /// Validates that the message reaches the CCTP body offset
    /// (`MESSAGE_BODY_INDEX`) -- the same bound [`parse_received_message`] uses --
    /// not merely enough to extract a nonce. A truncated envelope that still
    /// carried a non-zero nonce would otherwise construct and then revert in
    /// `receiveMessage` on-chain instead of failing here, where the caller routes
    /// it to operator reconciliation. This is the shared constructor for both a
    /// fresh poll and a persisted-envelope resume, so both enforce identical
    /// rules. (It does not guarantee a mintable body, only a complete header.)
    ///
    /// Module-private: external callers reconstruct via
    /// [`crate::Bridge::reconstruct_attestation`] so they never depend on this
    /// concrete representation.
    fn from_parts(message: Bytes, attestation: Bytes) -> Result<Self, CctpError> {
        if message.len() < MESSAGE_BODY_INDEX {
            return Err(CctpError::MessageTooShortForRecovery {
                length: message.len(),
            });
        }

        let nonce = extract_nonce_from_message(&message)?;

        Ok(Self {
            message,
            attestation,
            nonce,
        })
    }
}

// CCTP V2 message layout (see Circle's evm-cctp-contracts: src/messages/v2/MessageV2.sol):
// - Bytes 0-3: version (4 bytes)
// - Bytes 4-7: source domain (4 bytes)
// - Bytes 8-11: destination domain (4 bytes)
// - Bytes 12-43: nonce (32 bytes) <- we extract this
// - Bytes 44+: remaining message data
// Minimum length required: 44 bytes (to include the full nonce)
const NONCE_INDEX: usize = 12;
const NONCE_SIZE: usize = size_of::<FixedBytes<32>>();
const MIN_MESSAGE_LENGTH: usize = NONCE_INDEX + NONCE_SIZE;
const SOURCE_DOMAIN_INDEX: usize = 4;
const DESTINATION_DOMAIN_INDEX: usize = 8;
const DOMAIN_SIZE: usize = size_of::<u32>();
const MESSAGE_BODY_INDEX: usize = 148;

/// Extracts the 32-byte nonce from a CCTP V2 message.
///
/// Used to extract the real nonce from the attested message returned by Circle's API.
/// The nonce in the original MessageSent event is always bytes32(0) in CCTP V2;
/// Circle's attestation service fills in the real nonce. An all-zero nonce in an
/// attested message therefore means Circle has not filled it in (or the response
/// is malformed), so we reject it rather than advancing the bridge with a bogus
/// placeholder nonce.
fn extract_nonce_from_message(message: &[u8]) -> Result<FixedBytes<32>, CctpError> {
    if message.len() < MIN_MESSAGE_LENGTH {
        return Err(CctpError::MessageTooShort {
            length: message.len(),
        });
    }

    let nonce = FixedBytes::<32>::from_slice(&message[NONCE_INDEX..NONCE_INDEX + 32]);

    if nonce.is_zero() {
        return Err(CctpError::PlaceholderNonce);
    }

    Ok(nonce)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CctpReceivedMessage<'a> {
    source_domain: u32,
    destination_domain: u32,
    nonce: B256,
    message_body: &'a [u8],
}

fn extract_domain(message: &[u8], index: usize) -> Result<u32, CctpError> {
    let Some(domain) = message.get(index..index + DOMAIN_SIZE) else {
        return Err(CctpError::MessageTooShortForRecovery {
            length: message.len(),
        });
    };

    Ok(u32::from_be_bytes([
        domain[0], domain[1], domain[2], domain[3],
    ]))
}

fn parse_received_message(message: &[u8]) -> Result<CctpReceivedMessage<'_>, CctpError> {
    if message.len() < MESSAGE_BODY_INDEX {
        return Err(CctpError::MessageTooShortForRecovery {
            length: message.len(),
        });
    }

    Ok(CctpReceivedMessage {
        source_domain: extract_domain(message, SOURCE_DOMAIN_INDEX)?,
        destination_domain: extract_domain(message, DESTINATION_DOMAIN_INDEX)?,
        nonce: FixedBytes::<32>::from_slice(&message[NONCE_INDEX..NONCE_INDEX + NONCE_SIZE]),
        message_body: &message[MESSAGE_BODY_INDEX..],
    })
}

/// Runtime context for constructing a [`CctpBridge`].
///
/// Provides the minimal set of values needed to construct the bridge.
/// CCTP contract addresses default to production addresses but can be
/// overridden for testing with locally deployed contracts.
/// Providers are obtained from the wallets via [`Wallet::provider()`].
pub struct CctpCtx<EthWallet, BaseWallet> {
    /// USDC token address on Ethereum
    pub usdc_ethereum: Address,
    /// USDC token address on Base
    pub usdc_base: Address,
    /// Wallet for submitting transactions on Ethereum
    pub ethereum_wallet: EthWallet,
    /// Wallet for submitting transactions on Base
    pub base_wallet: BaseWallet,
    /// Circle attestation/fee API base URL (test-only override).
    #[cfg(any(test, feature = "test-support"))]
    pub circle_api_base: String,
    /// `TokenMessengerV2` contract address (test-only override).
    #[cfg(any(test, feature = "test-support"))]
    pub token_messenger: Address,
    /// `MessageTransmitterV2` contract address (test-only override).
    #[cfg(any(test, feature = "test-support"))]
    pub message_transmitter: Address,
}

/// Circle CCTP bridge for Ethereum <-> Base USDC transfers.
///
/// # Example
///
/// ```rust,ignore
/// let bridge = CctpBridge::try_from_ctx(CctpCtx {
///     usdc_ethereum: USDC_ETHEREUM,
///     usdc_base: USDC_BASE,
///     ethereum_wallet,
///     base_wallet,
/// })?;
///
/// let amount = U256::from(1_000_000); // 1 USDC
/// let receipt = bridge.burn(BridgeDirection::EthereumToBase, amount, recipient).await?;
/// ```
pub struct CctpBridge<EthWallet: Wallet, BaseWallet: Wallet> {
    ethereum: CctpEndpoint<EthWallet>,
    base: CctpEndpoint<BaseWallet>,
    http_client: reqwest::Client,
    circle_api_base: String,
}

/// Errors that can occur during CCTP bridge operations.
#[derive(Debug, thiserror::Error)]
pub enum CctpError {
    #[error("EVM error: {0}")]
    Evm(#[from] EvmError),
    #[error("Contract view error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
    #[error("ABI decode error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    /// A burn scan could not confirm presence or absence: the queried node is not
    /// confirmations-deep past `from_block`, so an empty result may be RPC lag
    /// rather than a true absence. Retryable -- the caller must NOT re-burn on it.
    #[error("burn scan inconclusive: node not caught up past block {from_block}")]
    ScanInconclusive { from_block: u64 },
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Attestation timeout after {attempts} attempts: {source}")]
    AttestationTimeout {
        attempts: usize,
        source: AttestationError,
    },
    /// A `status == "complete"` attestation came back malformed (a required
    /// field absent, or hex that does not decode). Unlike `AttestationTimeout`
    /// this is a definitively-hard error: retrying cannot fix a complete-but-bad
    /// response, so it short-circuits the retry loop and is routed to immediate
    /// bridge failure rather than retried to the deadline.
    #[error("malformed complete attestation: {source}")]
    MalformedAttestation { source: AttestationError },
    #[error("MessageSent event not found in transaction receipt")]
    MessageSentEventNotFound,
    #[error("MintAndWithdraw event not found in transaction receipt")]
    MintAndWithdrawEventNotFound,
    #[error("Message too short for nonce extraction: got {length} bytes, need at least 44")]
    MessageTooShort { length: usize },
    #[error("Message too short for receiveMessage recovery: got {length} bytes, need at least 148")]
    MessageTooShortForRecovery { length: usize },
    #[error("CCTP message destination domain mismatch: expected {expected}, got {actual}")]
    MessageDestinationDomainMismatch { expected: u32, actual: u32 },
    #[error("already-minted CCTP nonce {nonce} had no matching MessageReceived log")]
    AlreadyMintedMessageNotFound { nonce: B256 },
    #[error(
        "recovered CCTP MessageReceived log for nonce {nonce} did not match the attested message"
    )]
    RecoveredMintMessageMismatch { nonce: B256 },
    #[error("recovered CCTP mint log for nonce {nonce} is missing transaction hash or log index")]
    RecoveredMintLogMissingTxHash { nonce: B256 },
    #[error("recovered CCTP mint transaction reverted: {tx_hash}")]
    RecoveredMintReceiptReverted { tx_hash: TxHash },
    #[error("MintAndWithdraw event not found in recovered CCTP mint transaction: {tx_hash}")]
    RecoveredMintAndWithdrawEventNotFound { tx_hash: TxHash },
    #[error(
        "Attested message carries the all-zero placeholder nonce; Circle has not \
         filled in the real nonce yet or the response is malformed"
    )]
    PlaceholderNonce,
    #[error("Fee calculation overflow")]
    FeeCalculationOverflow,
    #[error("Float operation error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("Amount too large for fee calculation: {0}")]
    AmountConversion(#[from] alloy::primitives::ruint::FromUintError<u128>),
    #[error("Fast transfer fee not available for {direction:?}")]
    FastTransferFeeNotAvailable { direction: BridgeDirection },
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
    #[error("Failed to parse attestation response: {0}")]
    JsonParse(#[from] serde_json::Error),
    #[error("Attestation pending: {status}")]
    Pending { status: String },
    #[error("No messages in attestation response")]
    NoMessages,
    #[error("Attestation response missing or non-string required field: {field}")]
    MissingField { field: &'static str },
    #[error("Attestation not yet available (HTTP {status})")]
    NotYetAvailable { status: u16 },
}

impl AttestationError {
    /// Whether polling should keep retrying on this error.
    ///
    /// Transient sources -- the attestation simply has not landed yet (`Pending`,
    /// `NotYetAvailable`, `NoMessages`) or a transport hiccup (`Http`, which also
    /// wraps any `reqwest` JSON-decode failure) -- are retryable. `MissingField`
    /// and `HexDecode` are not: they only arise *after* the `status == "complete"`
    /// check, so they mean Circle returned a complete-but-malformed response that
    /// retrying cannot fix. Failing fast on those surfaces a terminal bridge
    /// failure for operator reconciliation instead of retrying to the 24h deadline.
    /// (`JsonParse` is classified retryable too, for completeness -- it is not
    /// produced on the current poll path, where `reqwest`'s `.json()` surfaces
    /// decode errors as `Http`.)
    fn is_retryable(&self) -> bool {
        match self {
            Self::Http(_)
            | Self::JsonParse(_)
            | Self::Pending { .. }
            | Self::NoMessages
            | Self::NotYetAvailable { .. } => true,
            Self::HexDecode(_) | Self::MissingField { .. } => false,
        }
    }

    /// Maps the error that ended the poll loop to its terminal [`CctpError`],
    /// single-sourcing the retry-vs-malformed decision the `.when` gate uses: a
    /// retryable error here means the loop exhausted `attempts` (a timeout), while
    /// a non-retryable one short-circuited the loop (a definitively-malformed
    /// complete response). They route differently downstream -- a timeout is
    /// retried to the deadline, a malformed attestation fails the bridge fast.
    fn into_terminal(self, attempts: usize) -> CctpError {
        if self.is_retryable() {
            CctpError::AttestationTimeout {
                attempts,
                source: self,
            }
        } else {
            CctpError::MalformedAttestation { source: self }
        }
    }
}

/// Fee entry from Circle's `/v2/burn/USDC/fees/{source}/{dest}` API.
///
/// The API returns an array of these entries, one per finality threshold level.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FeeEntry {
    /// Finality threshold: 1000 = fast transfer, 2000 = standard transfer
    finality_threshold: u32,
    /// Minimum fee in basis points (1 = 0.01%). May be fractional (e.g., 1.3).
    #[serde(deserialize_with = "deserialize_float_from_number_or_string")]
    minimum_fee: Float,
}

impl<EthWallet: Wallet, BaseWallet: Wallet> CctpBridge<EthWallet, BaseWallet> {
    /// Constructs a `CctpBridge` from a runtime context.
    pub fn try_from_ctx(ctx: CctpCtx<EthWallet, BaseWallet>) -> Result<Self, CctpError> {
        #[cfg(any(test, feature = "test-support"))]
        let token_messenger = ctx.token_messenger;
        #[cfg(not(any(test, feature = "test-support")))]
        let token_messenger = TOKEN_MESSENGER_V2;

        #[cfg(any(test, feature = "test-support"))]
        let message_transmitter = ctx.message_transmitter;
        #[cfg(not(any(test, feature = "test-support")))]
        let message_transmitter = MESSAGE_TRANSMITTER_V2;

        let ethereum = CctpEndpoint::new(
            ctx.usdc_ethereum,
            token_messenger,
            message_transmitter,
            ctx.ethereum_wallet,
        );

        let base = CctpEndpoint::new(
            ctx.usdc_base,
            token_messenger,
            message_transmitter,
            ctx.base_wallet,
        );

        #[cfg(any(test, feature = "test-support"))]
        {
            let mut bridge = Self::new(ethereum, base)?;
            bridge.circle_api_base = ctx.circle_api_base;
            Ok(bridge)
        }

        #[cfg(not(any(test, feature = "test-support")))]
        Self::new(ethereum, base)
    }

    fn new(
        ethereum: CctpEndpoint<EthWallet>,
        base: CctpEndpoint<BaseWallet>,
    ) -> Result<Self, CctpError> {
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
                target: "bridge",
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
            target: "bridge",
            ?direction,
            fast_fee = %format_float_with_fallback(&fast_fee),
            "Retrieved fast transfer fee (bps)"
        );

        // Calculate maxFee: amount * fee_bps / 10000, ceiling
        let amount_ed = Float::from_fixed_decimal(amount, 0)?;
        let divisor = float!(10000);
        let max_fee_ed = ((amount_ed * fast_fee)? / divisor)?;

        // Ceiling: truncate to integer, add 1 if there was a fractional part
        let (truncated, lossless) = max_fee_ed.to_fixed_decimal_lossy(0)?;
        let max_fee = if lossless {
            truncated
        } else {
            truncated + U256::from(1)
        };

        Ok(max_fee)
    }

    /// Polls for attestation using CCTP V2 API.
    async fn poll_attestation_internal(
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

        info!(target: "bridge", %url, "Polling attestation API");

        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_secs(RETRY_INTERVAL_SECS))
            .with_max_times(MAX_ATTEMPTS);

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct MessageEntry {
            // Parsed leniently as raw `Value`, not `Option<String>`: a complete
            // response carrying a non-string `message`/`attestation` (e.g. a JSON
            // number) is a definitively-malformed complete attestation. Strong
            // `Option<String>` deserialization would instead fail the whole
            // `response.json()` as a retryable transport/JSON error, letting that
            // bad complete response retry to the deadline. Keeping the fields raw
            // defers the string check to *after* the `status == "complete"` gate,
            // where a missing-or-non-string field becomes a terminal failure.
            attestation: Option<serde_json::Value>,
            message: Option<serde_json::Value>,
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

            let attestation_hex = entry
                .attestation
                .as_ref()
                .and_then(|value| value.as_str())
                .ok_or(AttestationError::MissingField {
                    field: "attestation",
                })?;
            let message_hex = entry
                .message
                .as_ref()
                .and_then(|value| value.as_str())
                .ok_or(AttestationError::MissingField { field: "message" })?;

            let message = Bytes::from(alloy::hex::decode(message_hex)?);
            let attestation = Bytes::from(alloy::hex::decode(attestation_hex)?);

            Ok((message, attestation))
        };

        let (message, attestation) = fetch_attestation
            .retry(backoff)
            // A complete-but-malformed response cannot be fixed by retrying, so
            // stop the loop immediately on a non-retryable error; transient
            // sources keep retrying until `MAX_ATTEMPTS`.
            .when(AttestationError::is_retryable)
            .notify(|err, dur| match err {
                AttestationError::Pending { status } => {
                    debug!(target: "bridge", %status, ?dur, "Attestation pending, retrying");
                }
                AttestationError::NotYetAvailable { status } => {
                    debug!(target: "bridge", status, ?dur, "API non-success, retrying");
                }
                err => warn!(target: "bridge", ?err, ?dur, "Attestation error, retrying"),
            })
            .await
            .map_err(|err| err.into_terminal(MAX_ATTEMPTS))
            // The `.when` short-circuit returns without invoking `.notify`, so the
            // bridge layer is otherwise silent on a fast-fail. Log it here so a
            // terminal malformed-attestation failure is traceable at the bridge
            // target, distinct from an exhausted-retry timeout.
            .inspect_err(|error| {
                if let CctpError::MalformedAttestation { .. } = error {
                    warn!(target: "bridge", ?error, "Malformed complete attestation; failing bridge fast");
                }
            })?;

        // Validate through the same constructor a persisted-envelope resume uses,
        // so a fresh poll and a reconstruction enforce identical rules: a
        // truncated envelope or an all-zero placeholder nonce (the bytes32(0) the
        // MessageSent event leaks) fails fast here rather than being recorded and
        // then rejected on a later resume.
        AttestationResponse::from_parts(message, attestation)
    }

    /// Burns USDC on the source chain for the given bridge direction.
    async fn burn_internal<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<crate::BurnReceipt, CctpError> {
        let max_fee = self.query_fast_transfer_fee(amount, direction).await?;

        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum
                    .ensure_usdc_approval::<Registry>(amount)
                    .await?;
                self.ethereum
                    .deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base.ensure_usdc_approval::<Registry>(amount).await?;
                self.base
                    .deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
                    .await
            }
        }
    }

    /// Mints USDC on the destination chain for the given bridge direction.
    ///
    /// Returns the actual minted amount and fee collected from the `MintAndWithdraw` event.
    /// This is the source of truth for what the recipient actually received after fee deduction.
    async fn mint_internal<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<MintReceipt, CctpError> {
        match direction {
            BridgeDirection::EthereumToBase => {
                self.base
                    .claim::<Registry>(direction, message, attestation)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.ethereum
                    .claim::<Registry>(direction, message, attestation)
                    .await
            }
        }
    }

    #[cfg(test)]
    fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }

    /// Returns the receipt of an already-executed mint for the attested
    /// `message` on the destination chain of `direction`, or `None` if its nonce
    /// is unused.
    ///
    /// Used by crash-recovery resume to avoid re-submitting `receiveMessage`
    /// (which reverts on a consumed nonce) when a mint already landed but its
    /// confirming event was not yet persisted. Passing the full message (not
    /// just the nonce) lets the reconstruction match the on-chain log against
    /// the attested source domain and body.
    pub async fn find_existing_mint(
        &self,
        direction: BridgeDirection,
        message: &[u8],
    ) -> Result<Option<crate::MintReceipt>, CctpError> {
        let receipt = match direction {
            BridgeDirection::EthereumToBase => {
                self.base
                    .find_existing_mint::<OpenChainErrorRegistry>(direction, message)
                    .await?
            }
            BridgeDirection::BaseToEthereum => {
                self.ethereum
                    .find_existing_mint::<OpenChainErrorRegistry>(direction, message)
                    .await?
            }
        };

        Ok(receipt.map(|receipt| crate::MintReceipt {
            tx: receipt.tx,
            amount: receipt.amount,
            fee: receipt.fee_collected,
        }))
    }

    /// Returns `holder`'s USDC balance on Base, the destination chain for
    /// AlpacaToBase mints.
    ///
    /// Used as an idempotency guard before re-depositing on resume from `Bridged`:
    /// if the freshly minted USDC is no longer in the wallet, the vault deposit
    /// already landed, so re-submitting it would double-deposit (or revert).
    pub async fn base_usdc_balance(&self, holder: Address) -> Result<U256, CctpError> {
        self.base
            .usdc_balance::<OpenChainErrorRegistry>(holder)
            .await
    }
}

#[async_trait]
impl<EthWallet, BaseWallet> crate::Bridge for CctpBridge<EthWallet, BaseWallet>
where
    EthWallet: Wallet,
    BaseWallet: Wallet,
{
    type Error = CctpError;
    type Attestation = AttestationResponse;

    async fn burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<crate::BurnReceipt, Self::Error> {
        self.burn_internal::<OpenChainErrorRegistry>(direction, amount, recipient)
            .await
    }

    async fn poll_attestation(
        &self,
        direction: BridgeDirection,
        burn_tx: TxHash,
    ) -> Result<Self::Attestation, Self::Error> {
        self.poll_attestation_internal(direction, burn_tx).await
    }

    async fn mint(
        &self,
        direction: BridgeDirection,
        attestation: &Self::Attestation,
    ) -> Result<crate::MintReceipt, Self::Error> {
        let internal = self
            .mint_internal::<OpenChainErrorRegistry>(
                direction,
                attestation.message.clone(),
                attestation.attestation.clone(),
            )
            .await?;

        Ok(crate::MintReceipt {
            tx: internal.tx,
            amount: internal.amount,
            fee: internal.fee_collected,
        })
    }

    fn reconstruct_attestation(
        &self,
        message: Vec<u8>,
        attestation: Vec<u8>,
    ) -> Result<Self::Attestation, Self::Error> {
        AttestationResponse::from_parts(Bytes::from(message), Bytes::from(attestation))
    }

    /// Scans the burn source chain for an already-submitted burn matching
    /// `(amount, destinationDomain, recipient)` at or after `from_block`, for
    /// crash-safe resume. Delegates to the source endpoint for the given
    /// direction.
    async fn find_recent_burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<TxHash>, Self::Error> {
        let dest_domain = direction.dest_domain();
        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum
                    .find_recent_burn(amount, dest_domain, recipient, from_block)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base
                    .find_recent_burn(amount, dest_domain, recipient, from_block)
                    .await
            }
        }
    }

    /// Scans the mint destination chain for an already-submitted mint to
    /// `recipient` strictly after `from_block`, for crash-safe resume. Delegates
    /// to the destination endpoint for the given direction.
    async fn find_recent_mint(
        &self,
        direction: BridgeDirection,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<crate::MintReceipt>, Self::Error> {
        let receipt = match direction {
            BridgeDirection::EthereumToBase => {
                self.base.find_recent_mint(recipient, from_block).await?
            }
            BridgeDirection::BaseToEthereum => {
                self.ethereum
                    .find_recent_mint(recipient, from_block)
                    .await?
            }
        };

        Ok(receipt.map(|receipt| crate::MintReceipt {
            tx: receipt.tx,
            amount: receipt.amount,
            fee: receipt.fee_collected,
        }))
    }

    async fn destination_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error> {
        match direction {
            BridgeDirection::EthereumToBase => self.base.current_block().await,
            BridgeDirection::BaseToEthereum => self.ethereum.current_block().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::address;
    use alloy::primitives::{B256, b256, keccak256};
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::signers::Signer;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::{SolCall, SolEvent};
    use httpmock::prelude::*;
    use itertools::Itertools;
    use proptest::prelude::*;
    use rand::Rng;
    use st0x_evm::NoOpErrorRegistry;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::{Attestation, Bridge};

    const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

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
    ) -> Result<
        CctpBridge<
            RawPrivateKeyWallet<impl Provider + Clone + use<>>,
            RawPrivateKeyWallet<impl Provider + Clone + use<>>,
        >,
        Box<dyn std::error::Error>,
    > {
        let ethereum_provider = ProviderBuilder::new().connect(ethereum_endpoint).await?;

        let base_provider = ProviderBuilder::new().connect(base_endpoint).await?;

        let ethereum_wallet = RawPrivateKeyWallet::new(private_key, ethereum_provider, 1)?;
        let base_wallet = RawPrivateKeyWallet::new(private_key, base_provider, 1)?;

        let ethereum = CctpEndpoint::new(
            usdc_address,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            ethereum_wallet,
        );

        let base = CctpEndpoint::new(
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            base_wallet,
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

        fetch_attestation.retry(backoff).await.unwrap();
        assert_eq!(mock.calls(), 1, "Expected exactly 1 API call");
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

        let error = fetch_attestation.retry(backoff).await.unwrap_err();

        assert!(
            matches!(error, AttestationError::NotYetAvailable { status: 404 }),
            "Expected AttestationError::NotYetAvailable with status 404"
        );
        assert!(
            mock.calls() == 4,
            "Expected exactly 4 attempts (1 initial + 3 retries)"
        );
    }

    #[tokio::test]
    async fn poll_attestation_rejects_all_zero_nonce() {
        let server = MockServer::start();

        // Complete attestation whose message carries the placeholder bytes32(0) nonce that
        // CCTP V2 emits in the MessageSent event. Circle should never return this on a complete
        // attestation; the bridge must fail fast rather than thread it through as a real nonce.
        // Full-length so it reaches the nonce check rather than the envelope-length guard.
        let zero_nonce_message =
            build_nonce_message(&[0u8; NONCE_INDEX], [0u8; 32], &[0u8; MESSAGE_BODY_INDEX]);

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": "0x1234567890abcdef",
                    "message": alloy::hex::encode_prefixed(&zero_nonce_message),
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let error = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CctpError::PlaceholderNonce),
            "Expected CctpError::PlaceholderNonce, got: {error:?}"
        );
        assert_eq!(mock.calls(), 1, "Expected exactly 1 API call");
    }

    #[tokio::test]
    async fn poll_attestation_fails_fast_on_malformed_complete_response() {
        let server = MockServer::start();

        // A `complete` attestation that is missing the required `message` field.
        // This is a definitively-hard error (a complete response cannot become
        // well-formed by retrying), so the poll must fail fast with
        // `MalformedAttestation` after a single call -- NOT retry to the 60-attempt
        // timeout (which at the real 5s interval would take 5 minutes).
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": "0x1234567890abcdef",
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let error = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::MalformedAttestation {
                    source: AttestationError::MissingField { field: "message" }
                }
            ),
            "Expected CctpError::MalformedAttestation for a complete-but-missing-field \
             response, got: {error:?}"
        );
        assert_eq!(
            mock.calls(),
            1,
            "a malformed complete response must fail fast, not retry to the timeout"
        );
    }

    #[tokio::test]
    async fn poll_attestation_fails_fast_on_non_string_complete_field() {
        let server = MockServer::start();

        // A `complete` response whose `message` is a JSON number, not a hex
        // string. Strong `Option<String>` deserialization would fail the whole
        // `response.json()` as a retryable transport error, letting this
        // definitively-malformed complete response retry to the 60-attempt
        // timeout. The lenient `Value` parse instead classifies it as a terminal
        // `MalformedAttestation` after a single call.
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": "0x1234567890abcdef",
                    "message": 123,
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let error = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::MalformedAttestation {
                    source: AttestationError::MissingField { field: "message" }
                }
            ),
            "Expected CctpError::MalformedAttestation for a complete-but-non-string \
             field, got: {error:?}"
        );
        assert_eq!(
            mock.calls(),
            1,
            "a non-string complete field must fail fast, not retry to the timeout"
        );
    }

    #[tokio::test]
    async fn poll_attestation_fails_fast_on_non_string_attestation_field() {
        let server = MockServer::start();

        // The `attestation` field is extracted before `message`, so a non-string
        // `attestation` on a complete response must fail fast and report the
        // `attestation` field label (not `message`). Guards the symmetric arm of
        // the lenient-parse validation.
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": 123,
                    "message": "0x1234567890abcdef",
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let error = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::MalformedAttestation {
                    source: AttestationError::MissingField {
                        field: "attestation"
                    }
                }
            ),
            "Expected MalformedAttestation reporting the `attestation` field, got: {error:?}"
        );
        assert_eq!(
            mock.calls(),
            1,
            "a non-string attestation field must fail fast, not retry to the timeout"
        );
    }

    #[tokio::test]
    async fn poll_attestation_fails_fast_on_invalid_hex_complete_field() {
        let server = MockServer::start();

        // A `complete` response whose `message` is a string but not valid hex.
        // Hex decoding only happens after the `status == "complete"` gate, so this
        // is a terminal `MalformedAttestation` (HexDecode), not a retryable error.
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": "0x1234567890abcdef",
                    "message": "not-hex-at-all",
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let error = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::MalformedAttestation {
                    source: AttestationError::HexDecode(_)
                }
            ),
            "Expected CctpError::MalformedAttestation with a HexDecode source for an \
             invalid-hex complete field, got: {error:?}"
        );
        assert_eq!(
            mock.calls(),
            1,
            "an invalid-hex complete field must fail fast, not retry to the timeout"
        );
    }

    #[tokio::test]
    async fn poll_attestation_returns_real_nonce_from_attested_message() {
        let server = MockServer::start();

        let expected_nonce = [0xABu8; 32];
        let message = build_nonce_message(
            &[0u8; NONCE_INDEX],
            expected_nonce,
            &[0u8; MESSAGE_BODY_INDEX],
        );

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v2/messages/{ETHEREUM_DOMAIN}"));
            then.status(200).json_body(serde_json::json!({
                "messages": [{
                    "attestation": "0x1234567890abcdef",
                    "message": alloy::hex::encode_prefixed(&message),
                    "status": "complete"
                }]
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
        .unwrap()
        .with_circle_api_base(server.base_url());

        let burn_tx = b256!("1234567890123456789012345678901234567890123456789012345678901234");

        let response = bridge
            .poll_attestation_internal(BridgeDirection::EthereumToBase, burn_tx)
            .await
            .unwrap();

        assert_eq!(response.nonce, FixedBytes::from(expected_nonce));
        assert_eq!(mock.calls(), 1, "Expected exactly 1 API call");
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
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, U256::ZERO);

        bridge
            .ethereum
            .ensure_usdc_approval::<NoOpErrorRegistry>(amount)
            .await
            .unwrap();

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
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, higher_amount)
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

        bridge
            .ethereum
            .ensure_usdc_approval::<NoOpErrorRegistry>(amount)
            .await
            .unwrap();

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
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, initial_allowance_amount)
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
            .ensure_usdc_approval::<NoOpErrorRegistry>(required_amount)
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
    ) -> Result<
        CctpBridge<
            RawPrivateKeyWallet<impl Provider + Clone + use<>>,
            RawPrivateKeyWallet<impl Provider + Clone + use<>>,
        >,
        Box<dyn std::error::Error>,
    > {
        let ethereum_provider = ProviderBuilder::new().connect(ethereum_endpoint).await?;

        let base_provider = ProviderBuilder::new().connect(base_endpoint).await?;

        let ethereum_wallet = RawPrivateKeyWallet::new(private_key, ethereum_provider, 1)?;
        let base_wallet = RawPrivateKeyWallet::new(private_key, base_provider, 1)?;

        let ethereum = CctpEndpoint::new(
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            ethereum_wallet,
        );

        let base = CctpEndpoint::new(
            base_usdc_address,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            base_wallet,
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
        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(initial_allowance, U256::ZERO);

        bridge
            .base
            .ensure_usdc_approval::<NoOpErrorRegistry>(amount)
            .await
            .unwrap();

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
        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        bridge
            .base
            .approve_usdc::<NoOpErrorRegistry>(spender, higher_amount)
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

        bridge
            .base
            .ensure_usdc_approval::<NoOpErrorRegistry>(amount)
            .await
            .unwrap();

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
        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        bridge
            .base
            .approve_usdc::<NoOpErrorRegistry>(spender, initial_allowance_amount)
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
            .ensure_usdc_approval::<NoOpErrorRegistry>(required_amount)
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
        base_anvil: AnvilInstance,
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
                when.method(GET).path_includes("/v2/burn/USDC/fees/");
                then.status(200).json_body(serde_json::json!([
                    {"finalityThreshold": 1000, "minimumFee": 1},
                    {"finalityThreshold": 2000, "minimumFee": 0}
                ]));
            });

            Ok(Self {
                _ethereum_anvil: ethereum_anvil,
                base_anvil,
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
            CctpBridge<
                RawPrivateKeyWallet<impl Provider + Clone + use<>>,
                RawPrivateKeyWallet<impl Provider + Clone + use<>>,
            >,
            Box<dyn std::error::Error>,
        > {
            self.create_bridge_with_key(&self.deployer_key).await
        }

        /// Builds a bridge whose wallet is `private_key` rather than the deployer.
        /// Used to simulate a burn submitted by a different depositor so the
        /// depositor clause in `find_recent_burn`'s predicate can be exercised.
        async fn create_bridge_with_key(
            &self,
            private_key: &B256,
        ) -> Result<
            CctpBridge<
                RawPrivateKeyWallet<impl Provider + Clone + use<>>,
                RawPrivateKeyWallet<impl Provider + Clone + use<>>,
            >,
            Box<dyn std::error::Error>,
        > {
            let ethereum_provider = ProviderBuilder::new()
                .connect(&self.ethereum_endpoint)
                .await?;

            let base_provider = ProviderBuilder::new().connect(&self.base_endpoint).await?;

            let ethereum_wallet = RawPrivateKeyWallet::new(private_key, ethereum_provider, 1)?;
            let base_wallet = RawPrivateKeyWallet::new(private_key, base_provider, 1)?;

            let ethereum = CctpEndpoint::new(
                self.ethereum.usdc,
                self.ethereum.token_messenger,
                self.ethereum.message_transmitter,
                ethereum_wallet,
            );

            let base = CctpEndpoint::new(
                self.base.usdc,
                self.base.token_messenger,
                self.base.message_transmitter,
                base_wallet,
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

        let recipient = bridge.base.owner();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
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

        let recipient = bridge.ethereum.owner();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::BaseToEthereum, amount, recipient)
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

        let recipient = bridge.base.owner();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        assert!(!mint_receipt.tx.is_zero());
        assert_eq!(mint_receipt.amount, amount);
    }

    #[tokio::test]
    async fn test_full_bridge_base_to_ethereum() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        assert!(!mint_receipt.tx.is_zero());
        assert_eq!(mint_receipt.amount, amount);
    }

    #[tokio::test]
    async fn claim_on_base_parses_mint_and_withdraw_event() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(2_500_000u64); // 2.5 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Call claim() directly on the Evm instance
        let mint_receipt = bridge
            .base
            .claim::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        assert!(!mint_receipt.tx.is_zero(), "tx hash should be set");
        assert_eq!(
            mint_receipt.amount, amount,
            "amount should match burned amount"
        );
        assert_eq!(
            mint_receipt.fee_collected,
            U256::ZERO,
            "fee should be 0 in mock"
        );
    }

    #[tokio::test]
    async fn claim_on_ethereum_parses_mint_and_withdraw_event() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let amount = U256::from(7_500_000u64); // 7.5 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Call claim() directly on the Evm instance
        let mint_receipt = bridge
            .ethereum
            .claim::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        assert!(!mint_receipt.tx.is_zero(), "tx hash should be set");
        assert_eq!(
            mint_receipt.amount, amount,
            "amount should match burned amount"
        );
        assert_eq!(
            mint_receipt.fee_collected,
            U256::ZERO,
            "fee should be 0 in mock"
        );
    }

    #[tokio::test]
    async fn find_existing_mint_returns_none_before_mint_and_recovers_receipt_after() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(2_500_000u64); // 2.5 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Before the mint the nonce is unconsumed, so there is nothing to recover.
        let before = bridge
            .find_existing_mint(BridgeDirection::EthereumToBase, &message_with_nonce)
            .await
            .unwrap();
        assert_eq!(
            before, None,
            "unused nonce must report no existing mint, got: {before:?}"
        );

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        // After the mint the nonce is consumed; recovery reconstructs the exact
        // receipt (tx + net amount + fee) from the on-chain events without re-minting.
        let recovered = bridge
            .find_existing_mint(BridgeDirection::EthereumToBase, &message_with_nonce)
            .await
            .unwrap()
            .expect("consumed nonce must report the existing mint");

        assert_eq!(
            recovered.tx, mint_receipt.tx,
            "recovered mint tx must match"
        );
        assert_eq!(
            recovered.amount, mint_receipt.amount,
            "recovered net amount must match the MintAndWithdraw event"
        );
        assert_eq!(
            recovered.fee, mint_receipt.fee_collected,
            "recovered fee must match the MintAndWithdraw event"
        );

        // Wrong destination domain: this message was minted to Base, so trying to
        // reconstruct it as a Base->Ethereum transfer must reject, not recover.
        let wrong_direction_error = bridge
            .base
            .find_existing_mint::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                &message_with_nonce,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                wrong_direction_error,
                CctpError::MessageDestinationDomainMismatch {
                    expected: TEST_ETHEREUM_DOMAIN,
                    actual: TEST_BASE_DOMAIN,
                }
            ),
            "wrong destination domain must not recover: {wrong_direction_error:?}"
        );

        // A message whose body no longer matches the on-chain MessageReceived log
        // for its (consumed) nonce must not be recovered.
        let nonce = extract_nonce_from_message(&message_with_nonce).unwrap();
        let mut mismatched_message = message_with_nonce.to_vec();
        assert!(
            mismatched_message.len() > MESSAGE_BODY_INDEX,
            "test CCTP message must include a body byte to mutate"
        );
        mismatched_message[MESSAGE_BODY_INDEX] ^= 0xFF;

        let mismatched_message_error = bridge
            .base
            .find_existing_mint::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &mismatched_message,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                mismatched_message_error,
                CctpError::RecoveredMintMessageMismatch { nonce: event_nonce }
                    if event_nonce == nonce
            ),
            "message-body mismatch must not recover: {mismatched_message_error:?}"
        );
    }

    #[tokio::test]
    async fn mint_recovers_when_receive_message_nonce_already_used() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(3_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let first_mint = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation.clone(),
            )
            .await
            .unwrap();

        // A second mint of the same attested message hits the already-used-nonce
        // revert; claim() recovers the original mint instead of failing.
        let recovered_mint = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        assert_eq!(
            recovered_mint.tx, first_mint.tx,
            "recovery must return the original mint transaction"
        );
        assert_eq!(recovered_mint.amount, first_mint.amount);
        assert_eq!(recovered_mint.fee_collected, first_mint.fee_collected);

        // A revert whose nonce is NOT marked used on-chain is not an already-minted
        // case: the reactive path must re-surface the original submit error rather
        // than fabricate a recovery outcome. Mutating the nonce yields a
        // never-minted nonce on a structurally valid message (usedNonces == 0).
        let sentinel_error = || EvmError::Reverted {
            tx_hash: TxHash::repeat_byte(0xEE),
        };
        let mut unused_nonce_message = message_with_nonce.to_vec();
        unused_nonce_message[NONCE_INDEX..NONCE_INDEX + NONCE_SIZE].fill(0xAB);

        let unused_nonce_error = bridge
            .base
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &unused_nonce_message,
                sentinel_error(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                unused_nonce_error,
                CctpError::Evm(EvmError::Reverted { tx_hash })
                    if tx_hash == TxHash::repeat_byte(0xEE)
            ),
            "unused nonce must propagate the original submit error: {unused_nonce_error:?}"
        );
    }

    #[tokio::test]
    async fn test_mint_on_ethereum_with_invalid_attestation() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (_, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                invalid_attestation,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, CctpError::Evm(_)),
            "expected CctpError::Evm, got: {err:?}"
        );
        assert!(
            err.to_string().contains("ECDSA: invalid signature"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn test_mint_on_base_with_invalid_attestation() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let invalid_attestation = Bytes::from(vec![0u8; 65]);

        let err = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message,
                invalid_attestation,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, CctpError::Evm(_)),
            "expected CctpError::Evm, got: {err:?}"
        );
        assert!(
            err.to_string().contains("ECDSA: invalid signature"),
            "got: {err}"
        );
    }

    fn build_nonce_message(header: &[u8], nonce: [u8; 32], trailer: &[u8]) -> Vec<u8> {
        header
            .iter()
            .copied()
            .chain(nonce)
            .chain(trailer.iter().copied())
            .collect_vec()
    }

    #[test]
    fn parse_received_message_extracts_recovery_fields() {
        let nonce = [0xAB; 32];
        let body = [0x01, 0x02, 0x03];
        let mut message = vec![0u8; MESSAGE_BODY_INDEX + body.len()];
        message[SOURCE_DOMAIN_INDEX..SOURCE_DOMAIN_INDEX + DOMAIN_SIZE]
            .copy_from_slice(&TEST_ETHEREUM_DOMAIN.to_be_bytes());
        message[DESTINATION_DOMAIN_INDEX..DESTINATION_DOMAIN_INDEX + DOMAIN_SIZE]
            .copy_from_slice(&TEST_BASE_DOMAIN.to_be_bytes());
        message[NONCE_INDEX..NONCE_INDEX + NONCE_SIZE].copy_from_slice(&nonce);
        message[MESSAGE_BODY_INDEX..].copy_from_slice(&body);

        let parsed = parse_received_message(&message).unwrap();

        assert_eq!(parsed.source_domain, TEST_ETHEREUM_DOMAIN);
        assert_eq!(parsed.destination_domain, TEST_BASE_DOMAIN);
        assert_eq!(parsed.nonce, FixedBytes::from(nonce));
        assert_eq!(parsed.message_body, body);
    }

    #[test]
    fn parse_received_message_rejects_messages_without_body_offset() {
        let message = [0u8; MESSAGE_BODY_INDEX - 1];

        let err = parse_received_message(&message).unwrap_err();

        assert!(
            matches!(
                err,
                CctpError::MessageTooShortForRecovery {
                    length
                } if length == MESSAGE_BODY_INDEX - 1
            ),
            "expected MessageTooShortForRecovery, got: {err:?}"
        );
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
        let expected_nonce = [0xAB; 32];
        let message = build_nonce_message(&[0xCD; NONCE_INDEX], expected_nonce, &[]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    #[test]
    fn extract_nonce_rejects_all_zero_placeholder() {
        // The CCTP V2 MessageSent event carries an all-zero placeholder nonce.
        // Circle's attestation service fills in the real nonce; an all-zero nonce
        // in an attested message means it was never filled (or the response is
        // malformed), so extraction must reject it rather than advance the bridge
        // with a bogus nonce.
        let message = build_nonce_message(&[0xCD; NONCE_INDEX], [0u8; 32], &[0xEF; 56]);

        let err = extract_nonce_from_message(&message).unwrap_err();

        assert!(
            matches!(err, CctpError::PlaceholderNonce),
            "Expected PlaceholderNonce, got: {err:?}"
        );
    }

    #[test]
    fn from_parts_reconstructs_response_with_message_derived_nonce() {
        // A persisted envelope resumes a mint offline: `from_parts` must rebuild
        // the response with the nonce re-derived from the message (not trusting a
        // separately-stored copy) and preserve the attestation signature verbatim.
        let expected_nonce: [u8; 32] = core::array::from_fn(|index| {
            u8::try_from(index + 1).expect("index 0..31 + 1 always fits in u8")
        });
        // A full CCTP envelope: header + nonce + body, length >= MESSAGE_BODY_INDEX.
        let body = vec![0xCD; MESSAGE_BODY_INDEX];
        let message = build_nonce_message(&[0u8; NONCE_INDEX], expected_nonce, &body);
        let attestation = vec![0xAB; 65];

        let response = AttestationResponse::from_parts(
            Bytes::from(message.clone()),
            Bytes::from(attestation.clone()),
        )
        .unwrap();

        assert_eq!(response.nonce(), B256::from(expected_nonce));
        assert_eq!(response.as_bytes(), attestation.as_slice());
        assert_eq!(response.message_bytes(), message.as_slice());
    }

    #[test]
    fn from_parts_accepts_message_at_body_index_boundary() {
        // A message of exactly MESSAGE_BODY_INDEX bytes is the shortest envelope
        // `from_parts` accepts: one byte shorter is rejected (see
        // from_parts_rejects_truncated_envelope_with_valid_nonce). This pins the
        // boundary to MESSAGE_BODY_INDEX, not MIN_MESSAGE_LENGTH.
        let expected_nonce: [u8; 32] = core::array::from_fn(|index| {
            u8::try_from(index + 1).expect("index 0..31 + 1 always fits in u8")
        });
        let trailer_len = MESSAGE_BODY_INDEX - MIN_MESSAGE_LENGTH;
        let message =
            build_nonce_message(&[0u8; NONCE_INDEX], expected_nonce, &vec![0u8; trailer_len]);
        assert_eq!(message.len(), MESSAGE_BODY_INDEX);

        let response = AttestationResponse::from_parts(
            Bytes::from(message.clone()),
            Bytes::from(vec![0xAB; 65]),
        )
        .unwrap();

        assert_eq!(response.nonce(), B256::from(expected_nonce));
        assert_eq!(response.message_bytes(), message.as_slice());
    }

    #[test]
    fn from_parts_rejects_truncated_envelope_with_valid_nonce() {
        // A nonce-bearing but truncated envelope (long enough to extract a nonce,
        // too short to be a full CCTP message) is corrupt persisted data. It must
        // be rejected at reconstruction rather than reconstructing and reverting
        // in `receiveMessage` on-chain.
        let expected_nonce: [u8; 32] = core::array::from_fn(|index| {
            u8::try_from(index + 1).expect("index 0..31 + 1 always fits in u8")
        });
        let truncated_len = MESSAGE_BODY_INDEX - 1;
        let body_len = truncated_len - MIN_MESSAGE_LENGTH;
        let message =
            build_nonce_message(&[0u8; NONCE_INDEX], expected_nonce, &vec![0u8; body_len]);
        assert_eq!(message.len(), truncated_len);

        let err =
            AttestationResponse::from_parts(Bytes::from(message), Bytes::from(vec![0xAB; 65]))
                .unwrap_err();

        assert!(
            matches!(err, CctpError::MessageTooShortForRecovery { length } if length == truncated_len),
            "Expected MessageTooShortForRecovery, got: {err:?}"
        );
    }

    #[test]
    fn from_parts_rejects_placeholder_nonce() {
        // An all-zero nonce means the attested message was never filled in by
        // Circle; reconstructing from it must fail rather than mint with a bogus
        // nonce, mirroring a fresh poll. The envelope is full-length so it reaches
        // the nonce check rather than the length check.
        let message =
            build_nonce_message(&[0xCD; NONCE_INDEX], [0u8; 32], &[0xEF; MESSAGE_BODY_INDEX]);

        let err =
            AttestationResponse::from_parts(Bytes::from(message), Bytes::from(vec![0xAB; 65]))
                .unwrap_err();

        assert!(
            matches!(err, CctpError::PlaceholderNonce),
            "Expected PlaceholderNonce, got: {err:?}"
        );
    }

    #[test]
    fn fee_entry_deserializes_float_minimum_fee() {
        let json = r#"{"finalityThreshold": 1000, "minimumFee": 1.3}"#;

        let entry: FeeEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.finality_threshold, 1000);
        assert!(
            entry
                .minimum_fee
                .eq(Float::parse("1.3".to_string()).unwrap())
                .unwrap()
        );
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
            // The all-zero nonce is the CCTP placeholder and is rejected, not extracted.
            prop_assume!(nonce != [0u8; 32]);
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
            // The all-zero nonce is the CCTP placeholder and is rejected, not extracted.
            prop_assume!(nonce != [0u8; 32]);
            let message = build_nonce_message(&header, nonce, &trailer);

            let extracted = extract_nonce_from_message(&message).unwrap();

            prop_assert_eq!(extracted, FixedBytes::from(nonce));
        }
    }

    #[tokio::test]
    async fn ensure_usdc_approval_approves_when_existing_allowance_is_lower() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // Check initial allowance is 0
        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::ZERO,
            "Initial allowance should be 0"
        );

        // First burn sets allowance to 1 USDC
        let small_amount = U256::from(1_000_000u64); // 1 USDC
        bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                small_amount,
                recipient,
            )
            .await
            .unwrap();

        // After burn, allowance should be consumed (0) since we approved exactly what we needed
        let after_first_burn = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            after_first_burn,
            U256::ZERO,
            "Allowance should be 0 after burn consumed it"
        );

        // Second burn with larger amount should update allowance and succeed
        let large_amount = U256::from(100_000_000u64); // 100 USDC
        let receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                large_amount,
                recipient,
            )
            .await
            .unwrap();

        assert!(!receipt.tx.is_zero(), "Second burn should succeed");
        assert_eq!(receipt.amount, large_amount);
    }

    #[tokio::test]
    async fn mint_returns_amount_received_and_fee_from_mint_and_withdraw_event() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let burn_amount = U256::from(1_000_000u64); // 1 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                burn_amount,
                recipient,
            )
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        // The MintAndWithdraw event should contain:
        // - amount: actual USDC minted to recipient (net of fee)
        // - fee_collected: the actual fee taken by Circle
        //
        // For the test mock contracts, there's no actual fee deduction,
        // so amount should equal burn_amount and fee_collected should be 0.
        // In production, amount = burn_amount - fee_collected.
        assert!(!mint_receipt.tx.is_zero(), "Transaction hash should be set");
        assert_eq!(
            mint_receipt.amount, burn_amount,
            "Minted amount should match burn amount in test (no fee in mock)"
        );
        assert_eq!(
            mint_receipt.fee_collected,
            U256::ZERO,
            "Fee should be zero in mock contracts"
        );
    }

    #[tokio::test]
    async fn mint_returns_amount_received_and_fee_for_base_to_ethereum_direction() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let burn_amount = U256::from(5_000_000u64); // 5 USDC

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                burn_amount,
                recipient,
            )
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        assert!(!mint_receipt.tx.is_zero(), "Transaction hash should be set");
        assert_eq!(
            mint_receipt.amount, burn_amount,
            "Minted amount should match burn amount in test (no fee in mock)"
        );
        assert_eq!(
            mint_receipt.fee_collected,
            U256::ZERO,
            "Fee should be zero in mock contracts"
        );
    }

    #[tokio::test]
    async fn find_recent_mint_returns_receipt_of_real_mint() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let burn_amount = U256::from(5_000_000u64);

        // Capture the destination (Ethereum) head before minting, exactly as the
        // resume path does via `Bridge::destination_block`.
        let from_block = bridge
            .destination_block(BridgeDirection::BaseToEthereum)
            .await
            .unwrap();

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                burn_amount,
                recipient,
            )
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        let found = bridge
            .find_recent_mint(BridgeDirection::BaseToEthereum, recipient, from_block)
            .await
            .unwrap()
            .expect("scan must find the submitted mint");

        assert_eq!(
            found.tx, mint_receipt.tx,
            "scan must return the real mint's tx"
        );
        assert_eq!(
            found.amount, mint_receipt.amount,
            "adopted amount must match the mint",
        );
        assert_eq!(
            found.fee, mint_receipt.fee_collected,
            "adopted fee must match the mint",
        );
    }

    #[tokio::test]
    async fn find_recent_mint_returns_none_for_wrong_recipient_or_below_scan_bound() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let burn_amount = U256::from(5_000_000u64);

        let from_block = bridge
            .destination_block(BridgeDirection::BaseToEthereum)
            .await
            .unwrap();

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                burn_amount,
                recipient,
            )
            .await
            .unwrap();

        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, false)
            .await
            .unwrap();

        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                message_with_nonce,
                attestation,
            )
            .await
            .unwrap();

        let other_recipient = address!("0x000000000000000000000000000000000000dEaD");

        assert_eq!(
            bridge
                .find_recent_mint(BridgeDirection::BaseToEthereum, other_recipient, from_block)
                .await
                .unwrap(),
            None,
            "a mint to a different recipient must not be adopted",
        );

        // Advance the Ethereum head past the mint with an unrelated burn, then
        // scan from the new head: the mint now sits below the scan bound and must
        // not be adopted (as a prior transfer's mint would be excluded on resume).
        bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                burn_amount,
                recipient,
            )
            .await
            .unwrap();

        let head_above_mint = bridge
            .destination_block(BridgeDirection::BaseToEthereum)
            .await
            .unwrap();

        assert_eq!(
            bridge
                .find_recent_mint(BridgeDirection::BaseToEthereum, recipient, head_above_mint)
                .await
                .unwrap(),
            None,
            "a mint below the scan bound must not be adopted",
        );
    }

    #[tokio::test]
    async fn multiple_sequential_burns_on_base_succeed() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.ethereum.owner();
        let amount = U256::from(25_000_000u64); // 25 USDC

        for i in 1..=5 {
            let receipt = bridge
                .burn_internal::<NoOpErrorRegistry>(
                    BridgeDirection::BaseToEthereum,
                    amount,
                    recipient,
                )
                .await
                .unwrap();

            assert!(!receipt.tx.is_zero(), "Burn {i}: tx hash should be set");
            assert_eq!(receipt.amount, amount, "Burn {i}: amount should match");
        }
    }

    #[tokio::test]
    async fn find_recent_burn_returns_tx_of_real_burn() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();
        let recipient = bridge.ethereum.owner();
        let amount = U256::from(25_000_000u64);

        let base_provider = ProviderBuilder::new()
            .connect(cctp.base_endpoint.as_str())
            .await
            .unwrap();
        let from_block = base_provider.get_block_number().await.unwrap();

        let receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::BaseToEthereum, amount, recipient)
            .await
            .unwrap();

        let found = bridge
            .find_recent_burn(
                BridgeDirection::BaseToEthereum,
                amount,
                recipient,
                from_block,
            )
            .await
            .unwrap();

        assert_eq!(
            found,
            Some(receipt.tx),
            "scan must return the real burn's tx for the matching amount + recipient",
        );
    }

    #[tokio::test]
    async fn find_recent_burn_returns_none_for_wrong_amount_or_recipient() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();
        let recipient = bridge.ethereum.owner();
        let amount = U256::from(25_000_000u64);

        let base_provider = ProviderBuilder::new()
            .connect(cctp.base_endpoint.as_str())
            .await
            .unwrap();
        let from_block = base_provider.get_block_number().await.unwrap();

        // Burn a few times to advance the Base head past from_block + the scan
        // finality margin so the non-matching scans below conclude a true absence.
        for _ in 0..3 {
            bridge
                .burn_internal::<NoOpErrorRegistry>(
                    BridgeDirection::BaseToEthereum,
                    amount,
                    recipient,
                )
                .await
                .unwrap();
        }

        let other_recipient = address!("0x000000000000000000000000000000000000dEaD");
        assert_eq!(
            bridge
                .find_recent_burn(
                    BridgeDirection::BaseToEthereum,
                    U256::from(999u64),
                    recipient,
                    from_block,
                )
                .await
                .unwrap(),
            None,
            "a burn of a different amount must not be adopted",
        );
        assert_eq!(
            bridge
                .find_recent_burn(
                    BridgeDirection::BaseToEthereum,
                    amount,
                    other_recipient,
                    from_block,
                )
                .await
                .unwrap(),
            None,
            "a burn to a different mintRecipient must not be adopted",
        );
    }

    #[tokio::test]
    async fn find_recent_burn_ignores_burn_from_a_different_depositor() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();
        let recipient = bridge.ethereum.owner();
        let amount = U256::from(25_000_000u64);

        let base_provider = ProviderBuilder::new()
            .connect(cctp.base_endpoint.as_str())
            .await
            .unwrap();
        let from_block = base_provider.get_block_number().await.unwrap();

        // A different EOA burns the same amount to the same recipient on Base.
        // Depositor is the only field separating it from our own burn, so this
        // pins the depositor clause in find_recent_burn's predicate: dropping it
        // would let us adopt another sender's burn and mint against a burn we
        // never made.
        let other_key = B256::from_slice(&cctp.base_anvil.keys()[2].to_bytes());
        let other_address = PrivateKeySigner::from_bytes(&other_key).unwrap().address();
        LocalCctp::mint_usdc(
            &cctp.base_endpoint,
            &cctp.deployer_key,
            cctp.base.usdc,
            other_address,
        )
        .await
        .unwrap();
        let other_bridge = cctp.create_bridge_with_key(&other_key).await.unwrap();

        // Burn a few times so the Base head advances past from_block + the scan
        // finality margin, so the scan below concludes a true absence rather than
        // a retryable ScanInconclusive.
        for _ in 0..3 {
            other_bridge
                .burn_internal::<NoOpErrorRegistry>(
                    BridgeDirection::BaseToEthereum,
                    amount,
                    recipient,
                )
                .await
                .unwrap();
        }

        assert_eq!(
            bridge
                .find_recent_burn(
                    BridgeDirection::BaseToEthereum,
                    amount,
                    recipient,
                    from_block,
                )
                .await
                .unwrap(),
            None,
            "a burn from a different depositor must not be adopted",
        );
    }
}
