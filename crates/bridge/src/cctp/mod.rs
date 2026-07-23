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
    /// A durably-recorded burn tx is still pending: no receipt yet, but the tx
    /// is still visible via `get_transaction_by_hash` (broadcast but unmined). A
    /// crash/timeout resume returns this so the caller delayed-redrives and
    /// never re-burns a tx that may still land. Retryable -- not a revert.
    #[error("recorded burn tx {burn_tx} still pending: broadcast but not yet mined")]
    BurnTxPending { burn_tx: TxHash },
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
    #[error("MessageSent event not found in burn receipt {tx_hash}")]
    MessageSentEventNotFound { tx_hash: TxHash },
    #[error("MintAndWithdraw event not found in transaction receipt")]
    MintAndWithdrawEventNotFound,
    #[error("transaction {tx_hash} receipt has no block number")]
    TxReceiptMissingBlock { tx_hash: TxHash },
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
    /// The mint recovery window expired without ever getting a conclusive
    /// `usedNonces()` read: every remaining probe itself failed with a
    /// transient, retryable error, so whether the mint landed is UNKNOWN --
    /// not "definitely not minted". Distinct from the case where every probe
    /// read the nonce as genuinely unconsumed (which re-surfaces the original
    /// `receiveMessage` submission error as a true terminal failure). A
    /// caller should redrive rather than declare a terminal failure on state
    /// the probe loop never actually observed.
    #[error("CCTP mint recovery window expired with the nonce state unknown: {probe_error}")]
    MintRecoveryInconclusive {
        #[source]
        probe_error: Box<Self>,
    },
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

impl CctpError {
    /// Returns `true` if this error represents a transaction revert (as opposed
    /// to a transport failure or other non-revert EVM error).
    ///
    /// Revert classification is an intentional, supported part of `CctpError`'s
    /// public contract: a two-phase burn consumer that records the returned burn
    /// tx before confirmation calls it to distinguish a redrivable revert-class
    /// burn failure from a terminal one.
    ///
    /// Used by `burn_internal` to gate the allowance-check retry: only reverts
    /// can be allowance-related; transport errors and other non-revert errors
    /// are not.
    ///
    /// For `CctpError::Evm` variants, delegates to [`EvmError::is_revert()`],
    /// which is exhaustive over all `EvmError` variants (including feature-gated
    /// ones). See that method for the full revert-classification rules.
    ///
    /// For `CctpError::Contract` (top-level, from `#[from] alloy::contract::Error`):
    /// only revert-class when the inner error carries actual revert data (same
    /// reasoning as `EvmError::Contract` — a network blip wraps as Contract with
    /// no revert data and must not trigger the retry).
    pub fn is_revert(&self) -> bool {
        match self {
            // Delegate to EvmError::is_revert(), which is exhaustive over all
            // EvmError variants (including feature-gated ones) and lives in
            // crates/evm where the feature flags are visible. This gives
            // compile-time enforcement: a new EvmError variant forces an explicit
            // revert/non-revert classification in EvmError::is_revert() before
            // it can compile.
            Self::Evm(evm_err) => evm_err.is_revert(),
            // `CctpError::Contract` (from `#[from] alloy::contract::Error`):
            // only revert-class when the inner error carries actual revert data.
            // A pure transport failure (connection reset, timeout) produces a
            // Contract error with no revert data and must not trigger the retry.
            Self::Contract(contract_err) => contract_err.as_revert_data().is_some(),
            // `CctpError::RpcTransport` is produced exclusively by direct provider
            // calls (get_logs, get_block_number) in scan/recovery paths — never by
            // the depositForBurn submission path. It cannot carry an EVM revert.
            Self::RpcTransport(_)
            | Self::SolType(_)
            | Self::ScanInconclusive { .. }
            | Self::BurnTxPending { .. }
            | Self::Http(_)
            | Self::AttestationTimeout { .. }
            | Self::MalformedAttestation { .. }
            | Self::MessageSentEventNotFound { .. }
            | Self::MintAndWithdrawEventNotFound
            | Self::TxReceiptMissingBlock { .. }
            | Self::MessageTooShort { .. }
            | Self::MessageTooShortForRecovery { .. }
            | Self::MessageDestinationDomainMismatch { .. }
            | Self::AlreadyMintedMessageNotFound { .. }
            | Self::RecoveredMintMessageMismatch { .. }
            | Self::RecoveredMintLogMissingTxHash { .. }
            | Self::RecoveredMintReceiptReverted { .. }
            | Self::RecoveredMintAndWithdrawEventNotFound { .. }
            | Self::PlaceholderNonce
            | Self::MintRecoveryInconclusive { .. }
            | Self::FeeCalculationOverflow
            | Self::Float(_)
            | Self::AmountConversion(_)
            | Self::FastTransferFeeNotAvailable { .. }
            | Self::HexDecode(_)
            | Self::FeeValueParse(_) => false,
        }
    }
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

    /// Applies the zero-grace, single-miss fast drop policy to both endpoints'
    /// `burn_status`. Test-only seam: lets downstream consumers' resume tests
    /// classify an absent recorded burn tx as `Dropped` immediately rather than
    /// waiting out the production 30 s grace window, without exposing
    /// `BurnDropConfig` across the crate boundary.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_fast_burn_drop_policy(mut self) -> Self {
        self.ethereum = self
            .ethereum
            .with_burn_drop_config(evm::BurnDropConfig::fast());
        self.base = self.base.with_burn_drop_config(evm::BurnDropConfig::fast());
        self
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
    ///
    /// Internal retry logic: if the first `depositForBurn` reverts, `ensure_standing_allowance`
    /// is re-run on the endpoint and the Circle fee is re-queried immediately before the
    /// retry burn. This avoids a stale fee from a Circle fee spike that occurs during the
    /// ~30 s `wait_for_node_sync` window on the cold allowance path.
    ///
    /// The retry is one-shot: if the second burn also fails, the error propagates to the
    /// job retry queue. Retrying on any revert-class failure (not just allowance reverts)
    /// is intentional: the dRPC load-balancing race can route the post-revert allowance
    /// re-read to a fresh node, making the allowance appear sufficient and incorrectly
    /// skipping the retry. One-shot bounds the cost for non-allowance reverts.
    async fn burn_internal<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<crate::BurnReceipt, CctpError> {
        // ensure_standing_allowance runs first: on the cold path it submits an
        // approve and then spins wait_for_node_sync (~30 s worst case). Fetching
        // max_fee afterward keeps the Circle fee fresh immediately before the burn
        // and avoids a stale bound from a spike that occurs during the sync window.
        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum
                    .ensure_standing_allowance::<Registry>()
                    .await?;
                let max_fee = self.query_fast_transfer_fee(amount, direction).await?;
                let first = self
                    .ethereum
                    .deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
                    .await;
                self.retry_burn_if_revert::<Registry, _>(
                    &self.ethereum,
                    first,
                    amount,
                    recipient,
                    direction,
                )
                .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base.ensure_standing_allowance::<Registry>().await?;
                let max_fee = self.query_fast_transfer_fee(amount, direction).await?;
                let first = self
                    .base
                    .deposit_for_burn::<Registry>(amount, recipient, direction, max_fee)
                    .await;
                self.retry_burn_if_revert::<Registry, _>(
                    &self.base, first, amount, recipient, direction,
                )
                .await
            }
        }
    }

    /// Performs the one-shot retry burn if `first_result` is a revert-class error.
    ///
    /// Re-runs `ensure_standing_allowance` on `endpoint` and re-queries the Circle
    /// fast-transfer fee immediately before the retry burn, so the retry uses a
    /// fresh fee bound rather than the potentially-stale one from the first attempt.
    ///
    /// If `ensure_standing_allowance` fails during the retry, returns the original
    /// burn error (not the sync error) as the actionable root cause.
    ///
    /// Retrying on any revert-class failure is intentional (see `burn_internal` doc).
    /// The one-shot bound prevents double-burning: a pre-flight revert or confirmed
    /// on-chain revert rolls back state, so one retry cannot double-burn.
    async fn retry_burn_if_revert<Registry: IntoErrorRegistry, EndpointWallet: Wallet>(
        &self,
        endpoint: &evm::CctpEndpoint<EndpointWallet>,
        first_result: Result<crate::BurnReceipt, CctpError>,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
    ) -> Result<crate::BurnReceipt, CctpError> {
        let Err(original_error) = first_result else {
            return first_result;
        };

        if !original_error.is_revert() {
            return Err(original_error);
        }

        warn!(
            target: "bridge",
            ?original_error,
            "depositForBurn reverted; re-running ensure_standing_allowance and retrying once"
        );

        let sync_result = endpoint.ensure_standing_allowance::<Registry>().await;

        if let Err(sync_err) = &sync_result {
            // Note: a TxReceiptMissingBlock sync_err here means the approve tx
            // was mined (a receipt was returned) but lacked a block number in
            // the receipt. The allowance is live on-chain; the next job-level
            // retry will find allowance >= threshold and skip the approve.
            warn!(
                target: "bridge",
                ?sync_err,
                ?original_error,
                "node-sync gate failed during allowance retry; returning original burn revert"
            );
        }

        evm::apply_sync_result(original_error, sync_result)?;

        // Re-query the fee immediately before the retry burn so a Circle fee
        // spike during the sync window does not cause the retry to fail with a
        // stale fee bound.
        let retry_max_fee = self.query_fast_transfer_fee(amount, direction).await?;

        endpoint
            .deposit_for_burn::<Registry>(amount, recipient, direction, retry_max_fee)
            .await
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
    ///
    /// A message still carrying the reserved zero nonce (unattested, or a
    /// malformed attestation response) errors with
    /// [`CctpError::PlaceholderNonce`] rather than reporting `None`: no party
    /// could ever have delivered `receiveMessage` for it, so "unused" would
    /// misrepresent a message that was never attested. A caller resuming from
    /// a pre-attestation envelope must handle this case deliberately (e.g. by
    /// re-polling the attestation) rather than treating it as "not yet
    /// minted".
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

    /// Returns `holder`'s USDC balance on Ethereum, the source chain for
    /// AlpacaToBase burns.
    ///
    /// Used as a fallback settlement gate before executing the CCTP burn:
    /// verifies that withdrawn USDC is present in the market-maker wallet
    /// before attempting to burn it. Delegates to the Ethereum endpoint,
    /// not Base.
    pub async fn ethereum_usdc_balance(&self, holder: Address) -> Result<U256, CctpError> {
        self.ethereum
            .usdc_balance::<OpenChainErrorRegistry>(holder)
            .await
    }

    /// Returns the number of confirmations `tx_hash` has on Ethereum, or `None`
    /// if the transaction is not yet mined.
    ///
    /// Used to gate the AlpacaToBase CCTP burn on the Alpaca withdrawal tx
    /// being settled on Ethereum: Alpaca reports "Complete" before the
    /// on-chain tx is visible network-wide on load-balanced nodes, so waiting
    /// for the required confirmations prevents burning against a balance
    /// that only exists on a lagging node. The returned count follows the
    /// repo-wide `required_confirmations` contract: the inclusion block counts
    /// as confirmation 1 (so a mined-in-head tx returns 1, not 0).
    pub async fn ethereum_tx_confirmations(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<u64>, CctpError> {
        self.ethereum.tx_confirmations(tx_hash).await
    }

    /// Returns the block in which `tx_hash` was mined on Ethereum, the chain
    /// where BaseToEthereum mints land.
    ///
    /// The BaseToAlpaca deposit leg uses this to bound
    /// [`find_recent_usdc_transfer`](Self::find_recent_usdc_transfer) from the
    /// known mint tx: the deposit send to Alpaca lands at or after the mint, so
    /// the mint's block is the scan lower bound.
    pub async fn ethereum_tx_block(&self, tx_hash: TxHash) -> Result<u64, CctpError> {
        self.ethereum.tx_block(tx_hash).await
    }

    /// Sends `amount` (USDC smallest unit, 6 decimals) of Ethereum USDC from the
    /// bot wallet to `to`, waiting for confirmation, and returns the tx hash.
    ///
    /// Used by the BaseToAlpaca deposit leg to forward minted USDC to Alpaca's
    /// deposit address. The CCTP mint credits the bot's own wallet, so an explicit
    /// transfer is required to fund Alpaca -- the mint alone does not deposit.
    pub async fn send_usdc_on_ethereum(
        &self,
        to: Address,
        amount: U256,
    ) -> Result<TxHash, CctpError> {
        self.ethereum
            .send_usdc::<OpenChainErrorRegistry>(to, amount)
            .await
    }

    /// Scans Ethereum for a USDC `Transfer(from, to, value == amount)` at or
    /// after `from_block`, returning the most recent matching tx hash.
    ///
    /// Pre-send idempotency guard for the BaseToAlpaca deposit leg: a crash
    /// between the deposit send and recording it lands the aggregate back in
    /// `Bridged`, and this detects the already-submitted send so resume adopts it
    /// instead of forwarding the minted USDC a second time. Returns a retryable
    /// [`CctpError::ScanInconclusive`] rather than `Ok(None)` when the queried node
    /// is not confirmations-deep past `from_block`, so the caller never re-sends
    /// off a stale empty scan.
    pub async fn find_recent_usdc_transfer(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError> {
        self.ethereum
            .find_recent_usdc_transfer(from, to, amount, from_block)
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

    /// Broadcasts the burn and returns its tx hash without awaiting the receipt.
    /// Re-runs `ensure_standing_allowance` and re-queries the Circle fast-transfer
    /// fee on every call (matching `burn_internal`'s cold-path ordering), so a
    /// caller's one-shot retry re-broadcasts with a fresh allowance and fee.
    async fn submit_burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<TxHash, Self::Error> {
        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum
                    .ensure_standing_allowance::<OpenChainErrorRegistry>()
                    .await?;
                let max_fee = self.query_fast_transfer_fee(amount, direction).await?;
                self.ethereum
                    .submit_deposit_for_burn(amount, recipient, direction, max_fee)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base
                    .ensure_standing_allowance::<OpenChainErrorRegistry>()
                    .await?;
                let max_fee = self.query_fast_transfer_fee(amount, direction).await?;
                self.base
                    .submit_deposit_for_burn(amount, recipient, direction, max_fee)
                    .await
            }
        }
    }

    async fn confirm_burn(
        &self,
        direction: BridgeDirection,
        tx_hash: TxHash,
        amount: U256,
    ) -> Result<crate::BurnReceipt, Self::Error> {
        match direction {
            BridgeDirection::EthereumToBase => {
                self.ethereum
                    .confirm_burn::<OpenChainErrorRegistry>(tx_hash, amount)
                    .await
            }
            BridgeDirection::BaseToEthereum => {
                self.base
                    .confirm_burn::<OpenChainErrorRegistry>(tx_hash, amount)
                    .await
            }
        }
    }

    async fn burn_status(
        &self,
        direction: BridgeDirection,
        tx_hash: TxHash,
    ) -> Result<crate::BurnTxStatus, Self::Error> {
        match direction {
            BridgeDirection::EthereumToBase => self.ethereum.burn_status(tx_hash).await,
            BridgeDirection::BaseToEthereum => self.base.burn_status(tx_hash).await,
        }
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

    async fn source_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error> {
        match direction {
            BridgeDirection::EthereumToBase => self.ethereum.current_block().await,
            BridgeDirection::BaseToEthereum => self.base.current_block().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::contract::Error as ContractError;
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::address;
    use alloy::primitives::{B256, Bytes, b256, keccak256};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::rpc::types::TransactionReceipt;
    use alloy::signers::Signer;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::{SolCall, SolEvent};
    use alloy::transports::{RpcError, TransportError};
    use httpmock::prelude::*;
    use itertools::Itertools;
    use proptest::prelude::*;
    use rand::Rng;
    use serde_json::value::to_raw_value;
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use st0x_evm::AbiDecodedErrorType;
    use st0x_evm::Evm;
    use st0x_evm::NoOpErrorRegistry;
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::{USDC_BASE, USDC_ETHEREUM};

    use super::evm::MintRecoveryConfig;
    use super::*;
    use crate::{Attestation, Bridge};

    // --- is_revert unit tests ---

    fn transport_error(code: i64, message: &'static str) -> CctpError {
        CctpError::Evm(EvmError::Transport(RpcError::ErrorResp(ErrorPayload {
            code,
            message: Cow::Borrowed(message),
            data: None,
        })))
    }

    /// Code 3 (Ethereum JSON-RPC spec, Anvil simulation mode) is revert-class
    /// regardless of the message content. This test uses a non-matching message to
    /// prove the `code == 3` branch fires independently of the message fallback.
    #[test]
    fn is_revert_true_for_transport_code_3() {
        assert!(transport_error(3, "some unrelated error").is_revert());
    }

    /// Code -32000 + "execution reverted" message (Geth/Infura/Alchemy/dRPC
    /// preflight; repo submit.rs mock) is revert-class.
    #[test]
    fn is_revert_true_for_transport_code_minus_32000_execution_reverted() {
        assert!(transport_error(-32000, "execution reverted").is_revert());
    }

    /// Code -32003 + "execution reverted" message (some Alchemy endpoints) is
    /// revert-class.
    #[test]
    fn is_revert_true_for_transport_code_minus_32003_execution_reverted() {
        assert!(transport_error(-32003, "execution reverted").is_revert());
    }

    /// Message-based detection: any code with `"execution reverted"` in the
    /// message is revert-class.
    #[test]
    fn is_revert_true_for_transport_message_execution_reverted() {
        assert!(
            transport_error(-32099, "execution reverted: ERC20: insufficient allowance")
                .is_revert()
        );
    }

    /// Code -32000 + "nonce too low" is NOT revert-class: the code is shared by
    /// multiple failure types; only the message distinguishes them.
    #[test]
    fn is_revert_false_for_transport_nonce_too_low() {
        assert!(!transport_error(-32000, "nonce too low").is_revert());
    }

    /// Code -32001 with no revert message is NOT revert-class.
    #[test]
    fn is_revert_false_for_transport_unrelated_code() {
        assert!(!transport_error(-32001, "some other error").is_revert());
    }

    /// `ScanInconclusive` is not revert-class.
    #[test]
    fn is_revert_false_for_scan_inconclusive() {
        assert!(!CctpError::ScanInconclusive { from_block: 0 }.is_revert());
    }

    /// `BurnTxPending` is not revert-class: a still-pending recorded burn is a
    /// retryable settlement-wait, never an on-chain revert.
    #[test]
    fn is_revert_false_for_burn_tx_pending() {
        assert!(
            !CctpError::BurnTxPending {
                burn_tx: TxHash::ZERO
            }
            .is_revert()
        );
    }

    /// `PlaceholderNonce` is not revert-class.
    #[test]
    fn is_revert_false_for_placeholder_nonce() {
        assert!(!CctpError::PlaceholderNonce.is_revert());
    }

    /// `TxReceiptMissingBlock` is not revert-class: it is a post-mining
    /// infrastructure error, not an on-chain revert.
    #[test]
    fn is_revert_false_for_tx_receipt_missing_block() {
        assert!(
            !CctpError::TxReceiptMissingBlock {
                tx_hash: TxHash::ZERO
            }
            .is_revert()
        );
    }

    /// `MessageSentEventNotFound` is not revert-class: it is a post-commit
    /// receipt-parsing error, not an on-chain revert.
    #[test]
    fn is_revert_false_for_message_sent_event_not_found() {
        assert!(
            !CctpError::MessageSentEventNotFound {
                tx_hash: TxHash::ZERO
            }
            .is_revert()
        );
    }

    /// `Http` (reqwest) is not revert-class: network transport errors must not
    /// trigger the allowance retry.
    #[test]
    fn is_revert_false_for_http_error() {
        // Construct a reqwest error from a known-bad URL via the blocking client.
        let err = reqwest::blocking::get("http://0.0.0.0:0").unwrap_err();
        assert!(!CctpError::Http(err.without_url()).is_revert());
    }

    /// `EvmError::Transaction` (PendingTransactionError) is not revert-class:
    /// it covers errors while WAITING for a submitted tx to confirm (not a
    /// pre-flight simulation reject). Ensures the unconditional non-revert arm
    /// in `EvmError::is_revert()` covers this variant.
    #[test]
    fn is_revert_false_for_evm_transaction() {
        use alloy::providers::PendingTransactionError;
        let err = CctpError::Evm(EvmError::Transaction(
            PendingTransactionError::FailedToRegister,
        ));
        assert!(
            !err.is_revert(),
            "EvmError::Transaction must not be revert-class"
        );
    }

    /// `EvmError::NodeBehindRequiredBlock` is not revert-class: it is a
    /// node-sync timeout, not an EVM revert. Ensures the unconditional
    /// non-revert arm in `EvmError::is_revert()` covers this variant.
    #[test]
    fn is_revert_false_for_evm_node_behind_required_block() {
        let err = CctpError::Evm(EvmError::NodeBehindRequiredBlock {
            observed_tip: 10,
            required_block: 20,
            attempts: 30,
        });
        assert!(
            !err.is_revert(),
            "EvmError::NodeBehindRequiredBlock must not be revert-class"
        );
    }

    /// `EvmError::Reverted` (confirmed on-chain revert, post-mining) is
    /// revert-class. This is the primary production path the retry defends against.
    #[test]
    fn is_revert_true_for_evm_reverted() {
        assert!(
            CctpError::Evm(EvmError::Reverted {
                tx_hash: TxHash::ZERO
            })
            .is_revert()
        );
    }

    /// `EvmError::DecodedRevert` (decoded Solidity error from a confirmed revert)
    /// is revert-class.
    #[test]
    fn is_revert_true_for_evm_decoded_revert() {
        assert!(
            CctpError::Evm(EvmError::DecodedRevert(AbiDecodedErrorType::Unknown(
                vec![]
            )))
            .is_revert()
        );
    }

    /// `EvmError::Contract` carrying actual EVM revert data (a `TransportError::ErrorResp`
    /// with message containing "revert" and hex-encoded data) is revert-class.
    #[test]
    fn is_revert_true_for_evm_contract_with_revert_data() {
        let raw = to_raw_value(&"0x1234").expect("valid json");
        let payload = ErrorPayload {
            code: 3,
            message: Cow::Borrowed("execution reverted"),
            data: Some(raw),
        };
        let contract_err = ContractError::TransportError(TransportError::ErrorResp(payload));
        assert!(CctpError::Evm(EvmError::Contract(contract_err)).is_revert());
    }

    /// `EvmError::Contract` from a transport failure (no revert data) is NOT
    /// revert-class. This is the path that `decode_rpc_revert` produces for
    /// network errors -- see `error_decoding.rs::decode_rpc_revert_returns_contract_for_non_revert`.
    #[test]
    fn is_revert_false_for_evm_contract_without_revert_data() {
        let contract_err =
            ContractError::TransportError(TransportError::local_usage_str("connection refused"));
        assert!(!CctpError::Evm(EvmError::Contract(contract_err)).is_revert());
    }

    /// `CctpError::Contract` with actual revert data is revert-class.
    #[test]
    fn is_revert_true_for_top_level_contract_error_with_revert_data() {
        let raw = to_raw_value(&"0x1234").expect("valid json");
        let payload = ErrorPayload {
            code: 3,
            message: Cow::Borrowed("execution reverted"),
            data: Some(raw),
        };
        let contract_err = ContractError::TransportError(TransportError::ErrorResp(payload));
        assert!(CctpError::Contract(contract_err).is_revert());
    }

    /// `CctpError::Contract` from a pure transport failure (no revert data) is NOT
    /// revert-class. A local client error or network blip must not trigger the
    /// allowance retry.
    #[test]
    fn is_revert_false_for_top_level_contract_error_without_revert_data() {
        let contract_err =
            ContractError::TransportError(TransportError::local_usage_str("connection refused"));
        assert!(!CctpError::Contract(contract_err).is_revert());
    }

    // --- end is_revert unit tests ---

    fn setup_anvil() -> (AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    /// Test-only [`Provider`] wrapper that answers `get_logs` with an empty
    /// result for the first `empty_scans` invocations before delegating to
    /// `inner`. Simulates the dRPC read-after-write lag
    /// `find_received_message_tx`'s backward log scan is exposed to:
    /// `usedNonces()` already reports the nonce consumed but the
    /// `MessageReceived` log has not yet been indexed by the queried node.
    /// Wrapped by [`FlakyProbeWallet`], never constructed directly by tests.
    #[derive(Clone)]
    struct FlakyGetLogsProvider<InnerProvider> {
        inner: InnerProvider,
        remaining_empty_scans: Arc<AtomicU32>,
    }

    #[async_trait]
    impl<InnerProvider: Provider + Clone> Provider for FlakyGetLogsProvider<InnerProvider> {
        fn root(&self) -> &alloy::providers::RootProvider {
            self.inner.root()
        }

        async fn get_logs(
            &self,
            filter: &alloy::rpc::types::Filter,
        ) -> alloy::transports::TransportResult<Vec<alloy::rpc::types::Log>> {
            let should_return_empty = self
                .remaining_empty_scans
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();

            if should_return_empty {
                return Ok(Vec::new());
            }

            self.inner.get_logs(filter).await
        }
    }

    /// Test-only `Wallet` wrapper around `recover_already_minted`'s two probe
    /// paths: the `usedNonces()` view call (`Evm::call`) and the
    /// `MessageReceived` log scan (`Provider::get_logs`, reached through
    /// `find_received_message_tx`). Fails the first `revert_failures`
    /// invocations of the call path with a deterministic on-chain-revert
    /// shape, then the next `call_failures` invocations with a transient
    /// transport-shaped failure, and/or answers the first `empty_log_scans`
    /// invocations of the log scan with no logs, before delegating to `inner`
    /// for the rest. Counts every `call` invocation (successful or not) into
    /// `call_count`, an externally-supplied `Arc` so a test can read it after
    /// the wallet has been moved into a `CctpEndpoint`.
    ///
    /// Reused by the retry, fail-fast, log-lag, and recovery-window tests
    /// below rather than one single-use wrapper per test.
    struct FlakyProbeWallet<InnerWallet: Wallet> {
        inner: InnerWallet,
        provider: FlakyGetLogsProvider<InnerWallet::Provider>,
        remaining_revert_failures: AtomicU32,
        remaining_call_failures: AtomicU32,
        call_count: Arc<AtomicU32>,
    }

    /// Named failure-injection counts for [`FlakyProbeWallet::new`], bundled
    /// into a struct so the two same-typed `u32` counters -- which drive
    /// genuinely different failure paths (transient `usedNonces()` call
    /// errors vs. empty `get_logs` scans) -- cannot be silently transposed at
    /// a call site, mirroring [`MintRecoveryConfig`]'s own named-field
    /// bundling.
    #[derive(Debug, Clone, Copy, Default)]
    struct FlakyProbeFailures {
        /// Leading `usedNonces()` calls that fail with a transient
        /// transport-shaped error before delegating to the real chain.
        call_failures: u32,
        /// Leading `get_logs` scans that answer empty before delegating to
        /// the real chain.
        empty_log_scans: u32,
    }

    impl<InnerWallet: Wallet> FlakyProbeWallet<InnerWallet> {
        fn new(
            inner: InnerWallet,
            failures: FlakyProbeFailures,
            call_count: Arc<AtomicU32>,
        ) -> Self {
            let provider = FlakyGetLogsProvider {
                inner: inner.provider().clone(),
                remaining_empty_scans: Arc::new(AtomicU32::new(failures.empty_log_scans)),
            };
            Self {
                inner,
                provider,
                remaining_revert_failures: AtomicU32::new(0),
                remaining_call_failures: AtomicU32::new(failures.call_failures),
                call_count,
            }
        }

        /// Injects `revert_failures` revert-shaped (`EvmError::DecodedRevert`)
        /// failures on the first calls, checked before the transient
        /// `call_failures` injected by [`Self::new`]. Used to prove
        /// `recover_already_minted` retries a revert-shaped probe error the
        /// same as any other transient one, since `usedNonces()` cannot
        /// deterministically revert.
        fn with_revert_failures(mut self, revert_failures: u32) -> Self {
            self.remaining_revert_failures = AtomicU32::new(revert_failures);
            self
        }

        /// Returns a handle to the remaining-empty-log-scans counter, so a
        /// test can capture it before the wallet moves into a `CctpEndpoint`
        /// and assert afterward that the injected empty scan was actually
        /// consumed (not merely that recovery happened to succeed anyway).
        fn remaining_empty_log_scans(&self) -> Arc<AtomicU32> {
            Arc::clone(&self.provider.remaining_empty_scans)
        }
    }

    #[async_trait]
    impl<InnerWallet: Wallet> Evm for FlakyProbeWallet<InnerWallet> {
        type Provider = FlakyGetLogsProvider<InnerWallet::Provider>;

        fn provider(&self) -> &Self::Provider {
            &self.provider
        }

        async fn call<Registry: IntoErrorRegistry, Call: SolCall + Send>(
            &self,
            contract: Address,
            call: Call,
        ) -> Result<Call::Return, EvmError>
        where
            Self: Sized,
        {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let should_revert = self
                .remaining_revert_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();

            if should_revert {
                return Err(EvmError::DecodedRevert(AbiDecodedErrorType::Unknown(vec![
                    0x12, 0x34, 0x56, 0x78,
                ])));
            }

            let should_fail = self
                .remaining_call_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();

            if should_fail {
                // `Evm::call`'s only implementation (`execute_call`) funnels every
                // provider failure through `decode_rpc_revert`, which always wraps a
                // non-decodable RPC failure as `alloy::contract::Error::TransportError`
                // -- `EvmError::Transport` is never constructed on this path (only by
                // the unrelated node-sync spin-poll and tx-submission paths). Inject
                // the shape the real probe actually produces, not a synthetic one.
                return Err(EvmError::Contract(ContractError::TransportError(
                    RpcError::ErrorResp(ErrorPayload {
                        code: -32000,
                        message: Cow::Borrowed("connection reset (synthetic transient failure)"),
                        data: None,
                    }),
                )));
            }

            self.inner.call::<Registry, Call>(contract, call).await
        }
    }

    #[async_trait]
    impl<InnerWallet: Wallet> Wallet for FlakyProbeWallet<InnerWallet> {
        fn address(&self) -> Address {
            self.inner.address()
        }

        async fn send_pending(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TxHash, EvmError> {
            self.inner.send_pending(contract, calldata, note).await
        }

        async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            self.inner.await_receipt(tx_hash).await
        }

        async fn send(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            self.inner.send(contract, calldata, note).await
        }
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
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        let base = CctpEndpoint::new(
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            base_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

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
    async fn ensure_standing_allowance_sets_max_when_allowance_is_zero_ethereum() {
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

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

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
            "initial allowance should be zero"
        );

        bridge
            .ethereum
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "standing allowance must be set to U256::MAX"
        );
    }

    #[tokio::test]
    async fn ensure_standing_allowance_no_op_when_above_threshold_ethereum() {
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

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // Pre-set allowance to U256::MAX (the standing target).
        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, U256::MAX)
            .await
            .unwrap();

        let ethereum_provider = ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await
            .unwrap();

        let tx_count_before = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        bridge
            .ethereum
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let tx_count_after = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        assert_eq!(
            tx_count_before, tx_count_after,
            "no approve tx must be submitted when allowance is at or above threshold"
        );

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance must remain U256::MAX"
        );
    }

    #[tokio::test]
    async fn ensure_standing_allowance_tops_up_when_below_threshold_ethereum() {
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

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // Pre-set allowance to 1 (well below threshold).
        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, U256::from(1u8))
            .await
            .unwrap();

        let initial_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::from(1u8),
            "initial allowance should be 1"
        );

        bridge
            .ethereum
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance must be topped up to U256::MAX"
        );
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
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        let base = CctpEndpoint::new(
            base_usdc_address,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
            base_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        Ok(CctpBridge::new(ethereum, base)?)
    }

    #[tokio::test]
    async fn ensure_standing_allowance_sets_max_when_allowance_is_zero_base() {
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

        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::ZERO,
            "initial allowance should be zero"
        );

        bridge
            .base
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "standing allowance must be set to U256::MAX"
        );
    }

    #[tokio::test]
    async fn ensure_standing_allowance_no_op_when_above_threshold_base() {
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

        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        // Pre-set allowance to U256::MAX (the standing target).
        bridge
            .base
            .approve_usdc::<NoOpErrorRegistry>(spender, U256::MAX)
            .await
            .unwrap();

        let base_provider = ProviderBuilder::new()
            .connect(&base_endpoint)
            .await
            .unwrap();

        let tx_count_before = base_provider.get_transaction_count(owner).await.unwrap();

        bridge
            .base
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let tx_count_after = base_provider.get_transaction_count(owner).await.unwrap();

        assert_eq!(
            tx_count_before, tx_count_after,
            "no approve tx must be submitted when allowance is at or above threshold"
        );

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance must remain U256::MAX"
        );
    }

    /// The threshold is `U256::MAX / 2`. At exactly this value the condition is
    /// `allowance >= threshold`, so no approve must fire.
    #[tokio::test]
    async fn ensure_standing_allowance_no_op_at_exact_threshold() {
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

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // STANDING_ALLOWANCE_THRESHOLD = U256::MAX / 2 (defined in evm.rs)
        let threshold = U256::MAX / U256::from(2u8);

        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, threshold)
            .await
            .unwrap();

        let ethereum_provider = ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await
            .unwrap();

        let tx_count_before = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        bridge
            .ethereum
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let tx_count_after = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        assert_eq!(
            tx_count_before, tx_count_after,
            "no approve tx must fire when allowance equals the threshold exactly"
        );

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();

        assert_eq!(
            final_allowance, threshold,
            "allowance must remain unchanged at exactly the threshold"
        );
    }

    /// One below the threshold (`U256::MAX / 2 - 1`) must trigger a top-up to
    /// `U256::MAX`: guards the `>=` boundary condition in
    /// `ensure_standing_allowance`.
    #[tokio::test]
    async fn ensure_standing_allowance_tops_up_at_threshold_minus_one() {
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

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // STANDING_ALLOWANCE_THRESHOLD = U256::MAX / 2 (defined in evm.rs).
        // One below it must trigger a top-up.
        let below_threshold = U256::MAX / U256::from(2u8) - U256::from(1u8);

        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, below_threshold)
            .await
            .unwrap();

        bridge
            .ethereum
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();

        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance one below threshold must be topped up to U256::MAX"
        );
    }

    #[tokio::test]
    async fn ensure_standing_allowance_tops_up_when_below_threshold_base() {
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

        let owner = bridge.base.owner();
        let spender = bridge.base.token_messenger_address();

        // Pre-set allowance to 1 (well below threshold).
        bridge
            .base
            .approve_usdc::<NoOpErrorRegistry>(spender, U256::from(1u8))
            .await
            .unwrap();

        let initial_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            initial_allowance,
            U256::from(1u8),
            "initial allowance should be 1"
        );

        bridge
            .base
            .ensure_standing_allowance::<NoOpErrorRegistry>()
            .await
            .unwrap();

        let final_allowance = bridge
            .base
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance must be topped up to U256::MAX"
        );
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
            )
            .with_node_sync_poll_interval(Duration::ZERO);

            let base = CctpEndpoint::new(
                self.base.usdc,
                self.base.token_messenger,
                self.base.message_transmitter,
                base_wallet,
            )
            .with_node_sync_poll_interval(Duration::ZERO);

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
        // case: once the recovery window expires, the reactive path must re-surface
        // the original submit error rather than fabricate a recovery outcome.
        // Mutating the nonce yields a never-minted nonce on a structurally valid
        // message (usedNonces == 0).
        let sentinel_error = || EvmError::Reverted {
            tx_hash: TxHash::repeat_byte(0xEE),
        };
        let mut unused_nonce_message = message_with_nonce.to_vec();
        unused_nonce_message[NONCE_INDEX..NONCE_INDEX + NONCE_SIZE].fill(0xAB);

        // The window is minutes long by design; auto-advance the clock through it
        // instead of sleeping. The probes themselves are I/O-bound, so they still
        // run for real.
        tokio::time::pause();

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

    /// Our own `receiveMessage` failing does not mean the mint will not happen:
    /// the burn is irreversible and the attestation is valid, so any relayer can
    /// deliver it moments later. Recovery must wait out its window and return
    /// that mint, instead of reporting a post-burn failure that strands the
    /// rebalancing guard.
    ///
    /// Does NOT pause the clock: `tokio::time::pause`'s paused-clock
    /// auto-advance fires a sleeping timer as soon as the runtime has no other
    /// *ready* work, even while an unrelated real I/O future (like
    /// `mint_internal`'s sign/broadcast/confirm round trips) is still pending
    /// on the network -- it is not a barrier against that concurrent I/O, only
    /// against other timers. Relying on it here would make the "recovery must
    /// not succeed on the very first probe" property a race, not a guarantee.
    ///
    /// Instead this injects a short, real probe cadence via
    /// `with_mint_recovery_config` (an order of magnitude below how long a
    /// mint against a local anvil chain takes) with a generous probe budget,
    /// so the two concurrent futures race in real time with a wide safety
    /// margin, and asserts on `call_count` (an observable probe count) rather
    /// than on wall-clock elapsed time to prove recovery actually waited past
    /// the first probe instead of asserting on race-prone timing.
    #[tokio::test]
    async fn mint_recovers_when_another_party_mints_inside_the_recovery_window() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(2_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // A second endpoint on the same chain/contracts, purely to count
        // usedNonces() probes (no injected failures) and to drive a short
        // recovery cadence.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let counting_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 0,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        );
        let recovering_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            counting_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO)
        // A large probe budget costs nothing on the passing path -- the loop
        // returns as soon as `is_nonce_used` reads consumed -- and only
        // widens how long a genuine failure would take to surface, so it is
        // sized generously (~40 s ceiling) rather than tightly (~2 s) to
        // avoid flaking under a loaded CI runner racing two anvil instances
        // plus a full sign/broadcast/receipt round trip.
        .with_mint_recovery_config(MintRecoveryConfig {
            probe_interval: Duration::from_millis(20),
            probes: 2_000,
        });

        let (recovered, relayed_mint) = tokio::join!(
            recovering_endpoint.recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            ),
            bridge.mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation.clone(),
            )
        );

        let relayed_mint = relayed_mint.unwrap();
        let recovered = recovered.unwrap();

        assert_eq!(
            recovered.tx, relayed_mint.tx,
            "recovery must return the mint that landed inside the window"
        );
        assert_eq!(recovered.amount, relayed_mint.amount);
        assert_eq!(recovered.fee_collected, relayed_mint.fee_collected);

        let probes = call_count.load(Ordering::SeqCst);
        assert!(
            probes >= 2,
            "recovery must not have succeeded on the very first probe -- got {probes} \
             usedNonces() calls"
        );
    }

    /// A destination-domain mismatch is structurally deterministic: it only
    /// depends on the fixed attested `message` bytes and the fixed
    /// `direction` argument, neither of which change across probes. It must
    /// fail fast with the original submit error, checked before the probe
    /// loop even starts -- not merely on the first probe -- since a
    /// wrong-direction message whose nonce was never consumed on this chain
    /// would otherwise report the nonce "unconsumed" and burn the whole
    /// recovery window before this error is ever reached.
    #[tokio::test]
    async fn recover_already_minted_fails_fast_on_destination_domain_mismatch_before_any_probe() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_500_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (_, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // The nonce is deliberately left unconsumed (no mint performed): a
        // wrong-direction message can never mint here regardless of chain
        // state, so the mismatch must fail fast without ever probing
        // usedNonces().
        let sentinel_error = EvmError::Reverted {
            tx_hash: TxHash::repeat_byte(0xEE),
        };

        // Wrap the endpoint's wallet purely to count usedNonces() probes (no
        // injected failures): the domain check must fail fast before any
        // probe, so a zero call count is the observable proof -- not an
        // elapsed-time guess, which can flake under CI load or a slow anvil
        // round trip, or blow up to the full multi-minute window if the
        // check ever moves back after the usedNonces() read.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let counting_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 0,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        );
        let counting_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            counting_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        // The message was attested for Base (EthereumToBase); asking recovery
        // to reconstruct it as BaseToEthereum is a destination-domain
        // mismatch regardless of chain state.
        let error = counting_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::BaseToEthereum,
                &message_with_nonce,
                sentinel_error,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::Evm(EvmError::Reverted { tx_hash })
                    if tx_hash == TxHash::repeat_byte(0xEE)
            ),
            "a destination-domain mismatch must surface the original submit error: {error:?}"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            0,
            "a destination-domain mismatch must fail fast before any usedNonces() probe"
        );
    }

    /// A revert-shaped `usedNonces()` probe error (`EvmError::is_revert()` ==
    /// `true`) must be retried, not treated as a decided terminal outcome.
    /// `usedNonces(bytes32)` is a plain public-mapping getter with no
    /// `require`/branch logic (see the vendored `MessageTransmitterV2` ABI),
    /// so it cannot deterministically revert for a well-formed call; a
    /// revert-shaped response here is necessarily a provider/transport
    /// artifact, not an on-chain fact. An earlier version of this function
    /// fast-failed on this shape -- this test proves the opposite: recovery
    /// must survive it exactly like any other transient probe error and
    /// still find the mint that had already landed.
    #[tokio::test]
    async fn recover_already_minted_retries_after_a_revert_shaped_probe_error() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_500_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Mint fully lands before recovery ever probes, so the only thing
        // that could prevent recovery from returning it is the injected
        // revert-shaped probe failure.
        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        let sentinel_error = EvmError::Reverted {
            tx_hash: TxHash::repeat_byte(0xEE),
        };

        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let reverting_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 0,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        )
        .with_revert_failures(1);
        let reverting_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            reverting_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        tokio::time::pause();

        let recovered = reverting_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                sentinel_error,
            )
            .await
            .unwrap();

        assert_eq!(
            recovered.tx, mint_receipt.tx,
            "recovery must return the already-landed mint despite the revert-shaped probe error"
        );
        assert_eq!(recovered.amount, mint_receipt.amount);
        assert_eq!(recovered.fee_collected, mint_receipt.fee_collected);
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "the revert-shaped probe error must be retried (probe 1), not fail fast, so a \
             second probe (probe 2) is what actually observes the nonce consumed"
        );
    }

    /// A transient probe error (an RPC/transport blip, not a structural
    /// mismatch) must not surface the original submit error: `recover_already_minted`
    /// retries and finds the mint that had already landed, instead of giving up
    /// on the very first bad read.
    #[tokio::test]
    async fn recover_already_minted_retries_after_a_transient_probe_error() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_750_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Mint fully lands before recovery ever probes, so the only thing that
        // can prevent recovery from returning it is the injected probe failure.
        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        // A second endpoint on the same chain/contracts, whose first `call` (the
        // usedNonces() probe) fails once with a synthetic transient transport
        // error before delegating to the real chain.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let flaky_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 1,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        );
        let flaky_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            flaky_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        tokio::time::pause();

        let recovered = flaky_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            recovered.tx, mint_receipt.tx,
            "recovery must return the already-landed mint despite the first probe erroring"
        );
        assert_eq!(recovered.amount, mint_receipt.amount);
        assert_eq!(recovered.fee_collected, mint_receipt.fee_collected);
    }

    /// Proves `last_probe_error` is reset to `None` on a later clean
    /// `Ok(false)` read, not left "sticky" from an earlier transient probe
    /// failure. `recover_already_minted`'s terminal branch decides between
    /// the original `submit_error` and `MintRecoveryInconclusive` purely on
    /// whether `last_probe_error` is `Some` once the window ends: injects one
    /// transient probe failure (probe 1) against a nonce that is genuinely
    /// never minted, with a 2-probe window so probe 2 gets a real clean
    /// unconsumed read as the window closes. If a future refactor made
    /// `last_probe_error` sticky (never reset on a clean read), this would
    /// wrongly surface `MintRecoveryInconclusive` here instead of the
    /// original submit error -- misclassifying a genuinely terminal outcome
    /// as inconclusive and redriving forever.
    #[tokio::test]
    async fn recover_already_minted_resets_last_probe_error_after_transient_failure() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_650_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (_, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Deliberately never mint: the nonce is genuinely never consumed, so
        // a correct implementation must surface the original submit error
        // once the window ends, not MintRecoveryInconclusive.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let flaky_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 1,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        );
        let flaky_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            flaky_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO)
        .with_mint_recovery_config(MintRecoveryConfig {
            probe_interval: Duration::from_millis(5),
            probes: 2,
        });

        let error = flaky_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CctpError::Evm(EvmError::Reverted { tx_hash })
                    if tx_hash == TxHash::repeat_byte(0xEE)
            ),
            "the clean unconsumed read on probe 2 must reset last_probe_error, surfacing the \
             original submit error rather than MintRecoveryInconclusive: {error:?}"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "both probes must have run: probe 1 failed transiently, probe 2 read cleanly"
        );
    }

    /// The one production-analogous retry scenario: `usedNonces()` already
    /// reports the nonce consumed, but the backward `MessageReceived` log scan
    /// comes up empty because a load-balanced RPC node's log index lags
    /// behind the state its own `usedNonces()` view call already reflects.
    /// This is what actually produces `CctpError::AlreadyMintedMessageNotFound`
    /// -- the variant `reconstruct_existing_mint` retries, bounded by
    /// `SCAN_ATTEMPTS` -- and is distinct from
    /// `recover_already_minted_retries_after_a_transient_probe_error`, which
    /// only fails the `usedNonces()` call itself and never reaches the log
    /// scan. The retry happens once, after the probe loop confirms the nonce
    /// consumed, not once per `usedNonces()` probe: `call_count` (the
    /// `usedNonces()` invocation count) therefore stays at 1 here, unlike the
    /// old design this replaced.
    #[tokio::test]
    async fn recover_already_minted_retries_after_the_message_received_log_lags() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_250_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Mint fully lands before recovery ever probes, so usedNonces() reads
        // true on the first probe; only the log scan needs to catch up.
        let mint_receipt = bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        // A second endpoint on the same chain/contracts, whose first
        // `get_logs` (the MessageReceived backward scan) answers empty before
        // delegating to the real chain.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let flaky_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 0,
                empty_log_scans: 1,
            },
            Arc::clone(&call_count),
        );
        let remaining_empty_log_scans = flaky_wallet.remaining_empty_log_scans();
        let flaky_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            flaky_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        let recovered = flaky_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            recovered.tx, mint_receipt.tx,
            "recovery must return the already-landed mint despite the first log scan missing it"
        );
        assert_eq!(recovered.amount, mint_receipt.amount);
        assert_eq!(recovered.fee_collected, mint_receipt.fee_collected);
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "the log-scan retry must not re-probe usedNonces(); it already read consumed"
        );
        assert_eq!(
            remaining_empty_log_scans.load(Ordering::SeqCst),
            0,
            "the injected empty log scan must actually have been consumed by a retry, \
             not bypassed"
        );
    }

    /// A nonce confirmed consumed by `usedNonces()` but whose receipt can
    /// never be reconstructed (the `MessageReceived` log scan stays empty
    /// well past `SCAN_ATTEMPTS`) must still surface as
    /// `CctpError::MintRecoveryInconclusive`, not the raw
    /// `AlreadyMintedMessageNotFound` that `reconstruct_existing_mint`
    /// produces once its own retries are exhausted. The mint is known to
    /// have landed -- the authoritative `usedNonces()` read already proved
    /// it -- so anything other than `MintRecoveryInconclusive` here would
    /// strand the caller on a false "never minted" terminal failure.
    #[tokio::test]
    async fn recover_already_minted_reports_inconclusive_when_reconstruction_exhausts_retries() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_100_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (attestation, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Mint fully lands before recovery ever probes, so usedNonces() reads
        // true on the first probe; the log scan must never catch up.
        bridge
            .mint_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                message_with_nonce.clone(),
                attestation,
            )
            .await
            .unwrap();

        // A second endpoint on the same chain/contracts whose `get_logs` (the
        // MessageReceived backward scan) answers empty every time. 10 empty
        // scans exceeds evm.rs's SCAN_ATTEMPTS (5), so
        // reconstruct_existing_mint exhausts its own retries and returns
        // AlreadyMintedMessageNotFound instead of ever finding the log.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let flaky_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 0,
                empty_log_scans: 10,
            },
            Arc::clone(&call_count),
        );
        let flaky_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            flaky_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO);

        let error = flaky_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            )
            .await
            .unwrap_err();

        let CctpError::MintRecoveryInconclusive { probe_error } = error else {
            panic!(
                "a nonce confirmed consumed whose receipt could not be reconstructed must \
                 surface MintRecoveryInconclusive, not the raw error; got: {error:?}"
            );
        };
        assert!(
            matches!(*probe_error, CctpError::AlreadyMintedMessageNotFound { .. }),
            "the wrapped probe_error must be the exhausted-retries \
             AlreadyMintedMessageNotFound; got: {probe_error:?}"
        );
    }

    /// The window-exhaustion branch of `recover_already_minted` -- every
    /// probe from some point on fails with a transient (non-revert) error
    /// all the way to the end of the configured window -- is the only
    /// production code path that constructs `CctpError::MintRecoveryInconclusive`
    /// from a run of transient probe failures, as opposed to a
    /// reconstruction failure after a confirmed-consumed nonce (covered by
    /// `recover_already_minted_reports_inconclusive_when_reconstruction_exhausts_retries`
    /// above). This drives the real branch -- not a hand-fed error through a
    /// stub -- and asserts the caller receives `MintRecoveryInconclusive`
    /// wrapping the last transient error, not the original submit error.
    #[tokio::test]
    async fn recover_already_minted_reports_inconclusive_when_every_probe_errors_transiently() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(900_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();
        let message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();
        let (_, message_with_nonce) = cctp.sign_message(&message).await.unwrap();

        // Deliberately never mint: every usedNonces() probe must fail
        // transiently instead, so the window expires without a single
        // conclusive read.
        let flaky_provider = ProviderBuilder::new()
            .connect(&cctp.base_endpoint)
            .await
            .unwrap();
        let call_count = Arc::new(AtomicU32::new(0));
        let flaky_wallet = FlakyProbeWallet::new(
            RawPrivateKeyWallet::new(&cctp.deployer_key, flaky_provider, 1).unwrap(),
            FlakyProbeFailures {
                call_failures: 10,
                empty_log_scans: 0,
            },
            Arc::clone(&call_count),
        );
        let flaky_endpoint = CctpEndpoint::new(
            cctp.base.usdc,
            cctp.base.token_messenger,
            cctp.base.message_transmitter,
            flaky_wallet,
        )
        .with_node_sync_poll_interval(Duration::ZERO)
        .with_mint_recovery_config(MintRecoveryConfig {
            probe_interval: Duration::from_millis(5),
            probes: 3,
        });

        let error = flaky_endpoint
            .recover_already_minted::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                &message_with_nonce,
                EvmError::Reverted {
                    tx_hash: TxHash::repeat_byte(0xEE),
                },
            )
            .await
            .unwrap_err();

        let CctpError::MintRecoveryInconclusive { probe_error } = error else {
            panic!(
                "a window that expires with every probe erroring transiently must surface \
                 MintRecoveryInconclusive, not a decided outcome; got: {error:?}"
            );
        };
        assert!(
            matches!(*probe_error, CctpError::Evm(_)),
            "the wrapped probe_error must be the last transient EvmError, not the original \
             submit error; got: {probe_error:?}"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "the window must have been exhausted (all 3 configured probes attempted)"
        );
    }

    /// A message carrying the reserved zero nonce (unattested, or a malformed
    /// attestation response) was never assigned a real nonce, so no party
    /// could ever have delivered `receiveMessage` for it: this is structurally
    /// deterministic for the fixed `message` bytes, and must fail fast with
    /// `PlaceholderNonce` rather than reporting "not yet minted" -- which
    /// would otherwise let `recover_already_minted` burn its whole recovery
    /// window re-parsing the same bytes on every probe.
    #[tokio::test]
    async fn find_existing_mint_fails_fast_on_placeholder_nonce() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_000_000u64);

        let burn_receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap();

        // Pre-attestation message: still carries the reserved zero nonce.
        let raw_message = cctp
            .extract_message_from_burn_tx(burn_receipt.tx, true)
            .await
            .unwrap();

        let error = bridge
            .base
            .find_existing_mint::<NoOpErrorRegistry>(BridgeDirection::EthereumToBase, &raw_message)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CctpError::PlaceholderNonce),
            "a zero-nonce message must fail fast with PlaceholderNonce, got: {error:?}"
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

        // An invalid attestation still runs the full mint-recovery window before
        // the failure is declared; auto-advance the clock through it rather than
        // sleeping out its minutes.
        tokio::time::pause();

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

        // `message` is the raw, pre-attestation envelope (still carrying the
        // reserved zero nonce): recovery's zero-nonce check fails fast here
        // instead of exhausting the recovery window, so no clock manipulation
        // is needed.
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
    async fn standing_allowance_set_on_first_burn_and_persists_across_burns() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

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
            "initial allowance should be 0 before first burn"
        );

        // First burn: ensure_standing_allowance sets U256::MAX, then burn succeeds.
        let small_amount = U256::from(1_000_000u64); // 1 USDC
        bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                small_amount,
                recipient,
            )
            .await
            .unwrap();

        let after_first_burn = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            after_first_burn,
            U256::MAX - U256::from(small_amount),
            "allowance must be U256::MAX minus the burned amount"
        );

        // Second burn: allowance is still far above threshold, so ensure_standing_allowance
        // is a no-op. The burn succeeds without re-approving.
        let large_amount = U256::from(100_000_000u64); // 100 USDC
        let receipt = bridge
            .burn_internal::<NoOpErrorRegistry>(
                BridgeDirection::EthereumToBase,
                large_amount,
                recipient,
            )
            .await
            .unwrap();

        assert!(!receipt.tx.is_zero(), "second burn should succeed");
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
    async fn find_recent_usdc_transfer_matches_on_from_to_value_and_scan_bound() {
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

        let sender = bridge.ethereum.owner();
        let recipient = address!("0x000000000000000000000000000000000000bEEF");
        let never_funded = address!("0x000000000000000000000000000000000000dEaD");
        let amount = U256::from(7_000_000u64); // 7 USDC

        // Capture the head before the send, exactly as the deposit leg captures
        // the mint's block as the scan lower bound.
        let from_block = bridge.ethereum.current_block().await.unwrap();

        let send_tx = bridge
            .ethereum
            .send_usdc::<NoOpErrorRegistry>(recipient, amount)
            .await
            .unwrap();

        // The deposit send (`>= from_block`) lands at `send_block`; the first
        // block strictly above it is the exclusion bound for the below-bound case.
        let above_block = bridge.ethereum.current_block().await.unwrap() + 1;

        // The scan is finality-gated: it returns Ok(None) only once the head is
        // a small margin past the bound. Advance the head with unrelated sends so
        // the absence assertions resolve to None, not a retryable ScanInconclusive.
        for _ in 0..4 {
            bridge
                .ethereum
                .send_usdc::<NoOpErrorRegistry>(never_funded, amount)
                .await
                .unwrap();
        }

        assert_eq!(
            bridge
                .find_recent_usdc_transfer(sender, recipient, amount, from_block)
                .await
                .unwrap(),
            Some(send_tx),
            "scan must adopt the exact (from, to, value) transfer at/after the bound",
        );

        let other_recipient = address!("0x000000000000000000000000000000000000Cafe");
        assert_eq!(
            bridge
                .find_recent_usdc_transfer(sender, other_recipient, amount, from_block)
                .await
                .unwrap(),
            None,
            "a transfer to a different recipient must not be adopted",
        );

        assert_eq!(
            bridge
                .find_recent_usdc_transfer(sender, recipient, amount + U256::from(1), from_block)
                .await
                .unwrap(),
            None,
            "a transfer whose value differs must not be adopted",
        );

        // Scanning from a bound above the send's block excludes it (mirroring the
        // find_recent_mint below-bound exclusion); the head is already far enough
        // past `above_block` for the absence to resolve to None.
        assert_eq!(
            bridge
                .find_recent_usdc_transfer(sender, recipient, amount, above_block)
                .await
                .unwrap(),
            None,
            "a transfer below the scan bound must not be adopted",
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

    // -------------------------------------------------------------------------
    // ethereum_usdc_balance and ethereum_tx_confirmations delegation tests
    // -------------------------------------------------------------------------

    /// Hypothesis: ethereum_usdc_balance reads from the Ethereum endpoint, not
    /// Base. Deploy USDC on one chain but not the other; the call must succeed
    /// on the one that has the contract (Ethereum) and reflect the minted amount.
    #[tokio::test]
    async fn ethereum_usdc_balance_reads_from_ethereum_endpoint() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        // Deploy USDC only on the Ethereum endpoint.
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

        let holder = PrivateKeySigner::from_bytes(&private_key)
            .unwrap()
            .address();

        let balance = bridge.ethereum_usdc_balance(holder).await.unwrap();

        // deploy_mock_usdc mints 1_000_000_000_000 to the deployer.
        assert_eq!(
            balance,
            U256::from(1_000_000_000_000u64),
            "ethereum_usdc_balance must return the Ethereum USDC balance, not Base"
        );
    }

    /// Hypothesis: ethereum_tx_confirmations returns None for a tx hash that
    /// does not exist on-chain (unmined).
    #[tokio::test]
    async fn ethereum_tx_confirmations_returns_none_for_unmined_tx() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            USDC_ETHEREUM,
        )
        .await
        .unwrap();

        let nonexistent_tx =
            b256!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let result = bridge
            .ethereum_tx_confirmations(nonexistent_tx)
            .await
            .unwrap();

        assert_eq!(
            result, None,
            "ethereum_tx_confirmations must return None for an unmined tx"
        );
    }

    /// Hypothesis: ethereum_tx_confirmations returns Some(N) where N >= 1 after
    /// mining a tx and advancing the chain head.
    #[tokio::test]
    async fn ethereum_tx_confirmations_counts_confirmations_correctly() {
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

        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer.clone()))
            .connect(&ethereum_endpoint)
            .await
            .unwrap();
        let token = MockMintBurnToken::new(usdc_address, &provider);

        // Mine a tx (mint to self) so we have a real tx hash.
        let receipt = token
            .mint(signer.address(), U256::from(1u64))
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let tx_hash = receipt.transaction_hash;

        // Immediately after mining: 1 confirmation (the inclusion block counts).
        let confs_immediate = bridge
            .ethereum_tx_confirmations(tx_hash)
            .await
            .unwrap()
            .expect("tx is mined");

        assert_eq!(
            confs_immediate, 1,
            "tx in the current head has 1 confirmation (inclusion block counts)"
        );

        // After 2 more blocks: 3 confirmations (inclusion block + 2).
        provider.anvil_mine(Some(2), None).await.unwrap();

        let confs_after_2 = bridge
            .ethereum_tx_confirmations(tx_hash)
            .await
            .unwrap()
            .expect("tx is still mined");

        assert_eq!(
            confs_after_2, 3,
            "tx has 3 confirmations after 2 blocks mined (inclusion block + 2)"
        );
    }

    /// The two-phase burn split (`submit_burn` -> `confirm_burn`) completes a real
    /// CCTP burn: `submit_burn` returns the broadcast tx hash WITHOUT awaiting the
    /// receipt, and `confirm_burn` then awaits it and returns a `BurnReceipt`
    /// carrying that tx and the burned amount.
    #[tokio::test]
    async fn submit_burn_returns_hash_then_confirm_burn_validates_receipt() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let amount = U256::from(1_000_000u64); // 1 USDC

        let burn_tx =
            Bridge::submit_burn(&bridge, BridgeDirection::EthereumToBase, amount, recipient)
                .await
                .unwrap();

        assert!(
            !burn_tx.is_zero(),
            "submit_burn must return the broadcast burn tx hash"
        );

        let burn_receipt =
            Bridge::confirm_burn(&bridge, BridgeDirection::EthereumToBase, burn_tx, amount)
                .await
                .unwrap();

        assert_eq!(
            burn_receipt.tx, burn_tx,
            "confirm_burn must return the same tx hash submit_burn broadcast"
        );
        assert_eq!(
            burn_receipt.amount, amount,
            "confirm_burn must carry the burned input amount through onto the receipt"
        );
    }

    /// `confirm_burn` returns `MessageSentEventNotFound` when the burn receipt
    /// mines successfully but emits no `MessageSent` event (the post-commit error
    /// class). Uses a STOP-only TokenMessenger so the tx succeeds (status = 1) with
    /// no logs.
    #[tokio::test]
    async fn confirm_burn_returns_message_sent_event_not_found_when_log_absent() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .unwrap();

        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        let no_wallet_provider = ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await
            .unwrap();
        no_wallet_provider
            .anvil_set_code(TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        let fee_server = MockServer::start();
        fee_server.mock(|when, then| {
            when.method(GET).path_includes("/v2/burn/USDC/fees/");
            then.status(200).json_body(serde_json::json!([
                {"finalityThreshold": 1000, "minimumFee": 1},
                {"finalityThreshold": 2000, "minimumFee": 0}
            ]));
        });

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap()
        .with_circle_api_base(fee_server.base_url());

        let recipient = PrivateKeySigner::from_bytes(&private_key)
            .unwrap()
            .address();
        let amount = U256::from(1_000u64);

        let burn_tx =
            Bridge::submit_burn(&bridge, BridgeDirection::EthereumToBase, amount, recipient)
                .await
                .unwrap();

        let error = Bridge::confirm_burn(&bridge, BridgeDirection::EthereumToBase, burn_tx, amount)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CctpError::MessageSentEventNotFound { .. }),
            "confirm_burn must return MessageSentEventNotFound when the receipt has \
             no MessageSent event; got: {error:?}"
        );
    }

    /// `burn_status` maps a mined-success and a mined-revert receipt to the right
    /// variants, regardless of the drop policy (a present receipt short-circuits).
    #[tokio::test]
    async fn burn_status_maps_mined_receipts_to_success_and_revert() {
        let (_anvil, endpoint, private_key) = setup_anvil();
        let bridge = create_bridge(&endpoint, &endpoint, &private_key, USDC_ETHEREUM)
            .await
            .unwrap();

        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&endpoint)
            .await
            .unwrap();

        // STOP contract: a tx to it mines successfully (status = 1).
        let stop_addr = address!("0x00000000000000000000000000000000000000aa");
        provider
            .anvil_set_code(stop_addr, alloy::primitives::Bytes::from(vec![0x00u8]))
            .await
            .unwrap();
        let success_receipt = provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(stop_addr.into()),
                gas: Some(100_000),
                ..Default::default()
            })
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(success_receipt.status(), "STOP tx must mine successfully");

        // REVERT contract (PUSH1 0, PUSH1 0, REVERT): explicit gas skips estimation
        // so the reverting tx is broadcast and mines with status = 0.
        let revert_addr = address!("0x00000000000000000000000000000000000000bb");
        provider
            .anvil_set_code(
                revert_addr,
                alloy::primitives::Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]),
            )
            .await
            .unwrap();
        let reverted_receipt = provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(revert_addr.into()),
                gas: Some(100_000),
                ..Default::default()
            })
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(!reverted_receipt.status(), "REVERT tx must mine reverted");

        assert_eq!(
            bridge
                .ethereum
                .burn_status(success_receipt.transaction_hash)
                .await
                .unwrap(),
            crate::BurnTxStatus::MinedSuccess
        );
        assert_eq!(
            bridge
                .ethereum
                .burn_status(reverted_receipt.transaction_hash)
                .await
                .unwrap(),
            crate::BurnTxStatus::MinedReverted
        );
    }

    /// The CRITICAL case for double-burn safety: a tx with no receipt that is
    /// still visible in the mempool classifies as `Pending`, NEVER `Dropped`, so
    /// the caller never re-burns a still-pending burn. Uses `--no-mining` anvil so
    /// the tx is accepted into the mempool but never mined.
    #[tokio::test]
    async fn burn_status_reports_pending_for_mempool_visible_tx() {
        let anvil = Anvil::new().arg("--no-mining").spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let bridge = create_bridge(&endpoint, &endpoint, &private_key, USDC_ETHEREUM)
            .await
            .unwrap();

        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&endpoint)
            .await
            .unwrap();

        // Submitted to the mempool but never mined (no auto-mining): the receipt
        // is absent while get_transaction_by_hash returns the pending tx.
        let pending = provider
            .send_transaction(alloy::rpc::types::TransactionRequest {
                to: Some(address!("0x00000000000000000000000000000000000000cc").into()),
                value: Some(U256::from(1u64)),
                gas: Some(21_000),
                ..Default::default()
            })
            .await
            .unwrap();
        let pending_tx = *pending.tx_hash();

        // Even with a zero-grace, single-miss config (which would conclude Dropped
        // for a truly-absent tx), a mempool-visible tx must be Pending.
        let status = bridge
            .ethereum
            .burn_status_with_config(pending_tx, super::evm::BurnDropConfig::fast())
            .await
            .unwrap();

        assert_eq!(
            status,
            crate::BurnTxStatus::Pending,
            "a mempool-visible (still-pending) burn must classify as Pending, never Dropped"
        );
    }

    /// A tx absent from both the receipt lookup and the mempool, observed past the
    /// grace window and the consecutive-miss threshold, classifies as `Dropped`
    /// so the caller can fail closed and require operator verification. Uses a
    /// zero grace + single miss to keep the test fast.
    #[tokio::test]
    async fn burn_status_reports_dropped_for_absent_tx_past_grace() {
        let (_anvil, endpoint, private_key) = setup_anvil();
        let bridge = create_bridge(&endpoint, &endpoint, &private_key, USDC_ETHEREUM)
            .await
            .unwrap();

        let unknown_tx =
            b256!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let status = bridge
            .ethereum
            .burn_status_with_config(unknown_tx, super::evm::BurnDropConfig::fast())
            .await
            .unwrap();

        assert_eq!(
            status,
            crate::BurnTxStatus::Dropped,
            "an absent tx past the grace + consecutive-miss threshold must classify as Dropped"
        );
    }

    /// Hypothesis: deposit_for_burn (via burn()) returns
    /// CctpError::MessageSentEventNotFound when the token messenger accepts the
    /// call and mines a receipt (tx succeeds) but emits no MessageSent log.
    ///
    /// This is the post-commit error class: the burn tx IS on-chain. Callers
    /// must NOT retry the burn on this error -- they must surface it for
    /// operator reconciliation.
    #[tokio::test]
    async fn burn_returns_message_sent_event_not_found_when_receipt_has_no_event() {
        let (_ethereum_anvil, ethereum_endpoint, private_key) = setup_anvil();
        let (_base_anvil, base_endpoint, _) = setup_anvil();

        // Deploy real USDC so ensure_standing_allowance succeeds.
        let usdc_address = deploy_mock_usdc(&ethereum_endpoint, &private_key)
            .await
            .unwrap();

        // Place a STOP-only contract at the TokenMessenger address: any call to
        // it succeeds (receipt status = 1) with empty return data and no events,
        // triggering MessageSentEventNotFound after the receipt is confirmed.
        let stop_bytecode = alloy::primitives::Bytes::from(vec![0x00u8]);
        let no_wallet_provider = ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await
            .unwrap();
        no_wallet_provider
            .anvil_set_code(TOKEN_MESSENGER_V2, stop_bytecode)
            .await
            .unwrap();

        // Mock the Circle fee endpoint so burn() can proceed past query_fast_transfer_fee.
        let fee_server = MockServer::start();
        fee_server.mock(|when, then| {
            when.method(GET).path_includes("/v2/burn/USDC/fees/");
            then.status(200).json_body(serde_json::json!([
                {"finalityThreshold": 1000, "minimumFee": 1},
                {"finalityThreshold": 2000, "minimumFee": 0}
            ]));
        });

        let bridge = create_bridge(
            &ethereum_endpoint,
            &base_endpoint,
            &private_key,
            usdc_address,
        )
        .await
        .unwrap()
        .with_circle_api_base(fee_server.base_url());

        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let recipient = signer.address();
        let amount = U256::from(1_000u64);

        let error = Bridge::burn(&bridge, BridgeDirection::EthereumToBase, amount, recipient)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CctpError::MessageSentEventNotFound { .. }),
            "burn must return MessageSentEventNotFound when the receipt has no \
             MessageSent event (post-commit error); got: {error:?}"
        );
    }

    /// Verifies that `deposit_for_burn_with_allowance_retry` retries exactly once
    /// when the first `depositForBurn` reverts due to insufficient allowance (zero
    /// allowance at the token messenger).
    ///
    /// This exercises the defense-in-depth retry path. The retry calls
    /// `ensure_standing_allowance`, which sets allowance to `U256::MAX`, and then
    /// the second `depositForBurn` succeeds.
    #[tokio::test]
    async fn deposit_for_burn_retries_once_on_allowance_revert() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let recipient = bridge.base.owner();
        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();
        let amount = U256::from(1_000_000u64); // 1 USDC

        // Start with zero allowance to guarantee the first depositForBurn reverts.
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
            "test precondition: allowance must be zero"
        );

        let max_fee = bridge
            .query_fast_transfer_fee(amount, BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        // Call the retry wrapper directly, bypassing ensure_standing_allowance.
        // The first depositForBurn should revert (zero allowance), the retry should
        // re-approve to U256::MAX via ensure_standing_allowance, then succeed.
        let burn_receipt = bridge
            .ethereum
            .deposit_for_burn_with_allowance_retry::<NoOpErrorRegistry>(
                amount,
                recipient,
                BridgeDirection::EthereumToBase,
                max_fee,
            )
            .await
            .unwrap();

        assert!(
            !burn_receipt.tx.is_zero(),
            "retry must produce a valid burn tx"
        );
        assert_eq!(burn_receipt.amount, amount, "burn amount must match");

        // Verify that the approve tx was submitted during the retry path.
        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX - U256::from(amount),
            "after a successful retry, allowance must be U256::MAX minus the burn amount"
        );
    }

    /// Verifies that `deposit_for_burn_with_allowance_retry` retries exactly once
    /// on any revert-class error, and terminates after the second attempt regardless
    /// of why the burn reverted.
    ///
    /// Uses `Address::ZERO` as an invalid `mintRecipient`: Circle's TokenMessenger
    /// V2 reverts with `InvalidMintRecipient` on both the first and second attempts.
    /// With `U256::MAX` allowance, `ensure_standing_allowance` is a no-op (no
    /// approve tx mined). Both deposit attempts are pre-flight reverts in Anvil
    /// simulation mode, so no transactions are mined and the nonce does not advance.
    /// The returned error is a revert-class error.
    #[tokio::test]
    async fn deposit_for_burn_retries_once_on_any_revert_then_propagates() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();
        let amount = U256::from(1_000_000u64); // 1 USDC

        // Pre-set allowance to U256::MAX so ensure_standing_allowance is a no-op.
        bridge
            .ethereum
            .approve_usdc::<NoOpErrorRegistry>(spender, U256::MAX)
            .await
            .unwrap();

        let max_fee = bridge
            .query_fast_transfer_fee(amount, BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        // Take the tx count after the approve so the subsequent deposit_for_burn
        // call is the baseline for measuring retry attempts.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&cctp.ethereum_endpoint)
            .await
            .unwrap();
        let tx_count_before = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        let zero_recipient = Address::ZERO;
        let result = bridge
            .ethereum
            .deposit_for_burn_with_allowance_retry::<NoOpErrorRegistry>(
                amount,
                zero_recipient,
                BridgeDirection::EthereumToBase,
                max_fee,
            )
            .await;

        // The second depositForBurn revert propagates; it is revert-class.
        let error = result.unwrap_err();
        assert!(
            error.is_revert(),
            "revert error must propagate after one retry; got: {error:?}"
        );

        // PRIMARY proof: no transactions were mined. Both depositForBurn calls
        // reverted as pre-flight in Anvil simulation; ensure_standing_allowance was
        // a no-op (allowance already MAX, no approve tx). No nonce was consumed.
        let tx_count_after = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();
        assert_eq!(
            tx_count_after,
            tx_count_before,
            "no transactions must have been mined (both burns pre-flight revert, \
             ensure_standing_allowance no-op); got {} unexpected additional txs",
            tx_count_after.saturating_sub(tx_count_before)
        );

        // The allowance must be unchanged (ensure_standing_allowance was a no-op).
        let final_allowance = bridge
            .ethereum
            .usdc()
            .allowance(owner, spender)
            .call()
            .await
            .unwrap();
        assert_eq!(
            final_allowance,
            U256::MAX,
            "allowance must be unchanged when ensure_standing_allowance is a no-op"
        );
    }

    // NOTE: Integration-level tests for the two remaining paths from finding #5
    // are skipped as not cheaply constructible:
    //
    // 1. "sync-failure branch" (wait_for_node_sync exhausts during retry):
    //    Requires a provider that always returns a stale block number for the
    //    sync wait while still supporting real `submit()` calls for the approve.
    //    CctpEndpoint uses a single wallet/provider for both, so there is no
    //    injection point short of introducing a split-provider wrapper that
    //    does not exist in the current test infrastructure.
    //
    // 2. "non-revert transport error no-retry" at the integration level:
    //    Requires injecting a transport error into wallet.submit() inside
    //    deposit_for_burn. No mock wallet exists; the Wallet trait has no
    //    error-injection hook. The property IS covered at the unit level:
    //    `is_revert_false_for_transport_nonce_too_low` and related tests prove
    //    that transport errors return false from is_revert(), and the guard
    //    `if !original_error.is_revert() { return Err(original_error); }` in
    //    deposit_for_burn_with_allowance_retry returns immediately.

    /// Verifies the exactly-once retry guarantee: when the first `depositForBurn`
    /// reverts with insufficient allowance, the retry re-approves and fires a
    /// second burn, but if that second burn also reverts, the error is returned
    /// immediately with no third attempt.
    #[tokio::test]
    async fn deposit_for_burn_retry_fires_exactly_once_on_second_revert() {
        let cctp = LocalCctp::new().await.unwrap();
        let bridge = cctp.create_bridge().await.unwrap();

        let owner = bridge.ethereum.owner();
        let spender = bridge.ethereum.token_messenger_address();

        // Start with zero allowance so the first depositForBurn reverts with an
        // allowance-class error, triggering the retry path.
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
            "test precondition: allowance must be zero"
        );

        let amount = U256::from(1_000_000u64); // 1 USDC
        let max_fee = bridge
            .query_fast_transfer_fee(amount, BridgeDirection::EthereumToBase)
            .await
            .unwrap();

        // Take tx count after setup so we can count only retry-path txs.
        let ethereum_provider = ProviderBuilder::new()
            .connect(&cctp.ethereum_endpoint)
            .await
            .unwrap();
        let tx_count_before = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();

        // Use zero recipient for the second burn to cause a non-allowance revert:
        // TokenMessenger V2 reverts with `InvalidMintRecipient` on Address::ZERO,
        // which is not allowance-related, so no third attempt must fire.
        let zero_recipient = Address::ZERO;
        let result = bridge
            .ethereum
            .deposit_for_burn_with_allowance_retry::<NoOpErrorRegistry>(
                amount,
                zero_recipient,
                BridgeDirection::EthereumToBase,
                max_fee,
            )
            .await;

        // The second burn reverted for a non-allowance reason; that error propagates.
        let error = result.unwrap_err();
        assert!(
            error.is_revert(),
            "second burn revert must propagate; got: {error:?}"
        );

        // PRIMARY proof of exactly-once: exactly one tx was mined (the approve
        // from ensure_standing_allowance). The first and second depositForBurn
        // calls both revert as pre-flight (Anvil simulation), so they do not mine
        // a transaction or advance the nonce. No third depositForBurn was issued.
        //
        // Note: the "no nonce advance on revert" property is specific to Anvil's
        // simulation mode (failed `eth_sendRawTransaction` pre-flights are rejected
        // before mining). On a live network, a reverting transaction that makes it
        // into a block DOES advance the nonce. The `is_revert()` check above is a
        // secondary sanity check confirming the error kind is preserved across the
        // retry chain — it cannot prove exactly-once because it would also pass if
        // three deposit attempts were made and the final one reverted.
        let tx_count_after = ethereum_provider
            .get_transaction_count(owner)
            .await
            .unwrap();
        assert_eq!(
            tx_count_after,
            tx_count_before + 1,
            "exactly one tx (the approve) must have been mined; \
             got {} additional txs",
            tx_count_after.saturating_sub(tx_count_before)
        );
    }
}
