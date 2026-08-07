//! Aggregate modeling the lifecycle of minting tokenized
//! equities from underlying Alpaca shares.
//!
//! Tracks the workflow from requesting a mint through
//! Alpaca's tokenization API to receiving onchain tokens.
//!
//! # State Flow
//!
//! ```text
//! MintRequested -> MintAccepted -> TokensReceived -> TokensWrapped -> DepositedIntoRaindex
//!       |               |               |                 |
//!       v               v               v                 v
//!     Failed          Failed          Failed           Failed
//! ```
//!
//! - `MintRequested` can be rejected by Alpaca during `RequestMint`
//! - `MintAccepted` can fail via `Poll` (rejection/timeout/error)
//! - `DepositedIntoRaindex` and `Failed` are terminal states
//!
//! # Alpaca API Integration
//!
//! The mint process integrates with Alpaca's tokenization API:
//!
//! 1. **Request**: System initiates mint request with symbol,
//!    quantity, and destination wallet
//! 2. **Acceptance**: Alpaca responds with `issuer_request_id` and
//!    `tokenization_request_id`
//! 3. **Transfer**: Alpaca executes onchain transfer, system detects
//!    transaction
//! 4. **Wrapping**: Tokens wrapped into ERC-4626 vault shares
//! 5. **Vault Deposit**: Wrapped tokens deposited into Raindex vault (terminal)
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (DepositedIntoRaindex, Failed) reject all state-changing commands
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::{info, warn};

use st0x_dto::{EquityMintOperation, EquityMintStatus, TransferOperation};
use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Id;
use st0x_tokenization::{IssuerRequestId, TokenizationRequestId, TokenizationRequestStatus};

#[cfg(test)]
use crate::bot_gas::BotGasReceiptCostEnqueuer;
use crate::mint_authorization::SignedMintAuthorization;
use crate::rebalancing::equity::EquityTransferServices;

/// Errors that can occur during tokenized equity mint operations.
///
/// These errors enforce state machine constraints and prevent
/// invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum TokenizedEquityMintError {
    /// Command sent to a non-existent aggregate (must use RequestMint to initialize)
    #[error("Aggregate not initialized: use RequestMint to start a new mint")]
    NotInitialized,
    /// Attempted to request mint when already in progress
    #[error("Mint already in progress")]
    AlreadyInProgress,
    /// Attempted to deposit to vault before tokens were wrapped
    #[error("Cannot deposit to vault: tokens not wrapped")]
    TokensNotWrapped,
    /// Cannot proceed: mint has not been accepted yet
    #[error("Cannot proceed: mint not accepted")]
    NotAccepted,
    /// Attempted to wrap before tokens were received
    #[error("Cannot wrap: tokens not received for wrapping")]
    TokensNotReceivedForWrap,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Attempted to reconcile a mint that is not in the `Failed` state
    #[error("Cannot reconcile: mint is not in the Failed state")]
    NotFailed,
    /// Attempted to act on a mint already resolved out-of-band (`Reconciled`)
    #[error("Already reconciled")]
    AlreadyReconciled,
    /// Attempted to reconcile without an operator-supplied reason.
    #[error("Cannot reconcile: reason is required")]
    ReconcileReasonRequired,
    /// Tokenizer API failed to submit the mint request.
    /// Tokenizer errors can't be wrapped with #[from] because they may
    /// contain types that don't implement Serialize/Deserialize (required
    /// by DomainError).
    #[error("Mint request failed: {error_message}")]
    RequestFailed { error_message: String },
    /// Completed mint response missing tx_hash
    #[error("Missing tx_hash in completed mint response")]
    MissingTxHash,
    /// Alpaca returned a token symbol that doesn't match the requested symbol
    #[error(
        "Token symbol mismatch: expected t{expected_symbol} \
         but Alpaca returned {actual_token_symbol}"
    )]
    TokenSymbolMismatch {
        expected_symbol: Symbol,
        actual_token_symbol: String,
    },
    /// Completed mint response missing token_symbol
    #[error(
        "Missing token symbol in completed mint response \
         (expected t{expected_symbol})"
    )]
    MissingTokenSymbol { expected_symbol: Symbol },
    /// Negative quantity is invalid for minting
    #[error("Negative quantity: {value:?}")]
    NegativeQuantity {
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        value: Float,
    },
    /// Float arithmetic or conversion error
    #[error("Float error: {0}")]
    Float(String),
    /// Vault lookup failed for the given symbol
    #[error("Vault lookup failed for {0}")]
    VaultLookupFailed(Symbol),
    /// Vault deposit transaction failed
    #[error("Vault deposit failed")]
    VaultDepositFailed,
    /// Mint-authorization signing failed. Like `RequestFailed`, the source
    /// (`MintAuthorizationError`) wraps non-serde types (EVM transport),
    /// which the DomainError serde bound forbids embedding, so only the
    /// rendered message is carried.
    #[error("Mint authorization signing failed: {error_message}")]
    AuthorizationSigningFailed { error_message: String },
    /// Attempted to record an authorization delivery before any
    /// authorization was signed -- the delivery job only fires after
    /// `MintAuthorizationSigned` is persisted, so this is a caller bug.
    #[error("Cannot record authorization delivery: no authorization signed")]
    AuthorizationNotSigned,
}

impl PartialEq for TokenizedEquityMintError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NotInitialized, Self::NotInitialized)
            | (Self::AlreadyInProgress, Self::AlreadyInProgress)
            | (Self::TokensNotWrapped, Self::TokensNotWrapped)
            | (Self::NotAccepted, Self::NotAccepted)
            | (Self::TokensNotReceivedForWrap, Self::TokensNotReceivedForWrap)
            | (Self::AlreadyCompleted, Self::AlreadyCompleted)
            | (Self::AlreadyFailed, Self::AlreadyFailed)
            | (Self::NotFailed, Self::NotFailed)
            | (Self::AlreadyReconciled, Self::AlreadyReconciled)
            | (Self::ReconcileReasonRequired, Self::ReconcileReasonRequired)
            | (Self::MissingTxHash, Self::MissingTxHash)
            | (Self::VaultDepositFailed, Self::VaultDepositFailed)
            | (Self::AuthorizationNotSigned, Self::AuthorizationNotSigned) => true,
            (
                Self::RequestFailed { error_message: a },
                Self::RequestFailed { error_message: b },
            )
            | (
                Self::AuthorizationSigningFailed { error_message: a },
                Self::AuthorizationSigningFailed { error_message: b },
            )
            | (Self::Float(a), Self::Float(b)) => a == b,
            (Self::VaultLookupFailed(a), Self::VaultLookupFailed(b))
            | (
                Self::MissingTokenSymbol { expected_symbol: a },
                Self::MissingTokenSymbol { expected_symbol: b },
            ) => a == b,
            (Self::NegativeQuantity { value: a }, Self::NegativeQuantity { value: b }) => {
                a.eq(*b).unwrap_or(false)
            }
            (
                Self::TokenSymbolMismatch {
                    expected_symbol: exp_a,
                    actual_token_symbol: act_a,
                },
                Self::TokenSymbolMismatch {
                    expected_symbol: exp_b,
                    actual_token_symbol: act_b,
                },
            ) => exp_a == exp_b && act_a == act_b,
            _ => false,
        }
    }
}

impl Eq for TokenizedEquityMintError {}

impl From<FloatError> for TokenizedEquityMintError {
    fn from(error: FloatError) -> Self {
        Self::Float(error.to_string())
    }
}

/// Commands for the TokenizedEquityMint aggregate.
#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    /// Request tokenization from Alpaca and poll until tokens arrive or failure.
    ///
    /// Flow: MintRequested -> MintAccepted -> TokensReceived (success)
    ///                     or MintRejected (immediate failure)
    ///                     or MintAcceptanceFailed (failure after acceptance)
    /// Calls `request_mint()` on the tokenizer service.
    ///
    /// Emits MintRequested + MintAccepted (success)
    ///     or MintRequested + MintRejected (immediate rejection)
    RequestMint {
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Float,
        wallet: Address,
    },
    /// Test/fixture-only: identical to `RequestMint` but takes `requested_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    RequestMintAt {
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Float,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    /// Calls `poll_mint_until_complete()` on the tokenizer service.
    ///
    /// Emits TokensReceived (success)
    ///     or MintAcceptanceFailed (rejection/timeout/error)
    Poll,
    /// Test/fixture-only: identical to `Poll` but takes `received_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history. Only the success (`TokensReceived`) path
    /// honours the timestamp; the fixture never drives a rejection.
    #[cfg(any(test, feature = "test-support"))]
    PollAt {
        received_at: DateTime<Utc>,
    },
    /// Signs and persists the recipient authorization for an
    /// orchestrator-mode mint (the asset's `vault_mode` on issuance's
    /// status endpoint reads `"orchestrator"`). Generates the mint's nonce,
    /// signs via the mint-authorizer service, and emits
    /// `MintAuthorizationSigned` -- persisted BEFORE any delivery attempt.
    /// No-op when an authorization is already signed or delivered: the
    /// nonce is this mint's on-chain idempotency key, so at-least-once saga
    /// re-drives must reuse the persisted one, never mint a fresh one.
    ///
    /// Emits MintAuthorizationSigned (or nothing when already signed).
    SignMintAuthorization {
        /// The asset's tokenized-equity ERC-20 address -- the `token` the
        /// EIP-712 MintAuth binds; issuance mints exactly this token.
        token: Address,
    },
    /// Records issuance's acknowledgement (`200`) that the delivered
    /// authorization was validated and recorded. Dispatched by the durable
    /// delivery job. Usually lands AFTER the mint advanced past
    /// `MintAccepted` (the in-flight `Poll` holds the aggregate until
    /// Alpaca completes, which requires the delivery) -- still recorded
    /// there as an audit fact. No-op when already delivered or terminal.
    /// Post-advance the aggregate keeps no delivered marker (the progress
    /// is dropped with the authorization's purpose), so a re-driven
    /// acknowledgement there may record more than once; each is an
    /// independent audit fact, deliberately not deduplicated.
    ///
    /// Emits MintAuthorizationDelivered (or nothing).
    RecordMintAuthorizationDelivered,
    /// Record that a wrap transaction has been submitted (before confirmation).
    SubmitWrap {
        wrap_tx_hash: TxHash,
    },
    /// Record that a vault deposit transaction has been submitted (before confirmation).
    SubmitVaultDeposit {
        vault_deposit_tx_hash: TxHash,
    },
    WrapTokens {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        /// Block where the wrap tx confirmed. Mandatory on the live command so
        /// a new `TokensWrapped` event can never skip the node-sync wait before
        /// deposit. The persisted event keeps `Option<u64>` for legacy replay.
        wrap_block: u64,
    },
    /// Test/fixture-only: identical to `WrapTokens` but takes `wrapped_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    WrapTokensAt {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        wrap_block: u64,
        wrapped_at: DateTime<Utc>,
    },
    DepositToVault {
        vault_deposit_tx_hash: TxHash,
    },
    /// Test/fixture-only: identical to `DepositToVault` but takes
    /// `deposited_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    DepositToVaultAt {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Operator or timeout-driven failure from `MintAccepted` state, or operator
    /// force-fail from `MintRequested` (requested at the provider but never
    /// accepted). Both emit `MintAcceptanceFailed`; the pre-acceptance case
    /// moved no shares to inflight, so there is nothing to restore.
    FailAcceptance {
        reason: String,
    },
    /// Operator or timeout-driven failure from `TokensReceived` state.
    FailWrapping {
        reason: String,
    },
    /// Operator or timeout-driven failure from `TokensWrapped` state.
    FailRaindexDeposit {
        reason: String,
    },
    /// Recover a failed mint after the provider reports completion later.
    RecoverProviderCompletion {
        issuer_request_id: IssuerRequestId,
        wallet: Address,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        fees: Option<Float>,
    },
    /// Reconcile a mint stranded in the terminal `Failed` state to the terminal
    /// `Reconciled` state. The residual tokens/equity were handled out-of-band
    /// (e.g. via wrap-equity/vault-deposit), so this is a bookkeeping resolution
    /// rather than a re-drive. Valid ONLY from `Failed`.
    ///
    /// Accepts any `Failed` variant, including a pre-departure `MintRejected`
    /// failure that stranded nothing. Such a mint needs no reconcile (the
    /// dashboard already excludes it from the stuck list); reconciling it is a
    /// harmless no-residue bookkeeping close, so the operator decides whether it
    /// is warranted rather than the aggregate second-guessing the failure cause.
    Reconcile {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedEquityMintEvent {
    MintRequested {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    /// Alpaca rejected the mint request before acceptance.
    /// Shares remain in offchain available - no funds were moved.
    MintRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },

    MintAccepted {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    /// Recipient authorization signed for an orchestrator-mode mint,
    /// persisted before the first delivery attempt so a crash cannot lose
    /// the nonce: the nonce is this mint's on-chain idempotency key, and
    /// every delivery retry must reuse it (and the signature)
    /// byte-identically.
    MintAuthorizationSigned {
        nonce: B256,
        signature: Bytes,
        signed_at: DateTime<Utc>,
    },
    /// Issuance validated and recorded the delivered authorization
    /// (idempotent `200` on the internal mint-authorization call).
    MintAuthorizationDelivered { delivered_at: DateTime<Utc> },
    /// Mint failed before tokens were received. Two cases:
    /// - failed after `MintAccepted`: shares were moved to inflight (Hedging),
    ///   so the inventory reactor restores them to offchain available (Cancel).
    /// - operator force-fail from `MintRequested` (pre-acceptance): NO shares
    ///   were moved to inflight, so there is nothing to restore.
    ///
    /// `mint_inventory_update` gates the Cancel on the tracking stage having
    /// reached `Accepted`, so the pre-acceptance case is a no-op there.
    MintAcceptanceFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    TokensReceived {
        tx_hash: TxHash,
        shares_minted: U256,
        /// Tokenization fees charged by Alpaca (if reported).
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        fees: Option<Float>,
        received_at: DateTime<Utc>,
    },

    /// Wrap transaction submitted to the blockchain, pending confirmation.
    WrapSubmitted {
        wrap_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    /// Unwrapped tokens have been wrapped into ERC-4626 vault shares.
    TokensWrapped {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        wrapped_at: DateTime<Utc>,
        /// Block where the wrap tx confirmed; `None` for events emitted before this field was
        /// added (schema backward-compatibility).
        #[serde(default)]
        wrap_block: Option<u64>,
    },
    /// Wrapping failed after tokens were received.
    WrappingFailed {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        /// Reason for the wrapping failure (timeout, EVM revert, operator action).
        /// Absent in events emitted before this field was added.
        #[serde(default)]
        reason: Option<String>,
        failed_at: DateTime<Utc>,
    },

    /// Vault deposit transaction submitted, pending confirmation.
    VaultDepositSubmitted {
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    /// Wrapped tokens deposited to Raindex vault.
    DepositedIntoRaindex {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Raindex deposit failed after tokens were wrapped.
    /// Wrapped tokens remain in wallet, can be retried or manually recovered.
    RaindexDepositFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    ProviderCompletionRecovered {
        issuer_request_id: IssuerRequestId,
        wallet: Address,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        shares_minted: U256,
        /// Tokenization fees charged by Alpaca (if reported).
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        fees: Option<Float>,
        recovered_at: DateTime<Utc>,
    },
    /// An operator reconciled a terminal `Failed` mint out-of-band. Marks the
    /// transfer resolved without re-driving the failed leg.
    OperatorReconciled {
        reason: String,
        reconciled_at: DateTime<Utc>,
    },
}

impl PartialEq for TokenizedEquityMintEvent {
    #[allow(clippy::too_many_lines)]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::MintRequested {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    requested_at: req_a,
                },
                Self::MintRequested {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    requested_at: req_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && req_a == req_b
            }
            (
                Self::MintRejected {
                    reason: reason_a,
                    rejected_at: time_a,
                },
                Self::MintRejected {
                    reason: reason_b,
                    rejected_at: time_b,
                },
            )
            | (
                Self::MintAcceptanceFailed {
                    reason: reason_a,
                    failed_at: time_a,
                },
                Self::MintAcceptanceFailed {
                    reason: reason_b,
                    failed_at: time_b,
                },
            )
            | (
                Self::RaindexDepositFailed {
                    reason: reason_a,
                    failed_at: time_a,
                },
                Self::RaindexDepositFailed {
                    reason: reason_b,
                    failed_at: time_b,
                },
            )
            | (
                Self::OperatorReconciled {
                    reason: reason_a,
                    reconciled_at: time_a,
                },
                Self::OperatorReconciled {
                    reason: reason_b,
                    reconciled_at: time_b,
                },
            ) => reason_a == reason_b && time_a == time_b,
            (
                Self::MintAccepted {
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    accepted_at: acc_a,
                },
                Self::MintAccepted {
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    accepted_at: acc_b,
                },
            ) => iss_a == iss_b && tok_a == tok_b && acc_a == acc_b,
            (
                Self::MintAuthorizationSigned {
                    nonce,
                    signature,
                    signed_at,
                },
                Self::MintAuthorizationSigned {
                    nonce: nonce_b,
                    signature: sig_b,
                    signed_at: time_b,
                },
            ) => (nonce, signature, signed_at) == (nonce_b, sig_b, time_b),
            (
                Self::MintAuthorizationDelivered {
                    delivered_at: time_a,
                },
                Self::MintAuthorizationDelivered {
                    delivered_at: time_b,
                },
            ) => time_a == time_b,
            (
                Self::TokensReceived {
                    tx_hash: hash_a,
                    shares_minted: mint_a,
                    fees: fees_a,
                    received_at: time_a,
                },
                Self::TokensReceived {
                    tx_hash: hash_b,
                    shares_minted: mint_b,
                    fees: fees_b,
                    received_at: time_b,
                },
            ) => {
                hash_a == hash_b
                    && mint_a == mint_b
                    && match (fees_a, fees_b) {
                        (Some(a), Some(b)) => a.eq(*b).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && time_a == time_b
            }
            (
                Self::TokensWrapped {
                    wrap_tx_hash: hash_a,
                    wrapped_shares: shares_a,
                    wrapped_at: time_a,
                    wrap_block: block_a,
                },
                Self::TokensWrapped {
                    wrap_tx_hash: hash_b,
                    wrapped_shares: shares_b,
                    wrapped_at: time_b,
                    wrap_block: block_b,
                },
            ) => hash_a == hash_b && shares_a == shares_b && time_a == time_b && block_a == block_b,
            (
                Self::WrappingFailed {
                    symbol: sym_a,
                    quantity: qty_a,
                    reason: reason_a,
                    failed_at: time_a,
                },
                Self::WrappingFailed {
                    symbol: sym_b,
                    quantity: qty_b,
                    reason: reason_b,
                    failed_at: time_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && reason_a == reason_b
                    && time_a == time_b
            }
            (
                Self::WrapSubmitted {
                    wrap_tx_hash: hash_a,
                    submitted_at: time_a,
                },
                Self::WrapSubmitted {
                    wrap_tx_hash: hash_b,
                    submitted_at: time_b,
                },
            )
            | (
                Self::VaultDepositSubmitted {
                    vault_deposit_tx_hash: hash_a,
                    submitted_at: time_a,
                },
                Self::VaultDepositSubmitted {
                    vault_deposit_tx_hash: hash_b,
                    submitted_at: time_b,
                },
            )
            | (
                Self::DepositedIntoRaindex {
                    vault_deposit_tx_hash: hash_a,
                    deposited_at: time_a,
                },
                Self::DepositedIntoRaindex {
                    vault_deposit_tx_hash: hash_b,
                    deposited_at: time_b,
                },
            ) => hash_a == hash_b && time_a == time_b,
            (
                Self::ProviderCompletionRecovered {
                    issuer_request_id: issuer_a,
                    wallet: wallet_a,
                    tokenization_request_id: id_a,
                    tx_hash: hash_a,
                    shares_minted: shares_a,
                    fees: fees_a,
                    recovered_at: time_a,
                },
                Self::ProviderCompletionRecovered {
                    issuer_request_id: issuer_b,
                    wallet: wallet_b,
                    tokenization_request_id: id_b,
                    tx_hash: hash_b,
                    shares_minted: shares_b,
                    fees: fees_b,
                    recovered_at: time_b,
                },
            ) => {
                issuer_a == issuer_b
                    && wallet_a == wallet_b
                    && id_a == id_b
                    && hash_a == hash_b
                    && shares_a == shares_b
                    && match (fees_a, fees_b) {
                        (Some(a), Some(b)) => a.eq(*b).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && time_a == time_b
            }
            _ => false,
        }
    }
}

impl Eq for TokenizedEquityMintEvent {}

impl DomainEvent for TokenizedEquityMintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::MintRequested { .. } => "TokenizedEquityMintEvent::MintRequested".to_string(),
            Self::MintRejected { .. } => "TokenizedEquityMintEvent::MintRejected".to_string(),
            Self::MintAccepted { .. } => "TokenizedEquityMintEvent::MintAccepted".to_string(),
            Self::MintAuthorizationSigned { .. } => {
                "TokenizedEquityMintEvent::MintAuthorizationSigned".to_string()
            }
            Self::MintAuthorizationDelivered { .. } => {
                "TokenizedEquityMintEvent::MintAuthorizationDelivered".to_string()
            }
            Self::MintAcceptanceFailed { .. } => {
                "TokenizedEquityMintEvent::MintAcceptanceFailed".to_string()
            }
            Self::TokensReceived { .. } => "TokenizedEquityMintEvent::TokensReceived".to_string(),
            Self::WrapSubmitted { .. } => "TokenizedEquityMintEvent::WrapSubmitted".to_string(),
            Self::TokensWrapped { .. } => "TokenizedEquityMintEvent::TokensWrapped".to_string(),
            Self::VaultDepositSubmitted { .. } => {
                "TokenizedEquityMintEvent::VaultDepositSubmitted".to_string()
            }
            Self::WrappingFailed { .. } => "TokenizedEquityMintEvent::WrappingFailed".to_string(),
            Self::DepositedIntoRaindex { .. } => {
                "TokenizedEquityMintEvent::DepositedIntoRaindex".to_string()
            }
            Self::RaindexDepositFailed { .. } => {
                "TokenizedEquityMintEvent::RaindexDepositFailed".to_string()
            }
            Self::ProviderCompletionRecovered { .. } => {
                "TokenizedEquityMintEvent::ProviderCompletionRecovered".to_string()
            }
            Self::OperatorReconciled { .. } => {
                "TokenizedEquityMintEvent::OperatorReconciled".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Recipient-authorization progress for an orchestrator-mode mint sitting
/// in `MintAccepted`.
///
/// Defaults to `NotSigned` so event streams and snapshots from before this
/// field existed replay unchanged -- and a vault-direct mint's progress
/// simply never leaves `NotSigned`. The progress is dropped once tokens
/// arrive: from `TokensReceived` on, the authorization has served its
/// purpose (issuance minted with it) and is no longer actionable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MintAuthorizationProgress {
    /// No authorization signed -- vault-direct mint, or signing has not
    /// happened yet.
    #[default]
    NotSigned,
    /// Signed and persisted; issuance has not yet acknowledged receipt.
    Signed(SignedMintAuthorization),
    /// Issuance acknowledged recording the authorization.
    Delivered(SignedMintAuthorization),
}

/// Tokenized equity mint aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid
/// states unrepresentable. Each variant contains exactly the data
/// valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedEquityMint {
    /// Mint request initiated with symbol, quantity, and destination wallet
    MintRequested {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },

    /// Alpaca API accepted the mint request and returned tracking identifiers
    MintAccepted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        /// Orchestrator-mode recipient-authorization progress; stays
        /// `NotSigned` for vault-direct mints.
        #[serde(default)]
        authorization: MintAuthorizationProgress,
    },

    /// Onchain token transfer detected with transaction details
    TokensReceived {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        shares_minted: U256,
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        fees: Option<Float>,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
    },

    /// Wrap transaction submitted, awaiting confirmation
    WrapSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        shares_minted: U256,
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        fees: Option<Float>,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
    },

    /// Tokens have been wrapped into ERC-4626 vault shares
    TokensWrapped {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        shares_minted: U256,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        /// Block where the wrap tx confirmed; `None` for aggregates persisted before this field.
        #[serde(default)]
        wrap_block: Option<u64>,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        wrapped_at: DateTime<Utc>,
    },

    /// Vault deposit transaction submitted, awaiting confirmation
    VaultDepositSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        shares_minted: U256,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        wrapped_at: DateTime<Utc>,
        vault_deposit_tx_hash: TxHash,
    },

    /// Wrapped tokens deposited to Raindex vault
    DepositedIntoRaindex {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        /// Alpaca cross-system identifiers for auditing
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Token receipt transaction
        token_tx_hash: TxHash,
        /// Wrapping transaction
        wrap_tx_hash: TxHash,
        /// Vault deposit transaction
        vault_deposit_tx_hash: TxHash,
        requested_at: DateTime<Utc>,
        deposited_at: DateTime<Utc>,
    },

    /// Mint operation failed (terminal state)
    Failed {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },

    /// An operator reconciled a terminal `Failed` mint out-of-band (terminal
    /// state). Retains the identifying symbol/quantity and the original failure
    /// reason so the projection still reports the real transfer instead of a
    /// zero-value record.
    Reconciled {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        /// The failure reason carried over from the `Failed` state.
        failure_reason: String,
        /// The operator-supplied reconciliation reason.
        reconcile_reason: String,
        requested_at: DateTime<Utc>,
        reconciled_at: DateTime<Utc>,
    },
}

impl PartialEq for TokenizedEquityMint {
    // allowed due to the whole method being a single mapping
    #[allow(clippy::too_many_lines)]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::MintRequested {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    requested_at: req_a,
                },
                Self::MintRequested {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    requested_at: req_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && req_a == req_b
            }
            (
                Self::MintAccepted {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    requested_at: req_a,
                    accepted_at: acc_a,
                    authorization: auth_a,
                },
                Self::MintAccepted {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    requested_at: req_b,
                    accepted_at: acc_b,
                    authorization: auth_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && req_a == req_b
                    && acc_a == acc_b
                    && auth_a == auth_b
            }
            (
                Self::TokensReceived {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    tx_hash: hash_a,
                    shares_minted: mint_a,
                    fees: fees_a,
                    requested_at: req_a,
                    accepted_at: acc_a,
                    received_at: recv_a,
                },
                Self::TokensReceived {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    tx_hash: hash_b,
                    shares_minted: mint_b,
                    fees: fees_b,
                    requested_at: req_b,
                    accepted_at: acc_b,
                    received_at: recv_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && hash_a == hash_b
                    && mint_a == mint_b
                    && match (fees_a, fees_b) {
                        (Some(a), Some(b)) => a.eq(*b).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && req_a == req_b
                    && acc_a == acc_b
                    && recv_a == recv_b
            }
            (
                Self::WrapSubmitted {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    tx_hash: hash_a,
                    shares_minted: mint_a,
                    fees: fees_a,
                    requested_at: req_a,
                    accepted_at: acc_a,
                    received_at: recv_a,
                    wrap_tx_hash: wrap_hash_a,
                },
                Self::WrapSubmitted {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    tx_hash: hash_b,
                    shares_minted: mint_b,
                    fees: fees_b,
                    requested_at: req_b,
                    accepted_at: acc_b,
                    received_at: recv_b,
                    wrap_tx_hash: wrap_hash_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && hash_a == hash_b
                    && mint_a == mint_b
                    && match (fees_a, fees_b) {
                        (Some(a), Some(b)) => a.eq(*b).unwrap_or(false),
                        (None, None) => true,
                        _ => false,
                    }
                    && req_a == req_b
                    && acc_a == acc_b
                    && recv_a == recv_b
                    && wrap_hash_a == wrap_hash_b
            }
            (
                Self::TokensWrapped {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    tx_hash: hash_a,
                    shares_minted: mint_a,
                    wrap_tx_hash: wrap_hash_a,
                    wrapped_shares: wrap_shares_a,
                    wrap_block: wrap_block_a,
                    requested_at: req_a,
                    accepted_at: acc_a,
                    received_at: recv_a,
                    wrapped_at: wrap_a,
                },
                Self::TokensWrapped {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    tx_hash: hash_b,
                    shares_minted: mint_b,
                    wrap_tx_hash: wrap_hash_b,
                    wrapped_shares: wrap_shares_b,
                    wrap_block: wrap_block_b,
                    requested_at: req_b,
                    accepted_at: acc_b,
                    received_at: recv_b,
                    wrapped_at: wrap_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && hash_a == hash_b
                    && mint_a == mint_b
                    && wrap_hash_a == wrap_hash_b
                    && wrap_shares_a == wrap_shares_b
                    && wrap_block_a == wrap_block_b
                    && req_a == req_b
                    && acc_a == acc_b
                    && recv_a == recv_b
                    && wrap_a == wrap_b
            }
            (
                Self::VaultDepositSubmitted {
                    symbol: sym_a,
                    quantity: qty_a,
                    wallet: wal_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    tx_hash: hash_a,
                    shares_minted: mint_a,
                    wrap_tx_hash: wrap_hash_a,
                    wrapped_shares: wrap_shares_a,
                    requested_at: req_a,
                    accepted_at: acc_a,
                    received_at: recv_a,
                    wrapped_at: wrap_a,
                    vault_deposit_tx_hash: vault_hash_a,
                },
                Self::VaultDepositSubmitted {
                    symbol: sym_b,
                    quantity: qty_b,
                    wallet: wal_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    tx_hash: hash_b,
                    shares_minted: mint_b,
                    wrap_tx_hash: wrap_hash_b,
                    wrapped_shares: wrap_shares_b,
                    requested_at: req_b,
                    accepted_at: acc_b,
                    received_at: recv_b,
                    wrapped_at: wrap_b,
                    vault_deposit_tx_hash: vault_hash_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && wal_a == wal_b
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && hash_a == hash_b
                    && mint_a == mint_b
                    && wrap_hash_a == wrap_hash_b
                    && wrap_shares_a == wrap_shares_b
                    && req_a == req_b
                    && acc_a == acc_b
                    && recv_a == recv_b
                    && wrap_a == wrap_b
                    && vault_hash_a == vault_hash_b
            }
            (
                Self::DepositedIntoRaindex {
                    symbol: sym_a,
                    quantity: qty_a,
                    issuer_request_id: iss_a,
                    tokenization_request_id: tok_a,
                    token_tx_hash: token_hash_a,
                    wrap_tx_hash: wrap_hash_a,
                    vault_deposit_tx_hash: vault_hash_a,
                    requested_at: req_a,
                    deposited_at: dep_a,
                },
                Self::DepositedIntoRaindex {
                    symbol: sym_b,
                    quantity: qty_b,
                    issuer_request_id: iss_b,
                    tokenization_request_id: tok_b,
                    token_tx_hash: token_hash_b,
                    wrap_tx_hash: wrap_hash_b,
                    vault_deposit_tx_hash: vault_hash_b,
                    requested_at: req_b,
                    deposited_at: dep_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && iss_a == iss_b
                    && tok_a == tok_b
                    && token_hash_a == token_hash_b
                    && wrap_hash_a == wrap_hash_b
                    && vault_hash_a == vault_hash_b
                    && req_a == req_b
                    && dep_a == dep_b
            }
            (
                Self::Failed {
                    symbol: sym_a,
                    quantity: qty_a,
                    reason: reason_a,
                    requested_at: req_a,
                    failed_at: fail_a,
                },
                Self::Failed {
                    symbol: sym_b,
                    quantity: qty_b,
                    reason: reason_b,
                    requested_at: req_b,
                    failed_at: fail_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && reason_a == reason_b
                    && req_a == req_b
                    && fail_a == fail_b
            }
            (
                Self::Reconciled {
                    symbol: sym_a,
                    quantity: qty_a,
                    failure_reason: fail_reason_a,
                    reconcile_reason: rec_reason_a,
                    requested_at: req_a,
                    reconciled_at: reconciled_a,
                },
                Self::Reconciled {
                    symbol: sym_b,
                    quantity: qty_b,
                    failure_reason: fail_reason_b,
                    reconcile_reason: rec_reason_b,
                    requested_at: req_b,
                    reconciled_at: reconciled_b,
                },
            ) => {
                sym_a == sym_b
                    && qty_a.eq(*qty_b).unwrap_or(false)
                    && fail_reason_a == fail_reason_b
                    && rec_reason_a == rec_reason_b
                    && req_a == req_b
                    && reconciled_a == reconciled_b
            }
            _ => false,
        }
    }
}

impl Eq for TokenizedEquityMint {}

impl TokenizedEquityMint {
    /// Returns the requested quantity carried by the aggregate in every
    /// non-terminal state. Wrapped-equity recovery compares this against the
    /// wallet snapshot so an audit mismatch surfaces before dispatch.
    pub(crate) fn quantity(&self) -> Float {
        match self {
            Self::MintRequested { quantity, .. }
            | Self::MintAccepted { quantity, .. }
            | Self::TokensReceived { quantity, .. }
            | Self::WrapSubmitted { quantity, .. }
            | Self::TokensWrapped { quantity, .. }
            | Self::VaultDepositSubmitted { quantity, .. }
            | Self::DepositedIntoRaindex { quantity, .. }
            | Self::Failed { quantity, .. }
            | Self::Reconciled { quantity, .. } => *quantity,
        }
    }

    /// Returns `true` when the aggregate has reached a terminal state and no
    /// further job-driven processing is expected.
    ///
    /// An exhaustive `match` is intentional: adding a new variant to the enum
    /// without updating this function causes a compile error, preventing silent
    /// mis-classification of new states.
    pub(crate) fn is_terminal(&self) -> bool {
        match self {
            Self::DepositedIntoRaindex { .. } | Self::Failed { .. } | Self::Reconciled { .. } => {
                true
            }
            Self::MintRequested { .. }
            | Self::MintAccepted { .. }
            | Self::TokensReceived { .. }
            | Self::WrapSubmitted { .. }
            | Self::TokensWrapped { .. }
            | Self::VaultDepositSubmitted { .. } => false,
        }
    }

    pub(crate) fn to_dto(&self, id: &IssuerRequestId) -> TransferOperation {
        use EquityMintStatus::*;

        let IssuerRequestId(issuer_request_id) = id;

        match self {
            Self::MintRequested {
                symbol,
                quantity,
                requested_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Minting,
                started_at: *requested_at,
                updated_at: *requested_at,
            }),

            Self::MintAccepted {
                symbol,
                quantity,
                requested_at,
                accepted_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Minting,
                started_at: *requested_at,
                updated_at: *accepted_at,
            }),

            Self::TokensReceived {
                symbol,
                quantity,
                requested_at,
                received_at,
                ..
            }
            | Self::WrapSubmitted {
                symbol,
                quantity,
                requested_at,
                received_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Wrapping,
                started_at: *requested_at,
                updated_at: *received_at,
            }),

            Self::TokensWrapped {
                symbol,
                quantity,
                requested_at,
                wrapped_at,
                ..
            }
            | Self::VaultDepositSubmitted {
                symbol,
                quantity,
                requested_at,
                wrapped_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Depositing,
                started_at: *requested_at,
                updated_at: *wrapped_at,
            }),

            Self::DepositedIntoRaindex {
                symbol,
                quantity,
                requested_at,
                deposited_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Completed {
                    completed_at: *deposited_at,
                },
                started_at: *requested_at,
                updated_at: *deposited_at,
            }),

            Self::Failed {
                symbol,
                quantity,
                requested_at,
                failed_at,
                ..
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Failed {
                    failed_at: *failed_at,
                },
                started_at: *requested_at,
                updated_at: *failed_at,
            }),

            Self::Reconciled {
                symbol,
                quantity,
                failure_reason,
                reconcile_reason,
                requested_at,
                reconciled_at,
            } => TransferOperation::EquityMint(EquityMintOperation {
                id: Id::new(issuer_request_id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: Reconciled {
                    reconciled_at: *reconciled_at,
                    failure_reason: failure_reason.clone(),
                    reconcile_reason: reconcile_reason.clone(),
                },
                started_at: *requested_at,
                updated_at: *reconciled_at,
            }),
        }
    }
}

/// Returns mint aggregate IDs whose latest event is non-terminal and should be
/// resumed after restart.
///
/// The delivery acknowledgement normally trails the whole mint (the ack is
/// recorded as an audit fact after `DepositedIntoRaindex`), so a completed
/// mint's LATEST event is `MintAuthorizationDelivered` -- latest-event
/// membership alone would resume every completed orchestrator-mode mint on
/// every boot, forever. The `NOT EXISTS` clause excludes any aggregate that
/// deposited: a deposit anywhere in the stream means the mint is complete
/// regardless of trailing acknowledgements, while a `Delivered` latest
/// WITHOUT a deposit (the ack landed while still `MintAccepted`) is a
/// genuine interruption and stays included.
pub(crate) async fn interrupted_mint_ids(
    pool: &SqlitePool,
) -> Result<Vec<IssuerRequestId>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'TokenizedEquityMint' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = 'TokenizedEquityMint' \
           AND last_ev.event_type IN ( \
               'TokenizedEquityMintEvent::MintAccepted', \
               'TokenizedEquityMintEvent::MintAuthorizationSigned', \
               'TokenizedEquityMintEvent::MintAuthorizationDelivered', \
               'TokenizedEquityMintEvent::ProviderCompletionRecovered', \
               'TokenizedEquityMintEvent::TokensReceived', \
               'TokenizedEquityMintEvent::WrapSubmitted', \
               'TokenizedEquityMintEvent::TokensWrapped', \
               'TokenizedEquityMintEvent::VaultDepositSubmitted' \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM events deposited \
               WHERE deposited.aggregate_type = 'TokenizedEquityMint' \
                 AND deposited.aggregate_id = last_ev.aggregate_id \
                 AND deposited.event_type = \
                     'TokenizedEquityMintEvent::DepositedIntoRaindex' \
           ) \
         ORDER BY latest.aggregate_id",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.parse()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Our tokenized equity tokens use 18 decimals.
pub(crate) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// Why a share quantity cannot convert to 18-decimal share-wei.
///
/// Narrower than `TokenizedEquityMintError` so consumers outside the
/// aggregate (the recipient-authorization signer) can carry it in their
/// public error types without exposing the aggregate's whole error surface.
/// `pub` (re-exported by `crate::mint_authorization`) for exactly that.
#[derive(Debug, thiserror::Error)]
pub enum QuantityScalingError {
    /// Negative quantity is invalid for minting.
    #[error("negative quantity: {value:?}")]
    Negative { value: Float },
    /// The quantity does not scale losslessly (sub-atto precision) or its
    /// share-wei overflow `U256`.
    #[error(transparent)]
    Float(#[from] FloatError),
}

impl From<QuantityScalingError> for TokenizedEquityMintError {
    fn from(error: QuantityScalingError) -> Self {
        match error {
            QuantityScalingError::Negative { value } => Self::NegativeQuantity { value },
            QuantityScalingError::Float(float_error) => float_error.into(),
        }
    }
}

/// Scales a share quantity to 18-decimal share-wei, rejecting negatives and
/// precision loss. Also the single conversion the recipient-authorization
/// signer (`crate::mint_authorization`) uses, so the signed `MintAuth`
/// amount can never diverge from the amount the mint itself computes.
pub(crate) fn quantity_to_u256_18_decimals(value: Float) -> Result<U256, QuantityScalingError> {
    if value.lt(Float::zero()?)? {
        return Err(QuantityScalingError::Negative { value });
    }

    Ok(value.to_fixed_decimal(TOKENIZED_EQUITY_DECIMALS)?)
}

#[async_trait]
impl EventSourced for TokenizedEquityMint {
    type Id = IssuerRequestId;
    type Event = TokenizedEquityMintEvent;
    type Command = TokenizedEquityMintCommand;
    type Error = TokenizedEquityMintError;
    type Services = EquityTransferServices;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "TokenizedEquityMint";
    const PROJECTION: Nil = Nil;
    // v3: removed the vestigial `receipt_id` field and added the
    // `ProviderCompletionRecovered` event for in-process failed-transfer recovery.
    // v4: added the terminal `Reconciled` state and the `OperatorReconciled`
    // event for operator reconciliation of stuck `Failed` mints. Additive only;
    // bumped to clear stale snapshots so they rebuild from events under the new
    // schema (existing events replay unchanged).
    // v5: added the `authorization` field on `MintAccepted` plus the
    // `MintAuthorizationSigned`/`MintAuthorizationDelivered` events for
    // orchestrator-mode recipient authorizations (RAI-1243). Additive only;
    // bumped to clear stale snapshots (existing events replay unchanged --
    // the field is `#[serde(default)]`).
    const SCHEMA_VERSION: u64 = 5;

    fn originate(event: &Self::Event) -> Option<Self> {
        use TokenizedEquityMintEvent::*;
        match event {
            MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => Some(Self::MintRequested {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                requested_at: *requested_at,
            }),
            _ => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        Ok(match event {
            MintRequested { .. } => None,
            WrappingFailed {
                symbol,
                quantity,
                reason,
                failed_at,
            } => match entity {
                Self::TokensReceived { requested_at, .. }
                | Self::WrapSubmitted { requested_at, .. } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason
                        .clone()
                        .unwrap_or_else(|| "ERC-4626 wrapping failed".to_string()),
                    requested_at: *requested_at,
                    failed_at: *failed_at,
                }),
                _ => None,
            },
            MintRejected {
                reason,
                rejected_at,
            } => {
                let Self::MintRequested {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };
                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason.clone(),
                    requested_at: *requested_at,
                    failed_at: *rejected_at,
                })
            }

            MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => {
                let Self::MintRequested {
                    symbol,
                    quantity,
                    wallet,
                    requested_at,
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::MintAccepted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    authorization: MintAuthorizationProgress::default(),
                })
            }

            MintAuthorizationSigned {
                nonce, signature, ..
            } => {
                let Self::MintAccepted {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    requested_at,
                    accepted_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::MintAccepted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    authorization: MintAuthorizationProgress::Signed(SignedMintAuthorization {
                        nonce: *nonce,
                        signature: signature.clone(),
                    }),
                })
            }

            MintAuthorizationDelivered { .. } => match entity {
                Self::MintAccepted {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    requested_at,
                    accepted_at,
                    authorization: MintAuthorizationProgress::Signed(signed),
                } => Some(Self::MintAccepted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    authorization: MintAuthorizationProgress::Delivered(signed.clone()),
                }),
                // The acknowledgement was recorded after the mint advanced
                // (the usual ordering -- `Poll` holds the aggregate for the
                // whole Alpaca wait): an audit marker only, no state change.
                Self::TokensReceived { .. }
                | Self::WrapSubmitted { .. }
                | Self::TokensWrapped { .. }
                | Self::VaultDepositSubmitted { .. }
                | Self::DepositedIntoRaindex { .. } => Some(entity.clone()),
                Self::MintRequested { .. }
                | Self::MintAccepted { .. }
                | Self::Failed { .. }
                | Self::Reconciled { .. } => return Ok(None),
            },

            MintAcceptanceFailed { reason, failed_at } => {
                // Emitted from `MintAccepted` (post-acceptance failure) or
                // `MintRequested` (operator force-fail before acceptance); both
                // carry the symbol/quantity/requested_at the `Failed` state needs.
                let (Self::MintAccepted {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                }
                | Self::MintRequested {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                }) = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason.clone(),
                    requested_at: *requested_at,
                    failed_at: *failed_at,
                })
            }

            TokensReceived {
                tx_hash,
                shares_minted,
                fees,
                received_at,
            } => {
                let Self::MintAccepted {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    requested_at,
                    accepted_at,
                    // The authorization served its purpose once tokens
                    // arrive; later states deliberately do not carry it.
                    authorization: _,
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::TokensReceived {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    shares_minted: *shares_minted,
                    fees: *fees,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                })
            }

            ProviderCompletionRecovered {
                issuer_request_id,
                wallet,
                tokenization_request_id,
                tx_hash,
                shares_minted,
                fees,
                recovered_at,
            } => {
                let Self::Failed {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::TokensReceived {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    shares_minted: *shares_minted,
                    fees: *fees,
                    requested_at: *requested_at,
                    accepted_at: *recovered_at,
                    received_at: *recovered_at,
                })
            }

            WrapSubmitted {
                wrap_tx_hash,
                submitted_at: _,
            } => {
                let Self::TokensReceived {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    shares_minted,
                    fees,
                    requested_at,
                    accepted_at,
                    received_at,
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::WrapSubmitted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    shares_minted: *shares_minted,
                    fees: *fees,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                    wrap_tx_hash: *wrap_tx_hash,
                })
            }

            TokensWrapped {
                wrap_tx_hash,
                wrapped_shares,
                wrapped_at,
                wrap_block,
            } => match entity {
                Self::TokensReceived {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    shares_minted,
                    requested_at,
                    accepted_at,
                    received_at,
                    ..
                }
                | Self::WrapSubmitted {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    shares_minted,
                    requested_at,
                    accepted_at,
                    received_at,
                    ..
                } => Some(Self::TokensWrapped {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    shares_minted: *shares_minted,
                    wrap_tx_hash: *wrap_tx_hash,
                    wrapped_shares: *wrapped_shares,
                    wrap_block: *wrap_block,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                    wrapped_at: *wrapped_at,
                }),
                _ => None,
            },

            VaultDepositSubmitted {
                vault_deposit_tx_hash,
                submitted_at: _,
            } => {
                let Self::TokensWrapped {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    shares_minted,
                    wrap_tx_hash,
                    wrapped_shares,
                    requested_at,
                    accepted_at,
                    received_at,
                    wrapped_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::VaultDepositSubmitted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    shares_minted: *shares_minted,
                    wrap_tx_hash: *wrap_tx_hash,
                    wrapped_shares: *wrapped_shares,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                    wrapped_at: *wrapped_at,
                    vault_deposit_tx_hash: *vault_deposit_tx_hash,
                })
            }

            DepositedIntoRaindex {
                vault_deposit_tx_hash,
                deposited_at,
            } => match entity {
                Self::TokensWrapped {
                    symbol,
                    quantity,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    wrap_tx_hash,
                    requested_at,
                    ..
                }
                | Self::VaultDepositSubmitted {
                    symbol,
                    quantity,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    wrap_tx_hash,
                    requested_at,
                    ..
                } => Some(Self::DepositedIntoRaindex {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    token_tx_hash: *tx_hash,
                    wrap_tx_hash: *wrap_tx_hash,
                    vault_deposit_tx_hash: *vault_deposit_tx_hash,
                    requested_at: *requested_at,
                    deposited_at: *deposited_at,
                }),
                _ => None,
            },

            RaindexDepositFailed { reason, failed_at } => match entity {
                Self::TokensWrapped {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                }
                | Self::VaultDepositSubmitted {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason.clone(),
                    requested_at: *requested_at,
                    failed_at: *failed_at,
                }),
                _ => None,
            },

            OperatorReconciled {
                reason,
                reconciled_at,
            } => {
                let Self::Failed {
                    symbol,
                    quantity,
                    reason: failure_reason,
                    requested_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Reconciled {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    failure_reason: failure_reason.clone(),
                    reconcile_reason: reason.clone(),
                    requested_at: *requested_at,
                    reconciled_at: *reconciled_at,
                })
            }
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;

        let (issuer_request_id, symbol, quantity, wallet, now) = match command {
            TokenizedEquityMintCommand::RequestMint {
                issuer_request_id,
                symbol,
                quantity,
                wallet,
            } => (issuer_request_id, symbol, quantity, wallet, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            TokenizedEquityMintCommand::RequestMintAt {
                issuer_request_id,
                symbol,
                quantity,
                wallet,
                requested_at,
            } => (issuer_request_id, symbol, quantity, wallet, requested_at),
            _ => return Err(TokenizedEquityMintError::NotInitialized),
        };

        if quantity.lt(Float::zero()?)? {
            return Err(TokenizedEquityMintError::NegativeQuantity { value: quantity });
        }

        info!(
            target: "tokenization",
            %symbol,
            ?quantity,
            %wallet,
            "Initiating mint request"
        );

        let mint_requested = MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at: now,
        };

        let alpaca_request = match services
            .tokenizer
            .request_mint(
                symbol,
                FractionalShares::new(quantity),
                wallet,
                issuer_request_id.clone(),
            )
            .await
        {
            Ok(req) => req,
            Err(error) => {
                return Err(TokenizedEquityMintError::RequestFailed {
                    error_message: error.to_string(),
                });
            }
        };

        if matches!(alpaca_request.status, TokenizationRequestStatus::Rejected) {
            warn!(
                target: "tokenization",
                symbol = %alpaca_request.underlying_symbol,
                "Mint request rejected by Alpaca"
            );
            return Ok(vec![
                mint_requested,
                MintRejected {
                    reason: "Rejected by Alpaca".to_string(),
                    rejected_at: now,
                },
            ]);
        }

        info!(
            target: "tokenization",
            symbol = %alpaca_request.underlying_symbol,
            request_id = %alpaca_request.id,
            "Mint request accepted by Alpaca"
        );

        Ok(vec![
            mint_requested,
            MintAccepted {
                issuer_request_id,
                tokenization_request_id: alpaca_request.id,
                accepted_at: now,
            },
        ])
    }

    #[allow(clippy::too_many_lines)]
    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        match command {
            TokenizedEquityMintCommand::RequestMint { .. } => {
                Err(TokenizedEquityMintError::AlreadyInProgress)
            }
            #[cfg(any(test, feature = "test-support"))]
            TokenizedEquityMintCommand::RequestMintAt { .. } => {
                Err(TokenizedEquityMintError::AlreadyInProgress)
            }

            TokenizedEquityMintCommand::Poll => self.transition_poll(services, None).await,

            #[cfg(any(test, feature = "test-support"))]
            TokenizedEquityMintCommand::PollAt { received_at } => {
                self.transition_poll(services, Some(received_at)).await
            }

            TokenizedEquityMintCommand::SignMintAuthorization { token } => {
                self.transition_sign_authorization(services, token).await
            }

            TokenizedEquityMintCommand::RecordMintAuthorizationDelivered => {
                self.transition_record_authorization_delivered()
            }

            TokenizedEquityMintCommand::SubmitWrap { wrap_tx_hash } => match self {
                Self::TokensReceived { .. } => Ok(vec![WrapSubmitted {
                    wrap_tx_hash,
                    submitted_at: Utc::now(),
                }]),
                Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Err(TokenizedEquityMintError::TokensNotReceivedForWrap)
                }
                Self::WrapSubmitted { .. }
                | Self::TokensWrapped { .. }
                | Self::VaultDepositSubmitted { .. }
                | Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
            },

            TokenizedEquityMintCommand::SubmitVaultDeposit {
                vault_deposit_tx_hash,
            } => match self {
                Self::TokensWrapped { .. } => Ok(vec![VaultDepositSubmitted {
                    vault_deposit_tx_hash,
                    submitted_at: Utc::now(),
                }]),
                Self::MintRequested { .. }
                | Self::MintAccepted { .. }
                | Self::TokensReceived { .. }
                | Self::WrapSubmitted { .. } => Err(TokenizedEquityMintError::TokensNotWrapped),
                Self::VaultDepositSubmitted { .. } | Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
            },

            TokenizedEquityMintCommand::WrapTokens {
                wrap_tx_hash,
                wrapped_shares,
                wrap_block,
            } => self.transition_wrap_tokens(wrap_tx_hash, wrapped_shares, wrap_block, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            TokenizedEquityMintCommand::WrapTokensAt {
                wrap_tx_hash,
                wrapped_shares,
                wrap_block,
                wrapped_at,
            } => self.transition_wrap_tokens(wrap_tx_hash, wrapped_shares, wrap_block, wrapped_at),

            TokenizedEquityMintCommand::DepositToVault {
                vault_deposit_tx_hash,
            } => self.transition_deposit_to_vault(vault_deposit_tx_hash, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            TokenizedEquityMintCommand::DepositToVaultAt {
                vault_deposit_tx_hash,
                deposited_at,
            } => self.transition_deposit_to_vault(vault_deposit_tx_hash, deposited_at),

            TokenizedEquityMintCommand::FailAcceptance { reason } => match self {
                // `MintRequested` is pre-acceptance: a mint stuck there (requested
                // at the provider but never accepted) is force-failed by the
                // operator. Acceptance never happened, so `MintAcceptanceFailed`
                // is the honest terminal -- the same event the post-acceptance
                // failure emits, broadened in `evolve` to also start from here.
                Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Ok(vec![MintAcceptanceFailed {
                        reason,
                        failed_at: Utc::now(),
                    }])
                }
                Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
                // Past acceptance and mid-pipeline: failing acceptance no longer
                // applies. Listed explicitly (not `_`) so a new state variant is
                // a compile error here rather than a silent AlreadyInProgress.
                Self::TokensReceived { .. }
                | Self::WrapSubmitted { .. }
                | Self::TokensWrapped { .. }
                | Self::VaultDepositSubmitted { .. } => {
                    Err(TokenizedEquityMintError::AlreadyInProgress)
                }
            },

            TokenizedEquityMintCommand::FailWrapping { reason } => match self {
                Self::TokensReceived {
                    symbol, quantity, ..
                }
                | Self::WrapSubmitted {
                    symbol, quantity, ..
                } => {
                    warn!(
                        target: "rebalance",
                        %symbol, %reason,
                        "Marking mint as wrapping-failed"
                    );
                    Ok(vec![WrappingFailed {
                        symbol: symbol.clone(),
                        quantity: *quantity,
                        reason: Some(reason),
                        failed_at: Utc::now(),
                    }])
                }
                Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::FailRaindexDeposit { reason } => match self {
                Self::TokensWrapped { .. } | Self::VaultDepositSubmitted { .. } => {
                    Ok(vec![RaindexDepositFailed {
                        reason,
                        failed_at: Utc::now(),
                    }])
                }
                Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::RecoverProviderCompletion {
                issuer_request_id,
                wallet,
                tokenization_request_id,
                tx_hash,
                fees,
            } => match self {
                Self::Failed { quantity, .. } => Ok(vec![ProviderCompletionRecovered {
                    issuer_request_id,
                    wallet,
                    tokenization_request_id,
                    tx_hash,
                    shares_minted: quantity_to_u256_18_decimals(*quantity)?,
                    fees,
                    recovered_at: Utc::now(),
                }]),
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
                Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::Reconcile { reason } => match self {
                Self::Failed { symbol, .. } => {
                    if reason.trim().is_empty() {
                        return Err(TokenizedEquityMintError::ReconcileReasonRequired);
                    }

                    warn!(
                        target: "rebalance",
                        %symbol, %reason,
                        "Reconciling stuck failed mint out-of-band"
                    );
                    Ok(vec![OperatorReconciled {
                        reason,
                        reconciled_at: Utc::now(),
                    }])
                }
                Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
                _ => Err(TokenizedEquityMintError::NotFailed),
            },
        }
    }
}

impl TokenizedEquityMint {
    /// Human-readable state label for logs: `Debug` on the aggregate dumps
    /// every field, and `std::mem::discriminant` prints an opaque token
    /// that does not identify the state.
    const fn state_name(&self) -> &'static str {
        match self {
            Self::MintRequested { .. } => "MintRequested",
            Self::MintAccepted { .. } => "MintAccepted",
            Self::TokensReceived { .. } => "TokensReceived",
            Self::WrapSubmitted { .. } => "WrapSubmitted",
            Self::TokensWrapped { .. } => "TokensWrapped",
            Self::VaultDepositSubmitted { .. } => "VaultDepositSubmitted",
            Self::DepositedIntoRaindex { .. } => "DepositedIntoRaindex",
            Self::Failed { .. } => "Failed",
            Self::Reconciled { .. } => "Reconciled",
        }
    }

    /// Signs the recipient authorization exactly once per mint: the nonce
    /// minted here is the mint's on-chain idempotency key, so a re-drive
    /// finding one already signed emits nothing rather than re-signing.
    async fn transition_sign_authorization(
        &self,
        services: &EquityTransferServices,
        token: Address,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        use TokenizedEquityMintEvent::*;
        match self {
            Self::MintAccepted {
                authorization: MintAuthorizationProgress::NotSigned,
                symbol,
                quantity,
                issuer_request_id,
                ..
            } => {
                let nonce = B256::random();
                let signed = services
                    .mint_authorizer
                    .sign(token, *quantity, nonce)
                    .await
                    .map_err(
                        |error| TokenizedEquityMintError::AuthorizationSigningFailed {
                            error_message: error.to_string(),
                        },
                    )?;

                info!(
                    target: "tokenization",
                    %issuer_request_id,
                    %symbol,
                    %token,
                    nonce = %signed.nonce,
                    "Signed mint recipient authorization"
                );

                Ok(vec![MintAuthorizationSigned {
                    nonce: signed.nonce,
                    signature: signed.signature,
                    signed_at: Utc::now(),
                }])
            }
            Self::MintAccepted {
                authorization:
                    MintAuthorizationProgress::Signed(_) | MintAuthorizationProgress::Delivered(_),
                issuer_request_id,
                ..
            } => {
                info!(
                    target: "tokenization",
                    %issuer_request_id,
                    "Mint authorization already signed; re-drive is a no-op"
                );
                Ok(vec![])
            }
            Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
            // Tokens already arrived: issuance has consumed the
            // authorization (or never needed one), so a late signing
            // attempt is a benign race, not an error the saga must handle.
            Self::TokensReceived { .. }
            | Self::WrapSubmitted { .. }
            | Self::TokensWrapped { .. }
            | Self::VaultDepositSubmitted { .. }
            | Self::DepositedIntoRaindex { .. } => {
                warn!(
                    target: "tokenization",
                    state = self.state_name(),
                    "Ignoring authorization signing for a mint past acceptance"
                );
                Ok(vec![])
            }
            Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
        }
    }

    /// Records issuance's delivery acknowledgement. The acknowledgement
    /// normally lands AFTER the mint has advanced past `MintAccepted`:
    /// `Poll` holds the per-aggregate lock for the whole Alpaca wait, and
    /// issuance only completes the mint once the authorization arrived, so
    /// this command queues behind `Poll` and runs against `TokensReceived`
    /// or later. The event is still recorded there -- the acknowledgement
    /// is a fact regardless of mint progress. Terminal states are no-ops;
    /// only delivery before signing is a hard error (the job loads the
    /// signed authorization from this aggregate, so its absence is a
    /// caller bug).
    fn transition_record_authorization_delivered(
        &self,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        use TokenizedEquityMintEvent::*;
        match self {
            Self::MintAccepted {
                authorization: MintAuthorizationProgress::Signed(_),
                issuer_request_id,
                ..
            } => {
                info!(
                    target: "tokenization",
                    %issuer_request_id,
                    "Mint authorization delivery acknowledged by issuance"
                );
                Ok(vec![MintAuthorizationDelivered {
                    delivered_at: Utc::now(),
                }])
            }
            Self::MintAccepted {
                authorization: MintAuthorizationProgress::Delivered(_),
                ..
            } => Ok(vec![]),
            Self::MintAccepted {
                authorization: MintAuthorizationProgress::NotSigned,
                ..
            } => Err(TokenizedEquityMintError::AuthorizationNotSigned),
            Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
            Self::TokensReceived {
                issuer_request_id, ..
            }
            | Self::WrapSubmitted {
                issuer_request_id, ..
            }
            | Self::TokensWrapped {
                issuer_request_id, ..
            }
            | Self::VaultDepositSubmitted {
                issuer_request_id, ..
            }
            | Self::DepositedIntoRaindex {
                issuer_request_id, ..
            } => {
                info!(
                    target: "tokenization",
                    %issuer_request_id,
                    "Mint authorization delivery acknowledged by issuance \
                     after the mint advanced"
                );
                Ok(vec![MintAuthorizationDelivered {
                    delivered_at: Utc::now(),
                }])
            }
            Self::Failed { .. } | Self::Reconciled { .. } => {
                warn!(
                    target: "tokenization",
                    state = self.state_name(),
                    "Ignoring authorization-delivery acknowledgement for a \
                     terminal mint"
                );
                Ok(vec![])
            }
        }
    }

    /// Shared body for `WrapTokens`/`WrapTokensAt`.
    fn transition_wrap_tokens(
        &self,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        wrap_block: u64,
        wrapped_at: DateTime<Utc>,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        use TokenizedEquityMintEvent::*;

        match self {
            Self::TokensReceived { .. } | Self::WrapSubmitted { .. } => Ok(vec![TokensWrapped {
                wrap_tx_hash,
                wrapped_shares,
                wrapped_at,
                wrap_block: Some(wrap_block),
            }]),
            Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                Err(TokenizedEquityMintError::TokensNotReceivedForWrap)
            }
            Self::TokensWrapped { .. }
            | Self::VaultDepositSubmitted { .. }
            | Self::DepositedIntoRaindex { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
            Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
        }
    }

    /// Shared body for `DepositToVault`/`DepositToVaultAt`.
    fn transition_deposit_to_vault(
        &self,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        use TokenizedEquityMintEvent::*;

        match self {
            Self::TokensWrapped { .. } | Self::VaultDepositSubmitted { .. } => {
                Ok(vec![DepositedIntoRaindex {
                    vault_deposit_tx_hash,
                    deposited_at,
                }])
            }
            Self::MintRequested { .. }
            | Self::MintAccepted { .. }
            | Self::TokensReceived { .. }
            | Self::WrapSubmitted { .. } => Err(TokenizedEquityMintError::TokensNotWrapped),
            Self::DepositedIntoRaindex { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
            Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
        }
    }

    /// Shared body for `Poll`/`PollAt`: only `received_at` (the success
    /// path's timestamp) is parameterized -- the failure paths keep
    /// `Utc::now()` since fixture seeding only ever drives the happy path.
    async fn transition_poll(
        &self,
        services: &EquityTransferServices,
        override_received_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        use TokenizedEquityMintEvent::*;

        match self {
            Self::MintAccepted {
                symbol,
                quantity,
                tokenization_request_id,
                ..
            } => {
                let completed = match services
                    .tokenizer
                    .poll_mint_until_complete(tokenization_request_id)
                    .await
                {
                    Ok(req) => req,
                    Err(error) => {
                        warn!(target: "tokenization", %error, %symbol, %tokenization_request_id, "Polling failed");
                        return Ok(vec![MintAcceptanceFailed {
                            reason: format!("Polling failed: {error}"),
                            failed_at: Utc::now(),
                        }]);
                    }
                };

                match completed.status {
                    TokenizationRequestStatus::Completed => {
                        let tx_hash = completed
                            .tx_hash
                            .ok_or(TokenizedEquityMintError::MissingTxHash)?;

                        // Validate that Alpaca returned the expected
                        // tokenized symbol (e.g. "tAAPL" for AAPL).
                        let expected_token_symbol = format!("t{symbol}");
                        match &completed.token_symbol {
                            Some(actual) if *actual == expected_token_symbol => {}
                            Some(actual) => {
                                return Err(TokenizedEquityMintError::TokenSymbolMismatch {
                                    expected_symbol: symbol.clone(),
                                    actual_token_symbol: actual.clone(),
                                });
                            }
                            None => {
                                return Err(TokenizedEquityMintError::MissingTokenSymbol {
                                    expected_symbol: symbol.clone(),
                                });
                            }
                        }

                        let shares_minted = quantity_to_u256_18_decimals(*quantity)?;

                        info!(
                            target: "tokenization",
                            %symbol,
                            %tx_hash,
                            "Mint polling completed: tokens received"
                        );

                        Ok(vec![TokensReceived {
                            tx_hash,
                            shares_minted,
                            fees: completed.fees,
                            received_at: override_received_at.unwrap_or_else(Utc::now),
                        }])
                    }
                    TokenizationRequestStatus::Rejected => {
                        warn!(
                            target: "tokenization",
                            %symbol,
                            "Mint rejected by Alpaca after acceptance"
                        );
                        Ok(vec![MintAcceptanceFailed {
                            reason: "Rejected by Alpaca after acceptance".to_string(),
                            failed_at: Utc::now(),
                        }])
                    }
                    TokenizationRequestStatus::Pending => {
                        warn!(
                            target: "tokenization",
                            %symbol,
                            "Unexpected pending status after polling"
                        );
                        Ok(vec![MintAcceptanceFailed {
                            reason: "Unexpected Pending status after polling".to_string(),
                            failed_at: Utc::now(),
                        }])
                    }
                }
            }
            Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
            Self::TokensReceived { .. }
            | Self::WrapSubmitted { .. }
            | Self::TokensWrapped { .. }
            | Self::VaultDepositSubmitted { .. }
            | Self::DepositedIntoRaindex { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
            Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            Self::Reconciled { .. } => Err(TokenizedEquityMintError::AlreadyReconciled),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};
    use st0x_float_macro::float;
    use st0x_raindex::RaindexVaultId;
    use st0x_tokenization::mock::{MockMintPollOutcome, MockMintRequestOutcome, MockTokenizer};
    use st0x_tokenization::{Tokenizer, issuer_request_id, tokenization_request_id};
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::mint_authorization::{ConfiguredMintAuthorizer, MockMintAuthorizer};
    use crate::onchain::mock::MockRaindex;
    use crate::vault_lookup::MockVaultLookup;

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO))
    }

    fn mint_services(tokenizer: MockTokenizer) -> EquityTransferServices {
        EquityTransferServices {
            tokenizer: Arc::new(tokenizer),
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            wrapper: Arc::new(MockWrapper::new()),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        }
    }

    /// Like [`mint_services`] but with a working (mock) mint authorizer, for
    /// tests exercising the orchestrator-mode signing path.
    fn mint_services_with_authorizer(tokenizer: MockTokenizer) -> EquityTransferServices {
        EquityTransferServices {
            mint_authorizer: ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer)),
            ..mint_services(tokenizer)
        }
    }

    fn mint_command() -> TokenizedEquityMintCommand {
        TokenizedEquityMintCommand::RequestMint {
            issuer_request_id: issuer_request_id("ISS001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(10),
            wallet: Address::ZERO,
        }
    }

    fn mint_requested_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        }
    }

    fn mint_accepted_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: tokenization_request_id("TOK456"),
            accepted_at: Utc::now(),
        }
    }

    fn tokens_received_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            fees: None,
            received_at: Utc::now(),
        }
    }

    fn tokens_wrapped_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(100_000_000_000_000_000_000_u128),
            wrapped_at: Utc::now(),
            wrap_block: None,
        }
    }

    fn vault_deposited_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        }
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, ?, '1.0', ?, '{}')",
        )
        .bind("TokenizedEquityMint")
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    fn mint_requested_payload(symbol: &str) -> String {
        format!(
            r#"{{"MintRequested":{{"symbol":"{symbol}","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}}}"#
        )
    }

    fn mint_accepted_payload(id: &IssuerRequestId) -> String {
        format!(
            r#"{{"MintAccepted":{{"issuer_request_id":"{id}","tokenization_request_id":"TOK001","accepted_at":"2026-01-01T00:00:00Z"}}}}"#
        )
    }

    #[tokio::test]
    async fn initialize_emits_requested_and_accepted() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { symbol, .. } if symbol == &Symbol::new("AAPL").unwrap()),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(
                &events[1],
                TokenizedEquityMintEvent::MintAccepted { issuer_request_id, .. }
                    if *issuer_request_id == st0x_tokenization::issuer_request_id("ISS001")
            ),
            "Expected MintAccepted with ISS001, got: {:?}",
            events[1]
        );
    }

    #[tokio::test]
    async fn poll_after_acceptance_emits_tokens_received() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));

        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensReceived { .. }),
            "Expected TokensReceived state, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_completed_with_fees_propagates_to_tokens_received() {
        let expected_fees = float!("0.25");
        let tokenizer = MockTokenizer::new().with_fees(expected_fees);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));

        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();

        match entity {
            TokenizedEquityMint::TokensReceived { fees, .. } => {
                let fees = fees.expect("fees should be Some when tokenizer returns fees");
                assert!(
                    fees.eq(expected_fees).unwrap(),
                    "Expected fees={expected_fees:?}, got: {fees:?}"
                );
            }
            other => panic!("Expected TokensReceived state, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_completed_with_wrong_token_symbol_errors() {
        let tokenizer = MockTokenizer::new().with_token_symbol_override("tGME");
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        let error = store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::TokenSymbolMismatch {
                        ref expected_symbol,
                        ref actual_token_symbol,
                    }
                )) if expected_symbol == &Symbol::new("AAPL").unwrap()
                    && actual_token_symbol == "tGME"
            ),
            "Expected TokenSymbolMismatch, got: {error:?}"
        );

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::MintAccepted { .. }),
            "Expected MintAccepted after validation error, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_completed_with_missing_token_symbol_errors() {
        let tokenizer = MockTokenizer::new().with_no_token_symbol();
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        let error = store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::MissingTokenSymbol {
                        ref expected_symbol,
                    }
                )) if expected_symbol == &Symbol::new("AAPL").unwrap()
            ),
            "Expected MissingTokenSymbol, got: {error:?}"
        );

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::MintAccepted { .. }),
            "Expected MintAccepted after validation error, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_rejected_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after rejected poll, got: {entity:?}"
        );
    }

    #[test]
    fn test_evolve_accepted_rejects_wrong_state() {
        let deposited = TokenizedEquityMint::DepositedIntoRaindex {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: tokenization_request_id("TOK456"),
            token_tx_hash: TxHash::random(),
            wrap_tx_hash: TxHash::random(),
            vault_deposit_tx_hash: TxHash::random(),
            requested_at: Utc::now(),
            deposited_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: issuer_request_id("ISS999"),
            tokenization_request_id: tokenization_request_id("TOK999"),
            accepted_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&deposited, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_tokens_received_rejects_wrong_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            fees: None,
            received_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejected_rejects_non_requested_states() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: tokenization_request_id("TOK456"),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
            authorization: crate::tokenized_equity_mint::MintAuthorizationProgress::default(),
        };

        let event = TokenizedEquityMintEvent::MintRejected {
            reason: "Should not apply".to_string(),
            rejected_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_acceptance_failed_rejects_inapplicable_states() {
        // MintAcceptanceFailed applies from MintRequested (operator force-fail)
        // and MintAccepted (post-acceptance failure); it must NOT apply to a
        // state past acceptance -- a mid-pipeline TokensReceived or a terminal
        // Failed mint.
        let already_failed = TokenizedEquityMint::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            reason: "previous failure".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };
        // Mid-pipeline (past acceptance) and a terminal failure: the two classes
        // of state that must reject the event. TokensReceived stands in for the
        // mid-pipeline states (WrapSubmitted/TokensWrapped/VaultDepositSubmitted),
        // which the production `evolve` treats identically (anything that is not
        // MintRequested/MintAccepted maps to None).
        let tokens_received = replay::<TokenizedEquityMint>(vec![
            mint_requested_event(),
            mint_accepted_event(),
            tokens_received_event(),
        ])
        .unwrap()
        .unwrap();

        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        for state in [already_failed, tokens_received] {
            assert_eq!(
                TokenizedEquityMint::evolve(&state, &event).unwrap(),
                None,
                "MintAcceptanceFailed must not apply to {state:?}"
            );
        }
    }

    #[test]
    fn test_evolve_rejects_mint_requested_on_existing_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: float!(50),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_vault_deposited_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: tokenization_request_id("TOK456"),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
            authorization: crate::tokenized_equity_mint::MintAuthorizationProgress::default(),
        };

        let event = TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn quantity_to_u256_18_decimals_rejects_negative() {
        let error = quantity_to_u256_18_decimals(float!(-5)).unwrap_err();
        assert!(
            matches!(error, QuantityScalingError::Negative { .. }),
            "Expected Negative, got: {error:?}"
        );
    }

    #[test]
    fn quantity_to_u256_18_decimals_converts_correctly() {
        let result = quantity_to_u256_18_decimals(float!(3)).unwrap();
        assert_eq!(result, U256::from(3_000_000_000_000_000_000_u128));
    }

    #[test]
    fn quantity_to_u256_18_decimals_zero_returns_zero() {
        let result = quantity_to_u256_18_decimals(float!(0)).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn quantity_to_u256_18_decimals_accepts_exactly_18_decimal_places() {
        let value = Float::parse("1.123456789012345678".to_string()).unwrap();
        let result = quantity_to_u256_18_decimals(value).unwrap();
        assert_eq!(result, U256::from(1_123_456_789_012_345_678_u128));
    }

    #[test]
    fn test_evolve_tokens_wrapped_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(100.5),
            wallet: Address::random(),
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: tokenization_request_id("TOK456"),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
            authorization: crate::tokenized_equity_mint::MintAuthorizationProgress::default(),
        };

        let event = TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(100_000_000_000_000_000_000_u128),
            wrapped_at: Utc::now(),
            wrap_block: None,
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn request_mint_rejected_by_alpaca_emits_rejected() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::Rejected);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after rejection, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn request_mint_api_error_returns_error() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::ApiError);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        let error = store.send(&id, mint_command()).await.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::RequestFailed { .. }
                ))
            ),
            "Expected RequestFailed, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_pending_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Pending);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after pending poll, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_error_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::PollError);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after poll error, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn request_mint_passes_issuer_request_id_to_tokenizer() {
        let tokenizer: Arc<MockTokenizer> = Arc::new(MockTokenizer::new());
        let store = TestStore::<TokenizedEquityMint>::new(EquityTransferServices {
            tokenizer: Arc::clone(&tokenizer) as Arc<dyn Tokenizer>,
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            wrapper: Arc::new(MockWrapper::new()),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        });
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();

        let recorded_id = tokenizer.last_issuer_request_id().unwrap();
        assert_eq!(recorded_id, issuer_request_id("ISS001"));
    }

    #[test]
    fn reject_mint_evolves_from_requested_to_failed() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(10),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRejected {
            reason: "Insufficient balance".to_string(),
            rejected_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event)
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, TokenizedEquityMint::Failed { ref reason, .. } if reason == "Insufficient balance"),
            "Expected Failed with reason, got: {result:?}"
        );
    }

    #[test]
    fn wrapping_failed_evolves_from_tokens_received_to_failed() {
        let tokens_received = TokenizedEquityMint::TokensReceived {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(10),
            wallet: Address::random(),
            issuer_request_id: issuer_request_id("ISS001"),
            tokenization_request_id: tokenization_request_id("REQ001"),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(10_000_000_000_000_000_000_u128),
            fees: None,
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
            received_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::WrappingFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(10),
            reason: None,
            failed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&tokens_received, &event)
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after wrapping failure, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn wrap_tokens_is_pure_state_transition() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::random(),
                    wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn deposit_to_vault_is_pure_state_transition() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::random(),
                    wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected DepositedIntoRaindex, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_at_uses_supplied_timestamp() {
        let received_at = Utc::now() - chrono::Duration::hours(2);

        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![mint_requested_event(), mint_accepted_event()])
            .when(TokenizedEquityMintCommand::PollAt { received_at })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let TokenizedEquityMintEvent::TokensReceived {
            received_at: event_received_at,
            ..
        } = &events[0]
        else {
            panic!("Expected TokensReceived, got: {:?}", events[0]);
        };
        assert_eq!(*event_received_at, received_at);
    }

    #[tokio::test]
    async fn wrap_tokens_at_uses_supplied_timestamp() {
        let wrapped_at = Utc::now() - chrono::Duration::hours(3);

        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![
                mint_requested_event(),
                mint_accepted_event(),
                tokens_received_event(),
            ])
            .when(TokenizedEquityMintCommand::WrapTokensAt {
                wrap_tx_hash: TxHash::random(),
                wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
                wrap_block: 1,
                wrapped_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let TokenizedEquityMintEvent::TokensWrapped {
            wrapped_at: event_wrapped_at,
            ..
        } = &events[0]
        else {
            panic!("Expected TokensWrapped, got: {:?}", events[0]);
        };
        assert_eq!(*event_wrapped_at, wrapped_at);
    }

    #[tokio::test]
    async fn deposit_to_vault_at_uses_supplied_timestamp() {
        let deposited_at = Utc::now() - chrono::Duration::hours(4);

        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![
                mint_requested_event(),
                mint_accepted_event(),
                tokens_received_event(),
                tokens_wrapped_event(),
            ])
            .when(TokenizedEquityMintCommand::DepositToVaultAt {
                vault_deposit_tx_hash: TxHash::random(),
                deposited_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let TokenizedEquityMintEvent::DepositedIntoRaindex {
            deposited_at: event_deposited_at,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositedIntoRaindex, got: {:?}", events[0]);
        };
        assert_eq!(*event_deposited_at, deposited_at);
    }

    #[tokio::test]
    async fn request_mint_at_uses_supplied_timestamp() {
        let requested_at = Utc::now() - chrono::Duration::hours(5);

        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(TokenizedEquityMintCommand::RequestMintAt {
                issuer_request_id: issuer_request_id("ISS001"),
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(10),
                wallet: Address::ZERO,
                requested_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 2);
        let TokenizedEquityMintEvent::MintRequested {
            requested_at: event_requested_at,
            ..
        } = &events[0]
        else {
            panic!("Expected MintRequested, got: {:?}", events[0]);
        };
        assert_eq!(*event_requested_at, requested_at);

        let TokenizedEquityMintEvent::MintAccepted {
            accepted_at: event_accepted_at,
            ..
        } = &events[1]
        else {
            panic!("Expected MintAccepted, got: {:?}", events[1]);
        };
        assert_eq!(*event_accepted_at, requested_at);
    }

    #[tokio::test]
    async fn interrupted_mint_ids_returns_only_non_terminal_mints() {
        let pool = crate::test_utils::setup_test_db().await;

        let mint_accepted_id = issuer_request_id("mint-accepted");
        let mint_deposited_id = issuer_request_id("mint-deposited");

        insert_event(
            &pool,
            &mint_accepted_id.to_string(),
            0,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &mint_accepted_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&mint_accepted_id),
        )
        .await;

        insert_event(
            &pool,
            &mint_deposited_id.to_string(),
            0,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            &mint_deposited_id.to_string(),
            1,
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
            r#"{"DepositedIntoRaindex":{"vault_deposit_tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","deposited_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = interrupted_mint_ids(&pool).await.unwrap();
        assert_eq!(result, vec![mint_accepted_id]);
    }

    /// Raw persisted payload for `MintAuthorizationSigned`, pinning the hex
    /// wire format events must keep forever (65-byte `0x42` signature).
    fn mint_authorization_signed_payload() -> String {
        format!(
            r#"{{"MintAuthorizationSigned":{{"nonce":"0x{}","signature":"0x{}","signed_at":"2026-01-01T00:00:00Z"}}}}"#,
            "07".repeat(32),
            "42".repeat(65),
        )
    }

    /// The acknowledgement normally trails the whole mint, so a completed
    /// stream ends `DepositedIntoRaindex` -> `MintAuthorizationDelivered`.
    /// Latest-event membership alone would resume that mint on every boot
    /// forever; the deposit anywhere in the stream must exclude it. The
    /// inverse case -- `Delivered` latest with NO deposit (the ack landed
    /// while still `MintAccepted`) -- is a genuine interruption and must
    /// stay included.
    #[tokio::test]
    async fn interrupted_mint_ids_excludes_completed_mint_with_trailing_ack() {
        let pool = crate::test_utils::setup_test_db().await;

        let completed_id = issuer_request_id("mint-completed-trailing-ack");
        for (sequence, event_type, payload) in [
            (
                0,
                "TokenizedEquityMintEvent::MintRequested",
                mint_requested_payload("RKLB"),
            ),
            (
                1,
                "TokenizedEquityMintEvent::MintAccepted",
                mint_accepted_payload(&completed_id),
            ),
            (
                2,
                "TokenizedEquityMintEvent::MintAuthorizationSigned",
                mint_authorization_signed_payload(),
            ),
            (
                3,
                "TokenizedEquityMintEvent::DepositedIntoRaindex",
                r#"{"DepositedIntoRaindex":{"vault_deposit_tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","deposited_at":"2026-01-01T00:00:00Z"}}"#.to_string(),
            ),
            (
                4,
                "TokenizedEquityMintEvent::MintAuthorizationDelivered",
                r#"{"MintAuthorizationDelivered":{"delivered_at":"2026-01-01T00:00:01Z"}}"#
                    .to_string(),
            ),
        ] {
            insert_event(
                &pool,
                &completed_id.to_string(),
                sequence,
                event_type,
                &payload,
            )
            .await;
        }

        let interrupted_id = issuer_request_id("mint-acked-while-accepted");
        for (sequence, event_type, payload) in [
            (
                0,
                "TokenizedEquityMintEvent::MintRequested",
                mint_requested_payload("RKLB"),
            ),
            (
                1,
                "TokenizedEquityMintEvent::MintAccepted",
                mint_accepted_payload(&interrupted_id),
            ),
            (
                2,
                "TokenizedEquityMintEvent::MintAuthorizationSigned",
                mint_authorization_signed_payload(),
            ),
            (
                3,
                "TokenizedEquityMintEvent::MintAuthorizationDelivered",
                r#"{"MintAuthorizationDelivered":{"delivered_at":"2026-01-01T00:00:01Z"}}"#
                    .to_string(),
            ),
        ] {
            insert_event(
                &pool,
                &interrupted_id.to_string(),
                sequence,
                event_type,
                &payload,
            )
            .await;
        }

        let result = interrupted_mint_ids(&pool).await.unwrap();
        assert_eq!(
            result,
            vec![interrupted_id],
            "the completed mint's trailing ack must not make it interrupted, \
             while the still-accepted acked mint must stay resumable"
        );
    }

    /// A mint whose LATEST event is `MintAuthorizationSigned` is still
    /// mid-flight (state `MintAccepted`): startup recovery must resume it,
    /// or an orchestrator-mode mint interrupted between signing and
    /// delivery would strand until manual intervention.
    #[tokio::test]
    async fn interrupted_mint_ids_includes_mint_awaiting_authorization_delivery() {
        let pool = crate::test_utils::setup_test_db().await;

        let signed_id = issuer_request_id("mint-auth-signed");
        insert_event(
            &pool,
            &signed_id.to_string(),
            0,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("RKLB"),
        )
        .await;
        insert_event(
            &pool,
            &signed_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&signed_id),
        )
        .await;
        insert_event(
            &pool,
            &signed_id.to_string(),
            2,
            "TokenizedEquityMintEvent::MintAuthorizationSigned",
            &mint_authorization_signed_payload(),
        )
        .await;

        let result = interrupted_mint_ids(&pool).await.unwrap();
        assert_eq!(result, vec![signed_id]);
    }

    /// Replays a raw persisted stream through the store: the pinned hex
    /// payload must materialize as `Signed` progress carrying the exact
    /// nonce and signature bytes -- this is the read-side half of the
    /// permanent wire-format contract.
    #[tokio::test]
    async fn persisted_authorization_stream_replays_to_signed_progress() {
        let pool = crate::test_utils::setup_test_db().await;
        let id = issuer_request_id("mint-auth-replay");

        insert_event(
            &pool,
            &id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("RKLB"),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            3,
            "TokenizedEquityMintEvent::MintAuthorizationSigned",
            &mint_authorization_signed_payload(),
        )
        .await;

        let store = st0x_event_sorcery::test_store(
            pool,
            mint_services_with_authorizer(MockTokenizer::new()),
        );
        let entity = store.load(&id).await.unwrap().unwrap();

        let TokenizedEquityMint::MintAccepted {
            authorization: MintAuthorizationProgress::Signed(signed),
            ..
        } = entity
        else {
            panic!("expected Signed authorization after replay, got: {entity:?}");
        };
        assert_eq!(signed.nonce, B256::repeat_byte(0x07));
        assert_eq!(
            signed.signature,
            alloy::primitives::Bytes::from(vec![0x42; 65])
        );
    }

    /// A pre-authorization event stream (no authorization events, as every
    /// stream persisted before this feature) must still replay -- landing on
    /// `NotSigned`, the truthful default for a vault-direct mint.
    #[tokio::test]
    async fn pre_authorization_stream_replays_with_not_signed_default() {
        let pool = crate::test_utils::setup_test_db().await;
        let id = issuer_request_id("mint-pre-auth");

        insert_event(
            &pool,
            &id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;

        let store = st0x_event_sorcery::test_store(pool, mint_services(MockTokenizer::new()));
        let entity = store.load(&id).await.unwrap().unwrap();

        assert!(
            matches!(
                entity,
                TokenizedEquityMint::MintAccepted {
                    authorization: MintAuthorizationProgress::NotSigned,
                    ..
                }
            ),
            "expected NotSigned default after legacy replay, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn sign_authorization_persists_signed_progress() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services_with_authorizer(
            MockTokenizer::new(),
        ));
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::MintAccepted {
            authorization: MintAuthorizationProgress::Signed(signed),
            ..
        } = entity
        else {
            panic!("expected Signed authorization, got: {entity:?}");
        };
        assert_eq!(
            signed.signature,
            alloy::primitives::Bytes::from(vec![0x42; 65])
        );
        assert_ne!(signed.nonce, B256::ZERO);
    }

    /// THE nonce-fixity guarantee: a re-driven signing command must emit
    /// nothing and keep the original nonce -- the nonce is the mint's
    /// on-chain idempotency key, and a fresh one on retry could double-mint.
    #[tokio::test]
    async fn re_signing_keeps_the_original_nonce() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services_with_authorizer(
            MockTokenizer::new(),
        ));
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();

        let sign_command = TokenizedEquityMintCommand::SignMintAuthorization {
            token: Address::repeat_byte(0x11),
        };
        store.send(&id, sign_command.clone()).await.unwrap();
        let first = store.load(&id).await.unwrap().unwrap();

        store.send(&id, sign_command).await.unwrap();
        let second = store.load(&id).await.unwrap().unwrap();

        let (
            TokenizedEquityMint::MintAccepted {
                authorization: MintAuthorizationProgress::Signed(first_signed),
                ..
            },
            TokenizedEquityMint::MintAccepted {
                authorization: MintAuthorizationProgress::Signed(second_signed),
                ..
            },
        ) = (first, second)
        else {
            panic!("expected both loads to be Signed");
        };
        assert_eq!(first_signed.nonce, second_signed.nonce);
        assert_eq!(first_signed.signature, second_signed.signature);
    }

    /// Each mint owns its own random nonce: two aggregates must never share
    /// one (a replacement mint gets a fresh nonce by construction).
    #[tokio::test]
    async fn distinct_mints_generate_independent_nonces() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services_with_authorizer(
            MockTokenizer::new(),
        ));
        let sign_command = TokenizedEquityMintCommand::SignMintAuthorization {
            token: Address::repeat_byte(0x11),
        };

        let mut nonces = Vec::new();
        for id_str in ["ISS-NONCE-A", "ISS-NONCE-B"] {
            let id = issuer_request_id(id_str);
            store
                .send(
                    &id,
                    TokenizedEquityMintCommand::RequestMint {
                        issuer_request_id: id.clone(),
                        symbol: Symbol::new("AAPL").unwrap(),
                        quantity: float!(10),
                        wallet: Address::ZERO,
                    },
                )
                .await
                .unwrap();
            store.send(&id, sign_command.clone()).await.unwrap();

            let entity = store.load(&id).await.unwrap().unwrap();
            let TokenizedEquityMint::MintAccepted {
                authorization: MintAuthorizationProgress::Signed(signed),
                ..
            } = entity
            else {
                panic!("expected Signed authorization, got: {entity:?}");
            };
            nonces.push(signed.nonce);
        }

        assert_ne!(nonces[0], nonces[1]);
    }

    /// An orchestrator-mode mint reaching signing with no `[orchestrator]`
    /// config must fail loudly, never guess an address or sign nothing.
    #[tokio::test]
    async fn signing_with_disabled_authorizer_fails_loudly() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();

        let error = store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::AuthorizationSigningFailed { .. }
                ))
            ),
            "expected AuthorizationSigningFailed, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn recording_delivery_transitions_to_delivered_and_is_idempotent() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services_with_authorizer(
            MockTokenizer::new(),
        ));
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
            )
            .await
            .unwrap();
        // Idempotent: the at-least-once delivery job may acknowledge twice.
        store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                entity,
                TokenizedEquityMint::MintAccepted {
                    authorization: MintAuthorizationProgress::Delivered(_),
                    ..
                }
            ),
            "expected Delivered authorization, got: {entity:?}"
        );
    }

    /// The usual production ordering: `Poll` holds the aggregate for the
    /// whole Alpaca wait and issuance completes the mint only after the
    /// authorization arrived, so the acknowledgement lands after the mint
    /// advanced past `MintAccepted`. It must still be recorded, and the
    /// aggregate must replay through the trailing event.
    #[tokio::test]
    async fn recording_delivery_after_mint_advanced_still_records() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = st0x_event_sorcery::test_store(
            pool.clone(),
            mint_services_with_authorizer(MockTokenizer::new()),
        );
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
            )
            .await
            .unwrap();

        // The audit fact must actually persist: an advanced-state
        // acknowledgement that quietly emitted no event would leave the
        // state assertion below passing while recording nothing.
        let delivered_events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("TokenizedEquityMintEvent::MintAuthorizationDelivered")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            delivered_events, 1,
            "the trailing acknowledgement must persist its audit event"
        );

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensReceived { .. }),
            "state must stay TokensReceived after the trailing \
             acknowledgement, got: {entity:?}"
        );
    }

    /// Replays a raw persisted stream where the acknowledgement landed
    /// after `TokensReceived` (the usual production ordering): the trailing
    /// `MintAuthorizationDelivered` must apply as a no-op, not fail the
    /// lifecycle as an unexpected event.
    #[tokio::test]
    async fn persisted_stream_with_delivery_after_tokens_received_replays() {
        let pool = crate::test_utils::setup_test_db().await;
        let id = issuer_request_id("mint-auth-late-ack");

        insert_event(
            &pool,
            &id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            &mint_requested_payload("RKLB"),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            3,
            "TokenizedEquityMintEvent::MintAuthorizationSigned",
            &mint_authorization_signed_payload(),
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            4,
            "TokenizedEquityMintEvent::TokensReceived",
            r#"{"TokensReceived":{"tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000001","shares_minted":"0xde0b6b3a7640000","fees":null,"received_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            &id.to_string(),
            5,
            "TokenizedEquityMintEvent::MintAuthorizationDelivered",
            r#"{"MintAuthorizationDelivered":{"delivered_at":"2026-01-01T00:00:01Z"}}"#,
        )
        .await;

        let store = st0x_event_sorcery::test_store(
            pool,
            mint_services_with_authorizer(MockTokenizer::new()),
        );
        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensReceived { .. }),
            "expected TokensReceived after replay with a trailing \
             acknowledgement, got: {entity:?}"
        );
    }

    /// The delivery job only fires after `MintAuthorizationSigned` persists,
    /// so an acknowledgement with nothing signed is a caller bug.
    #[tokio::test]
    async fn recording_delivery_without_signed_authorization_errors() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services_with_authorizer(
            MockTokenizer::new(),
        ));
        let id = issuer_request_id("ISS001");
        store.send(&id, mint_command()).await.unwrap();

        let error = store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::AuthorizationNotSigned
                ))
            ),
            "expected AuthorizationNotSigned, got: {error:?}"
        );
    }

    /// The on-disk shape of the new events is permanent -- pin it against
    /// literal JSON (hex-encoded nonce/signature), not just a round-trip.
    #[test]
    fn authorization_events_pin_hex_wire_format() {
        let signed = TokenizedEquityMintEvent::MintAuthorizationSigned {
            nonce: B256::repeat_byte(0x07),
            signature: alloy::primitives::Bytes::from(vec![0xab, 0xcd]),
            signed_at: "2026-01-02T03:04:05Z".parse().unwrap(),
        };
        assert_eq!(
            serde_json::to_value(&signed).unwrap(),
            serde_json::json!({
                "MintAuthorizationSigned": {
                    "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
                    "signature": "0xabcd",
                    "signed_at": "2026-01-02T03:04:05Z"
                }
            })
        );

        let delivered = TokenizedEquityMintEvent::MintAuthorizationDelivered {
            delivered_at: "2026-01-02T03:04:05Z".parse().unwrap(),
        };
        assert_eq!(
            serde_json::to_value(&delivered).unwrap(),
            serde_json::json!({
                "MintAuthorizationDelivered": {
                    "delivered_at": "2026-01-02T03:04:05Z"
                }
            })
        );
    }

    #[test]
    fn evolve_full_happy_path_with_event_helpers() {
        let mut state: Option<TokenizedEquityMint> = None;

        let events = [
            mint_requested_event(),
            mint_accepted_event(),
            tokens_received_event(),
            tokens_wrapped_event(),
            vault_deposited_event(),
        ];

        for event in &events {
            state = state.as_ref().map_or_else(
                || TokenizedEquityMint::originate(event),
                |current| TokenizedEquityMint::evolve(current, event).unwrap(),
            );
        }

        assert!(
            matches!(
                state,
                Some(TokenizedEquityMint::DepositedIntoRaindex { .. })
            ),
            "Expected DepositedIntoRaindex after full lifecycle, got: {state:?}"
        );
    }

    #[test]
    fn to_dto_maps_in_progress_variants() {
        let id = issuer_request_id("MINT-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);

        let requested = TokenizedEquityMint::MintRequested {
            symbol: symbol.clone(),
            quantity: float!(10),
            wallet: Address::ZERO,
            requested_at: now,
        };
        let dto = requested.to_dto(&id);
        let TransferOperation::EquityMint(op) = dto else {
            panic!("Expected EquityMint, got: {dto:?}");
        };
        assert_eq!(op.id, Id::new(id.to_string()));
        assert_eq!(op.symbol, symbol);
        assert_eq!(op.quantity, FractionalShares::new(float!(10)));
        assert!(
            matches!(op.status, EquityMintStatus::Minting),
            "Expected Minting, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, now);

        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: symbol.clone(),
            quantity: float!(10),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("ISS001"),
            tokenization_request_id: tokenization_request_id("TOK001"),
            requested_at: now,
            accepted_at: later,
            authorization: crate::tokenized_equity_mint::MintAuthorizationProgress::default(),
        };
        let TransferOperation::EquityMint(op) = accepted.to_dto(&id) else {
            panic!("Expected EquityMint");
        };
        assert!(
            matches!(op.status, EquityMintStatus::Minting),
            "Expected Minting, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);

        let received = TokenizedEquityMint::TokensReceived {
            symbol: symbol.clone(),
            quantity: float!(10),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("ISS001"),
            tokenization_request_id: tokenization_request_id("TOK001"),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(10_000_000_000_000_000_000_u128),
            fees: None,
            requested_at: now,
            accepted_at: now,
            received_at: later,
        };
        let TransferOperation::EquityMint(op) = received.to_dto(&id) else {
            panic!("Expected EquityMint");
        };
        assert!(
            matches!(op.status, EquityMintStatus::Wrapping),
            "Expected Wrapping, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);

        let wrapped = TokenizedEquityMint::TokensWrapped {
            symbol,
            quantity: float!(10),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("ISS001"),
            tokenization_request_id: tokenization_request_id("TOK001"),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(10_000_000_000_000_000_000_u128),
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
            wrap_block: None,
            requested_at: now,
            accepted_at: now,
            received_at: now,
            wrapped_at: later,
        };
        let TransferOperation::EquityMint(op) = wrapped.to_dto(&id) else {
            panic!("Expected EquityMint");
        };
        assert!(
            matches!(op.status, EquityMintStatus::Depositing),
            "Expected Depositing, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);
    }

    #[test]
    fn to_dto_maps_terminal_variants() {
        let id = issuer_request_id("MINT-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);

        let deposited = TokenizedEquityMint::DepositedIntoRaindex {
            symbol: symbol.clone(),
            quantity: float!(10),
            issuer_request_id: issuer_request_id("ISS001"),
            tokenization_request_id: tokenization_request_id("TOK001"),
            token_tx_hash: TxHash::random(),
            wrap_tx_hash: TxHash::random(),
            vault_deposit_tx_hash: TxHash::random(),
            requested_at: now,
            deposited_at: later,
        };
        let TransferOperation::EquityMint(op) = deposited.to_dto(&id) else {
            panic!("Expected EquityMint");
        };
        match op.status {
            EquityMintStatus::Completed { completed_at } => {
                assert_eq!(completed_at, later);
            }
            other => panic!("Expected Completed, got: {other:?}"),
        }
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);

        let failed = TokenizedEquityMint::Failed {
            symbol,
            quantity: float!(10),
            reason: "Something went wrong".to_string(),
            requested_at: now,
            failed_at: later,
        };
        let TransferOperation::EquityMint(op) = failed.to_dto(&id) else {
            panic!("Expected EquityMint");
        };
        match op.status {
            EquityMintStatus::Failed { failed_at } => {
                assert_eq!(failed_at, later);
            }
            other => panic!("Expected Failed, got: {other:?}"),
        }
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);
    }

    #[test]
    fn provider_completion_recovery_reopens_failed_mint_as_tokens_received() {
        let symbol = Symbol::new("AAPL").unwrap();
        let requested_at = Utc::now();
        let recovered_at = requested_at + chrono::Duration::seconds(60);
        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "poll timeout".to_string(),
            requested_at,
            failed_at: requested_at + chrono::Duration::seconds(30),
        };

        let recovered = TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("ISS001"),
            wallet: Address::ZERO,
            tokenization_request_id: tokenization_request_id("TOK001"),
            tx_hash: TxHash::ZERO,
            shares_minted: U256::from(10) * U256::from(10).pow(U256::from(18)),
            fees: None,
            recovered_at,
        };

        let result = TokenizedEquityMint::evolve(&failed, &recovered)
            .unwrap()
            .expect("recovered state");

        assert!(
            matches!(
                result,
                TokenizedEquityMint::TokensReceived {
                    symbol: ref recovered_symbol,
                    ref tokenization_request_id,
                    ..
                } if *recovered_symbol == symbol
                    && *tokenization_request_id == st0x_tokenization::tokenization_request_id("TOK001")
            ),
            "expected recovered mint to move to TokensReceived, got {result:?}"
        );
    }

    #[test]
    fn provider_completion_recovered_fees_serialize_as_decimal_string() {
        use st0x_float_macro::float;

        // Regression: without the custom Float serde annotation, `fees` would
        // serialize via Float's default representation -- diverging from
        // `TokensReceived.fees` and breaking event replay from the events
        // table. Assert the decimal-string form (independent of the type under
        // test) and a clean round-trip.
        let event = TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("ISS001"),
            wallet: Address::ZERO,
            tokenization_request_id: tokenization_request_id("TOK001"),
            tx_hash: TxHash::ZERO,
            shares_minted: U256::from(10),
            fees: Some(float!("0.25")),
            recovered_at: Utc::now(),
        };

        let serialized = serde_json::to_value(&event).unwrap();
        assert_eq!(
            serialized["ProviderCompletionRecovered"]["fees"],
            serde_json::json!("0.25")
        );

        let deserialized: TokenizedEquityMintEvent = serde_json::from_value(serialized).unwrap();
        let TokenizedEquityMintEvent::ProviderCompletionRecovered { fees, .. } = deserialized
        else {
            panic!("expected ProviderCompletionRecovered, got {deserialized:?}");
        };
        assert!(fees.unwrap().eq(float!("0.25")).unwrap());
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_active_mint() {
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![mint_requested_event()])
            .when(TokenizedEquityMintCommand::RecoverProviderCompletion {
                issuer_request_id: issuer_request_id("ISS001"),
                wallet: Address::ZERO,
                tokenization_request_id: tokenization_request_id("TOK001"),
                tx_hash: TxHash::ZERO,
                fees: None,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::AlreadyInProgress)
            ),
            "active mints must not be provider-completion recovered, got {error:?}"
        );
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_completed_mint() {
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![
                mint_requested_event(),
                mint_accepted_event(),
                tokens_received_event(),
                tokens_wrapped_event(),
                vault_deposited_event(),
            ])
            .when(TokenizedEquityMintCommand::RecoverProviderCompletion {
                issuer_request_id: issuer_request_id("ISS001"),
                wallet: Address::ZERO,
                tokenization_request_id: tokenization_request_id("TOK001"),
                tx_hash: TxHash::ZERO,
                fees: None,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::AlreadyCompleted)
            ),
            "completed mints must not be recovered, got {error:?}"
        );
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_reconciled_mint() {
        let mut history = failed_mint_history();
        history.push(TokenizedEquityMintEvent::OperatorReconciled {
            reason: "credited offline".to_string(),
            reconciled_at: Utc::now(),
        });

        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(history)
            .when(TokenizedEquityMintCommand::RecoverProviderCompletion {
                issuer_request_id: issuer_request_id("ISS001"),
                wallet: Address::ZERO,
                tokenization_request_id: tokenization_request_id("TOK001"),
                tx_hash: TxHash::ZERO,
                fees: None,
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::AlreadyReconciled)
            ),
            "a reconciled mint must report AlreadyReconciled, not recover, got {error:?}"
        );
    }

    #[tokio::test]
    async fn fail_acceptance_from_accepted_transitions_to_failed() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "Timed out".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fail_acceptance_from_requested_force_fails_to_failed() {
        // A mint stuck at MintRequested (requested but never accepted) can be
        // operator-force-failed via FailAcceptance, emitting MintAcceptanceFailed
        // and terminating in Failed.
        let history = vec![mint_requested_event()];

        let reason = "stuck pre-acceptance; operator force-fail";
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(history.clone())
            .when(TokenizedEquityMintCommand::FailAcceptance {
                reason: reason.to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: event_reason,
            ..
        } = &events[0]
        else {
            panic!("expected MintAcceptanceFailed, got {:?}", events[0]);
        };
        assert_eq!(event_reason, reason);

        let state = replay::<TokenizedEquityMint>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let TokenizedEquityMint::Failed {
            reason: state_reason,
            ..
        } = state
        else {
            panic!("force-fail from MintRequested must terminate in Failed, got {state:?}");
        };
        assert_eq!(
            state_reason, reason,
            "the operator reason must survive into the Failed terminal state"
        );
    }

    #[tokio::test]
    async fn fail_acceptance_from_mid_pipeline_is_rejected() {
        // FailAcceptance broadened to accept MintRequested/MintAccepted; a mint
        // that moved past acceptance (TokensReceived) must still be rejected, so
        // the success arm did not over-widen.
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![
                mint_requested_event(),
                mint_accepted_event(),
                tokens_received_event(),
            ])
            .when(TokenizedEquityMintCommand::FailAcceptance {
                reason: "should be rejected".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::AlreadyInProgress)
            ),
            "FailAcceptance from a mid-pipeline state must be AlreadyInProgress, got {error:?}"
        );
    }

    #[tokio::test]
    async fn fail_wrapping_from_tokens_received_transitions_to_failed() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::FailWrapping {
                    reason: "RPC timeout".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fail_raindex_deposit_from_tokens_wrapped_transitions_to_failed() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::random(),
                    wrapped_shares: U256::from(100u64),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                TokenizedEquityMintCommand::FailRaindexDeposit {
                    reason: "Vault deposit timeout".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fail_wrapping_rejected_from_wrong_state() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        // MintAccepted state -- FailWrapping requires TokensReceived
        store.send(&id, mint_command()).await.unwrap();

        let result = store
            .send(
                &id,
                TokenizedEquityMintCommand::FailWrapping {
                    reason: "should not work".to_string(),
                },
            )
            .await;

        assert!(
            result.is_err(),
            "FailWrapping should fail from MintAccepted"
        );
    }

    #[tokio::test]
    async fn fail_acceptance_rejected_from_terminal_state() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = issuer_request_id("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "first fail".to_string(),
                },
            )
            .await
            .unwrap();

        let result = store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "second fail".to_string(),
                },
            )
            .await;

        assert!(
            result.is_err(),
            "FailAcceptance should fail from Failed state"
        );
    }

    fn failed_mint_history() -> Vec<TokenizedEquityMintEvent> {
        vec![
            mint_requested_event(),
            mint_accepted_event(),
            TokenizedEquityMintEvent::MintAcceptanceFailed {
                reason: "timed out".to_string(),
                failed_at: Utc::now(),
            },
        ]
    }

    #[tokio::test]
    async fn reconcile_from_failed_emits_operator_reconciled_and_replays_to_reconciled() {
        let history = failed_mint_history();

        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(history.clone())
            .when(TokenizedEquityMintCommand::Reconcile {
                reason: "wrapped manually via wrap-equity".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let TokenizedEquityMintEvent::OperatorReconciled { reason, .. } = &events[0] else {
            panic!("Expected OperatorReconciled, got {:?}", events[0]);
        };
        assert_eq!(reason, "wrapped manually via wrap-equity");

        let state = replay::<TokenizedEquityMint>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let TokenizedEquityMint::Reconciled {
            failure_reason,
            reconcile_reason,
            quantity,
            ..
        } = state
        else {
            panic!("reconciled mint should be Reconciled, got {state:?}");
        };
        assert_eq!(failure_reason, "timed out");
        assert_eq!(reconcile_reason, "wrapped manually via wrap-equity");
        assert!(
            quantity.eq(float!(100.5)).unwrap(),
            "reconciled state must preserve the requested quantity, got {quantity:?}"
        );
    }

    #[test]
    fn evolve_operator_reconciled_from_failed_yields_reconciled() {
        let failed = replay::<TokenizedEquityMint>(failed_mint_history())
            .unwrap()
            .unwrap();
        let reconciled_at = Utc::now();

        let next = TokenizedEquityMint::evolve(
            &failed,
            &TokenizedEquityMintEvent::OperatorReconciled {
                reason: "credited offline".to_string(),
                reconciled_at,
            },
        )
        .unwrap()
        .unwrap();

        assert!(
            matches!(
                next,
                TokenizedEquityMint::Reconciled {
                    ref reconcile_reason,
                    ..
                } if reconcile_reason == "credited offline"
            ),
            "OperatorReconciled from Failed must yield Reconciled, got {next:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_from_non_failed_is_rejected() {
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(vec![mint_requested_event(), mint_accepted_event()])
            .when(TokenizedEquityMintCommand::Reconcile {
                reason: "should be rejected".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::NotFailed)
            ),
            "reconcile from a non-failed mint must be rejected as NotFailed, got {error:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_with_blank_reason_is_rejected() {
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(failed_mint_history())
            .when(TokenizedEquityMintCommand::Reconcile {
                reason: "   ".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::ReconcileReasonRequired)
            ),
            "reconcile with a blank reason must be rejected as ReconcileReasonRequired, got {error:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_from_reconciled_state_is_rejected() {
        let mut history = failed_mint_history();
        history.push(TokenizedEquityMintEvent::OperatorReconciled {
            reason: "already reconciled".to_string(),
            reconciled_at: Utc::now(),
        });

        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(history)
            .when(TokenizedEquityMintCommand::Reconcile {
                reason: "second attempt".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::AlreadyReconciled)
            ),
            "reconcile from an already-reconciled mint reports AlreadyReconciled, got {error:?}"
        );
    }

    #[test]
    fn mint_error_partial_eq_is_reflexive_for_reconcile_variants() {
        assert_eq!(
            TokenizedEquityMintError::AlreadyReconciled,
            TokenizedEquityMintError::AlreadyReconciled
        );
        assert_eq!(
            TokenizedEquityMintError::NotFailed,
            TokenizedEquityMintError::NotFailed
        );
        assert_ne!(
            TokenizedEquityMintError::NotFailed,
            TokenizedEquityMintError::AlreadyReconciled
        );
    }

    #[test]
    fn to_dto_reconciled_carries_failure_and_reconcile_reason() {
        let requested_at = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let reconciled = TokenizedEquityMint::Reconciled {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(10),
            failure_reason: "timed out".to_string(),
            reconcile_reason: "credited offline".to_string(),
            requested_at,
            reconciled_at,
        };

        let TransferOperation::EquityMint(operation) =
            reconciled.to_dto(&issuer_request_id("ISS001"))
        else {
            panic!("expected an EquityMint operation");
        };
        let serialized = serde_json::to_value(&operation.status).expect("serialization failed");
        assert_eq!(serialized["status"], serde_json::json!("reconciled"));
        assert_eq!(
            serialized["reconciledAt"],
            serde_json::json!("2026-01-02T00:00:00Z")
        );
        assert_eq!(serialized["failureReason"], serde_json::json!("timed out"));
        assert_eq!(
            serialized["reconcileReason"],
            serde_json::json!("credited offline")
        );
        assert_eq!(operation.started_at, requested_at);
        assert_eq!(operation.updated_at, reconciled_at);
        assert_eq!(
            operation.quantity.to_string(),
            FractionalShares::new(float!(10)).to_string()
        );
    }

    #[test]
    fn is_terminal_classifies_every_variant() {
        // Exhaustively checks every TokenizedEquityMint variant so that adding a
        // new variant without updating is_terminal causes a compile error (the
        // exhaustive match) AND a test failure (unexpected true/false here).
        let now = Utc::now();
        let sym = Symbol::new("tAAPL").unwrap();
        let id = issuer_request_id("ISS001");
        let tok = tokenization_request_id("TOK001");

        assert!(
            !TokenizedEquityMint::MintRequested {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                requested_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !TokenizedEquityMint::MintAccepted {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                issuer_request_id: id.clone(),
                tokenization_request_id: tok.clone(),
                requested_at: now,
                accepted_at: now,
                authorization: crate::tokenized_equity_mint::MintAuthorizationProgress::default(),
            }
            .is_terminal(),
        );

        assert!(
            !TokenizedEquityMint::TokensReceived {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                issuer_request_id: id.clone(),
                tokenization_request_id: tok.clone(),
                tx_hash: TxHash::default(),
                shares_minted: U256::ZERO,
                fees: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !TokenizedEquityMint::WrapSubmitted {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                issuer_request_id: id.clone(),
                tokenization_request_id: tok.clone(),
                tx_hash: TxHash::default(),
                shares_minted: U256::ZERO,
                fees: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrap_tx_hash: TxHash::default(),
            }
            .is_terminal(),
        );

        assert!(
            !TokenizedEquityMint::TokensWrapped {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                issuer_request_id: id.clone(),
                tokenization_request_id: tok.clone(),
                tx_hash: TxHash::default(),
                shares_minted: U256::ZERO,
                wrap_tx_hash: TxHash::default(),
                wrapped_shares: U256::ZERO,
                wrap_block: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrapped_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !TokenizedEquityMint::VaultDepositSubmitted {
                symbol: sym.clone(),
                quantity: float!(1),
                wallet: Address::ZERO,
                issuer_request_id: id.clone(),
                tokenization_request_id: tok.clone(),
                tx_hash: TxHash::default(),
                shares_minted: U256::ZERO,
                wrap_tx_hash: TxHash::default(),
                wrapped_shares: U256::ZERO,
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrapped_at: now,
                vault_deposit_tx_hash: TxHash::default(),
            }
            .is_terminal(),
        );

        assert!(
            TokenizedEquityMint::DepositedIntoRaindex {
                symbol: sym.clone(),
                quantity: float!(1),
                issuer_request_id: id,
                tokenization_request_id: tok,
                token_tx_hash: TxHash::default(),
                wrap_tx_hash: TxHash::default(),
                vault_deposit_tx_hash: TxHash::default(),
                requested_at: now,
                deposited_at: now,
            }
            .is_terminal(),
        );

        assert!(
            TokenizedEquityMint::Failed {
                symbol: sym,
                quantity: float!(1),
                reason: "test".to_string(),
                requested_at: now,
                failed_at: now,
            }
            .is_terminal(),
        );
    }
}
