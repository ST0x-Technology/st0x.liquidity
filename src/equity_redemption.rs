//! Aggregate modeling the lifecycle of redeeming tokenized
//! equities for underlying shares.
//!
//! Tracks the workflow from withdrawing tokens from the Raindex vault
//! through sending to Alpaca's redemption wallet to share delivery.
//!
//! # Pure handlers, orchestrated side effects
//!
//! This aggregate is a pure event recorder (`type Services = ()`): its command
//! handlers emit events from the values the command carries and never perform
//! I/O. The onchain/broker steps -- the Raindex withdrawal, the ERC-4626
//! unwrap, and the send to Alpaca's redemption wallet -- are driven by the
//! [`CrossVenueEquityTransfer`](crate::rebalancing::equity::CrossVenueEquityTransfer)
//! orchestrator running inside a durable apalis job. Each step splits into a
//! submit command (records the broadcast tx hash) and a confirm command
//! (records the receipt-derived outcome), so a crash leaves the aggregate in a
//! `*Submitted` state the resume loop can re-confirm without re-broadcasting.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states (each non-terminal
//! state can also transition to `Failed`):
//!
//! ```text
//!     VaultWithdrawPending --> VaultWithdrawSubmitted --> WithdrawnFromRaindex
//!                                                              |
//!                                                              v
//!     UnwrapPending --> UnwrapSubmitted --> TokensUnwrapped --> SendPending
//!                                                                   |
//!                                                                   v
//!     TokensSent --> Pending --> Completed
//! ```
//!
//! - `Redeem` opens the redemption at `VaultWithdrawPending`
//! - `SubmitWithdraw` / `ConfirmWithdraw` record the vault withdrawal tx and
//!   the receipt-derived actual amount (`WithdrawnFromRaindex`)
//! - `SubmitUnwrap` / `ConfirmUnwrap` record the ERC-4626 unwrap tx and the
//!   resolved underlying token + amount (`TokensUnwrapped`)
//! - `PrepareSend` / `RecordSendOutcome` record the send-to-Alpaca outcome
//!   (`TokensSent`, or `Failed` for a missing wallet / failed send)
//! - `Pending` indicates Alpaca detected the transfer
//! - `Completed` and `Failed` are terminal states
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - Failed state preserves context depending on when failure occurred
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, TxHash, U256};
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use tracing::{info, warn};
use uuid::Uuid;

use st0x_dto::{EquityRedemptionOperation, EquityRedemptionStatus, TransferOperation};
use st0x_event_sorcery::{DomainEvent, EventSourced, JobQueue, Nil};
use st0x_execution::Symbol;
use st0x_finance::{FractionalShares, Id};

use crate::tokenized_equity_mint::TokenizationRequestId;

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// Unique identifier for a redemption aggregate instance.
///
/// Mirrors [`crate::tokenized_equity_mint::IssuerRequestId`]: a UUID chosen at
/// enqueue time so apalis retries and bot restarts always target the same
/// aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct RedemptionAggregateId(pub(crate) Uuid);

impl RedemptionAggregateId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Display for RedemptionAggregateId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for RedemptionAggregateId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(value)?))
    }
}

/// Deterministic redemption aggregate id for tests. Maps a human-readable label
/// to a UUID v5 so test aggregate ids stay valid [`RedemptionAggregateId`]
/// values.
#[cfg(test)]
pub(crate) fn redemption_aggregate_id(label: &str) -> RedemptionAggregateId {
    RedemptionAggregateId(Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes()))
}

/// Errors that can occur during equity redemption operations.
///
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EquityRedemptionError {
    /// Actual withdrawal amount could not be converted to fractional shares.
    ///
    /// Raised by the `ConfirmUnwrap` handler when the orchestrator-supplied
    /// unwrapped amount cannot be represented as a [`Float`] -- the one piece
    /// of financial validation the aggregate still performs.
    #[error(
        "Raindex withdrawal amount {amount} for tx {tx_hash} could not be converted to shares: {error_message}"
    )]
    RaindexWithdrawQuantityConversionFailed {
        tx_hash: TxHash,
        amount: U256,
        error_message: String,
    },
    /// A confirmed withdrawal reported a zero actual amount. The orchestrator's
    /// receipt decode already rejects an empty transfer; this guard also fails
    /// fast on a directly-issued (e.g. CLI) `ConfirmWithdraw` now that the
    /// command path no longer goes through `step::confirm_vault_withdraw`.
    #[error("Cannot confirm withdrawal: actual withdrawn amount is zero for tx {tx_hash}")]
    ZeroWithdrawAmount { tx_hash: TxHash },
    /// A confirmed unwrap reported a zero amount. Fails fast at the CQRS
    /// boundary so a zero never drives the downstream Alpaca redemption send.
    #[error("Cannot confirm unwrap: unwrapped amount is zero for tx {tx_hash}")]
    ZeroUnwrapAmount { tx_hash: TxHash },
    /// Transaction failed with a known tx hash
    #[error("Transaction failed: {tx_hash}")]
    TransactionFailed { tx_hash: TxHash },
    /// Attempted to unwrap tokens when redemption is already in progress
    #[error("Cannot unwrap tokens: redemption already in progress")]
    CannotUnwrapAlreadyStarted,
    /// Attempted to send tokens before unwrapping
    #[error("Cannot send: tokens not unwrapped")]
    TokensNotUnwrapped,
    /// Attempted to detect redemption before sending tokens
    #[error("Cannot detect redemption: tokens not sent")]
    TokensNotSent,
    /// Attempted to await completion before redemption was detected
    #[error("Cannot await completion: not in pending state")]
    NotPending,
    /// Attempted to reject before redemption was detected as pending
    #[error("Cannot reject: not in pending state")]
    NotPendingForRejection,
    /// Attempted to transition before the aggregate was initialized
    #[error("Not started")]
    NotStarted,
    /// Attempted to send tokens when redemption is already in progress
    #[error("Already started")]
    AlreadyStarted,
    /// Attempted to detect a redemption that was already detected
    #[error("Already detected")]
    AlreadyDetected,
    /// Attempted to modify a completed redemption operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed redemption operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Attempted to reconcile a redemption that is not in the `Failed` state
    #[error("Cannot reconcile: redemption is not in the Failed state")]
    NotFailed,
    /// Attempted to act on a redemption already resolved out-of-band (`Reconciled`)
    #[error("Already reconciled")]
    AlreadyReconciled,
    /// Attempted to reconcile without an operator-supplied reason.
    #[error("Cannot reconcile: reason is required")]
    ReconcileReasonRequired,
}

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    /// Submits vault withdrawal tx and emits VaultWithdrawSubmitted.
    Redeem {
        symbol: Symbol,
        quantity: Float,
        token: Address,
        amount: U256,
    },
    /// Records the orchestrator-confirmed withdrawal receipt (pure).
    /// Emits WithdrawnFromRaindex.
    ConfirmWithdraw {
        actual_wrapped_amount: U256,
        raindex_withdraw_block: u64,
    },
    /// Submits ERC-4626 unwrap tx and emits UnwrapSubmitted.
    UnwrapTokens,
    /// Records the orchestrator-confirmed unwrap result (pure).
    /// Emits TokensUnwrapped.
    ConfirmUnwrap {
        underlying_token: Address,
        unwrapped_amount: U256,
        unwrap_block: u64,
    },
    /// Records the outcome of the orchestrator's send-to-Alpaca step (pure).
    /// Emits TokensSent (success) or TransferFailed (missing wallet / send
    /// failure). Carries the raw outcome so the wallet/send invariants stay
    /// enforced at the aggregate boundary.
    RecordSendOutcome { outcome: SendOutcome },
    /// Alpaca detected the token transfer.
    Detect {
        tokenization_request_id: TokenizationRequestId,
    },
    /// Detection polling failed or timed out.
    FailDetection { failure: DetectionFailure },
    /// Redemption completed successfully.
    Complete,
    /// Alpaca rejected the redemption.
    RejectRedemption { reason: String },
    /// Recover provider completion after the aggregate had failed.
    RecoverProviderCompletion {
        tokenization_request_id: TokenizationRequestId,
    },
    /// Operator or timeout-driven failure from `WithdrawnFromRaindex` or
    /// `TokensUnwrapped` states.
    FailTransfer { reason: String },
    /// Records the orchestrator-submitted vault withdrawal tx (pure).
    /// Valid from `VaultWithdrawPending`.
    SubmitWithdraw { tx_hash: TxHash },
    /// Records the orchestrator-submitted ERC-4626 unwrap tx (pure).
    /// Valid from `UnwrapPending`.
    SubmitUnwrap { unwrap_tx_hash: TxHash },
    /// Records the orchestrator-broadcast redemption transfer tx (pure).
    /// Valid from `SendPending`. Persists the tx immediately after broadcast so
    /// resume finalizes it rather than re-broadcasting an irreversible transfer.
    SubmitSend {
        redemption_wallet: Address,
        redemption_tx: TxHash,
    },
    /// Finalizes a broadcast redemption send to `TokensSent` (pure).
    /// Valid from `SendSubmitted`.
    ConfirmSend,
    /// Prepares sending tokens (pure, no side effects).
    /// Valid from `TokensUnwrapped`.
    PrepareSend,
    /// Reconcile a redemption stranded in the terminal `Failed` state to the
    /// terminal `Reconciled` state. The residual equity was handled out-of-band
    /// (e.g. via wrap-equity/vault-deposit), so this is a bookkeeping resolution
    /// rather than a re-drive. Valid ONLY from `Failed`.
    Reconcile { reason: String },
}

/// Outcome of the orchestrator's send-to-Alpaca step, carried by
/// [`EquityRedemptionCommand::RecordSendOutcome`]. Commands are never
/// persisted, so this does not derive Serialize/Deserialize.
#[derive(Debug, Clone)]
pub(crate) enum SendOutcome {
    /// The provider transfer was submitted.
    Sent {
        redemption_wallet: Address,
        redemption_tx: TxHash,
    },
    /// No redemption wallet is configured for the provider.
    WalletNotConfigured,
    /// The provider rejected/failed the transfer.
    SendFailed,
}

/// Why redemption detection failed.
///
/// `Timeout` and `ApiError` are emitted automatically by the detection-polling
/// reactor. `Operator` marks an operator-initiated force-fail of a redemption
/// stuck in `TokensSent` (the tokens reached Alpaca but detection never fired),
/// distinguishing a manual intervention from an automated polling failure in
/// the event log; it carries the operator's `--reason` so the audit trail
/// records why the redemption was force-failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DetectionFailure {
    Timeout,
    ApiError { status_code: Option<u16> },
    Operator { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EquityRedemptionEvent {
    /// Vault withdrawal requested, awaiting submission.
    VaultWithdrawPending {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        pending_at: DateTime<Utc>,
    },
    /// Vault withdrawal transaction submitted, pending confirmation.
    VaultWithdrawSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },
    /// Tokens withdrawn from Raindex vault to wallet.
    WithdrawnFromRaindex {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        /// Original target amount requested from Raindex.
        wrapped_amount: U256,
        /// Actual wrapped-token amount transferred to the bot wallet.
        #[serde(default)]
        actual_wrapped_amount: Option<U256>,
        raindex_withdraw_tx: TxHash,
        /// Block number in which the Raindex withdrawal tx confirmed.
        ///
        /// `None` for events emitted before this field was added (schema
        /// backward-compatibility). When `None`, the RPC node-sync wait is
        /// skipped in `SubmitUnwrap`.
        #[serde(default)]
        raindex_withdraw_block: Option<u64>,
        withdrawn_at: DateTime<Utc>,
    },
    /// ERC-4626 wrapped tokens have been unwrapped.
    TokensUnwrapped {
        #[serde(
            default,
            serialize_with = "st0x_float_serde::serialize_option_float",
            deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
        )]
        quantity: Option<Float>,
        underlying_token: Address,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        /// Block number in which the unwrap tx confirmed.
        ///
        /// `None` for events emitted before this field was added (schema
        /// backward-compatibility). When `None`, the orchestrator skips the
        /// RPC node-sync wait before sending.
        #[serde(default)]
        unwrap_block: Option<u64>,
        unwrapped_at: DateTime<Utc>,
    },
    /// Unwrap requested, awaiting submission.
    UnwrapPending {
        pending_at: DateTime<Utc>,
    },
    /// Unwrap transaction submitted, pending confirmation.
    UnwrapSubmitted {
        unwrap_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },
    /// Send requested, awaiting submission.
    SendPending {
        pending_at: DateTime<Utc>,
    },
    /// Token transfer to Alpaca's redemption wallet broadcast, pending the
    /// bookkeeping finalization. Recorded immediately after broadcast so a crash
    /// before finalization resumes by finalizing the persisted tx rather than
    /// re-broadcasting an irreversible transfer.
    SendSubmitted {
        redemption_wallet: Address,
        redemption_tx: TxHash,
        submitted_at: DateTime<Utc>,
    },
    /// Raindex withdraw succeeded but transfer to redemption wallet failed.
    TransferFailed {
        tx_hash: Option<TxHash>,
        /// Reason for failure (timeout, EVM revert, operator action).
        /// Absent in events emitted before this field was added.
        #[serde(default)]
        reason: Option<String>,
        failed_at: DateTime<Utc>,
    },

    /// Tokens sent to Alpaca's redemption wallet.
    TokensSent {
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
    },
    /// Alpaca failed to detect the token transfer.
    /// Tokens were sent but detection failed - keep inflight until manually resolved.
    DetectionFailed {
        failure: DetectionFailure,
        failed_at: DateTime<Utc>,
    },

    Detected {
        tokenization_request_id: TokenizationRequestId,
        detected_at: DateTime<Utc>,
    },
    /// Alpaca rejected the redemption after detection.
    /// Tokens location unknown after rejection - keep inflight until manually resolved.
    RedemptionRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },

    Completed {
        completed_at: DateTime<Utc>,
    },
    ProviderCompletionRecovered {
        tokenization_request_id: TokenizationRequestId,
        recovered_at: DateTime<Utc>,
    },
    /// An operator reconciled a terminal `Failed` redemption out-of-band. Marks
    /// the transfer resolved without re-driving the failed leg.
    OperatorReconciled {
        reason: String,
        reconciled_at: DateTime<Utc>,
    },
}

fn resolve_withdrawn_wrapped_amount(
    wrapped_amount: U256,
    actual_wrapped_amount: Option<U256>,
) -> U256 {
    actual_wrapped_amount.unwrap_or(wrapped_amount)
}

/// Field-wise equality of two `WithdrawnFromRaindex` events (the widest event,
/// extracted from the `PartialEq` match for readability). Returns `false` for
/// any non-`WithdrawnFromRaindex` pair.
fn withdrawn_from_raindex_events_eq(
    left: &EquityRedemptionEvent,
    right: &EquityRedemptionEvent,
) -> bool {
    let (
        EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol: s1,
            quantity: q1,
            token: t1,
            wrapped_amount: w1,
            actual_wrapped_amount: aw1,
            raindex_withdraw_tx: r1,
            raindex_withdraw_block: rb1,
            withdrawn_at: wa1,
        },
        EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol: s2,
            quantity: q2,
            token: t2,
            wrapped_amount: w2,
            actual_wrapped_amount: aw2,
            raindex_withdraw_tx: r2,
            raindex_withdraw_block: rb2,
            withdrawn_at: wa2,
        },
    ) = (left, right)
    else {
        return false;
    };

    s1 == s2
        && q1.eq(*q2).unwrap_or(false)
        && t1 == t2
        && w1 == w2
        && aw1 == aw2
        && r1 == r2
        && rb1 == rb2
        && wa1 == wa2
}

/// Required by `cqrs_es::DomainEvent`.
impl PartialEq for EquityRedemptionEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::VaultWithdrawPending {
                    symbol: s1,
                    quantity: q1,
                    token: t1,
                    wrapped_amount: w1,
                    pending_at: pa1,
                },
                Self::VaultWithdrawPending {
                    symbol: s2,
                    quantity: q2,
                    token: t2,
                    wrapped_amount: w2,
                    pending_at: pa2,
                },
            ) => s1 == s2 && q1.eq(*q2).unwrap_or(false) && t1 == t2 && w1 == w2 && pa1 == pa2,
            (
                Self::VaultWithdrawSubmitted {
                    symbol: s1,
                    quantity: q1,
                    token: t1,
                    wrapped_amount: w1,
                    tx_hash: h1,
                    submitted_at: sa1,
                },
                Self::VaultWithdrawSubmitted {
                    symbol: s2,
                    quantity: q2,
                    token: t2,
                    wrapped_amount: w2,
                    tx_hash: h2,
                    submitted_at: sa2,
                },
            ) => {
                s1 == s2
                    && q1.eq(*q2).unwrap_or(false)
                    && t1 == t2
                    && w1 == w2
                    && h1 == h2
                    && sa1 == sa2
            }
            (
                Self::UnwrapSubmitted {
                    unwrap_tx_hash: h1,
                    submitted_at: sa1,
                },
                Self::UnwrapSubmitted {
                    unwrap_tx_hash: h2,
                    submitted_at: sa2,
                },
            ) => h1 == h2 && sa1 == sa2,
            (Self::UnwrapPending { pending_at: pa1 }, Self::UnwrapPending { pending_at: pa2 })
            | (Self::SendPending { pending_at: pa1 }, Self::SendPending { pending_at: pa2 }) => {
                pa1 == pa2
            }
            (
                left @ Self::WithdrawnFromRaindex { .. },
                right @ Self::WithdrawnFromRaindex { .. },
            ) => withdrawn_from_raindex_events_eq(left, right),
            (
                Self::TokensUnwrapped {
                    quantity: q1,
                    underlying_token: u1,
                    unwrap_tx_hash: h1,
                    unwrapped_amount: a1,
                    unwrap_block: b1,
                    unwrapped_at: t1,
                },
                Self::TokensUnwrapped {
                    quantity: q2,
                    underlying_token: u2,
                    unwrap_tx_hash: h2,
                    unwrapped_amount: a2,
                    unwrap_block: b2,
                    unwrapped_at: t2,
                },
            ) => {
                q1.is_some() == q2.is_some()
                    && q1
                        .zip(*q2)
                        .is_none_or(|(q1, q2)| q1.eq(q2).unwrap_or(false))
                    && u1 == u2
                    && h1 == h2
                    && b1 == b2
                    && a1 == a2
                    && t1 == t2
            }
            (
                Self::TransferFailed {
                    tx_hash: h1,
                    reason: r1,
                    failed_at: f1,
                },
                Self::TransferFailed {
                    tx_hash: h2,
                    reason: r2,
                    failed_at: f2,
                },
            ) => h1 == h2 && r1 == r2 && f1 == f2,
            (
                Self::TokensSent {
                    redemption_wallet: w1,
                    redemption_tx: t1,
                    sent_at: s1,
                },
                Self::TokensSent {
                    redemption_wallet: w2,
                    redemption_tx: t2,
                    sent_at: s2,
                },
            )
            | (
                Self::SendSubmitted {
                    redemption_wallet: w1,
                    redemption_tx: t1,
                    submitted_at: s1,
                },
                Self::SendSubmitted {
                    redemption_wallet: w2,
                    redemption_tx: t2,
                    submitted_at: s2,
                },
            ) => w1 == w2 && t1 == t2 && s1 == s2,
            (
                Self::DetectionFailed {
                    failure: f1,
                    failed_at: fa1,
                },
                Self::DetectionFailed {
                    failure: f2,
                    failed_at: fa2,
                },
            ) => f1 == f2 && fa1 == fa2,
            (
                Self::Detected {
                    tokenization_request_id: t1,
                    detected_at: d1,
                },
                Self::Detected {
                    tokenization_request_id: t2,
                    detected_at: d2,
                },
            ) => t1 == t2 && d1 == d2,
            (
                Self::RedemptionRejected {
                    reason: r1,
                    rejected_at: ra1,
                },
                Self::RedemptionRejected {
                    reason: r2,
                    rejected_at: ra2,
                },
            ) => r1 == r2 && ra1 == ra2,
            (Self::Completed { completed_at: c1 }, Self::Completed { completed_at: c2 }) => {
                c1 == c2
            }
            (
                Self::ProviderCompletionRecovered {
                    tokenization_request_id: id1,
                    recovered_at: t1,
                },
                Self::ProviderCompletionRecovered {
                    tokenization_request_id: id2,
                    recovered_at: t2,
                },
            ) => id1 == id2 && t1 == t2,
            (
                Self::OperatorReconciled {
                    reason: r1,
                    reconciled_at: t1,
                },
                Self::OperatorReconciled {
                    reason: r2,
                    reconciled_at: t2,
                },
            ) => r1 == r2 && t1 == t2,
            _ => false,
        }
    }
}

impl Eq for EquityRedemptionEvent {}

impl DomainEvent for EquityRedemptionEvent {
    fn event_type(&self) -> String {
        use EquityRedemptionEvent::*;
        match self {
            VaultWithdrawPending { .. } => {
                "EquityRedemptionEvent::VaultWithdrawPending".to_string()
            }
            VaultWithdrawSubmitted { .. } => {
                "EquityRedemptionEvent::VaultWithdrawSubmitted".to_string()
            }
            WithdrawnFromRaindex { .. } => {
                "EquityRedemptionEvent::WithdrawnFromRaindex".to_string()
            }
            UnwrapPending { .. } => "EquityRedemptionEvent::UnwrapPending".to_string(),
            UnwrapSubmitted { .. } => "EquityRedemptionEvent::UnwrapSubmitted".to_string(),
            SendPending { .. } => "EquityRedemptionEvent::SendPending".to_string(),
            SendSubmitted { .. } => "EquityRedemptionEvent::SendSubmitted".to_string(),
            TokensUnwrapped { .. } => "EquityRedemptionEvent::TokensUnwrapped".to_string(),
            TransferFailed { .. } => "EquityRedemptionEvent::TransferFailed".to_string(),
            TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
            DetectionFailed { .. } => "EquityRedemptionEvent::DetectionFailed".to_string(),
            Detected { .. } => "EquityRedemptionEvent::Detected".to_string(),
            RedemptionRejected { .. } => "EquityRedemptionEvent::RedemptionRejected".to_string(),
            Completed { .. } => "EquityRedemptionEvent::Completed".to_string(),
            ProviderCompletionRecovered { .. } => {
                "EquityRedemptionEvent::ProviderCompletionRecovered".to_string()
            }
            OperatorReconciled { .. } => "EquityRedemptionEvent::OperatorReconciled".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Equity redemption aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid states unrepresentable.
/// Each variant contains exactly the data valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EquityRedemption {
    /// Vault withdrawal requested, awaiting submission
    VaultWithdrawPending {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        pending_at: DateTime<Utc>,
    },

    /// Vault withdrawal submitted, awaiting confirmation
    VaultWithdrawSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    /// Tokens withdrawn from Raindex vault to wallet, not yet sent to Alpaca
    WithdrawnFromRaindex {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        raindex_withdraw_tx: TxHash,
        /// Block in which the Raindex withdrawal tx confirmed; `None` for pre-fix aggregates.
        #[serde(default)]
        raindex_withdraw_block: Option<u64>,
        withdrawn_at: DateTime<Utc>,
    },

    /// Unwrap requested, awaiting submission
    UnwrapPending {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        raindex_withdraw_tx: TxHash,
        /// Block in which the Raindex withdrawal tx confirmed; `None` for pre-fix aggregates.
        #[serde(default)]
        raindex_withdraw_block: Option<u64>,
        withdrawn_at: DateTime<Utc>,
    },

    /// Unwrap transaction submitted, awaiting confirmation
    UnwrapSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        wrapped_amount: U256,
        raindex_withdraw_tx: TxHash,
        /// Block in which the Raindex withdrawal tx confirmed; `None` for pre-fix aggregates.
        #[serde(default)]
        raindex_withdraw_block: Option<u64>,
        unwrap_tx_hash: TxHash,
        withdrawn_at: DateTime<Utc>,
    },

    /// Wrapped tokens have been unwrapped, ready to send to Alpaca
    TokensUnwrapped {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        underlying_token: Address,
        raindex_withdraw_tx: TxHash,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        /// Block in which the unwrap tx confirmed; `None` for pre-fix aggregates.
        #[serde(default)]
        unwrap_block: Option<u64>,
        withdrawn_at: DateTime<Utc>,
        unwrapped_at: DateTime<Utc>,
    },

    /// Send requested, awaiting submission
    SendPending {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        underlying_token: Address,
        raindex_withdraw_tx: TxHash,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        /// Block in which the unwrap tx confirmed; `None` for pre-fix aggregates.
        ///
        /// `None` disables the orchestrator's RPC node-sync wait before
        /// sending, for backward-compatibility with in-flight aggregates that
        /// were persisted before this field was added.
        #[serde(default)]
        unwrap_block: Option<u64>,
        withdrawn_at: DateTime<Utc>,
        unwrapped_at: DateTime<Utc>,
    },

    /// Token transfer to Alpaca's redemption wallet broadcast, awaiting the
    /// bookkeeping finalization to `TokensSent`. Resume confirms the persisted
    /// `redemption_tx` here without re-broadcasting the transfer.
    SendSubmitted {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        raindex_withdraw_tx: TxHash,
        unwrap_tx_hash: TxHash,
        redemption_wallet: Address,
        redemption_tx: TxHash,
        submitted_at: DateTime<Utc>,
    },

    /// Tokens sent to Alpaca's redemption wallet
    TokensSent {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        token: Address,
        raindex_withdraw_tx: TxHash,
        unwrap_tx_hash: Option<TxHash>,
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
    },

    /// Alpaca detected the token transfer and returned tracking identifier
    Pending {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        redemption_tx: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },

    /// Redemption successfully completed and account credited (terminal state)
    Completed {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        redemption_tx: TxHash,
        tokenization_request_id: TokenizationRequestId,
        /// When the redemption process started (sent_at from Pending state)
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },

    /// Redemption failed (terminal state)
    ///
    /// Fields preserve context depending on when failure occurred:
    /// - `raindex_withdraw_tx`: Present if Raindex withdraw succeeded
    /// - `redemption_tx`: Present if send succeeded
    /// - `tokenization_request_id`: Present if Alpaca detected the transfer
    Failed {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        raindex_withdraw_tx: Option<TxHash>,
        redemption_tx: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        /// Reason for failure (timeout, operator action, etc.).
        /// Absent for aggregates that failed before this field was added.
        #[serde(default)]
        reason: Option<String>,
        /// When the redemption process started (withdrawn_at or sent_at from prior state)
        started_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },

    /// An operator reconciled a terminal `Failed` redemption out-of-band
    /// (terminal state). Retains the identifying fields carried by `Failed` so
    /// the projection still reports the real transfer instead of a zero-value
    /// record.
    Reconciled {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        quantity: Float,
        raindex_withdraw_tx: Option<TxHash>,
        redemption_tx: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        /// The failure reason carried over from the `Failed` state.
        failure_reason: Option<String>,
        /// The operator-supplied reconciliation reason.
        reconcile_reason: String,
        started_at: DateTime<Utc>,
        reconciled_at: DateTime<Utc>,
    },
}

impl EquityRedemption {
    /// Returns the requested quantity carried by the aggregate in every
    /// state. Wrapped-equity recovery compares this against the wallet
    /// snapshot so an audit mismatch surfaces before dispatch.
    pub(crate) fn quantity(&self) -> Float {
        match self {
            Self::VaultWithdrawPending { quantity, .. }
            | Self::VaultWithdrawSubmitted { quantity, .. }
            | Self::WithdrawnFromRaindex { quantity, .. }
            | Self::UnwrapPending { quantity, .. }
            | Self::UnwrapSubmitted { quantity, .. }
            | Self::TokensUnwrapped { quantity, .. }
            | Self::SendPending { quantity, .. }
            | Self::SendSubmitted { quantity, .. }
            | Self::TokensSent { quantity, .. }
            | Self::Pending { quantity, .. }
            | Self::Completed { quantity, .. }
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
            Self::Completed { .. } | Self::Failed { .. } | Self::Reconciled { .. } => true,
            Self::VaultWithdrawPending { .. }
            | Self::VaultWithdrawSubmitted { .. }
            | Self::WithdrawnFromRaindex { .. }
            | Self::UnwrapPending { .. }
            | Self::UnwrapSubmitted { .. }
            | Self::TokensUnwrapped { .. }
            | Self::SendPending { .. }
            | Self::SendSubmitted { .. }
            | Self::TokensSent { .. }
            | Self::Pending { .. } => false,
        }
    }

    pub(crate) fn to_dto(&self, id: &RedemptionAggregateId) -> TransferOperation {
        match self {
            Self::VaultWithdrawPending {
                symbol,
                quantity,
                pending_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Withdrawing,
                started_at: *pending_at,
                updated_at: *pending_at,
            }),

            Self::VaultWithdrawSubmitted {
                symbol,
                quantity,
                submitted_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Withdrawing,
                started_at: *submitted_at,
                updated_at: *submitted_at,
            }),

            Self::WithdrawnFromRaindex {
                symbol,
                quantity,
                withdrawn_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Withdrawing,
                started_at: *withdrawn_at,
                updated_at: *withdrawn_at,
            }),

            Self::UnwrapPending {
                symbol,
                quantity,
                withdrawn_at,
                ..
            }
            | Self::UnwrapSubmitted {
                symbol,
                quantity,
                withdrawn_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Unwrapping,
                started_at: *withdrawn_at,
                updated_at: *withdrawn_at,
            }),

            Self::TokensUnwrapped {
                symbol,
                quantity,
                withdrawn_at,
                unwrapped_at,
                ..
            }
            | Self::SendPending {
                symbol,
                quantity,
                withdrawn_at,
                unwrapped_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Unwrapping,
                started_at: *withdrawn_at,
                updated_at: *unwrapped_at,
            }),

            Self::SendSubmitted {
                symbol,
                quantity,
                submitted_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Sending,
                started_at: *submitted_at,
                updated_at: *submitted_at,
            }),

            Self::TokensSent {
                symbol,
                quantity,
                sent_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Sending,
                started_at: *sent_at,
                updated_at: *sent_at,
            }),

            Self::Pending {
                symbol,
                quantity,
                sent_at,
                detected_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::PendingConfirmation,
                started_at: *sent_at,
                updated_at: *detected_at,
            }),

            Self::Completed {
                symbol,
                quantity,
                started_at,
                completed_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Completed {
                    completed_at: *completed_at,
                },
                started_at: *started_at,
                updated_at: *completed_at,
            }),

            Self::Failed {
                symbol,
                quantity,
                started_at,
                failed_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Failed {
                    failed_at: *failed_at,
                },
                started_at: *started_at,
                updated_at: *failed_at,
            }),

            // Reconciliation is a terminal operator resolution, so it maps to the
            // existing `Completed` DTO status (no new status needed) -- mirrors the
            // USDC `Reconciled -> Completed` mapping.
            Self::Reconciled {
                symbol,
                quantity,
                started_at,
                reconciled_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.to_string()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Completed {
                    completed_at: *reconciled_at,
                },
                started_at: *started_at,
                updated_at: *reconciled_at,
            }),
        }
    }
}

impl EventSourced for EquityRedemption {
    type Id = RedemptionAggregateId;
    type Event = EquityRedemptionEvent;
    type Command = EquityRedemptionCommand;
    type Error = EquityRedemptionError;
    type Jobs = Nil;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "EquityRedemption";
    const PROJECTION: Nil = Nil;
    // v3: added the `ProviderCompletionRecovered` event for in-process
    // failed-transfer recovery.
    // v4: `Failed.reason` is now materialized from `DetectionFailed`
    // (`Operator` failures) and `RedemptionRejected` events instead of always
    // `None`. Bumped to clear stale snapshots so historical rejections rebuild
    // from events under the corrected evolve logic.
    // v5: added the terminal `Reconciled` state and the `OperatorReconciled`
    // event for operator reconciliation of stuck `Failed` redemptions. Additive
    // only; bumped to clear stale snapshots so they rebuild from events under the
    // new schema (existing events replay unchanged).
    const SCHEMA_VERSION: u64 = 5;

    fn originate(event: &Self::Event) -> Option<Self> {
        use EquityRedemptionEvent::*;
        match event {
            VaultWithdrawPending {
                symbol,
                quantity,
                token,
                wrapped_amount,
                pending_at,
            } => Some(Self::VaultWithdrawPending {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                wrapped_amount: *wrapped_amount,
                pending_at: *pending_at,
            }),
            // Legacy: old aggregates start with VaultWithdrawSubmitted
            VaultWithdrawSubmitted {
                symbol,
                quantity,
                token,
                wrapped_amount,
                tx_hash,
                submitted_at,
            } => Some(Self::VaultWithdrawSubmitted {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                wrapped_amount: *wrapped_amount,
                tx_hash: *tx_hash,
                submitted_at: *submitted_at,
            }),
            // Legacy: old aggregates start with WithdrawnFromRaindex
            WithdrawnFromRaindex {
                symbol,
                quantity,
                token,
                wrapped_amount,
                actual_wrapped_amount,
                raindex_withdraw_tx,
                raindex_withdraw_block,
                withdrawn_at,
            } => Some(Self::WithdrawnFromRaindex {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                wrapped_amount: resolve_withdrawn_wrapped_amount(
                    *wrapped_amount,
                    *actual_wrapped_amount,
                ),
                raindex_withdraw_tx: *raindex_withdraw_tx,
                raindex_withdraw_block: *raindex_withdraw_block,
                withdrawn_at: *withdrawn_at,
            }),
            _ => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use EquityRedemptionEvent::*;

        Ok(match event {
            VaultWithdrawPending { .. } => None,
            VaultWithdrawSubmitted {
                symbol,
                quantity,
                token,
                wrapped_amount,
                tx_hash,
                submitted_at,
            } => match entity {
                Self::VaultWithdrawPending { .. } => Some(Self::VaultWithdrawSubmitted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    wrapped_amount: *wrapped_amount,
                    tx_hash: *tx_hash,
                    submitted_at: *submitted_at,
                }),
                // Legacy: VaultWithdrawSubmitted is handled as originate
                _ => None,
            },
            WithdrawnFromRaindex {
                symbol,
                quantity,
                token,
                wrapped_amount,
                actual_wrapped_amount,
                raindex_withdraw_tx,
                raindex_withdraw_block,
                withdrawn_at,
            } => match entity {
                Self::VaultWithdrawPending { .. } | Self::VaultWithdrawSubmitted { .. } => {
                    Some(Self::WithdrawnFromRaindex {
                        symbol: symbol.clone(),
                        quantity: *quantity,
                        token: *token,
                        wrapped_amount: resolve_withdrawn_wrapped_amount(
                            *wrapped_amount,
                            *actual_wrapped_amount,
                        ),
                        raindex_withdraw_tx: *raindex_withdraw_tx,
                        raindex_withdraw_block: *raindex_withdraw_block,
                        withdrawn_at: *withdrawn_at,
                    })
                }
                // Legacy: WithdrawnFromRaindex is handled as originate
                _ => None,
            },

            TransferFailed {
                tx_hash,
                reason,
                failed_at,
            } => match entity {
                Self::VaultWithdrawPending {
                    symbol,
                    quantity,
                    pending_at,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: None,
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
                    reason: reason.clone(),
                    started_at: *pending_at,
                    failed_at: *failed_at,
                }),
                Self::VaultWithdrawSubmitted {
                    symbol,
                    quantity,
                    submitted_at,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: None,
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
                    reason: reason.clone(),
                    started_at: *submitted_at,
                    failed_at: *failed_at,
                }),
                Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::UnwrapPending {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::TokensUnwrapped {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::SendPending {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::UnwrapSubmitted {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: Some(*raindex_withdraw_tx),
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
                    reason: reason.clone(),
                    started_at: *withdrawn_at,
                    failed_at: *failed_at,
                }),

                _ => return Ok(None),
            },

            UnwrapPending { pending_at: _ } => match entity {
                Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount,
                    raindex_withdraw_tx,
                    raindex_withdraw_block,
                    withdrawn_at,
                } => Some(Self::UnwrapPending {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    wrapped_amount: *wrapped_amount,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    raindex_withdraw_block: *raindex_withdraw_block,
                    withdrawn_at: *withdrawn_at,
                }),
                _ => None,
            },

            UnwrapSubmitted {
                unwrap_tx_hash,
                submitted_at: _,
            } => match entity {
                Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount,
                    raindex_withdraw_tx,
                    raindex_withdraw_block,
                    withdrawn_at,
                }
                | Self::UnwrapPending {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount,
                    raindex_withdraw_tx,
                    raindex_withdraw_block,
                    withdrawn_at,
                } => Some(Self::UnwrapSubmitted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    wrapped_amount: *wrapped_amount,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    raindex_withdraw_block: *raindex_withdraw_block,
                    unwrap_tx_hash: *unwrap_tx_hash,
                    withdrawn_at: *withdrawn_at,
                }),
                _ => None,
            },

            TokensUnwrapped {
                quantity: actual_quantity,
                underlying_token,
                unwrap_tx_hash,
                unwrapped_amount,
                unwrap_block,
                unwrapped_at,
            } => match entity {
                Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::UnwrapPending {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                }
                | Self::UnwrapSubmitted {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                } => Some(Self::TokensUnwrapped {
                    symbol: symbol.clone(),
                    quantity: actual_quantity.unwrap_or(*quantity),
                    token: *token,
                    underlying_token: *underlying_token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: *unwrap_tx_hash,
                    unwrapped_amount: *unwrapped_amount,
                    unwrap_block: *unwrap_block,
                    withdrawn_at: *withdrawn_at,
                    unwrapped_at: *unwrapped_at,
                }),
                _ => None,
            },

            SendPending { pending_at: _ } => match entity {
                Self::TokensUnwrapped {
                    symbol,
                    quantity,
                    token,
                    underlying_token,
                    raindex_withdraw_tx,
                    unwrap_tx_hash,
                    unwrapped_amount,
                    unwrap_block,
                    withdrawn_at,
                    unwrapped_at,
                } => Some(Self::SendPending {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    underlying_token: *underlying_token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: *unwrap_tx_hash,
                    unwrapped_amount: *unwrapped_amount,
                    unwrap_block: *unwrap_block,
                    withdrawn_at: *withdrawn_at,
                    unwrapped_at: *unwrapped_at,
                }),
                _ => None,
            },

            SendSubmitted {
                redemption_wallet,
                redemption_tx,
                submitted_at,
            } => match entity {
                Self::SendPending {
                    symbol,
                    quantity,
                    underlying_token,
                    raindex_withdraw_tx,
                    unwrap_tx_hash,
                    ..
                } => Some(Self::SendSubmitted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *underlying_token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: *unwrap_tx_hash,
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    submitted_at: *submitted_at,
                }),
                _ => None,
            },

            TokensSent {
                redemption_wallet,
                redemption_tx,
                sent_at,
            } => match entity {
                Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    ..
                } => Some(Self::TokensSent {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: None,
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    sent_at: *sent_at,
                }),
                Self::TokensUnwrapped {
                    symbol,
                    quantity,
                    underlying_token,
                    raindex_withdraw_tx,
                    unwrap_tx_hash,
                    ..
                }
                | Self::SendPending {
                    symbol,
                    quantity,
                    underlying_token,
                    raindex_withdraw_tx,
                    unwrap_tx_hash,
                    ..
                } => Some(Self::TokensSent {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *underlying_token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: Some(*unwrap_tx_hash),
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    sent_at: *sent_at,
                }),
                Self::SendSubmitted {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    unwrap_tx_hash,
                    ..
                } => Some(Self::TokensSent {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: Some(*unwrap_tx_hash),
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    sent_at: *sent_at,
                }),
                _ => None,
            },

            Detected {
                tokenization_request_id,
                detected_at,
            } => {
                let Self::TokensSent {
                    symbol,
                    quantity,
                    redemption_tx,
                    sent_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Pending {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    redemption_tx: *redemption_tx,
                    tokenization_request_id: tokenization_request_id.clone(),
                    sent_at: *sent_at,
                    detected_at: *detected_at,
                })
            }

            DetectionFailed { failure, failed_at } => {
                let Self::TokensSent {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    redemption_tx,
                    sent_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                let reason = match failure {
                    DetectionFailure::Operator { reason } => Some(reason.clone()),
                    DetectionFailure::Timeout | DetectionFailure::ApiError { .. } => None,
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: Some(*raindex_withdraw_tx),
                    redemption_tx: Some(*redemption_tx),
                    tokenization_request_id: None,
                    reason,
                    started_at: *sent_at,
                    failed_at: *failed_at,
                })
            }

            Completed { completed_at } => {
                let Self::Pending {
                    symbol,
                    quantity,
                    redemption_tx,
                    tokenization_request_id,
                    sent_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Completed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    redemption_tx: *redemption_tx,
                    tokenization_request_id: tokenization_request_id.clone(),
                    started_at: *sent_at,
                    completed_at: *completed_at,
                })
            }

            RedemptionRejected {
                reason,
                rejected_at,
            } => {
                let Self::Pending {
                    symbol,
                    quantity,
                    redemption_tx,
                    tokenization_request_id,
                    sent_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: None,
                    redemption_tx: Some(*redemption_tx),
                    tokenization_request_id: Some(tokenization_request_id.clone()),
                    reason: Some(reason.clone()),
                    started_at: *sent_at,
                    failed_at: *rejected_at,
                })
            }

            ProviderCompletionRecovered {
                tokenization_request_id,
                recovered_at,
            } => {
                let Self::Failed {
                    symbol,
                    quantity,
                    redemption_tx: Some(redemption_tx),
                    started_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Completed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    redemption_tx: *redemption_tx,
                    tokenization_request_id: tokenization_request_id.clone(),
                    started_at: *started_at,
                    completed_at: *recovered_at,
                })
            }

            OperatorReconciled {
                reason,
                reconciled_at,
            } => {
                let Self::Failed {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    redemption_tx,
                    tokenization_request_id,
                    reason: failure_reason,
                    started_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Reconciled {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    redemption_tx: *redemption_tx,
                    tokenization_request_id: tokenization_request_id.clone(),
                    failure_reason: failure_reason.clone(),
                    reconcile_reason: reason.clone(),
                    started_at: *started_at,
                    reconciled_at: *reconciled_at,
                })
            }
        })
    }

    fn initialize(
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            Redeem {
                symbol,
                quantity,
                token,
                amount,
            } => Ok(vec![VaultWithdrawPending {
                symbol,
                quantity,
                token,
                wrapped_amount: amount,
                pending_at: Utc::now(),
            }]),
            SubmitWithdraw { .. }
            | SubmitUnwrap { .. }
            | SubmitSend { .. }
            | ConfirmSend
            | PrepareSend
            | ConfirmWithdraw { .. }
            | ConfirmUnwrap { .. }
            | UnwrapTokens
            | RecordSendOutcome { .. }
            | Detect { .. }
            | FailDetection { .. }
            | Complete
            | RejectRedemption { .. }
            | RecoverProviderCompletion { .. }
            | Reconcile { .. }
            | FailTransfer { .. } => Err(EquityRedemptionError::NotStarted),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn transition(
        &self,
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            Redeem { .. } => match self {
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            SubmitWithdraw { tx_hash } => match self {
                Self::VaultWithdrawPending {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount,
                    ..
                } => {
                    info!(target: "rebalance", %token, %wrapped_amount, %tx_hash, "Recording Raindex vault withdrawal submission");
                    Ok(vec![VaultWithdrawSubmitted {
                        symbol: symbol.clone(),
                        quantity: *quantity,
                        token: *token,
                        wrapped_amount: *wrapped_amount,
                        tx_hash,
                        submitted_at: Utc::now(),
                    }])
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            ConfirmWithdraw {
                actual_wrapped_amount,
                raindex_withdraw_block,
            } => match self {
                Self::VaultWithdrawSubmitted { tx_hash, .. } if actual_wrapped_amount.is_zero() => {
                    Err(EquityRedemptionError::ZeroWithdrawAmount { tx_hash: *tx_hash })
                }
                Self::VaultWithdrawSubmitted {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount,
                    tx_hash,
                    ..
                } => Ok(vec![WithdrawnFromRaindex {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    wrapped_amount: *wrapped_amount,
                    actual_wrapped_amount: Some(actual_wrapped_amount),
                    raindex_withdraw_tx: *tx_hash,
                    raindex_withdraw_block: Some(raindex_withdraw_block),
                    withdrawn_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            UnwrapTokens => match self {
                Self::WithdrawnFromRaindex { .. } => Ok(vec![UnwrapPending {
                    pending_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::CannotUnwrapAlreadyStarted),
            },

            SubmitUnwrap { unwrap_tx_hash } => match self {
                Self::UnwrapPending { .. } => Ok(vec![UnwrapSubmitted {
                    unwrap_tx_hash,
                    submitted_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::CannotUnwrapAlreadyStarted),
            },

            ConfirmUnwrap {
                underlying_token,
                unwrapped_amount,
                unwrap_block,
            } => match self {
                Self::UnwrapSubmitted { unwrap_tx_hash, .. } if unwrapped_amount.is_zero() => {
                    Err(EquityRedemptionError::ZeroUnwrapAmount {
                        tx_hash: *unwrap_tx_hash,
                    })
                }
                Self::UnwrapSubmitted { unwrap_tx_hash, .. } => {
                    // Financial precision validation stays at the aggregate
                    // boundary (fail-fast, no defaults).
                    let quantity =
                        Float::from_fixed_decimal(unwrapped_amount, TOKENIZED_EQUITY_DECIMALS)
                            .map_err(|error| {
                                EquityRedemptionError::RaindexWithdrawQuantityConversionFailed {
                                    tx_hash: *unwrap_tx_hash,
                                    amount: unwrapped_amount,
                                    error_message: error.to_string(),
                                }
                            })?;

                    Ok(vec![TokensUnwrapped {
                        quantity: Some(quantity),
                        underlying_token,
                        unwrap_tx_hash: *unwrap_tx_hash,
                        unwrapped_amount,
                        unwrap_block: Some(unwrap_block),
                        unwrapped_at: Utc::now(),
                    }])
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::CannotUnwrapAlreadyStarted),
            },

            PrepareSend => match self {
                Self::TokensUnwrapped { .. } => Ok(vec![SendPending {
                    pending_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::TokensNotUnwrapped),
            },

            SubmitSend {
                redemption_wallet,
                redemption_tx,
            } => match self {
                Self::SendPending { .. } => Ok(vec![SendSubmitted {
                    redemption_wallet,
                    redemption_tx,
                    submitted_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::TokensNotUnwrapped),
            },

            ConfirmSend => match self {
                Self::SendSubmitted {
                    redemption_wallet,
                    redemption_tx,
                    ..
                } => Ok(vec![TokensSent {
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    sent_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::TokensNotSent),
            },

            RecordSendOutcome { outcome } => match self {
                Self::SendPending { symbol, .. } => match outcome {
                    SendOutcome::Sent {
                        redemption_wallet,
                        redemption_tx,
                    } => Ok(vec![TokensSent {
                        redemption_wallet,
                        redemption_tx,
                        sent_at: Utc::now(),
                    }]),
                    SendOutcome::WalletNotConfigured => {
                        warn!(target: "rebalance", %symbol, "Redemption wallet not configured");
                        Ok(vec![TransferFailed {
                            tx_hash: None,
                            reason: None,
                            failed_at: Utc::now(),
                        }])
                    }
                    SendOutcome::SendFailed => {
                        warn!(target: "rebalance", %symbol, "Send for redemption failed");
                        Ok(vec![TransferFailed {
                            tx_hash: None,
                            reason: None,
                            failed_at: Utc::now(),
                        }])
                    }
                },
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::TokensNotUnwrapped),
            },

            Detect {
                tokenization_request_id,
            } => match self {
                Self::TokensSent { .. } => Ok(vec![Detected {
                    tokenization_request_id,
                    detected_at: Utc::now(),
                }]),
                Self::VaultWithdrawPending { .. }
                | Self::VaultWithdrawSubmitted { .. }
                | Self::WithdrawnFromRaindex { .. }
                | Self::UnwrapPending { .. }
                | Self::UnwrapSubmitted { .. }
                | Self::TokensUnwrapped { .. }
                | Self::SendPending { .. }
                | Self::SendSubmitted { .. } => Err(EquityRedemptionError::TokensNotSent),
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            FailDetection { failure } => match self {
                Self::TokensSent { symbol, .. } => {
                    warn!(
                        target: "rebalance",
                        %symbol, ?failure,
                        "Marking redemption as detection-failed"
                    );
                    Ok(vec![DetectionFailed {
                        failure,
                        failed_at: Utc::now(),
                    }])
                }
                Self::VaultWithdrawPending { .. }
                | Self::VaultWithdrawSubmitted { .. }
                | Self::WithdrawnFromRaindex { .. }
                | Self::UnwrapPending { .. }
                | Self::UnwrapSubmitted { .. }
                | Self::TokensUnwrapped { .. }
                | Self::SendPending { .. }
                | Self::SendSubmitted { .. } => Err(EquityRedemptionError::TokensNotSent),
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            Complete => match self {
                Self::VaultWithdrawPending { .. }
                | Self::VaultWithdrawSubmitted { .. }
                | Self::WithdrawnFromRaindex { .. }
                | Self::UnwrapPending { .. }
                | Self::UnwrapSubmitted { .. }
                | Self::TokensUnwrapped { .. }
                | Self::SendPending { .. }
                | Self::SendSubmitted { .. }
                | Self::TokensSent { .. } => Err(EquityRedemptionError::NotPending),
                Self::Pending { .. } => Ok(vec![Completed {
                    completed_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
            },

            RejectRedemption { reason } => match self {
                Self::VaultWithdrawPending { .. }
                | Self::VaultWithdrawSubmitted { .. }
                | Self::WithdrawnFromRaindex { .. }
                | Self::UnwrapPending { .. }
                | Self::UnwrapSubmitted { .. }
                | Self::TokensUnwrapped { .. }
                | Self::SendPending { .. }
                | Self::SendSubmitted { .. }
                | Self::TokensSent { .. } => Err(EquityRedemptionError::NotPendingForRejection),
                Self::Pending { symbol, .. } => {
                    warn!(
                        target: "rebalance",
                        %symbol, %reason,
                        "Rejecting detected redemption"
                    );
                    Ok(vec![RedemptionRejected {
                        reason,
                        rejected_at: Utc::now(),
                    }])
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
            },

            RecoverProviderCompletion {
                tokenization_request_id,
            } => match self {
                Self::Failed {
                    redemption_tx: Some(_),
                    ..
                } => Ok(vec![ProviderCompletionRecovered {
                    tokenization_request_id,
                    recovered_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::TokensNotSent),
            },

            FailTransfer { reason } => match self {
                Self::VaultWithdrawPending { symbol, .. }
                | Self::VaultWithdrawSubmitted { symbol, .. }
                | Self::WithdrawnFromRaindex { symbol, .. }
                | Self::UnwrapPending { symbol, .. }
                | Self::UnwrapSubmitted { symbol, .. }
                | Self::TokensUnwrapped { symbol, .. }
                | Self::SendPending { symbol, .. } => {
                    warn!(
                        target: "rebalance",
                        %symbol, %reason,
                        "Marking redemption as transfer-failed"
                    );
                    Ok(vec![TransferFailed {
                        tx_hash: None,
                        reason: Some(reason),
                        failed_at: Utc::now(),
                    }])
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            Reconcile { reason } => match self {
                Self::Failed { symbol, .. } => {
                    if reason.trim().is_empty() {
                        return Err(EquityRedemptionError::ReconcileReasonRequired);
                    }

                    warn!(
                        target: "rebalance",
                        %symbol, %reason,
                        "Reconciling stuck failed redemption out-of-band"
                    );
                    Ok(vec![OperatorReconciled {
                        reason,
                        reconciled_at: Utc::now(),
                    }])
                }
                Self::Reconciled { .. } => Err(EquityRedemptionError::AlreadyReconciled),
                _ => Err(EquityRedemptionError::NotFailed),
            },
        }
    }
}

/// Returns symbols and quantities from `EquityRedemption` aggregates that
/// ended in `DetectionFailed` or `RedemptionRejected`.
///
/// `DetectionFailed` leaves tokens physically in Alpaca's redemption wallet.
/// `RedemptionRejected` has uncertain disposition -- tokens may or may not
/// have been returned. In both cases, no snapshot source tracks the actual
/// location. The caller should set their inflight balance directly so the
/// system does not re-trigger redemptions for tokens it no longer holds.
pub(crate) async fn symbols_with_stuck_redemptions(
    pool: &SqlitePool,
) -> Result<HashMap<Symbol, FractionalShares>, StuckRedemptionRecoveryError> {
    type StuckRedemptionRow = (String, Option<String>, Option<String>, Option<String>);

    let rows: Vec<StuckRedemptionRow> = sqlx::query_as(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
             GROUP BY aggregate_id \
         ), latest_withdrawn AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
               AND event_type = 'EquityRedemptionEvent::WithdrawnFromRaindex' \
             GROUP BY aggregate_id \
         ), latest_unwrapped AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
               AND event_type = 'EquityRedemptionEvent::TokensUnwrapped' \
             GROUP BY aggregate_id \
         ), latest_sent AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
               AND event_type = 'EquityRedemptionEvent::TokensSent' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id, \
                COALESCE( \
                    json_extract(first_ev.payload, '$.VaultWithdrawPending.symbol'), \
                    json_extract(first_ev.payload, '$.VaultWithdrawSubmitted.symbol'), \
                    json_extract(first_ev.payload, '$.WithdrawnFromRaindex.symbol') \
                ), \
                COALESCE( \
                    json_extract(sent_ev.payload, '$.TokensSent.quantity'), \
                    json_extract(unwrapped_ev.payload, '$.TokensUnwrapped.quantity'), \
                    json_extract(first_ev.payload, '$.VaultWithdrawPending.quantity'), \
                    json_extract(first_ev.payload, '$.VaultWithdrawSubmitted.quantity'), \
                    json_extract(first_ev.payload, '$.WithdrawnFromRaindex.quantity') \
                ), \
                json_extract(unwrapped_ev.payload, '$.TokensUnwrapped.unwrapped_amount') \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         INNER JOIN events first_ev \
             ON first_ev.aggregate_type = 'EquityRedemption' \
            AND first_ev.aggregate_id = latest.aggregate_id \
            AND first_ev.sequence = 0 \
         LEFT JOIN latest_withdrawn \
             ON latest_withdrawn.aggregate_id = latest.aggregate_id \
         LEFT JOIN events withdrawn_ev \
             ON withdrawn_ev.aggregate_type = 'EquityRedemption' \
            AND withdrawn_ev.aggregate_id = latest.aggregate_id \
            AND withdrawn_ev.sequence = latest_withdrawn.max_seq \
         LEFT JOIN latest_unwrapped \
             ON latest_unwrapped.aggregate_id = latest.aggregate_id \
         LEFT JOIN events unwrapped_ev \
             ON unwrapped_ev.aggregate_type = 'EquityRedemption' \
            AND unwrapped_ev.aggregate_id = latest.aggregate_id \
            AND unwrapped_ev.sequence = latest_unwrapped.max_seq \
         LEFT JOIN latest_sent \
             ON latest_sent.aggregate_id = latest.aggregate_id \
         LEFT JOIN events sent_ev \
             ON sent_ev.aggregate_type = 'EquityRedemption' \
            AND sent_ev.aggregate_id = latest.aggregate_id \
            AND sent_ev.sequence = latest_sent.max_seq \
         WHERE last_ev.aggregate_type = 'EquityRedemption' \
           AND last_ev.event_type IN ( \
               'EquityRedemptionEvent::DetectionFailed', \
               'EquityRedemptionEvent::RedemptionRejected' \
           )",
    )
    .fetch_all(pool)
    .await?;

    let mut result: HashMap<Symbol, FractionalShares> = HashMap::new();

    for (raw_aggregate_id, raw_symbol, raw_quantity, raw_wrapped_amount) in rows {
        let Ok(aggregate_id) = RedemptionAggregateId::from_str(&raw_aggregate_id) else {
            warn!(target: "rebalance",
                %raw_aggregate_id,
                "Stuck redemption has invalid aggregate id, skipping"
            );
            continue;
        };

        let Some(symbol) = parse_stuck_symbol(&raw_aggregate_id, raw_symbol) else {
            continue;
        };

        let Some(quantity) = parse_stuck_quantity(aggregate_id, raw_quantity, raw_wrapped_amount)?
        else {
            continue;
        };

        let entry = result.entry(symbol).or_insert(FractionalShares::ZERO);
        match *entry + quantity {
            Ok(sum) => *entry = sum,
            Err(error) => {
                warn!(target: "rebalance",
                    %error,
                    %raw_aggregate_id,
                    "Float overflow summing stuck redemption quantities, \
                     keeping accumulated value"
                );
            }
        }
    }

    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StuckRedemptionRecoveryError {
    #[error(transparent)]
    Persistence(#[from] sqlx::Error),
    #[error("stuck redemption {aggregate_id} has invalid actual wrapped amount")]
    InvalidActualWrappedAmount { aggregate_id: RedemptionAggregateId },
    #[error("stuck redemption {aggregate_id} has invalid requested quantity")]
    InvalidRequestedQuantity { aggregate_id: RedemptionAggregateId },
}

/// Returns the set of symbols that have at least one in-progress
/// EquityRedemption aggregate (i.e. an equity transfer is in progress).
///
/// Only `EquityRedemption` aggregates are checked, not `TokenizedEquityMint`,
/// because mints do not move tokens out of Raindex vaults. A mint in flight
/// does not create the same vault-drain risk that a redemption does, so
/// suppressing hedges during mints would be overly conservative.
///
/// Uses an explicit allowlist of active event types (not a denylist of terminal
/// events) so that adding a new event variant does not silently suppress hedges
/// until the SQL is updated.
///
/// Queries the event store directly so the result is durable across
/// restarts -- no runtime state required.
pub(crate) async fn symbols_with_active_transfers(
    pool: &SqlitePool,
) -> Result<HashSet<Symbol>, sqlx::Error> {
    let rows: Vec<(Option<String>,)> = sqlx::query_as(
        "
        WITH latest AS (
            SELECT aggregate_id, MAX(sequence) AS max_seq
            FROM events
            WHERE aggregate_type = 'EquityRedemption'
            GROUP BY aggregate_id
        )
        SELECT DISTINCT COALESCE(
               json_extract(first_ev.payload, '$.VaultWithdrawPending.symbol'),
               json_extract(first_ev.payload, '$.VaultWithdrawSubmitted.symbol'),
               json_extract(first_ev.payload, '$.WithdrawnFromRaindex.symbol')
        )
        FROM events last_ev
        INNER JOIN latest
            ON last_ev.aggregate_id = latest.aggregate_id
           AND last_ev.sequence = latest.max_seq
        INNER JOIN events first_ev
            ON first_ev.aggregate_type = 'EquityRedemption'
           AND first_ev.aggregate_id = latest.aggregate_id
           AND first_ev.sequence = 0
        WHERE last_ev.aggregate_type = 'EquityRedemption'
          AND last_ev.event_type IN (
              'EquityRedemptionEvent::VaultWithdrawPending',
              'EquityRedemptionEvent::VaultWithdrawSubmitted',
              'EquityRedemptionEvent::WithdrawnFromRaindex',
              'EquityRedemptionEvent::UnwrapPending',
              'EquityRedemptionEvent::UnwrapSubmitted',
              'EquityRedemptionEvent::TokensUnwrapped',
              'EquityRedemptionEvent::SendPending',
              'EquityRedemptionEvent::TokensSent',
              'EquityRedemptionEvent::Detected'
          )
        ",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(raw_symbol,)| {
            let value = raw_symbol.or_else(|| {
                warn!(target: "rebalance", "Active transfer has NULL symbol in payload, skipping");
                None
            })?;

            Symbol::new(&value)
                .inspect_err(|error| {
                    warn!(target: "rebalance", %error, raw_symbol = %value, "Active transfer has invalid symbol, skipping");
                })
                .ok()
        })
        .collect())
}

/// Returns redemption aggregate IDs whose latest event is non-terminal and
/// should be resumed after restart.
pub(crate) async fn interrupted_redemption_ids(
    pool: &SqlitePool,
) -> Result<Vec<RedemptionAggregateId>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = 'EquityRedemption' \
           AND last_ev.event_type IN ( \
               'EquityRedemptionEvent::VaultWithdrawPending', \
               'EquityRedemptionEvent::VaultWithdrawSubmitted', \
               'EquityRedemptionEvent::WithdrawnFromRaindex', \
               'EquityRedemptionEvent::UnwrapPending', \
               'EquityRedemptionEvent::UnwrapSubmitted', \
               'EquityRedemptionEvent::TokensUnwrapped', \
               'EquityRedemptionEvent::SendPending', \
               'EquityRedemptionEvent::TokensSent', \
               'EquityRedemptionEvent::Detected' \
           ) \
         ORDER BY latest.aggregate_id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|aggregate_id| {
            aggregate_id
                .parse::<RedemptionAggregateId>()
                .inspect_err(|error| {
                    warn!(target: "rebalance",
                        %error,
                        %aggregate_id,
                        "Interrupted redemption has invalid aggregate id, skipping"
                    );
                })
                .ok()
        })
        .collect())
}

fn parse_stuck_symbol(aggregate_id: &str, raw: Option<String>) -> Option<Symbol> {
    let value = raw.or_else(|| {
        warn!(target: "rebalance",
            %aggregate_id,
            "Stuck redemption has NULL symbol in \
             WithdrawnFromRaindex payload, skipping"
        );
        None
    })?;

    Symbol::new(&value)
        .inspect_err(|error| {
            warn!(target: "rebalance",
                %error,
                %aggregate_id,
                raw_symbol = %value,
                "Stuck redemption has invalid symbol, skipping"
            );
        })
        .ok()
}

fn parse_stuck_quantity(
    aggregate_id: RedemptionAggregateId,
    raw_quantity: Option<String>,
    raw_wrapped_amount: Option<String>,
) -> Result<Option<FractionalShares>, StuckRedemptionRecoveryError> {
    if raw_quantity.is_some() {
        return parse_requested_stuck_quantity(aggregate_id, raw_quantity);
    }

    if let Some(value) = raw_wrapped_amount {
        let Ok(amount) = U256::from_str(&value) else {
            return Err(StuckRedemptionRecoveryError::InvalidActualWrappedAmount { aggregate_id });
        };
        let Ok(quantity) = Float::from_fixed_decimal(amount, TOKENIZED_EQUITY_DECIMALS) else {
            return Err(StuckRedemptionRecoveryError::InvalidActualWrappedAmount { aggregate_id });
        };
        return Ok(Some(FractionalShares::new(quantity)));
    }

    parse_requested_stuck_quantity(aggregate_id, raw_quantity)
}

fn parse_requested_stuck_quantity(
    aggregate_id: RedemptionAggregateId,
    raw: Option<String>,
) -> Result<Option<FractionalShares>, StuckRedemptionRecoveryError> {
    let Some(value) = raw.or_else(|| {
        warn!(target: "rebalance",
            %aggregate_id,
            "Stuck redemption has NULL quantity in \
             WithdrawnFromRaindex payload, skipping"
        );
        None
    }) else {
        return Ok(None);
    };

    let quantity = Float::parse(value)
        .map_err(|_| StuckRedemptionRecoveryError::InvalidRequestedQuantity { aggregate_id })?;

    Ok(Some(FractionalShares::new(quantity)))
}

#[cfg(test)]
mod tests {
    use st0x_dto::EquityRedemptionStatus;
    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};
    use st0x_float_macro::float;

    use super::*;

    fn withdrawn_from_raindex_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            actual_wrapped_amount: None,
            raindex_withdraw_tx: TxHash::random(),
            raindex_withdraw_block: None,
            withdrawn_at: Utc::now(),
        }
    }

    fn tokens_sent_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        }
    }

    fn tokens_unwrapped_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokensUnwrapped {
            quantity: Some(float!(50.25)),
            underlying_token: Address::random(),
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrap_block: None,
            unwrapped_at: Utc::now(),
        }
    }

    fn detected_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        }
    }

    /// Event history for a redemption stranded in the terminal `Failed` state
    /// (withdrawn, tokens sent to Alpaca, then detection timed out).
    fn failed_redemption_history() -> Vec<EquityRedemptionEvent> {
        vec![
            withdrawn_from_raindex_event(),
            tokens_sent_event(),
            EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::Timeout,
                failed_at: Utc::now(),
            },
        ]
    }

    #[tokio::test]
    async fn redeem_from_uninitialized_produces_vault_withdraw_pending() {
        let events = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::Redeem {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(50.25),
                token: Address::random(),
                amount: U256::from(50_250_000_000_000_000_000_u128),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::VaultWithdrawPending { .. }
        ));
    }

    #[tokio::test]
    async fn detect_after_tokens_sent_produces_detected() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event(), tokens_sent_event()])
            .when(EquityRedemptionCommand::Detect {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Detected { .. }));
    }

    #[tokio::test]
    async fn complete_from_pending_produces_completed() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_sent_event(),
                detected_event(),
            ])
            .when(EquityRedemptionCommand::Complete)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn complete_redemption_flow_end_to_end() {
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("end-to-end");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(50.25),
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
                    raindex_withdraw_block: 100,
                },
            )
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitUnwrap {
                    unwrap_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmUnwrap {
                    underlying_token: Address::random(),
                    unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
                    unwrap_block: 101,
                },
            )
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::PrepareSend)
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::RecordSendOutcome {
                    outcome: SendOutcome::Sent {
                        redemption_wallet: Address::random(),
                        redemption_tx: TxHash::random(),
                    },
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
            )
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::Complete)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(entity, EquityRedemption::Completed { .. }));
    }

    /// The recorded `TokensSent` state must carry the underlying token (carried
    /// by `ConfirmUnwrap`), not the wrapped token the redemption started with.
    #[tokio::test]
    async fn send_records_underlying_token_not_wrapped_token() {
        let wrapped_token = Address::random();
        let underlying_token = Address::random();
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("underlying-token-fix");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    token: wrapped_token,
                    amount: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount: U256::from(10_000_000_000_000_000_000_u128),
                    raindex_withdraw_block: 100,
                },
            )
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitUnwrap {
                    unwrap_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmUnwrap {
                    underlying_token,
                    unwrapped_amount: U256::from(10_000_000_000_000_000_000_u128),
                    unwrap_block: 101,
                },
            )
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::PrepareSend)
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::RecordSendOutcome {
                    outcome: SendOutcome::Sent {
                        redemption_wallet: Address::random(),
                        redemption_tx: TxHash::random(),
                    },
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        let EquityRedemption::TokensSent { token, .. } = entity else {
            panic!("Expected TokensSent state, got: {entity:?}");
        };

        assert_eq!(
            token, underlying_token,
            "Recorded send should use the underlying token, not the wrapped token"
        );
    }

    /// The `WithdrawnFromRaindex` event preserves both the requested
    /// `wrapped_amount` and the receipt's `actual_wrapped_amount` for replay
    /// fidelity, but the resulting state collapses to the actual amount so the
    /// downstream unwrap operates on what was really withdrawn (receipt
    /// decoding itself is covered by `step.rs`).
    #[tokio::test]
    async fn confirm_withdraw_collapses_state_amount_to_actual_receipt_amount() {
        let requested = U256::from(37_143_292_455_000_000_000_u128);
        let actual = U256::from(33_681_456_848_531_939_569_u128);
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("partial-withdraw");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("COIN").unwrap(),
                    quantity: float!(37.143292455),
                    token: Address::random(),
                    amount: requested,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount: actual,
                    raindex_withdraw_block: 100,
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        let EquityRedemption::WithdrawnFromRaindex { wrapped_amount, .. } = entity else {
            panic!("Expected WithdrawnFromRaindex state, got: {entity:?}");
        };

        assert_eq!(
            wrapped_amount, actual,
            "Confirmed-withdrawal state should carry the actual receipt transfer amount"
        );
    }

    #[tokio::test]
    async fn confirm_withdraw_rejects_zero_actual_amount() {
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("zero-withdraw");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("COIN").unwrap(),
                    quantity: float!(10),
                    token: Address::random(),
                    amount: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount: U256::ZERO,
                    raindex_withdraw_block: 100,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::ZeroWithdrawAmount { .. }
                ))
            ),
            "a zero actual withdrawn amount must be rejected, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn confirm_unwrap_rejects_zero_amount() {
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("zero-unwrap");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("COIN").unwrap(),
                    quantity: float!(10),
                    token: Address::random(),
                    amount: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount: U256::from(10_000_000_000_000_000_000_u128),
                    raindex_withdraw_block: 100,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();
        store
            .send(
                &id,
                EquityRedemptionCommand::SubmitUnwrap {
                    unwrap_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                EquityRedemptionCommand::ConfirmUnwrap {
                    underlying_token: Address::random(),
                    unwrapped_amount: U256::ZERO,
                    unwrap_block: 101,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::ZeroUnwrapAmount { .. }
                ))
            ),
            "a zero unwrapped amount must be rejected, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cannot_detect_before_sending_tokens() {
        let error = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::Detect {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotStarted)
        ));
    }

    #[tokio::test]
    async fn cannot_complete_before_pending() {
        let error = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::Complete)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotStarted)
        ));
    }

    #[tokio::test]
    async fn fail_detection_from_tokens_sent_state() {
        let history = vec![withdrawn_from_raindex_event(), tokens_sent_event()];

        let events = TestHarness::<EquityRedemption>::with()
            .given(history.clone())
            .when(EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Timeout,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::Timeout,
                ..
            }
        ));

        let state = replay::<EquityRedemption>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let EquityRedemption::Failed { reason, .. } = state else {
            panic!("timeout detection failure must terminate in Failed, got {state:?}");
        };
        assert_eq!(
            reason, None,
            "automated timeout failures carry no operator reason",
        );
    }

    #[test]
    fn detection_failure_operator_serde_roundtrip() {
        let failure = DetectionFailure::Operator {
            reason: "ticket 42".to_string(),
        };

        let json = serde_json::to_string(&failure).unwrap();
        assert_eq!(json, r#"{"Operator":{"reason":"ticket 42"}}"#);

        let back: DetectionFailure = serde_json::from_str(&json).unwrap();
        assert_eq!(back, failure);
    }

    #[test]
    fn detection_failed_operator_event_deserializes_from_raw_json() {
        let raw = r#"{"DetectionFailed":{"failure":{"Operator":{"reason":"ticket 42"}},"failed_at":"2026-01-01T00:00:00Z"}}"#;

        let event: EquityRedemptionEvent = serde_json::from_str(raw).unwrap();
        let EquityRedemptionEvent::DetectionFailed { failure, .. } = event else {
            panic!("expected DetectionFailed, got {event:?}");
        };
        assert_eq!(
            failure,
            DetectionFailure::Operator {
                reason: "ticket 42".to_string(),
            },
        );
    }

    #[tokio::test]
    async fn fail_detection_operator_from_tokens_sent_reaches_failed() {
        // Operator force-fail of a redemption stuck in TokensSent: unlike an
        // automated `Timeout`, the `Operator` failure carries the operator's
        // reason, which must be persisted on the event and recoverable from
        // the replayed `Failed` state.
        let history = vec![withdrawn_from_raindex_event(), tokens_sent_event()];

        let events = TestHarness::<EquityRedemption>::with()
            .given(history.clone())
            .when(EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Operator {
                    reason: "tokens stuck at Alpaca, support ticket 42".to_string(),
                },
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let EquityRedemptionEvent::DetectionFailed { failure, .. } = &events[0] else {
            panic!("expected DetectionFailed, got {:?}", events[0]);
        };
        assert_eq!(
            *failure,
            DetectionFailure::Operator {
                reason: "tokens stuck at Alpaca, support ticket 42".to_string(),
            },
        );

        let state = replay::<EquityRedemption>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let EquityRedemption::Failed { reason, .. } = state else {
            panic!("operator detection failure must terminate in Failed, got {state:?}");
        };
        assert_eq!(
            reason.as_deref(),
            Some("tokens stuck at Alpaca, support ticket 42"),
            "operator reason must be recoverable from the replayed Failed state",
        );
    }

    #[tokio::test]
    async fn reject_redemption_from_pending_state() {
        let history = vec![
            withdrawn_from_raindex_event(),
            tokens_sent_event(),
            detected_event(),
        ];

        let events = TestHarness::<EquityRedemption>::with()
            .given(history.clone())
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "test rejection".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let EquityRedemptionEvent::RedemptionRejected { reason, .. } = &events[0] else {
            panic!("expected RedemptionRejected, got {:?}", events[0]);
        };
        assert_eq!(reason, "test rejection");

        let state = replay::<EquityRedemption>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let EquityRedemption::Failed { reason, .. } = state else {
            panic!("rejected redemption must terminate in Failed, got {state:?}");
        };
        assert_eq!(
            reason.as_deref(),
            Some("test rejection"),
            "rejection reason must be recoverable from the replayed Failed state",
        );
    }

    #[tokio::test]
    async fn cannot_reject_redemption_before_pending() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event(), tokens_sent_event()])
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "test rejection".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotPendingForRejection)
        ));
    }

    #[test]
    fn redemption_rejected_preserves_context_with_tokenization_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_tx = TxHash::random();

        let entity = replay::<EquityRedemption>(vec![
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol.clone(),
                quantity: float!(50.25),
                token: Address::random(),
                wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
                actual_wrapped_amount: None,
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: None,
                withdrawn_at: Utc::now(),
            },
            EquityRedemptionEvent::TokensSent {
                redemption_wallet: Address::random(),
                redemption_tx,
                sent_at: Utc::now(),
            },
            EquityRedemptionEvent::Detected {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                detected_at: Utc::now(),
            },
            EquityRedemptionEvent::RedemptionRejected {
                reason: "test rejection".to_string(),
                rejected_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        let EquityRedemption::Failed {
            symbol: failed_symbol,
            quantity,
            redemption_tx: failed_redemption_tx,
            tokenization_request_id,
            ..
        } = entity
        else {
            panic!("Expected Failed state, got {entity:?}");
        };

        assert_eq!(failed_symbol, symbol);
        assert!(quantity.eq(float!(50.25)).unwrap());
        assert_eq!(failed_redemption_tx, Some(redemption_tx));
        assert_eq!(
            tokenization_request_id,
            Some(TokenizationRequestId("REQ789".to_string()))
        );
    }

    #[tokio::test]
    async fn cannot_fail_detection_before_sending() {
        let error = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Timeout,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotStarted)
        ));
    }

    #[tokio::test]
    async fn cannot_reject_redemption_before_sending() {
        let error = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "test rejection".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotStarted)
        ));
    }

    #[test]
    fn test_evolve_detected_rejects_wrong_state() {
        let now = Utc::now();
        let completed = EquityRedemption::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            started_at: now,
            completed_at: now,
        };

        let event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ999".to_string()),
            detected_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&completed, &event).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_evolve_completed_rejects_wrong_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            token: Address::random(),
            raindex_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: None,
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::Completed {
            completed_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&tokens_sent, &event).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_evolve_detection_failed_rejects_non_tokens_sent_states() {
        let pending = EquityRedemption::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            sent_at: Utc::now(),
            detected_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&pending, &event).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_evolve_redemption_rejected_rejects_non_pending_states() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            token: Address::random(),
            raindex_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: None,
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::RedemptionRejected {
            reason: "test rejection".to_string(),
            rejected_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&tokens_sent, &event).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_evolve_rejects_tokens_sent_event_on_live_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            token: Address::random(),
            raindex_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: None,
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&tokens_sent, &event).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_originate_rejects_non_init_events() {
        let event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };

        let result = EquityRedemption::originate(&event);
        assert!(result.is_none());
    }

    /// A `SendFailed` outcome (the orchestrator's mapping of a send revert)
    /// records `TransferFailed`. The orchestrator's error-to-outcome mapping is
    /// covered separately in `rebalancing::equity`'s `send_to_alpaca` tests.
    #[tokio::test]
    async fn record_send_failed_outcome_emits_transfer_failed() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_unwrapped_event(),
                EquityRedemptionEvent::SendPending {
                    pending_at: Utc::now(),
                },
            ])
            .when(EquityRedemptionCommand::RecordSendOutcome {
                outcome: SendOutcome::SendFailed,
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "expected exactly one event");
        assert!(
            matches!(events[0], EquityRedemptionEvent::TransferFailed { .. }),
            "expected TransferFailed, got {:?}",
            events[0]
        );
    }

    /// A `WalletNotConfigured` outcome (no redemption wallet) records
    /// `TransferFailed`.
    #[tokio::test]
    async fn record_wallet_not_configured_outcome_emits_transfer_failed() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_unwrapped_event(),
                EquityRedemptionEvent::SendPending {
                    pending_at: Utc::now(),
                },
            ])
            .when(EquityRedemptionCommand::RecordSendOutcome {
                outcome: SendOutcome::WalletNotConfigured,
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "expected exactly one event");
        assert!(
            matches!(events[0], EquityRedemptionEvent::TransferFailed { .. }),
            "expected TransferFailed, got {:?}",
            events[0]
        );
    }

    #[tokio::test]
    async fn redeem_when_already_started_returns_already_started() {
        let store = TestStore::<EquityRedemption>::new();
        let id = redemption_aggregate_id("redemption-1");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    token: Address::random(),
                    amount: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    token: Address::random(),
                    amount: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(EquityRedemptionError::AlreadyStarted))
        ));
    }

    #[tokio::test]
    async fn redeem_when_pending_returns_already_started() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_sent_event(),
                detected_event(),
            ])
            .when(EquityRedemptionCommand::Redeem {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(10),
                token: Address::random(),
                amount: U256::from(10_000_000_000_000_000_000_u128),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::AlreadyStarted)
        ));
    }

    /// Insert a minimal event row into the events table.
    async fn insert_event(
        pool: &SqlitePool,
        aggregate_id: &RedemptionAggregateId,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, \
              event_version, payload, metadata) \
             VALUES ('EquityRedemption', ?1, ?2, ?3, '1', ?4, '{}')",
        )
        .bind(aggregate_id.to_string())
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    fn withdrawn_payload(symbol: &str) -> String {
        format!(
            r#"{{"WithdrawnFromRaindex":{{"symbol":"{symbol}","quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","raindex_withdraw_tx":"0x0000000000000000000000000000000000000000000000000000000000000001","withdrawn_at":"2026-01-01T00:00:00Z"}}}}"#
        )
    }

    fn tokens_unwrapped_payload(quantity: &str, unwrapped_amount: &str) -> String {
        format!(
            r#"{{"TokensUnwrapped":{{"quantity":"{quantity}","underlying_token":"0x0000000000000000000000000000000000000002","unwrap_tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","unwrapped_amount":"{unwrapped_amount}","unwrapped_at":"2026-01-01T00:00:00Z"}}}}"#
        )
    }

    #[tokio::test]
    async fn stuck_redemptions_returns_detection_failed_symbols() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: WithdrawnFromRaindex -> DetectionFailed (stuck)
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-1"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-1"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(result.len(), 1);
        let aapl = Symbol::new("AAPL").unwrap();
        assert!(result.contains_key(&aapl));
        assert!(
            result[&aapl].inner().eq(float!("10")).unwrap(),
            "Recovered quantity should be 10, got {:?}",
            result[&aapl]
        );
    }

    #[tokio::test]
    async fn stuck_redemptions_uses_actual_unwrapped_quantity_when_available() {
        let pool = crate::test_utils::setup_test_db().await;

        insert_event(
            &pool,
            &redemption_aggregate_id("partial-redemption"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("COIN"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("partial-redemption"),
            1,
            "EquityRedemptionEvent::TokensUnwrapped",
            &tokens_unwrapped_payload("7.5", "7500000000000000000"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("partial-redemption"),
            2,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(result.len(), 1);
        let coin = Symbol::new("COIN").unwrap();
        assert!(
            result[&coin].inner().eq(float!("7.5")).unwrap(),
            "Recovered quantity should use actual unwrapped quantity, got {:?}",
            result[&coin]
        );
    }

    #[tokio::test]
    async fn stuck_redemptions_errors_on_invalid_unwrapped_quantity() {
        let pool = crate::test_utils::setup_test_db().await;

        insert_event(
            &pool,
            &redemption_aggregate_id("invalid-actual"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("COIN"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("invalid-actual"),
            1,
            "EquityRedemptionEvent::TokensUnwrapped",
            &tokens_unwrapped_payload("invalid", "invalid"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("invalid-actual"),
            2,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let err = symbols_with_stuck_redemptions(&pool).await.unwrap_err();
        assert!(matches!(
            err,
            StuckRedemptionRecoveryError::InvalidRequestedQuantity { .. }
                | StuckRedemptionRecoveryError::InvalidActualWrappedAmount { .. }
        ));
    }

    #[tokio::test]
    async fn stuck_redemptions_returns_rejection_symbols() {
        let pool = crate::test_utils::setup_test_db().await;

        // TSLA: WithdrawnFromRaindex -> RedemptionRejected (stuck)
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-2"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-2"),
            1,
            "EquityRedemptionEvent::RedemptionRejected",
            r#"{"RedemptionRejected":{"reason":"test","rejected_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(result.len(), 1);
        let tsla = Symbol::new("TSLA").unwrap();
        assert!(result.contains_key(&tsla));
        assert!(
            result[&tsla].inner().eq(float!("10")).unwrap(),
            "Recovered quantity should be 10, got {:?}",
            result[&tsla]
        );
    }

    #[tokio::test]
    async fn stuck_redemptions_excludes_completed_and_transfer_failed() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: DetectionFailed (stuck)
        insert_event(
            &pool,
            &redemption_aggregate_id("stuck"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("stuck"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // MSFT: Completed (not stuck)
        insert_event(
            &pool,
            &redemption_aggregate_id("completed"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("MSFT"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("completed"),
            1,
            "EquityRedemptionEvent::Completed",
            r#"{"Completed":{"completed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // GOOG: TransferFailed (not stuck — tokens still in our wallet)
        insert_event(
            &pool,
            &redemption_aggregate_id("transfer-failed"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("GOOG"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("transfer-failed"),
            1,
            "EquityRedemptionEvent::TransferFailed",
            r#"{"TransferFailed":{"tx_hash":null,"failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(result.len(), 1, "Only AAPL should be stuck: {result:?}");
        let aapl = Symbol::new("AAPL").unwrap();
        assert!(result.contains_key(&aapl));
        assert!(
            result[&aapl].inner().eq(float!("10")).unwrap(),
            "Recovered quantity should be 10, got {:?}",
            result[&aapl]
        );
    }

    /// A redemption that was `DetectionFailed` and then operator-reconciled must
    /// not re-seed stranded inflight at the next startup: its latest event is
    /// `OperatorReconciled`, which the query's terminal allowlist excludes.
    #[tokio::test]
    async fn stuck_redemptions_excludes_operator_reconciled() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: DetectionFailed, still stuck.
        insert_event(
            &pool,
            &redemption_aggregate_id("stuck"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("stuck"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // MSFT: DetectionFailed then operator-reconciled, no longer stuck.
        insert_event(
            &pool,
            &redemption_aggregate_id("reconciled"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("MSFT"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("reconciled"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("reconciled"),
            2,
            "EquityRedemptionEvent::OperatorReconciled",
            r#"{"OperatorReconciled":{"reason":"deposited manually via vault-deposit","reconciled_at":"2026-01-01T00:01:00Z"}}"#,
        )
        .await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(
            result.len(),
            1,
            "only the un-reconciled DetectionFailed redemption should seed inflight: {result:?}"
        );
        assert!(result.contains_key(&Symbol::new("AAPL").unwrap()));
        assert!(
            !result.contains_key(&Symbol::new("MSFT").unwrap()),
            "an operator-reconciled redemption must not re-seed stranded inflight"
        );
    }

    #[tokio::test]
    async fn interrupted_redemption_ids_returns_only_non_terminal_redemptions() {
        let pool = crate::test_utils::setup_test_db().await;

        insert_event(
            &pool,
            &redemption_aggregate_id("resume-me"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("resume-me"),
            1,
            "EquityRedemptionEvent::TokensSent",
            r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000001","redemption_tx":"0x0000000000000000000000000000000000000000000000000000000000000002","sent_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        insert_event(
            &pool,
            &redemption_aggregate_id("completed"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("completed"),
            1,
            "EquityRedemptionEvent::Completed",
            r#"{"Completed":{"completed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = interrupted_redemption_ids(&pool).await.unwrap();
        assert_eq!(result, vec![redemption_aggregate_id("resume-me")]);
    }

    #[tokio::test]
    async fn stuck_redemptions_empty_when_no_events() {
        let pool = crate::test_utils::setup_test_db().await;

        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn stuck_redemptions_recovers_valid_symbol_alongside_malformed_rows() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: valid stuck redemption (DetectionFailed)
        insert_event(
            &pool,
            &redemption_aggregate_id("valid-stuck"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("valid-stuck"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // NULL symbol: malformed WithdrawnFromRaindex payload missing symbol
        insert_event(
            &pool,
            &redemption_aggregate_id("null-symbol"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            r#"{"WithdrawnFromRaindex":{"quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","raindex_withdraw_tx":"0x0000000000000000000000000000000000000000000000000000000000000001","withdrawn_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("null-symbol"),
            1,
            "EquityRedemptionEvent::RedemptionRejected",
            r#"{"RedemptionRejected":{"reason":"test","rejected_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // Invalid symbol: symbol fails Symbol::new validation
        insert_event(
            &pool,
            &redemption_aggregate_id("invalid-symbol"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload(""),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("invalid-symbol"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // Only AAPL should be recovered; the NULL and invalid rows are
        // skipped with warnings.
        let result = symbols_with_stuck_redemptions(&pool).await.unwrap();
        assert_eq!(
            result.len(),
            1,
            "Only valid AAPL should be recovered, got: {result:?}"
        );
        let aapl = Symbol::new("AAPL").unwrap();
        assert!(result.contains_key(&aapl));
        assert!(
            result[&aapl].inner().eq(float!("10")).unwrap(),
            "Recovered quantity should be 10, got {:?}",
            result[&aapl]
        );
    }

    #[test]
    fn to_dto_maps_in_progress_variants() {
        let id = redemption_aggregate_id("REDEEM-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);

        let withdrawn = EquityRedemption::WithdrawnFromRaindex {
            symbol: symbol.clone(),
            quantity: float!(50.25),
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            raindex_withdraw_tx: TxHash::random(),
            raindex_withdraw_block: None,
            withdrawn_at: now,
        };
        let TransferOperation::EquityRedemption(op) = withdrawn.to_dto(&id) else {
            panic!(
                "Expected EquityRedemption, got: {:?}",
                withdrawn.to_dto(&id)
            );
        };
        assert_eq!(op.id, Id::new(id.to_string()));
        assert_eq!(op.symbol, symbol);
        assert_eq!(op.quantity, FractionalShares::new(float!(50.25)));
        assert!(
            matches!(op.status, EquityRedemptionStatus::Withdrawing),
            "Expected Withdrawing, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, now);

        let unwrapped = EquityRedemption::TokensUnwrapped {
            symbol: symbol.clone(),
            quantity: float!(50.25),
            token: Address::random(),
            underlying_token: Address::random(),
            raindex_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrap_block: None,
            withdrawn_at: now,
            unwrapped_at: later,
        };
        let TransferOperation::EquityRedemption(op) = unwrapped.to_dto(&id) else {
            panic!("Expected EquityRedemption");
        };
        assert!(
            matches!(op.status, EquityRedemptionStatus::Unwrapping),
            "Expected Unwrapping, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);

        let sent = EquityRedemption::TokensSent {
            symbol: symbol.clone(),
            quantity: float!(50.25),
            token: Address::random(),
            raindex_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: Some(TxHash::random()),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: now,
        };
        let TransferOperation::EquityRedemption(op) = sent.to_dto(&id) else {
            panic!("Expected EquityRedemption");
        };
        assert!(
            matches!(op.status, EquityRedemptionStatus::Sending),
            "Expected Sending, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, now);

        let pending = EquityRedemption::Pending {
            symbol,
            quantity: float!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            sent_at: now,
            detected_at: later,
        };
        let TransferOperation::EquityRedemption(op) = pending.to_dto(&id) else {
            panic!("Expected EquityRedemption");
        };
        assert!(
            matches!(op.status, EquityRedemptionStatus::PendingConfirmation),
            "Expected PendingConfirmation, got: {:?}",
            op.status
        );
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);
    }

    #[tokio::test]
    async fn active_transfers_includes_active_event_types() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: latest event is WithdrawnFromRaindex (active)
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-active-1"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;

        // TSLA: latest event is Detected (active)
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-active-2"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-active-2"),
            1,
            "EquityRedemptionEvent::TokensSent",
            r#"{"TokensSent":{"sent_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-active-2"),
            2,
            "EquityRedemptionEvent::Detected",
            r#"{"Detected":{"tokenization_request_id":"TOK001","detected_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_active_transfers(&pool).await.unwrap();
        assert_eq!(result.len(), 2, "both active redemptions should appear");
        assert!(result.contains(&Symbol::new("AAPL").unwrap()));
        assert!(result.contains(&Symbol::new("TSLA").unwrap()));
    }

    #[tokio::test]
    async fn active_transfers_excludes_terminal_states() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: completed (terminal) — should be excluded
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-terminal-1"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-terminal-1"),
            1,
            "EquityRedemptionEvent::Completed",
            r#"{"Completed":{"redemption_tx":"0x0000000000000000000000000000000000000000000000000000000000000001","tokenization_request_id":"TOK001","completed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // TSLA: detection failed (terminal) — should be excluded
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-terminal-2"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-terminal-2"),
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = symbols_with_active_transfers(&pool).await.unwrap();
        assert!(
            result.is_empty(),
            "terminal redemptions should not appear, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn active_transfers_deduplicates_same_symbol() {
        let pool = crate::test_utils::setup_test_db().await;

        // Two active redemptions for the same symbol
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-dup-1"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-dup-2"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;

        let result = symbols_with_active_transfers(&pool).await.unwrap();
        assert_eq!(result.len(), 1, "duplicate symbol should appear only once");
        assert!(result.contains(&Symbol::new("AAPL").unwrap()));
    }

    #[tokio::test]
    async fn active_transfers_skips_null_symbol_rows() {
        let pool = crate::test_utils::setup_test_db().await;

        // Row with NULL symbol in payload (corrupt data)
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-null"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            r#"{"WithdrawnFromRaindex":{}}"#,
        )
        .await;

        // Valid row alongside the corrupt one
        insert_event(
            &pool,
            &redemption_aggregate_id("redemption-valid"),
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("NVDA"),
        )
        .await;

        let result = symbols_with_active_transfers(&pool).await.unwrap();
        assert_eq!(result.len(), 1, "null symbol row should be skipped");
        assert!(result.contains(&Symbol::new("NVDA").unwrap()));
    }

    #[test]
    fn to_dto_maps_terminal_variants() {
        let id = redemption_aggregate_id("REDEEM-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);

        let completed = EquityRedemption::Completed {
            symbol: symbol.clone(),
            quantity: float!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            started_at: now,
            completed_at: later,
        };
        let TransferOperation::EquityRedemption(op) = completed.to_dto(&id) else {
            panic!("Expected EquityRedemption");
        };
        let EquityRedemptionStatus::Completed { completed_at } = op.status else {
            panic!("Expected Completed, got: {:?}", op.status);
        };
        assert_eq!(completed_at, later);
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);

        let failed = EquityRedemption::Failed {
            symbol,
            quantity: float!(50.25),
            raindex_withdraw_tx: Some(TxHash::random()),
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK001".to_string())),
            reason: None,
            started_at: now,
            failed_at: later,
        };
        let TransferOperation::EquityRedemption(op) = failed.to_dto(&id) else {
            panic!("Expected EquityRedemption");
        };
        let EquityRedemptionStatus::Failed { failed_at } = op.status else {
            panic!("Expected Failed, got: {:?}", op.status);
        };
        assert_eq!(failed_at, later);
        assert_eq!(op.started_at, now);
        assert_eq!(op.updated_at, later);
    }

    #[test]
    fn provider_completion_recovery_moves_failed_redemption_to_completed() {
        let symbol = Symbol::new("AAPL").unwrap();
        let started_at = Utc::now();
        let recovered_at = started_at + chrono::Duration::seconds(60);
        let redemption_tx = TxHash::repeat_byte(1);
        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(redemption_tx),
            tokenization_request_id: None,
            reason: Some("detection timeout".to_string()),
            started_at,
            failed_at: started_at + chrono::Duration::seconds(30),
        };

        let recovered = EquityRedemptionEvent::ProviderCompletionRecovered {
            tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            recovered_at,
        };

        let result = EquityRedemption::evolve(&failed, &recovered)
            .unwrap()
            .expect("recovered state");

        assert!(
            matches!(
                result,
                EquityRedemption::Completed {
                    symbol: ref recovered_symbol,
                    ref tokenization_request_id,
                    redemption_tx: recovered_tx,
                    ..
                } if *recovered_symbol == symbol
                    && *tokenization_request_id == TokenizationRequestId("TOK001".to_string())
                    && recovered_tx == redemption_tx
            ),
            "expected recovered redemption to complete, got {result:?}"
        );
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_active_redemption() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event()])
            .when(EquityRedemptionCommand::RecoverProviderCompletion {
                tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::TokensNotSent)
            ),
            "active redemptions must not be provider-completion recovered, got {error:?}"
        );
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_completed_redemption() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_sent_event(),
                detected_event(),
                EquityRedemptionEvent::Completed {
                    completed_at: Utc::now(),
                },
            ])
            .when(EquityRedemptionCommand::RecoverProviderCompletion {
                tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::AlreadyCompleted)
            ),
            "completed redemptions must not be recovered, got {error:?}"
        );
    }

    #[tokio::test]
    async fn recover_provider_completion_rejected_for_reconciled_redemption() {
        let mut history = failed_redemption_history();
        history.push(EquityRedemptionEvent::OperatorReconciled {
            reason: "deposited manually".to_string(),
            reconciled_at: Utc::now(),
        });

        let error = TestHarness::<EquityRedemption>::with()
            .given(history)
            .when(EquityRedemptionCommand::RecoverProviderCompletion {
                tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::AlreadyReconciled)
            ),
            "a reconciled redemption must report AlreadyReconciled, not recover, got {error:?}"
        );
    }

    #[tokio::test]
    async fn fail_transfer_from_withdrawn_transitions_to_failed() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event()])
            .when(EquityRedemptionCommand::FailTransfer {
                reason: "Transfer timed out".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::TransferFailed { tx_hash: None, .. }
        ));
    }

    #[tokio::test]
    async fn fail_transfer_from_unwrapped_transitions_to_failed() {
        let events = TestHarness::<EquityRedemption>::with()
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_unwrapped_event(),
            ])
            .when(EquityRedemptionCommand::FailTransfer {
                reason: "Transfer timed out".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::TransferFailed { tx_hash: None, .. }
        ));
    }

    #[tokio::test]
    async fn fail_transfer_rejected_from_tokens_sent() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event(), tokens_sent_event()])
            .when(EquityRedemptionCommand::FailTransfer {
                reason: "should not work".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::AlreadyStarted)
        ));
    }

    #[tokio::test]
    async fn fail_transfer_rejected_before_start() {
        let error = TestHarness::<EquityRedemption>::with()
            .given_no_previous_events()
            .when(EquityRedemptionCommand::FailTransfer {
                reason: "should not work".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotStarted)
        ));
    }

    #[tokio::test]
    async fn reconcile_from_failed_emits_operator_reconciled_and_replays_to_reconciled() {
        let history = failed_redemption_history();

        let events = TestHarness::<EquityRedemption>::with()
            .given(history.clone())
            .when(EquityRedemptionCommand::Reconcile {
                reason: "deposited manually via vault-deposit".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let EquityRedemptionEvent::OperatorReconciled { reason, .. } = &events[0] else {
            panic!("Expected OperatorReconciled, got {:?}", events[0]);
        };
        assert_eq!(reason, "deposited manually via vault-deposit");

        let state = replay::<EquityRedemption>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize a state");
        let EquityRedemption::Reconciled {
            reconcile_reason,
            quantity,
            ..
        } = state
        else {
            panic!("reconciled redemption should be Reconciled, got {state:?}");
        };
        assert_eq!(reconcile_reason, "deposited manually via vault-deposit");
        assert!(
            quantity.eq(float!(50.25)).unwrap(),
            "reconciled state must preserve the requested quantity, got {quantity:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_from_non_failed_is_rejected() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(vec![withdrawn_from_raindex_event(), tokens_sent_event()])
            .when(EquityRedemptionCommand::Reconcile {
                reason: "should be rejected".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::NotFailed)
            ),
            "reconcile from a non-failed redemption must be rejected, got {error:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_with_blank_reason_is_rejected() {
        let error = TestHarness::<EquityRedemption>::with()
            .given(failed_redemption_history())
            .when(EquityRedemptionCommand::Reconcile {
                reason: "   ".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::ReconcileReasonRequired)
            ),
            "reconcile with a blank reason must be rejected as ReconcileReasonRequired, got {error:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_from_reconciled_state_is_rejected() {
        let mut history = failed_redemption_history();
        history.push(EquityRedemptionEvent::OperatorReconciled {
            reason: "already reconciled".to_string(),
            reconciled_at: Utc::now(),
        });

        let error = TestHarness::<EquityRedemption>::with()
            .given(history)
            .when(EquityRedemptionCommand::Reconcile {
                reason: "second attempt".to_string(),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::AlreadyReconciled)
            ),
            "reconcile from an already-reconciled redemption reports AlreadyReconciled, got {error:?}"
        );
    }

    #[test]
    fn reconciled_to_dto_reports_completed_preserving_quantity() {
        let started_at = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let reconciled = EquityRedemption::Reconciled {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            raindex_withdraw_tx: Some(TxHash::random()),
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: None,
            failure_reason: None,
            reconcile_reason: "deposited manually".to_string(),
            started_at,
            reconciled_at,
        };

        let TransferOperation::EquityRedemption(operation) =
            reconciled.to_dto(&redemption_aggregate_id("RED-001"))
        else {
            panic!("expected an EquityRedemption operation");
        };
        let EquityRedemptionStatus::Completed { completed_at } = operation.status else {
            panic!(
                "reconciled redemption must map to the Completed DTO status, got {:?}",
                operation.status
            );
        };
        assert_eq!(completed_at, reconciled_at);
        assert_eq!(operation.updated_at, reconciled_at);
        assert_eq!(operation.started_at, started_at);
        assert_eq!(
            operation.quantity.to_string(),
            FractionalShares::new(float!(50.25)).to_string()
        );
    }

    #[test]
    fn is_terminal_classifies_every_variant() {
        // Exhaustively checks every EquityRedemption variant so that adding a new
        // variant without updating is_terminal causes a compile error (the
        // exhaustive match) AND a test failure (unexpected true/false here).
        let now = Utc::now();
        let sym = Symbol::new("tAAPL").unwrap();
        let tok = TokenizationRequestId("TOK001".to_string());

        assert!(
            !EquityRedemption::VaultWithdrawPending {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                wrapped_amount: U256::ZERO,
                pending_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::VaultWithdrawSubmitted {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                wrapped_amount: U256::ZERO,
                tx_hash: TxHash::default(),
                submitted_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::WithdrawnFromRaindex {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                wrapped_amount: U256::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                raindex_withdraw_block: None,
                withdrawn_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::UnwrapPending {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                wrapped_amount: U256::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                raindex_withdraw_block: None,
                withdrawn_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::UnwrapSubmitted {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                wrapped_amount: U256::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                raindex_withdraw_block: None,
                unwrap_tx_hash: TxHash::default(),
                withdrawn_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::TokensUnwrapped {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                underlying_token: Address::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                unwrap_tx_hash: TxHash::default(),
                unwrapped_amount: U256::ZERO,
                unwrap_block: None,
                withdrawn_at: now,
                unwrapped_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::SendPending {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                underlying_token: Address::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                unwrap_tx_hash: TxHash::default(),
                unwrapped_amount: U256::ZERO,
                unwrap_block: None,
                withdrawn_at: now,
                unwrapped_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::TokensSent {
                symbol: sym.clone(),
                quantity: float!(1),
                token: Address::ZERO,
                raindex_withdraw_tx: TxHash::default(),
                unwrap_tx_hash: None,
                redemption_wallet: Address::ZERO,
                redemption_tx: TxHash::default(),
                sent_at: now,
            }
            .is_terminal(),
        );

        assert!(
            !EquityRedemption::Pending {
                symbol: sym.clone(),
                quantity: float!(1),
                redemption_tx: TxHash::default(),
                tokenization_request_id: tok.clone(),
                sent_at: now,
                detected_at: now,
            }
            .is_terminal(),
        );

        assert!(
            EquityRedemption::Completed {
                symbol: sym.clone(),
                quantity: float!(1),
                redemption_tx: TxHash::default(),
                tokenization_request_id: tok,
                started_at: now,
                completed_at: now,
            }
            .is_terminal(),
        );

        assert!(
            EquityRedemption::Reconciled {
                symbol: sym.clone(),
                quantity: float!(1),
                raindex_withdraw_tx: None,
                redemption_tx: None,
                tokenization_request_id: None,
                failure_reason: None,
                reconcile_reason: "deposited manually".to_string(),
                started_at: now,
                reconciled_at: now,
            }
            .is_terminal(),
        );

        assert!(
            EquityRedemption::Failed {
                symbol: sym,
                quantity: float!(1),
                raindex_withdraw_tx: None,
                redemption_tx: None,
                tokenization_request_id: None,
                reason: None,
                started_at: now,
                failed_at: now,
            }
            .is_terminal(),
        );
    }
}
