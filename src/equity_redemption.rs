//! Aggregate modeling the lifecycle of redeeming tokenized
//! equities for underlying shares.
//!
//! Tracks the workflow from withdrawing tokens from the Raindex vault
//! through sending to Alpaca's redemption wallet to share delivery.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//!     Redeem ------------> Failed
//!       |
//!       v
//!     WithdrawnFromRaindex --> Failed
//!       |
//!       v
//!     TokensUnwrapped ------> Failed
//!       |
//!       v
//!     TokensSent ------------> Failed
//!       |
//!       v
//!     Pending ---------------> Failed
//!       |
//!       v
//!     Completed
//! ```
//!
//! - `Redeem` withdraws wrapped tokens from the Raindex vault
//! - `WithdrawnFromRaindex` tracks tokens withdrawn, awaiting unwrap
//! - `UnwrapTokens` unwraps ERC-4626 shares into underlying tokens
//! - `TokensUnwrapped` tracks unwrapped tokens, ready to send
//! - `TokensSent` tracks tokens sent to Alpaca's redemption wallet
//! - `Pending` indicates Alpaca detected the transfer
//! - `Completed` and `Failed` are terminal states
//!
//! # Services
//!
//! The aggregate uses cqrs-es Services (`RedemptionServices`) with `Tokenizer` and `Vault`
//! traits to execute side effects atomically:
//!
//! - `vault.withdraw()` - Withdraws tokens from Rain OrderBook vault
//! - `tokenizer.send_for_redemption()` - Sends tokens to Alpaca's redemption wallet
//!
//! This pattern ensures that if Raindex withdraw succeeds but send fails, the aggregate stays
//! in `WithdrawnFromRaindex` state (tokens in wallet, not stranded).
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
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{info, warn};

use st0x_dto::{EquityRedemptionOperation, EquityRedemptionStatus, TransferOperation};
use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::Symbol;
use st0x_finance::{FractionalShares, Id};

use crate::rebalancing::equity::EquityTransferServices;
use crate::tokenization::Tokenizer;
use crate::tokenized_equity_mint::TokenizationRequestId;

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// Unique identifier for a redemption aggregate instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct RedemptionAggregateId(pub(crate) String);

impl RedemptionAggregateId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for RedemptionAggregateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RedemptionAggregateId {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}

/// Errors that can occur during equity redemption operations.
///
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EquityRedemptionError {
    /// Raindex vault lookup failed for the given token
    #[error("Token {0} not found in Raindex vault registry")]
    RaindexVaultNotFound(Address),
    /// Raindex vault withdrawal transaction failed.
    /// RaindexError can't be wrapped with #[from] because it contains
    /// alloy types that don't implement Serialize/Deserialize (required
    /// by DomainError).
    #[error(
        "Raindex vault withdraw failed for token {token}, \
         amount {amount}: {error_message}"
    )]
    RaindexWithdrawFailed {
        token: Address,
        amount: U256,
        error_message: String,
    },
    /// ERC-4626 unwrap operation failed.
    /// WrapperError can't be wrapped with #[from] because it contains
    /// alloy types that don't implement Serialize/Deserialize (required
    /// by DomainError).
    #[error(
        "Token unwrap failed for {token}, \
         wrapped_amount {wrapped_amount}: {error_message}"
    )]
    UnwrapFailed {
        token: Address,
        wrapped_amount: U256,
        error_message: String,
    },
    /// Underlying token address lookup failed after unwrapping.
    /// WrapperError can't be wrapped with #[from] for the same reason
    /// as UnwrapFailed above.
    #[error(
        "Underlying token lookup failed for {symbol}: \
         {error_message}"
    )]
    UnderlyingLookupFailed {
        symbol: Symbol,
        error_message: String,
    },
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
}

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    /// Withdraws wrapped tokens from the Raindex vault.
    /// Emits WithdrawnFromRaindex.
    Redeem {
        symbol: Symbol,
        quantity: Float,
        token: Address,
        amount: U256,
    },
    /// Unwrap ERC-4626 wrapped tokens after Raindex withdrawal.
    UnwrapTokens,
    /// Send unwrapped tokens to Alpaca's redemption wallet.
    SendTokens,
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
}

/// Reason for detection failure when polling Alpaca for redemption detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DetectionFailure {
    Timeout,
    ApiError { status_code: Option<u16> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EquityRedemptionEvent {
    /// Tokens withdrawn from Raindex vault to wallet.
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
        withdrawn_at: DateTime<Utc>,
    },
    /// ERC-4626 wrapped tokens have been unwrapped.
    TokensUnwrapped {
        underlying_token: Address,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        unwrapped_at: DateTime<Utc>,
    },
    /// Raindex withdraw succeeded but transfer to redemption wallet failed.
    TransferFailed {
        tx_hash: Option<TxHash>,
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
}

/// Required by `cqrs_es::DomainEvent`.
impl PartialEq for EquityRedemptionEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::WithdrawnFromRaindex {
                    symbol: s1,
                    quantity: q1,
                    token: t1,
                    wrapped_amount: w1,
                    raindex_withdraw_tx: r1,
                    withdrawn_at: wa1,
                },
                Self::WithdrawnFromRaindex {
                    symbol: s2,
                    quantity: q2,
                    token: t2,
                    wrapped_amount: w2,
                    raindex_withdraw_tx: r2,
                    withdrawn_at: wa2,
                },
            ) => {
                s1 == s2
                    && q1.eq(*q2).unwrap_or(false)
                    && t1 == t2
                    && w1 == w2
                    && r1 == r2
                    && wa1 == wa2
            }
            (
                Self::TokensUnwrapped {
                    underlying_token: u1,
                    unwrap_tx_hash: h1,
                    unwrapped_amount: a1,
                    unwrapped_at: t1,
                },
                Self::TokensUnwrapped {
                    underlying_token: u2,
                    unwrap_tx_hash: h2,
                    unwrapped_amount: a2,
                    unwrapped_at: t2,
                },
            ) => u1 == u2 && h1 == h2 && a1 == a2 && t1 == t2,
            (
                Self::TransferFailed {
                    tx_hash: h1,
                    failed_at: f1,
                },
                Self::TransferFailed {
                    tx_hash: h2,
                    failed_at: f2,
                },
            ) => h1 == h2 && f1 == f2,
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
            _ => false,
        }
    }
}

impl Eq for EquityRedemptionEvent {}

impl DomainEvent for EquityRedemptionEvent {
    fn event_type(&self) -> String {
        use EquityRedemptionEvent::*;
        match self {
            WithdrawnFromRaindex { .. } => {
                "EquityRedemptionEvent::WithdrawnFromRaindex".to_string()
            }
            TokensUnwrapped { .. } => "EquityRedemptionEvent::TokensUnwrapped".to_string(),
            TransferFailed { .. } => "EquityRedemptionEvent::TransferFailed".to_string(),
            TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
            DetectionFailed { .. } => "EquityRedemptionEvent::DetectionFailed".to_string(),
            Detected { .. } => "EquityRedemptionEvent::Detected".to_string(),
            RedemptionRejected { .. } => "EquityRedemptionEvent::RedemptionRejected".to_string(),
            Completed { .. } => "EquityRedemptionEvent::Completed".to_string(),
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
        withdrawn_at: DateTime<Utc>,
        unwrapped_at: DateTime<Utc>,
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
        /// When the redemption process started (withdrawn_at or sent_at from prior state)
        started_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl EquityRedemption {
    pub(crate) fn to_dto(&self, id: &RedemptionAggregateId) -> TransferOperation {
        match self {
            Self::WithdrawnFromRaindex {
                symbol,
                quantity,
                withdrawn_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.0.clone()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Withdrawing,
                started_at: *withdrawn_at,
                updated_at: *withdrawn_at,
            }),

            Self::TokensUnwrapped {
                symbol,
                quantity,
                withdrawn_at,
                unwrapped_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.0.clone()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Unwrapping,
                started_at: *withdrawn_at,
                updated_at: *unwrapped_at,
            }),

            Self::TokensSent {
                symbol,
                quantity,
                sent_at,
                ..
            } => TransferOperation::EquityRedemption(EquityRedemptionOperation {
                id: Id::new(id.0.clone()),
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
                id: Id::new(id.0.clone()),
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
                id: Id::new(id.0.clone()),
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
                id: Id::new(id.0.clone()),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                status: EquityRedemptionStatus::Failed {
                    failed_at: *failed_at,
                },
                started_at: *started_at,
                updated_at: *failed_at,
            }),
        }
    }
}

#[async_trait]
impl EventSourced for EquityRedemption {
    type Id = RedemptionAggregateId;
    type Event = EquityRedemptionEvent;
    type Command = EquityRedemptionCommand;
    type Error = EquityRedemptionError;
    type Services = EquityTransferServices;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "EquityRedemption";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 2;

    fn originate(event: &Self::Event) -> Option<Self> {
        use EquityRedemptionEvent::*;
        match event {
            WithdrawnFromRaindex {
                symbol,
                quantity,
                token,
                wrapped_amount,
                raindex_withdraw_tx,
                withdrawn_at,
            } => Some(Self::WithdrawnFromRaindex {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                wrapped_amount: *wrapped_amount,
                raindex_withdraw_tx: *raindex_withdraw_tx,
                withdrawn_at: *withdrawn_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use EquityRedemptionEvent::*;

        Ok(match event {
            WithdrawnFromRaindex { .. } => None,

            TransferFailed { tx_hash, failed_at } => match entity {
                Self::WithdrawnFromRaindex {
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
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: Some(*raindex_withdraw_tx),
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
                    started_at: *withdrawn_at,
                    failed_at: *failed_at,
                }),

                _ => return Ok(None),
            },

            TokensUnwrapped {
                underlying_token,
                unwrap_tx_hash,
                unwrapped_amount,
                unwrapped_at,
            } => {
                let Self::WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    raindex_withdraw_tx,
                    withdrawn_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::TokensUnwrapped {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    underlying_token: *underlying_token,
                    raindex_withdraw_tx: *raindex_withdraw_tx,
                    unwrap_tx_hash: *unwrap_tx_hash,
                    unwrapped_amount: *unwrapped_amount,
                    withdrawn_at: *withdrawn_at,
                    unwrapped_at: *unwrapped_at,
                })
            }

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

            DetectionFailed {
                failure: _,
                failed_at,
            } => {
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

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: Some(*raindex_withdraw_tx),
                    redemption_tx: Some(*redemption_tx),
                    tokenization_request_id: None,
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

            RedemptionRejected { rejected_at, .. } => {
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
                    started_at: *sent_at,
                    failed_at: *rejected_at,
                })
            }
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            Redeem {
                symbol,
                quantity,
                token,
                amount,
            } => {
                let vault_id = match services.raindex.lookup_vault_id(token).await {
                    Ok(id) => id,
                    Err(error) => {
                        warn!(%error, %token, "Raindex vault lookup failed");
                        return Err(EquityRedemptionError::RaindexVaultNotFound(token));
                    }
                };

                info!(?vault_id, %token, %amount, "Withdrawing tokens from Raindex vault");

                let raindex_withdraw_tx = match services
                    .raindex
                    .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
                    .await
                {
                    Ok(tx) => tx,
                    Err(error) => {
                        warn!(%error, %token, %amount, "Raindex vault withdrawal failed");
                        return Err(EquityRedemptionError::RaindexWithdrawFailed {
                            token,
                            amount,
                            error_message: error.to_string(),
                        });
                    }
                };

                Ok(vec![WithdrawnFromRaindex {
                    symbol,
                    quantity,
                    token,
                    wrapped_amount: amount,
                    raindex_withdraw_tx,
                    withdrawn_at: Utc::now(),
                }])
            }
            UnwrapTokens
            | SendTokens
            | Detect { .. }
            | FailDetection { .. }
            | Complete
            | RejectRedemption { .. } => Err(EquityRedemptionError::NotStarted),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            Redeem { .. } => match self {
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            UnwrapTokens => match self {
                Self::WithdrawnFromRaindex {
                    symbol,
                    token,
                    wrapped_amount,
                    ..
                } => {
                    let underlying_token = services
                        .wrapper
                        .lookup_underlying(symbol)
                        .inspect_err(|error| {
                            warn!(%error, %symbol, "Underlying token lookup failed");
                        })
                        .map_err(|error| EquityRedemptionError::UnderlyingLookupFailed {
                            symbol: symbol.clone(),
                            error_message: error.to_string(),
                        })?;

                    let owner = services.wrapper.owner();
                    let (unwrap_tx_hash, unwrapped_amount) = services
                        .wrapper
                        .to_underlying(*token, *wrapped_amount, owner, owner)
                        .await
                        .inspect_err(|error| {
                            warn!(%error, %token, "Token unwrap failed");
                        })
                        .map_err(|error| EquityRedemptionError::UnwrapFailed {
                            token: *token,
                            wrapped_amount: *wrapped_amount,
                            error_message: error.to_string(),
                        })?;

                    Ok(vec![TokensUnwrapped {
                        underlying_token,
                        unwrap_tx_hash,
                        unwrapped_amount,
                        unwrapped_at: Utc::now(),
                    }])
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                _ => Err(EquityRedemptionError::CannotUnwrapAlreadyStarted),
            },

            SendTokens => match self {
                Self::TokensUnwrapped {
                    underlying_token,
                    unwrapped_amount,
                    ..
                } => {
                    let token = *underlying_token;
                    let amount = *unwrapped_amount;

                    info!(%token, %amount, "Sending unwrapped tokens for redemption");

                    match Tokenizer::send_for_redemption(services.tokenizer.as_ref(), token, amount)
                        .await
                    {
                        Ok(redemption_tx) => {
                            let redemption_wallet =
                                Tokenizer::redemption_wallet(services.tokenizer.as_ref());

                            Ok(vec![TokensSent {
                                redemption_wallet,
                                redemption_tx,
                                sent_at: Utc::now(),
                            }])
                        }
                        Err(error) => {
                            warn!(%error, %token, %amount, "Send for redemption failed");
                            Ok(vec![TransferFailed {
                                tx_hash: None,
                                failed_at: Utc::now(),
                            }])
                        }
                    }
                }
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                _ => Err(EquityRedemptionError::TokensNotUnwrapped),
            },

            Detect {
                tokenization_request_id,
            } => match self {
                Self::TokensSent { .. } => Ok(vec![Detected {
                    tokenization_request_id,
                    detected_at: Utc::now(),
                }]),
                Self::WithdrawnFromRaindex { .. } | Self::TokensUnwrapped { .. } => {
                    Err(EquityRedemptionError::TokensNotSent)
                }
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            FailDetection { failure } => match self {
                Self::TokensSent { .. } => Ok(vec![DetectionFailed {
                    failure,
                    failed_at: Utc::now(),
                }]),
                Self::WithdrawnFromRaindex { .. } | Self::TokensUnwrapped { .. } => {
                    Err(EquityRedemptionError::TokensNotSent)
                }
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            Complete => match self {
                Self::WithdrawnFromRaindex { .. }
                | Self::TokensUnwrapped { .. }
                | Self::TokensSent { .. } => Err(EquityRedemptionError::NotPending),
                Self::Pending { .. } => Ok(vec![Completed {
                    completed_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
            },

            RejectRedemption { reason } => match self {
                Self::WithdrawnFromRaindex { .. }
                | Self::TokensUnwrapped { .. }
                | Self::TokensSent { .. } => Err(EquityRedemptionError::NotPendingForRejection),
                Self::Pending { .. } => Ok(vec![RedemptionRejected {
                    reason,
                    rejected_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
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
) -> Result<HashMap<Symbol, FractionalShares>, sqlx::Error> {
    let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'EquityRedemption' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id, \
                json_extract(first_ev.payload, \
                '$.WithdrawnFromRaindex.symbol'), \
                json_extract(first_ev.payload, \
                '$.WithdrawnFromRaindex.quantity') \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         INNER JOIN events first_ev \
             ON first_ev.aggregate_type = 'EquityRedemption' \
            AND first_ev.aggregate_id = latest.aggregate_id \
            AND first_ev.sequence = 0 \
         WHERE last_ev.aggregate_type = 'EquityRedemption' \
           AND last_ev.event_type IN ( \
               'EquityRedemptionEvent::DetectionFailed', \
               'EquityRedemptionEvent::RedemptionRejected' \
           )",
    )
    .fetch_all(pool)
    .await?;

    let mut result: HashMap<Symbol, FractionalShares> = HashMap::new();

    for (aggregate_id, raw_symbol, raw_quantity) in rows {
        let Some(symbol) = parse_stuck_symbol(&aggregate_id, raw_symbol) else {
            continue;
        };

        let Some(quantity) = parse_stuck_quantity(&aggregate_id, raw_quantity) else {
            continue;
        };

        let entry = result.entry(symbol).or_insert(FractionalShares::ZERO);
        match *entry + quantity {
            Ok(sum) => *entry = sum,
            Err(error) => {
                warn!(
                    %error,
                    %aggregate_id,
                    "Float overflow summing stuck redemption quantities, \
                     keeping accumulated value"
                );
            }
        }
    }

    Ok(result)
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
               'EquityRedemptionEvent::WithdrawnFromRaindex', \
               'EquityRedemptionEvent::TokensUnwrapped', \
               'EquityRedemptionEvent::TokensSent', \
               'EquityRedemptionEvent::Detected' \
           ) \
         ORDER BY latest.aggregate_id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(RedemptionAggregateId::new).collect())
}

fn parse_stuck_symbol(aggregate_id: &str, raw: Option<String>) -> Option<Symbol> {
    let value = raw.or_else(|| {
        warn!(
            %aggregate_id,
            "Stuck redemption has NULL symbol in \
             WithdrawnFromRaindex payload, skipping"
        );
        None
    })?;

    Symbol::new(&value)
        .inspect_err(|error| {
            warn!(
                %error,
                %aggregate_id,
                raw_symbol = %value,
                "Stuck redemption has invalid symbol, skipping"
            );
        })
        .ok()
}

fn parse_stuck_quantity(aggregate_id: &str, raw: Option<String>) -> Option<FractionalShares> {
    let value = raw.or_else(|| {
        warn!(
            %aggregate_id,
            "Stuck redemption has NULL quantity in \
             WithdrawnFromRaindex payload, skipping"
        );
        None
    })?;

    Float::parse(value.clone())
        .inspect_err(|error| {
            warn!(
                %error,
                %aggregate_id,
                raw_quantity = %value,
                "Stuck redemption has invalid quantity, skipping"
            );
        })
        .ok()
        .map(FractionalShares::new)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use st0x_dto::EquityRedemptionStatus;
    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::mock::MockTokenizer;
    use crate::wrapper::mock::MockWrapper;
    use st0x_float_macro::float;

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        }
    }

    fn withdrawn_from_raindex_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(50.25),
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            raindex_withdraw_tx: TxHash::random(),
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

    fn detected_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn redeem_from_uninitialized_produces_withdrawn_from_raindex() {
        let events = TestHarness::<EquityRedemption>::with(mock_services())
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
            EquityRedemptionEvent::WithdrawnFromRaindex { .. }
        ));
    }

    #[tokio::test]
    async fn detect_after_tokens_sent_produces_detected() {
        let events = TestHarness::<EquityRedemption>::with(mock_services())
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
        let events = TestHarness::<EquityRedemption>::with(mock_services())
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
        let store = TestStore::<EquityRedemption>::new(mock_services());
        let id = RedemptionAggregateId::new("end-to-end");

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
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::SendTokens)
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

    #[tokio::test]
    async fn send_tokens_uses_underlying_token_not_wrapped_token() {
        let wrapped_token = Address::random();
        let underlying_token = Address::random();

        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new().with_tokenized_shares(underlying_token)),
        };

        let store = TestStore::<EquityRedemption>::new(services);
        let id = RedemptionAggregateId::new("underlying-token-fix");

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
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        let EquityRedemption::TokensSent { token, .. } = entity else {
            panic!("Expected TokensSent state, got: {entity:?}");
        };

        assert_eq!(
            token, underlying_token,
            "SendTokens should use the underlying token, not the wrapped token"
        );
    }

    #[tokio::test]
    async fn cannot_detect_before_sending_tokens() {
        let error = TestHarness::<EquityRedemption>::with(mock_services())
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
        let error = TestHarness::<EquityRedemption>::with(mock_services())
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
        let events = TestHarness::<EquityRedemption>::with(mock_services())
            .given(vec![withdrawn_from_raindex_event(), tokens_sent_event()])
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
    }

    #[tokio::test]
    async fn reject_redemption_from_pending_state() {
        let events = TestHarness::<EquityRedemption>::with(mock_services())
            .given(vec![
                withdrawn_from_raindex_event(),
                tokens_sent_event(),
                detected_event(),
            ])
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "test rejection".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::RedemptionRejected { .. }
        ));
    }

    #[tokio::test]
    async fn cannot_reject_redemption_before_pending() {
        let error = TestHarness::<EquityRedemption>::with(mock_services())
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
                raindex_withdraw_tx: TxHash::random(),
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
        let error = TestHarness::<EquityRedemption>::with(mock_services())
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
        let error = TestHarness::<EquityRedemption>::with(mock_services())
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

    #[tokio::test]
    async fn send_tokens_with_failure_emits_transfer_failed() {
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new().with_send_failure()),
            wrapper: Arc::new(MockWrapper::new()),
        };

        let store = TestStore::<EquityRedemption>::new(services);
        let id = RedemptionAggregateId::new("send-fail");

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
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Failed { .. }),
            "Expected Failed state after send failure, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn unwrap_failure_returns_unwrap_failed_error() {
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::failing_unwrap()),
        };

        let error = TestHarness::<EquityRedemption>::with(services)
            .given(vec![withdrawn_from_raindex_event()])
            .when(EquityRedemptionCommand::UnwrapTokens)
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::UnwrapFailed { .. })
            ),
            "Expected UnwrapFailed error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn underlying_lookup_failure_returns_underlying_lookup_failed_error() {
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::failing_lookup()),
        };

        let error = TestHarness::<EquityRedemption>::with(services)
            .given(vec![withdrawn_from_raindex_event()])
            .when(EquityRedemptionCommand::UnwrapTokens)
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(EquityRedemptionError::UnderlyingLookupFailed { .. })
            ),
            "Expected UnderlyingLookupFailed error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redeem_when_already_started_returns_already_started() {
        let store = TestStore::<EquityRedemption>::new(mock_services());
        let id = RedemptionAggregateId::new("redemption-1");

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
        let store = TestStore::<EquityRedemption>::new(mock_services());
        let id = RedemptionAggregateId::new("redemption-1");

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

        store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        store
            .send(
                &id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ123".to_string()),
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

    /// Insert a minimal event row into the events table.
    async fn insert_event(
        pool: &SqlitePool,
        aggregate_id: &str,
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
        .bind(aggregate_id)
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

    #[tokio::test]
    async fn stuck_redemptions_returns_detection_failed_symbols() {
        let pool = crate::test_utils::setup_test_db().await;

        // AAPL: WithdrawnFromRaindex -> DetectionFailed (stuck)
        insert_event(
            &pool,
            "redemption-1",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            "redemption-1",
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
    async fn stuck_redemptions_returns_rejection_symbols() {
        let pool = crate::test_utils::setup_test_db().await;

        // TSLA: WithdrawnFromRaindex -> RedemptionRejected (stuck)
        insert_event(
            &pool,
            "redemption-2",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            "redemption-2",
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
            "stuck",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            "stuck",
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // MSFT: Completed (not stuck)
        insert_event(
            &pool,
            "completed",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("MSFT"),
        )
        .await;
        insert_event(
            &pool,
            "completed",
            1,
            "EquityRedemptionEvent::Completed",
            r#"{"Completed":{"completed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // GOOG: TransferFailed (not stuck — tokens still in our wallet)
        insert_event(
            &pool,
            "transfer-failed",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("GOOG"),
        )
        .await;
        insert_event(
            &pool,
            "transfer-failed",
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

    #[tokio::test]
    async fn interrupted_redemption_ids_returns_only_non_terminal_redemptions() {
        let pool = crate::test_utils::setup_test_db().await;

        insert_event(
            &pool,
            "resume-me",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            "resume-me",
            1,
            "EquityRedemptionEvent::TokensSent",
            r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000001","redemption_tx":"0x0000000000000000000000000000000000000000000000000000000000000002","sent_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        insert_event(
            &pool,
            "completed",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("TSLA"),
        )
        .await;
        insert_event(
            &pool,
            "completed",
            1,
            "EquityRedemptionEvent::Completed",
            r#"{"Completed":{"completed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        let result = interrupted_redemption_ids(&pool).await.unwrap();
        assert_eq!(result, vec![RedemptionAggregateId::new("resume-me")]);
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
            "valid-stuck",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload("AAPL"),
        )
        .await;
        insert_event(
            &pool,
            "valid-stuck",
            1,
            "EquityRedemptionEvent::DetectionFailed",
            r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // NULL symbol: malformed WithdrawnFromRaindex payload missing symbol
        insert_event(
            &pool,
            "null-symbol",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            r#"{"WithdrawnFromRaindex":{"quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","raindex_withdraw_tx":"0x0000000000000000000000000000000000000000000000000000000000000001","withdrawn_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_event(
            &pool,
            "null-symbol",
            1,
            "EquityRedemptionEvent::RedemptionRejected",
            r#"{"RedemptionRejected":{"reason":"test","rejected_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // Invalid symbol: symbol fails Symbol::new validation
        insert_event(
            &pool,
            "invalid-symbol",
            0,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            &withdrawn_payload(""),
        )
        .await;
        insert_event(
            &pool,
            "invalid-symbol",
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
        let id = RedemptionAggregateId::new("REDEEM-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);

        let withdrawn = EquityRedemption::WithdrawnFromRaindex {
            symbol: symbol.clone(),
            quantity: float!(50.25),
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            raindex_withdraw_tx: TxHash::random(),
            withdrawn_at: now,
        };
        let TransferOperation::EquityRedemption(op) = withdrawn.to_dto(&id) else {
            panic!(
                "Expected EquityRedemption, got: {:?}",
                withdrawn.to_dto(&id)
            );
        };
        assert_eq!(op.id, Id::new("REDEEM-001"));
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

    #[test]
    fn to_dto_maps_terminal_variants() {
        let id = RedemptionAggregateId::new("REDEEM-001");
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
}
