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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::{info, warn};

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::Symbol;

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
        quantity: Decimal,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EquityRedemptionEvent {
    /// Tokens withdrawn from Raindex vault to wallet.
    WithdrawnFromRaindex {
        symbol: Symbol,
        quantity: Decimal,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EquityRedemption {
    /// Tokens withdrawn from Raindex vault to wallet, not yet sent to Alpaca
    WithdrawnFromRaindex {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        wrapped_amount: U256,
        raindex_withdraw_tx: TxHash,
        withdrawn_at: DateTime<Utc>,
    },

    /// Wrapped tokens have been unwrapped, ready to send to Alpaca
    TokensUnwrapped {
        symbol: Symbol,
        quantity: Decimal,
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
        quantity: Decimal,
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
        quantity: Decimal,
        redemption_tx: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },

    /// Redemption successfully completed and account credited (terminal state)
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        redemption_tx: TxHash,
        tokenization_request_id: TokenizationRequestId,
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
        quantity: Decimal,
        raindex_withdraw_tx: Option<TxHash>,
        redemption_tx: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        failed_at: DateTime<Utc>,
    },
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
    const SCHEMA_VERSION: u64 = 1;

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
                    ..
                }
                | Self::TokensUnwrapped {
                    symbol,
                    quantity,
                    raindex_withdraw_tx,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    raindex_withdraw_tx: Some(*raindex_withdraw_tx),
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
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
                    failed_at: *failed_at,
                })
            }

            Completed { completed_at } => {
                let Self::Pending {
                    symbol,
                    quantity,
                    redemption_tx,
                    tokenization_request_id,
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
                    completed_at: *completed_at,
                })
            }

            RedemptionRejected { rejected_at, .. } => {
                let Self::Pending {
                    symbol,
                    quantity,
                    redemption_tx,
                    tokenization_request_id,
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
                        .lookup_unwrapped(symbol)
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

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::mock::MockTokenizer;
    use crate::wrapper::mock::MockWrapper;

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
            quantity: dec!(50.25),
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
                quantity: dec!(50.25),
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
                    quantity: dec!(50.25),
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
            wrapper: Arc::new(MockWrapper::new().with_unwrapped_token(underlying_token)),
        };

        let store = TestStore::<EquityRedemption>::new(services);
        let id = RedemptionAggregateId::new("underlying-token-fix");

        store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(10),
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
                quantity: dec!(50.25),
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
        assert_eq!(quantity, dec!(50.25));
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
        let completed = EquityRedemption::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            completed_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ999".to_string()),
            detected_at: Utc::now(),
        };

        let result = EquityRedemption::evolve(&completed, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_completed_rejects_wrong_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
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
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_detection_failed_rejects_non_tokens_sent_states() {
        let pending = EquityRedemption::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
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
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_redemption_rejected_rejects_non_pending_states() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
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
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejects_tokens_sent_event_on_live_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
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
        assert_eq!(result, None);
    }

    #[test]
    fn test_originate_rejects_non_init_events() {
        let event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };

        let result = EquityRedemption::originate(&event);
        assert_eq!(result, None);
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
                    quantity: dec!(50.25),
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
                    quantity: dec!(10),
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
                    quantity: dec!(10),
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
                    quantity: dec!(10),
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
                    quantity: dec!(10),
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
}
