//! Equity Redemption aggregate for converting onchain tokens back to offchain Alpaca shares.
//!
//! This module implements the CQRS-ES aggregate pattern for managing the asynchronous workflow
//! of redeeming tokenized equity shares. It tracks the complete lifecycle from withdrawing
//! tokens from the vault through sending to Alpaca's redemption wallet to final completion.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//! (start) --Withdraw--> VaultWithdrawn --Unwrap--> TokensUnwrapped --Send--> TokensSent --Detect--> Pending --Complete--> Completed
//!                              |                        |                        |                     |
//!                              v                        v                        v                     v
//!                           Failed                   Failed                   Failed                Failed
//! ```
//!
//! 1. **VaultWithdrawn**: Wrapped tokens withdrawn from Raindex vault
//! 2. **TokensUnwrapped**: Tokens unwrapped from ERC-4626 to unwrapped form
//! 3. **TokensSent**: Unwrapped tokens sent to Alpaca's redemption wallet
//! 4. **Pending**: Alpaca detected the transfer
//! 5. **Completed**: Terminal success state
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
//! This pattern ensures that if vault withdraw succeeds but send fails, the aggregate stays
//! in `VaultWithdrawn` state (tokens in wallet, not stranded).
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - Failed state preserves context depending on when failure occurred
//! - All state transitions are captured as events for complete audit trail

use std::sync::Arc;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteEventRepository;
use st0x_execution::Symbol;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::onchain::raindex::{Raindex, RaindexError};
use crate::tokenization::{Tokenizer, TokenizerError};
use crate::tokenized_equity_mint::TokenizationRequestId;
use crate::wrapper::{Wrapper, WrapperError};

/// Our tokenized equity tokens use 18 decimals.
pub(crate) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// SQLite-backed event store for EquityRedemption aggregates.
pub(crate) type RedemptionEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<EquityRedemption, Never>>;

/// Services required by the EquityRedemption aggregate.
///
/// Combines services for the full redemption flow:
/// - `raindex` - Withdraws from Rain OrderBook vault
/// - `wrapper` - Converts between wrapped and underlying tokens
/// - `tokenizer` - Sends tokens to Alpaca for redemption
#[derive(Clone)]
pub(crate) struct RedemptionServices {
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
}

/// Unique identifier for a redemption aggregate instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedemptionAggregateId(pub(crate) String);

impl RedemptionAggregateId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Errors that can occur during equity redemption operations.
///
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityRedemptionError {
    /// Rain OrderBook vault operation failed
    #[error("Raindex error: {0}")]
    Raindex(#[from] RaindexError),
    /// ERC-4626 unwrap operation failed
    #[error("Wrapper error: {0}")]
    Wrapper(#[from] WrapperError),
    /// Tokenizer operation failed
    #[error("Tokenizer error: {0}")]
    Tokenizer(#[from] TokenizerError),
    /// Vault not found for token in vault registry
    #[error("Vault not found for token {0}")]
    VaultNotFound(Address),
    /// Attempted to unwrap tokens when redemption already started
    #[error("Cannot unwrap tokens: redemption already in progress")]
    CannotUnwrapAlreadyStarted,
    /// Tokens not yet withdrawn from vault
    #[error("Cannot unwrap: tokens not withdrawn from vault")]
    TokensNotWithdrawn,
    /// Tokens not yet unwrapped
    #[error("Cannot send: tokens not unwrapped")]
    TokensNotUnwrapped,
    /// Attempted to detect redemption before sending tokens
    #[error("Cannot detect redemption: tokens not sent")]
    TokensNotSent,
    /// Attempted to complete before redemption was detected as pending
    #[error("Cannot complete: not in pending state")]
    NotPending,
    /// Attempted to reject before redemption was detected as pending
    #[error("Cannot reject: not in pending state")]
    NotPendingForRejection,
    /// Attempted to modify a completed redemption operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed redemption operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Lifecycle state error
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    /// Withdraw wrapped tokens from Raindex vault.
    /// First step in the redemption flow.
    WithdrawFromVault {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
    },
    /// Unwrap ERC-4626 wrapped tokens after vault withdrawal.
    /// Calls unwrapper.unwrap() via services to redeem wrapped tokens.
    UnwrapTokens,
    /// Redeem unwrapped tokens by sending to Alpaca's redemption wallet.
    /// Calls tokenizer.send_for_redemption() via services.
    Redeem {
        token: Address,
        amount: U256,
    },
    Detect {
        tokenization_request_id: TokenizationRequestId,
    },
    FailDetection {
        reason: String,
    },
    Complete,
    RejectRedemption {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum EquityRedemptionEvent {
    /// Wrapped tokens withdrawn from Raindex vault to wallet.
    VaultWithdrawn {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
        vault_withdraw_tx: TxHash,
        withdrawn_at: DateTime<Utc>,
    },

    /// ERC-4626 wrapped tokens have been unwrapped.
    TokensUnwrapped {
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        unwrapped_at: DateTime<Utc>,
    },

    /// Unwrapped tokens sent to Alpaca's redemption wallet.
    TokensSent {
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
    },

    /// Failed to send unwrapped tokens to Alpaca's redemption wallet.
    /// Tokens remain unwrapped in wallet - can be retried.
    SendFailed {
        token: Address,
        amount: U256,
        redemption_wallet: Address,
        failed_at: DateTime<Utc>,
    },

    /// Alpaca failed to detect the token transfer.
    /// Tokens were sent but detection failed - keep inflight until manually resolved.
    DetectionFailed {
        reason: String,
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
        match self {
            Self::VaultWithdrawn { .. } => "EquityRedemptionEvent::VaultWithdrawn".to_string(),
            Self::TokensUnwrapped { .. } => "EquityRedemptionEvent::TokensUnwrapped".to_string(),
            Self::TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
            Self::SendFailed { .. } => "EquityRedemptionEvent::SendFailed".to_string(),
            Self::DetectionFailed { .. } => "EquityRedemptionEvent::DetectionFailed".to_string(),
            Self::Detected { .. } => "EquityRedemptionEvent::Detected".to_string(),
            Self::RedemptionRejected { .. } => {
                "EquityRedemptionEvent::RedemptionRejected".to_string()
            }
            Self::Completed { .. } => "EquityRedemptionEvent::Completed".to_string(),
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
    /// Wrapped tokens withdrawn from Raindex vault to wallet.
    /// Next step is to unwrap the tokens.
    VaultWithdrawn {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
        vault_withdraw_tx: TxHash,
        withdrawn_at: DateTime<Utc>,
    },

    /// Wrapped tokens have been unwrapped.
    /// Next step is to send the unwrapped tokens to Alpaca's redemption wallet.
    TokensUnwrapped {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        vault_withdraw_tx: TxHash,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        unwrapped_at: DateTime<Utc>,
    },

    /// Unwrapped tokens sent to Alpaca's redemption wallet.
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        vault_withdraw_tx: TxHash,
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
        /// Present if tokens were unwrapped before sending.
        unwrap_tx_hash: Option<TxHash>,
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

    /// Redemption failed with error reason (terminal state)
    ///
    /// Fields preserve context depending on when failure occurred:
    /// - `vault_withdraw_tx`: Present if vault withdraw succeeded
    /// - `redemption_tx`: Present if send succeeded
    /// - `tokenization_request_id`: Present if Alpaca detected the transfer
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        vault_withdraw_tx: Option<TxHash>,
        redemption_tx: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl Aggregate for Lifecycle<EquityRedemption, Never> {
    type Command = EquityRedemptionCommand;
    type Event = EquityRedemptionEvent;
    type Error = EquityRedemptionError;
    type Services = RedemptionServices;

    fn aggregate_type() -> String {
        "EquityRedemption".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, EquityRedemption::apply_transition)
            .or_initialize(&event, EquityRedemption::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            EquityRedemptionCommand::WithdrawFromVault {
                symbol,
                quantity,
                token,
                amount,
            } => {
                self.handle_withdraw_from_vault(
                    services,
                    symbol.clone(),
                    *quantity,
                    *token,
                    *amount,
                )
                .await
            }

            EquityRedemptionCommand::UnwrapTokens => self.handle_unwrap_tokens(services).await,

            EquityRedemptionCommand::Redeem { token, amount } => {
                self.handle_send_tokens(services, *token, *amount).await
            }

            EquityRedemptionCommand::Detect {
                tokenization_request_id,
            } => self.handle_detect(tokenization_request_id),

            EquityRedemptionCommand::FailDetection { reason } => self.handle_fail_detection(reason),

            EquityRedemptionCommand::Complete => self.handle_complete(),

            EquityRedemptionCommand::RejectRedemption { reason } => {
                self.handle_reject_redemption(reason)
            }
        }
    }
}

impl Lifecycle<EquityRedemption, Never> {
    async fn handle_withdraw_from_vault(
        &self,
        services: &RedemptionServices,
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => {
                let vault_id = services.raindex.lookup_vault_id(token).await?;

                let vault_withdraw_tx = services
                    .raindex
                    .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
                    .await?;

                Ok(vec![EquityRedemptionEvent::VaultWithdrawn {
                    symbol,
                    quantity,
                    token,
                    amount,
                    vault_withdraw_tx,
                    withdrawn_at: Utc::now(),
                }])
            }
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            // Completed or any other state (in-progress states) - cannot start new redemption
            Ok(_) => Err(EquityRedemptionError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    async fn handle_unwrap_tokens(
        &self,
        services: &RedemptionServices,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Ok(EquityRedemption::VaultWithdrawn { token, amount, .. }) => {
                let owner = services.wrapper.owner();

                let (unwrap_tx_hash, unwrapped_amount) = services
                    .wrapper
                    .to_underlying(*token, *amount, owner, owner)
                    .await?;

                Ok(vec![EquityRedemptionEvent::TokensUnwrapped {
                    unwrap_tx_hash,
                    unwrapped_amount,
                    unwrapped_at: Utc::now(),
                }])
            }
            Err(LifecycleError::Uninitialized) => Err(EquityRedemptionError::TokensNotWithdrawn),
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Ok(EquityRedemption::Completed { .. }) => Err(EquityRedemptionError::AlreadyCompleted),
            Ok(_) => Err(EquityRedemptionError::CannotUnwrapAlreadyStarted),
            Err(e) => Err(e.into()),
        }
    }

    async fn handle_send_tokens(
        &self,
        services: &RedemptionServices,
        token: Address,
        amount: U256,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Ok(EquityRedemption::TokensUnwrapped { .. }) => {
                let redemption_tx = services
                    .tokenizer
                    .send_for_redemption(token, amount)
                    .await?;
                let redemption_wallet = services.tokenizer.redemption_wallet();

                Ok(vec![EquityRedemptionEvent::TokensSent {
                    redemption_wallet,
                    redemption_tx,
                    sent_at: Utc::now(),
                }])
            }
            Err(LifecycleError::Uninitialized) | Ok(EquityRedemption::VaultWithdrawn { .. }) => {
                Err(EquityRedemptionError::TokensNotUnwrapped)
            }
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Ok(_) => Err(EquityRedemptionError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_detect(
        &self,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                EquityRedemption::VaultWithdrawn { .. } | EquityRedemption::TokensUnwrapped { .. },
            ) => Err(EquityRedemptionError::TokensNotSent),
            Ok(EquityRedemption::TokensSent { .. }) => Ok(vec![EquityRedemptionEvent::Detected {
                tokenization_request_id: tokenization_request_id.clone(),
                detected_at: Utc::now(),
            }]),
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Ok(_) => Err(EquityRedemptionError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_complete(&self) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                EquityRedemption::VaultWithdrawn { .. }
                | EquityRedemption::TokensUnwrapped { .. }
                | EquityRedemption::TokensSent { .. },
            ) => Err(EquityRedemptionError::NotPending),
            Ok(EquityRedemption::Pending { .. }) => Ok(vec![EquityRedemptionEvent::Completed {
                completed_at: Utc::now(),
            }]),
            Ok(EquityRedemption::Completed { .. }) => Err(EquityRedemptionError::AlreadyCompleted),
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail_detection(
        &self,
        reason: &str,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                EquityRedemption::VaultWithdrawn { .. } | EquityRedemption::TokensUnwrapped { .. },
            ) => Err(EquityRedemptionError::TokensNotSent),
            Ok(EquityRedemption::TokensSent { .. }) => {
                Ok(vec![EquityRedemptionEvent::DetectionFailed {
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Ok(EquityRedemption::Pending { .. } | EquityRedemption::Completed { .. }) => {
                Err(EquityRedemptionError::AlreadyCompleted)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn handle_reject_redemption(
        &self,
        reason: &str,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                EquityRedemption::VaultWithdrawn { .. }
                | EquityRedemption::TokensUnwrapped { .. }
                | EquityRedemption::TokensSent { .. },
            ) => Err(EquityRedemptionError::NotPendingForRejection),
            Ok(EquityRedemption::Pending { .. }) => {
                Ok(vec![EquityRedemptionEvent::RedemptionRejected {
                    reason: reason.to_string(),
                    rejected_at: Utc::now(),
                }])
            }
            Ok(EquityRedemption::Completed { .. }) => Err(EquityRedemptionError::AlreadyCompleted),
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }
}

impl EquityRedemption {
    /// Apply a transition event to an existing redemption state.
    pub(crate) fn apply_transition(
        event: &EquityRedemptionEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            // VaultWithdrawn is an init event, not a transition
            EquityRedemptionEvent::VaultWithdrawn { .. } => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
            }),

            // TokensUnwrapped is a transition from VaultWithdrawn
            EquityRedemptionEvent::TokensUnwrapped {
                unwrap_tx_hash,
                unwrapped_amount,
                unwrapped_at,
            } => current.apply_tokens_unwrapped(
                *unwrap_tx_hash,
                *unwrapped_amount,
                *unwrapped_at,
                event,
            ),

            // TokensSent is a transition from TokensUnwrapped
            EquityRedemptionEvent::TokensSent {
                redemption_wallet,
                redemption_tx,
                sent_at,
            } => current.apply_tokens_sent(*redemption_wallet, *redemption_tx, *sent_at, event),

            // SendFailed is a terminal transition from TokensUnwrapped
            EquityRedemptionEvent::SendFailed { failed_at, .. } => {
                current.apply_send_failed(*failed_at, event)
            }

            EquityRedemptionEvent::DetectionFailed { reason, failed_at } => {
                current.apply_detection_failed(reason, *failed_at, event)
            }

            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => current.apply_detected(tokenization_request_id, *detected_at, event),

            EquityRedemptionEvent::RedemptionRejected {
                reason,
                rejected_at,
            } => current.apply_redemption_rejected(reason, *rejected_at, event),

            EquityRedemptionEvent::Completed { completed_at } => {
                current.apply_completed(*completed_at, event)
            }
        }
    }

    /// Create initial state from an initialization event.
    /// Only VaultWithdrawn can initialize the aggregate.
    pub(crate) fn from_event(event: &EquityRedemptionEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            EquityRedemptionEvent::VaultWithdrawn {
                symbol,
                quantity,
                token,
                amount,
                vault_withdraw_tx,
                withdrawn_at,
            } => Ok(Self::VaultWithdrawn {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                amount: *amount,
                vault_withdraw_tx: *vault_withdraw_tx,
                withdrawn_at: *withdrawn_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: format!("{event:?}"),
            }),
        }
    }

    /// Apply TokensUnwrapped transition from VaultWithdrawn state.
    fn apply_tokens_unwrapped(
        &self,
        unwrap_tx_hash: TxHash,
        unwrapped_amount: U256,
        unwrapped_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::VaultWithdrawn {
            symbol,
            quantity,
            token,
            vault_withdraw_tx,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::TokensUnwrapped {
            symbol: symbol.clone(),
            quantity: *quantity,
            token: *token,
            vault_withdraw_tx: *vault_withdraw_tx,
            unwrap_tx_hash,
            unwrapped_amount,
            unwrapped_at,
        })
    }

    /// Apply TokensSent transition from TokensUnwrapped state.
    fn apply_tokens_sent(
        &self,
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensUnwrapped {
            symbol,
            quantity,
            token,
            vault_withdraw_tx,
            unwrap_tx_hash,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::TokensSent {
            symbol: symbol.clone(),
            quantity: *quantity,
            token: *token,
            vault_withdraw_tx: *vault_withdraw_tx,
            redemption_wallet,
            redemption_tx,
            sent_at,
            unwrap_tx_hash: Some(*unwrap_tx_hash),
        })
    }

    /// Apply SendFailed transition from TokensUnwrapped state.
    /// Transitions to terminal Failed state.
    fn apply_send_failed(
        &self,
        failed_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensUnwrapped {
            symbol,
            quantity,
            vault_withdraw_tx,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            vault_withdraw_tx: Some(*vault_withdraw_tx),
            redemption_tx: None,
            tokenization_request_id: None,
            reason: "Failed to send tokens to redemption wallet".to_string(),
            failed_at,
        })
    }

    fn apply_detected(
        &self,
        tokenization_request_id: &TokenizationRequestId,
        detected_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensSent {
            symbol,
            quantity,
            redemption_tx,
            sent_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Pending {
            symbol: symbol.clone(),
            quantity: *quantity,
            redemption_tx: *redemption_tx,
            tokenization_request_id: tokenization_request_id.clone(),
            sent_at: *sent_at,
            detected_at,
        })
    }

    fn apply_completed(
        &self,
        completed_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Pending {
            symbol,
            quantity,
            redemption_tx,
            tokenization_request_id,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Completed {
            symbol: symbol.clone(),
            quantity: *quantity,
            redemption_tx: *redemption_tx,
            tokenization_request_id: tokenization_request_id.clone(),
            completed_at,
        })
    }

    fn apply_detection_failed(
        &self,
        reason: &str,
        failed_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensSent {
            symbol,
            quantity,
            vault_withdraw_tx,
            redemption_tx,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            vault_withdraw_tx: Some(*vault_withdraw_tx),
            redemption_tx: Some(*redemption_tx),
            tokenization_request_id: None,
            reason: reason.to_string(),
            failed_at,
        })
    }

    fn apply_redemption_rejected(
        &self,
        reason: &str,
        rejected_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Pending {
            symbol,
            quantity,
            redemption_tx,
            tokenization_request_id,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            vault_withdraw_tx: None,
            redemption_tx: Some(*redemption_tx),
            tokenization_request_id: Some(tokenization_request_id.clone()),
            reason: reason.to_string(),
            failed_at: rejected_at,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::onchain::mock::MockVault;
    use crate::tokenization::mock::MockTokenizer;
    use crate::wrapper::mock::MockWrapper;

    pub(crate) fn mock_redeemer_services() -> RedemptionServices {
        RedemptionServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockVault::new()),
            wrapper: Arc::new(MockWrapper::new()),
        }
    }

    #[tokio::test]
    async fn test_withdraw_from_vault_from_uninitialized() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let services = mock_redeemer_services();

        let events = aggregate
            .handle(
                EquityRedemptionCommand::WithdrawFromVault {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::VaultWithdrawn { .. }
        ));
    }

    fn apply_vault_withdrawn_unwrapped_and_tokens_sent(
        aggregate: &mut Lifecycle<EquityRedemption, Never>,
        symbol: Symbol,
    ) -> (Address, TxHash) {
        let redemption_wallet = Address::random();
        let redemption_tx = TxHash::random();
        let token = Address::random();

        let vault_event = EquityRedemptionEvent::VaultWithdrawn {
            symbol,
            quantity: dec!(50.25),
            token,
            amount: U256::from(50_250_000_000_000_000_000_u128),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        };
        aggregate.apply(vault_event);

        let unwrap_event = EquityRedemptionEvent::TokensUnwrapped {
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrapped_at: Utc::now(),
        };
        aggregate.apply(unwrap_event);

        let sent_event = EquityRedemptionEvent::TokensSent {
            redemption_wallet,
            redemption_tx,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        (redemption_wallet, redemption_tx)
    }

    #[tokio::test]
    async fn test_detect_after_tokens_sent() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &mock_redeemer_services(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Detected { .. }));
    }

    #[tokio::test]
    async fn test_complete_from_pending() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(EquityRedemptionCommand::Complete, &mock_redeemer_services())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn test_complete_redemption_flow_end_to_end() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let token = Address::random();
        let services = mock_redeemer_services();

        let withdraw_events = aggregate
            .handle(
                EquityRedemptionCommand::WithdrawFromVault {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    token,
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await
            .unwrap();
        assert_eq!(withdraw_events.len(), 1);
        aggregate.apply(withdraw_events[0].clone());

        let unwrap_events = aggregate
            .handle(EquityRedemptionCommand::UnwrapTokens, &services)
            .await
            .unwrap();
        assert_eq!(unwrap_events.len(), 1);
        aggregate.apply(unwrap_events[0].clone());

        let send_events = aggregate
            .handle(
                EquityRedemptionCommand::Redeem {
                    token,
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await
            .unwrap();
        assert_eq!(send_events.len(), 1);
        aggregate.apply(send_events[0].clone());

        let detect_events = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &services,
            )
            .await
            .unwrap();
        assert_eq!(detect_events.len(), 1);
        aggregate.apply(detect_events[0].clone());

        let complete_events = aggregate
            .handle(EquityRedemptionCommand::Complete, &services)
            .await
            .unwrap();
        assert_eq!(complete_events.len(), 1);
        aggregate.apply(complete_events[0].clone());

        assert!(matches!(
            aggregate,
            Lifecycle::Live(EquityRedemption::Completed { .. })
        ));
    }

    #[tokio::test]
    async fn test_cannot_detect_before_sending_tokens() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[tokio::test]
    async fn test_cannot_complete_before_pending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(EquityRedemptionCommand::Complete, &mock_redeemer_services())
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::NotPending)));
    }

    #[tokio::test]
    async fn test_fail_detection_from_tokens_sent_state() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::FailDetection {
                    reason: "Alpaca timeout".to_string(),
                },
                &mock_redeemer_services(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::DetectionFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_reject_redemption_from_pending_state() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::RejectRedemption {
                    reason: "Insufficient balance".to_string(),
                },
                &mock_redeemer_services(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::RedemptionRejected { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_redemption_before_pending() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let result = aggregate
            .handle(
                EquityRedemptionCommand::RejectRedemption {
                    reason: "Cannot reject yet".to_string(),
                },
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::NotPendingForRejection)
        ));
    }

    #[tokio::test]
    async fn test_redemption_rejected_preserves_context_with_tokenization_id() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let (_, redemption_tx) =
            apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol.clone());

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let rejected_event = EquityRedemptionEvent::RedemptionRejected {
            reason: "Insufficient balance".to_string(),
            rejected_at: Utc::now(),
        };
        aggregate.apply(rejected_event);

        let Lifecycle::Live(EquityRedemption::Failed {
            symbol: failed_symbol,
            quantity,
            redemption_tx: failed_redemption_tx,
            tokenization_request_id,
            reason,
            ..
        }) = aggregate
        else {
            panic!("Expected Failed state, got {aggregate:?}");
        };

        assert_eq!(failed_symbol, symbol);
        assert_eq!(quantity, dec!(50.25));
        assert_eq!(failed_redemption_tx, Some(redemption_tx));
        assert_eq!(
            tokenization_request_id,
            Some(TokenizationRequestId("REQ789".to_string()))
        );
        assert_eq!(reason, "Insufficient balance");
    }

    #[tokio::test]
    async fn test_cannot_fail_detection_before_sending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::FailDetection {
                    reason: "Cannot fail".to_string(),
                },
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[tokio::test]
    async fn test_cannot_reject_redemption_before_sending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::RejectRedemption {
                    reason: "Cannot reject".to_string(),
                },
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::NotPendingForRejection)
        ));
    }

    #[test]
    fn test_apply_detected_rejects_wrong_state() {
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

        let err = EquityRedemption::apply_transition(&event, &completed).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("Completed"));
        assert_eq!(evt, "EquityRedemptionEvent::Detected");
    }

    #[test]
    fn test_apply_completed_rejects_wrong_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
            unwrap_tx_hash: None,
        };

        let event = EquityRedemptionEvent::Completed {
            completed_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &tokens_sent).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("TokensSent"));
        assert_eq!(evt, "EquityRedemptionEvent::Completed");
    }

    #[test]
    fn test_apply_detection_failed_rejects_non_tokens_sent_states() {
        let pending = EquityRedemption::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            sent_at: Utc::now(),
            detected_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::DetectionFailed {
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &pending).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("Pending"));
        assert_eq!(evt, "EquityRedemptionEvent::DetectionFailed");
    }

    #[test]
    fn test_apply_redemption_rejected_rejects_non_pending_states() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
            unwrap_tx_hash: None,
        };

        let event = EquityRedemptionEvent::RedemptionRejected {
            reason: "Should not apply".to_string(),
            rejected_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &tokens_sent).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("TokensSent"));
        assert_eq!(evt, "EquityRedemptionEvent::RedemptionRejected");
    }

    #[test]
    fn test_apply_transition_rejects_tokens_sent_event_on_tokens_sent_state() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
            unwrap_tx_hash: None,
        };

        let event = EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &tokens_sent).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("TokensSent"));
        assert_eq!(evt, "EquityRedemptionEvent::TokensSent");
    }

    #[test]
    fn test_from_event_rejects_non_init_events() {
        let event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };

        let err = EquityRedemption::from_event(&event).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert_eq!(state, "Uninitialized");
        assert!(evt.contains("Detected"));
    }

    #[tokio::test]
    async fn test_unwrap_tokens_requires_vault_withdrawn_state() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let services = mock_redeemer_services();

        let result = aggregate
            .handle(EquityRedemptionCommand::UnwrapTokens, &services)
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::TokensNotWithdrawn)
        ));
    }

    #[tokio::test]
    async fn test_send_tokens_requires_tokens_unwrapped_state() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let services = mock_redeemer_services();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::Redeem {
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::TokensNotUnwrapped)
        ));
    }

    #[tokio::test]
    async fn test_send_tokens_after_vault_withdrawn_fails() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let token = Address::random();

        let vault_event = EquityRedemptionEvent::VaultWithdrawn {
            symbol,
            quantity: dec!(50.25),
            token,
            amount: U256::from(50_250_000_000_000_000_000_u128),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        };
        aggregate.apply(vault_event);

        let services = mock_redeemer_services();
        let result = aggregate
            .handle(
                EquityRedemptionCommand::Redeem {
                    token,
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::TokensNotUnwrapped)
        ));
    }

    #[tokio::test]
    async fn test_cannot_unwrap_after_tokens_sent() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_unwrapped_and_tokens_sent(&mut aggregate, symbol);

        let services = mock_redeemer_services();
        let result = aggregate
            .handle(EquityRedemptionCommand::UnwrapTokens, &services)
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::CannotUnwrapAlreadyStarted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_detect_before_send_after_unwrap() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let token = Address::random();

        let vault_event = EquityRedemptionEvent::VaultWithdrawn {
            symbol,
            quantity: dec!(50.25),
            token,
            amount: U256::from(50_250_000_000_000_000_000_u128),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        };
        aggregate.apply(vault_event);

        let unwrap_event = EquityRedemptionEvent::TokensUnwrapped {
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrapped_at: Utc::now(),
        };
        aggregate.apply(unwrap_event);

        let services = mock_redeemer_services();
        let result = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &services,
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[test]
    fn test_apply_tokens_sent_from_tokens_unwrapped() {
        let unwrapped = EquityRedemption::TokensUnwrapped {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrapped_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let result = EquityRedemption::apply_transition(&event, &unwrapped).unwrap();

        let EquityRedemption::TokensSent { unwrap_tx_hash, .. } = result else {
            panic!("Expected TokensSent state, got {result:?}");
        };
        assert!(unwrap_tx_hash.is_some());
    }

    #[test]
    fn test_apply_tokens_sent_rejects_wrong_state() {
        let pending = EquityRedemption::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            redemption_tx: TxHash::random(),
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            sent_at: Utc::now(),
            detected_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &pending).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("Pending"));
        assert_eq!(evt, "EquityRedemptionEvent::TokensSent");
    }

    #[test]
    fn test_from_event_rejects_tokens_unwrapped() {
        let event = EquityRedemptionEvent::TokensUnwrapped {
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrapped_at: Utc::now(),
        };

        let err = EquityRedemption::from_event(&event).unwrap_err();

        let LifecycleError::Mismatch { state, .. } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert_eq!(state, "Uninitialized");
    }

    #[test]
    fn test_apply_transition_rejects_tokens_unwrapped_from_tokens_sent() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
            unwrap_tx_hash: None,
        };

        let event = EquityRedemptionEvent::TokensUnwrapped {
            unwrap_tx_hash: TxHash::random(),
            unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            unwrapped_at: Utc::now(),
        };

        let err = EquityRedemption::apply_transition(&event, &tokens_sent).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("TokensSent"));
        assert_eq!(evt, "EquityRedemptionEvent::TokensUnwrapped");
    }
}
