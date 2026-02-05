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
//! (start) --Redeem--> VaultWithdrawn ---> TokensSent ---> Pending ---> Completed
//!              |              |               |             |
//!              v              v               v             v
//!            Failed        Failed          Failed        Failed
//! ```
//!
//! - `Redeem` command atomically withdraws from vault and sends to Alpaca
//! - `VaultWithdrawn` tracks tokens that left the vault but aren't yet sent
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
use crate::onchain::vault::{Vault, VaultError};
use crate::tokenization::{
    AlpacaTokenizationError, TokenizationRequestStatus, Tokenizer, TokenizerError,
};
use crate::tokenized_equity_mint::TokenizationRequestId;

/// Our tokenized equity tokens use 18 decimals.
pub(crate) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// SQLite-backed event store for EquityRedemption aggregates.
pub(crate) type RedemptionEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<EquityRedemption, Never>>;

/// Services required by the EquityRedemption aggregate.
///
/// Combines `Tokenizer` (for sending tokens to Alpaca) and `Vault` (for withdrawing
/// from Rain OrderBook) traits.
#[derive(Clone)]
pub(crate) struct RedemptionServices {
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    pub(crate) vault: Arc<dyn Vault>,
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
    /// Vault operation failed
    #[error("Vault error: {0}")]
    Vault(#[from] VaultError),
    /// Tokenizer operation failed
    #[error("Tokenizer error: {0}")]
    Tokenizer(#[from] TokenizerError),
    /// Attempted to detect redemption before sending tokens
    #[error("Cannot detect redemption: tokens not sent")]
    TokensNotSent,
    /// Attempted to await completion before redemption was detected
    #[error("Cannot await completion: not in pending state")]
    NotPending,
    /// Attempted to modify a completed redemption operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed redemption operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Poll returned unexpected pending status
    #[error("Poll for completion returned pending status unexpectedly")]
    UnexpectedPendingStatus,
    /// Lifecycle state error
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    /// Atomic command: withdraws from vault and sends to Alpaca.
    /// Emits VaultWithdrawn, then TokensSent on success.
    /// If vault withdraw succeeds but send fails, stays in VaultWithdrawn.
    Redeem {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
    },
    /// Polls Alpaca until they detect the token transfer.
    /// Emits Detected on success, DetectionFailed on timeout/error.
    Detect,
    /// Polls Alpaca until the redemption reaches a terminal state.
    /// Emits Completed or RedemptionRejected based on result.
    AwaitCompletion,
}

/// Reason for detection failure when polling Alpaca for redemption detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DetectionFailure {
    Timeout,
    ApiError { status_code: Option<u16> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum EquityRedemptionEvent {
    /// Tokens withdrawn from vault to wallet.
    VaultWithdrawn {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        vault_withdraw_tx: TxHash,
        withdrawn_at: DateTime<Utc>,
    },
    /// Vault withdraw succeeded but transfer to redemption wallet failed.
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
            Self::TransferFailed { .. } => "EquityRedemptionEvent::TransferFailed".to_string(),
            Self::TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
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
    /// Tokens withdrawn from vault to wallet, not yet sent to Alpaca
    VaultWithdrawn {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        vault_withdraw_tx: TxHash,
        withdrawn_at: DateTime<Utc>,
    },

    /// Tokens sent to Alpaca's redemption wallet
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        vault_withdraw_tx: TxHash,
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
    /// - `vault_withdraw_tx`: Present if vault withdraw succeeded
    /// - `redemption_tx`: Present if send succeeded
    /// - `tokenization_request_id`: Present if Alpaca detected the transfer
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        vault_withdraw_tx: Option<TxHash>,
        redemption_tx: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
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
        match command {
            EquityRedemptionCommand::Redeem {
                symbol,
                quantity,
                token,
                amount,
            } => {
                self.handle_redeem(services, symbol, quantity, token, amount)
                    .await
            }

            EquityRedemptionCommand::Detect => self.handle_detect(services).await,

            EquityRedemptionCommand::AwaitCompletion => {
                self.handle_await_completion(services).await
            }
        }
    }
}

impl Lifecycle<EquityRedemption, Never> {
    async fn handle_redeem(
        &self,
        services: &RedemptionServices,
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => {
                let vault_id = services.vault.lookup_vault_id(token).await?;

                let vault_withdraw_tx = services
                    .vault
                    .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
                    .await?;

                let now = Utc::now();
                let vault_withdrawn = EquityRedemptionEvent::VaultWithdrawn {
                    symbol: symbol.clone(),
                    quantity,
                    token,
                    vault_withdraw_tx,
                    withdrawn_at: now,
                };

                match services.tokenizer.send_for_redemption(token, amount).await {
                    Ok(redemption_tx) => {
                        let redemption_wallet = services.tokenizer.redemption_wallet();
                        Ok(vec![
                            vault_withdrawn,
                            EquityRedemptionEvent::TokensSent {
                                redemption_wallet,
                                redemption_tx,
                                sent_at: now,
                            },
                        ])
                    }
                    Err(e) => {
                        // Vault withdraw succeeded but transfer failed - emit both events
                        // Extract tx_hash if available (when tx was sent but receipt failed)
                        let tx_hash = match &e {
                            TokenizerError::Alpaca(AlpacaTokenizationError::Transaction {
                                tx_hash,
                                ..
                            }) => Some(*tx_hash),

                            TokenizerError::Alpaca(_) => None,
                        };
                        Ok(vec![
                            vault_withdrawn,
                            EquityRedemptionEvent::TransferFailed {
                                tx_hash,
                                failed_at: now,
                            },
                        ])
                    }
                }
            }
            Ok(EquityRedemption::Failed { .. }) => Err(EquityRedemptionError::AlreadyFailed),
            Ok(_) => Err(EquityRedemptionError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    async fn handle_detect(
        &self,
        services: &RedemptionServices,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        let redemption_tx = match self.live() {
            Err(LifecycleError::Uninitialized) | Ok(EquityRedemption::VaultWithdrawn { .. }) => {
                return Err(EquityRedemptionError::TokensNotSent);
            }
            Ok(EquityRedemption::TokensSent { redemption_tx, .. }) => *redemption_tx,
            Ok(EquityRedemption::Failed { .. }) => {
                return Err(EquityRedemptionError::AlreadyFailed);
            }
            Ok(_) => return Err(EquityRedemptionError::AlreadyCompleted),
            Err(e) => return Err(e.into()),
        };

        match services.tokenizer.poll_for_redemption(&redemption_tx).await {
            Ok(request) => Ok(vec![EquityRedemptionEvent::Detected {
                tokenization_request_id: request.id,
                detected_at: Utc::now(),
            }]),
            Err(e) => {
                let failure = detection_failure_from_error(&e);
                Ok(vec![EquityRedemptionEvent::DetectionFailed {
                    failure,
                    failed_at: Utc::now(),
                }])
            }
        }
    }

    async fn handle_await_completion(
        &self,
        services: &RedemptionServices,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        let tokenization_request_id = match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(EquityRedemption::VaultWithdrawn { .. } | EquityRedemption::TokensSent { .. }) => {
                return Err(EquityRedemptionError::NotPending);
            }
            Ok(EquityRedemption::Pending {
                tokenization_request_id,
                ..
            }) => tokenization_request_id.clone(),
            Ok(EquityRedemption::Completed { .. }) => {
                return Err(EquityRedemptionError::AlreadyCompleted);
            }
            Ok(EquityRedemption::Failed { .. }) => {
                return Err(EquityRedemptionError::AlreadyFailed);
            }
            Err(e) => return Err(e.into()),
        };

        let request = services
            .tokenizer
            .poll_redemption_until_complete(&tokenization_request_id)
            .await?;

        match request.status {
            TokenizationRequestStatus::Completed => Ok(vec![EquityRedemptionEvent::Completed {
                completed_at: Utc::now(),
            }]),
            TokenizationRequestStatus::Rejected => {
                Ok(vec![EquityRedemptionEvent::RedemptionRejected {
                    rejected_at: Utc::now(),
                }])
            }
            TokenizationRequestStatus::Pending => {
                Err(EquityRedemptionError::UnexpectedPendingStatus)
            }
        }
    }
}

fn detection_failure_from_error(e: &TokenizerError) -> DetectionFailure {
    match e {
        TokenizerError::Alpaca(AlpacaTokenizationError::PollTimeout { .. }) => {
            DetectionFailure::Timeout
        }
        TokenizerError::Alpaca(AlpacaTokenizationError::ApiError { status, .. }) => {
            DetectionFailure::ApiError {
                status_code: Some(status.as_u16()),
            }
        }
        TokenizerError::Alpaca(_) => DetectionFailure::ApiError { status_code: None },
    }
}

impl EquityRedemption {
    /// Apply a transition event to an existing redemption state.
    pub(crate) fn apply_transition(
        event: &EquityRedemptionEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            EquityRedemptionEvent::VaultWithdrawn { .. } => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
            }),

            EquityRedemptionEvent::TransferFailed { tx_hash, failed_at } => {
                current.apply_transfer_failed(*tx_hash, *failed_at, event)
            }

            EquityRedemptionEvent::TokensSent {
                redemption_wallet,
                redemption_tx,
                sent_at,
            } => current.apply_tokens_sent(*redemption_wallet, *redemption_tx, *sent_at, event),

            EquityRedemptionEvent::DetectionFailed { failed_at, .. } => {
                current.apply_detection_failed(*failed_at, event)
            }

            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => current.apply_detected(tokenization_request_id, *detected_at, event),

            EquityRedemptionEvent::RedemptionRejected { rejected_at } => {
                current.apply_redemption_rejected(*rejected_at, event)
            }

            EquityRedemptionEvent::Completed { completed_at } => {
                current.apply_completed(*completed_at, event)
            }
        }
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(event: &EquityRedemptionEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            EquityRedemptionEvent::VaultWithdrawn {
                symbol,
                quantity,
                token,
                vault_withdraw_tx,
                withdrawn_at,
            } => Ok(Self::VaultWithdrawn {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                vault_withdraw_tx: *vault_withdraw_tx,
                withdrawn_at: *withdrawn_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: format!("{event:?}"),
            }),
        }
    }

    fn apply_tokens_sent(
        &self,
        redemption_wallet: Address,
        redemption_tx: TxHash,
        sent_at: DateTime<Utc>,
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

        Ok(Self::TokensSent {
            symbol: symbol.clone(),
            quantity: *quantity,
            token: *token,
            vault_withdraw_tx: *vault_withdraw_tx,
            redemption_wallet,
            redemption_tx,
            sent_at,
        })
    }

    fn apply_transfer_failed(
        &self,
        tx_hash: Option<TxHash>,
        failed_at: DateTime<Utc>,
        event: &EquityRedemptionEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::VaultWithdrawn {
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
            redemption_tx: tx_hash,
            tokenization_request_id: None,
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
            failed_at,
        })
    }

    fn apply_redemption_rejected(
        &self,
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
            failed_at: rejected_at,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::onchain::mock::MockVault;
    use crate::tokenization::mock::{MockCompletionOutcome, MockDetectionOutcome, MockTokenizer};

    pub(crate) fn mock_redeemer_services() -> RedemptionServices {
        RedemptionServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            vault: Arc::new(MockVault::new()),
        }
    }

    fn services_with_detection(outcome: MockDetectionOutcome) -> RedemptionServices {
        RedemptionServices {
            tokenizer: Arc::new(MockTokenizer::new().with_detection_outcome(outcome)),
            vault: Arc::new(MockVault::new()),
        }
    }

    fn services_with_completion(outcome: MockCompletionOutcome) -> RedemptionServices {
        RedemptionServices {
            tokenizer: Arc::new(MockTokenizer::new().with_completion_outcome(outcome)),
            vault: Arc::new(MockVault::new()),
        }
    }

    #[tokio::test]
    async fn test_redeem_from_uninitialized() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let services = mock_redeemer_services();

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &services,
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::VaultWithdrawn { .. }
        ));
        assert!(matches!(
            events[1],
            EquityRedemptionEvent::TokensSent { .. }
        ));
    }

    fn apply_vault_withdrawn_and_tokens_sent(
        aggregate: &mut Lifecycle<EquityRedemption, Never>,
        symbol: Symbol,
    ) -> (Address, TxHash) {
        let redemption_wallet = Address::random();
        let redemption_tx = TxHash::random();

        let vault_event = EquityRedemptionEvent::VaultWithdrawn {
            symbol,
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        };
        aggregate.apply(vault_event);

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
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect,
                &services_with_detection(MockDetectionOutcome::Detected),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Detected { .. }));
    }

    #[tokio::test]
    async fn test_await_completion_from_pending() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::AwaitCompletion,
                &services_with_completion(MockCompletionOutcome::Completed),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn test_complete_redemption_flow_end_to_end() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let redeem_events = aggregate
            .handle(
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
                &mock_redeemer_services(),
            )
            .await
            .unwrap();
        assert_eq!(redeem_events.len(), 2);
        for event in redeem_events {
            aggregate.apply(event);
        }

        let detect_events = aggregate
            .handle(
                EquityRedemptionCommand::Detect,
                &services_with_detection(MockDetectionOutcome::Detected),
            )
            .await
            .unwrap();
        assert_eq!(detect_events.len(), 1);
        aggregate.apply(detect_events[0].clone());

        let complete_events = aggregate
            .handle(
                EquityRedemptionCommand::AwaitCompletion,
                &services_with_completion(MockCompletionOutcome::Completed),
            )
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
            .handle(EquityRedemptionCommand::Detect, &mock_redeemer_services())
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[tokio::test]
    async fn test_cannot_await_completion_before_pending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::AwaitCompletion,
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::NotPending)));
    }

    #[tokio::test]
    async fn test_detect_timeout_emits_detection_failed() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect,
                &services_with_detection(MockDetectionOutcome::Timeout),
            )
            .await
            .unwrap();

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
    async fn test_await_completion_rejected_emits_redemption_rejected() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::AwaitCompletion,
                &services_with_completion(MockCompletionOutcome::Rejected),
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
    async fn test_cannot_await_completion_from_tokens_sent() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let result = aggregate
            .handle(
                EquityRedemptionCommand::AwaitCompletion,
                &mock_redeemer_services(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::NotPending)));
    }

    #[tokio::test]
    async fn test_redemption_rejected_preserves_context_with_tokenization_id() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let (_, redemption_tx) =
            apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol.clone());

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let rejected_event = EquityRedemptionEvent::RedemptionRejected {
            rejected_at: Utc::now(),
        };
        aggregate.apply(rejected_event);

        let Lifecycle::Live(EquityRedemption::Failed {
            symbol: failed_symbol,
            quantity,
            redemption_tx: failed_redemption_tx,
            tokenization_request_id,
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
    }

    #[tokio::test]
    async fn test_detect_api_error_emits_detection_failed() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        apply_vault_withdrawn_and_tokens_sent(&mut aggregate, symbol);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect,
                &services_with_detection(MockDetectionOutcome::ApiError),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::ApiError { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_cannot_detect_from_vault_withdrawn() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let vault_event = EquityRedemptionEvent::VaultWithdrawn {
            symbol,
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        };
        aggregate.apply(vault_event);

        let result = aggregate
            .handle(EquityRedemptionCommand::Detect, &mock_redeemer_services())
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
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
            failure: DetectionFailure::Timeout,
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
        };

        let event = EquityRedemptionEvent::RedemptionRejected {
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
}
