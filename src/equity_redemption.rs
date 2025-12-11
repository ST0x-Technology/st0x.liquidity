//! Equity Redemption aggregate for converting onchain tokens back to offchain Alpaca shares.
//!
//! This module implements the CQRS-ES aggregate pattern for managing the asynchronous workflow
//! of redeeming tokenized equity shares. It tracks the complete lifecycle from sending tokens
//! to Alpaca's redemption wallet through detecting the redemption to final completion.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//! (start) --SendTokens--> TokensSent --Detect--> Pending --Complete--> Completed
//!                              |                    |
//!                              |                    |
//!                              v                    v
//!                           Failed <-----Fail----- Failed
//! ```
//!
//! - `TokensSent` and `Pending` can transition to `Failed`
//! - `Completed` and `Failed` are terminal states
//!
//! # Alpaca API Integration
//!
//! The redemption process integrates with Alpaca's redemption API:
//!
//! 1. **Send**: System initiates onchain transfer to Alpaca's redemption wallet
//! 2. **Detection**: Alpaca detects the transfer and returns `tokenization_request_id`
//! 3. **Processing**: Alpaca processes the redemption and credits the account
//! 4. **Completion**: System confirms redemption is complete
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - Failed state preserves context (tx_hash, tokenization_request_id) depending on when failure occurred
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteEventRepository;
use st0x_broker::Symbol;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::tokenized_equity_mint::TokenizationRequestId;

/// SQLite-backed event store for EquityRedemption aggregates.
pub(crate) type RedemptionEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<EquityRedemption, Never>>;

/// Errors that can occur during equity redemption operations.
///
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityRedemptionError {
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
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    SendTokens {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
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
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    },

    Detected {
        tokenization_request_id: TokenizationRequestId,
        detected_at: DateTime<Utc>,
    },
    /// Alpaca failed to detect the token transfer.
    /// Tokens were sent but detection failed - keep inflight until manually resolved.
    DetectionFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    Completed {
        completed_at: DateTime<Utc>,
    },
    /// Alpaca rejected the redemption after detection.
    /// Tokens location unknown after rejection - keep inflight until manually resolved.
    RedemptionRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },
}

impl DomainEvent for EquityRedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
            Self::Detected { .. } => "EquityRedemptionEvent::Detected".to_string(),
            Self::DetectionFailed { .. } => "EquityRedemptionEvent::DetectionFailed".to_string(),
            Self::Completed { .. } => "EquityRedemptionEvent::Completed".to_string(),
            Self::RedemptionRejected { .. } => {
                "EquityRedemptionEvent::RedemptionRejected".to_string()
            }
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
    /// Tokens sent to Alpaca's redemption wallet with transaction hash
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    },

    /// Alpaca detected the token transfer and returned tracking identifier
    Pending {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },

    /// Redemption successfully completed and account credited (terminal state)
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        completed_at: DateTime<Utc>,
    },

    /// Redemption failed with error reason (terminal state)
    ///
    /// Fields preserve context depending on when failure occurred:
    /// - `tx_hash`: Always present (failure can only occur after tokens sent)
    /// - `tokenization_request_id`: Present if Alpaca detected the transfer
    /// - `sent_at`: Always present (failure can only occur after tokens sent)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: Option<TokenizationRequestId>,
        reason: String,
        sent_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl Aggregate for Lifecycle<EquityRedemption, Never> {
    type Command = EquityRedemptionCommand;
    type Event = EquityRedemptionEvent;
    type Error = EquityRedemptionError;
    type Services = ();

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
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            EquityRedemptionCommand::SendTokens {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
            } => self.handle_send_tokens(symbol, *quantity, *redemption_wallet, *tx_hash),

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
    fn handle_send_tokens(
        &self,
        symbol: &Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
    ) -> Result<Vec<EquityRedemptionEvent>, EquityRedemptionError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Ok(vec![EquityRedemptionEvent::TokensSent {
                symbol: symbol.clone(),
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at: Utc::now(),
            }]),
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
            Err(LifecycleError::Uninitialized) => Err(EquityRedemptionError::TokensNotSent),
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
            Err(LifecycleError::Uninitialized) | Ok(EquityRedemption::TokensSent { .. }) => {
                Err(EquityRedemptionError::NotPending)
            }
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
            Err(LifecycleError::Uninitialized) => Err(EquityRedemptionError::TokensNotSent),
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
            Err(LifecycleError::Uninitialized) | Ok(EquityRedemption::TokensSent { .. }) => {
                Err(EquityRedemptionError::NotPendingForRejection)
            }
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
            EquityRedemptionEvent::TokensSent { .. } => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
            }),

            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => current.apply_detected(tokenization_request_id, *detected_at, event),

            EquityRedemptionEvent::DetectionFailed { reason, failed_at } => {
                current.apply_detection_failed(reason, *failed_at, event)
            }

            EquityRedemptionEvent::Completed { completed_at } => {
                current.apply_completed(*completed_at, event)
            }

            EquityRedemptionEvent::RedemptionRejected {
                reason,
                rejected_at,
            } => current.apply_redemption_rejected(reason, *rejected_at, event),
        }
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(event: &EquityRedemptionEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            EquityRedemptionEvent::TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at,
            } => Ok(Self::TokensSent {
                symbol: symbol.clone(),
                quantity: *quantity,
                redemption_wallet: *redemption_wallet,
                tx_hash: *tx_hash,
                sent_at: *sent_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: format!("{event:?}"),
            }),
        }
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
            tx_hash,
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
            tx_hash: *tx_hash,
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
            tx_hash,
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
            tx_hash: *tx_hash,
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
            tx_hash,
            sent_at,
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
            tx_hash: *tx_hash,
            tokenization_request_id: None,
            reason: reason.to_string(),
            sent_at: *sent_at,
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
            tx_hash,
            tokenization_request_id,
            sent_at,
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
            tx_hash: *tx_hash,
            tokenization_request_id: Some(tokenization_request_id.clone()),
            reason: reason.to_string(),
            sent_at: *sent_at,
            failed_at: rejected_at,
        })
    }
}

impl View<Self> for Lifecycle<EquityRedemption, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, EquityRedemption::apply_transition)
            .or_initialize(&event.payload, EquityRedemption::from_event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_send_tokens_from_uninitialized() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let events = aggregate
            .handle(
                EquityRedemptionCommand::SendTokens {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    redemption_wallet,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::TokensSent { .. }
        ));
    }

    #[tokio::test]
    async fn test_detect_after_tokens_sent() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &(),
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
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn test_complete_redemption_flow_end_to_end() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let send_events = aggregate
            .handle(
                EquityRedemptionCommand::SendTokens {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    redemption_wallet,
                    tx_hash,
                },
                &(),
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
                &(),
            )
            .await
            .unwrap();
        assert_eq!(detect_events.len(), 1);
        aggregate.apply(detect_events[0].clone());

        let complete_events = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
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
                &(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[tokio::test]
    async fn test_cannot_complete_before_pending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::NotPending)));
    }

    #[tokio::test]
    async fn test_fail_detection_from_tokens_sent_state() {
        let mut aggregate = Lifecycle::<EquityRedemption, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::FailDetection {
                    reason: "Alpaca timeout".to_string(),
                },
                &(),
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
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

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
                &(),
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
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let result = aggregate
            .handle(
                EquityRedemptionCommand::RejectRedemption {
                    reason: "Cannot reject yet".to_string(),
                },
                &(),
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
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol: symbol.clone(),
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

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
            tx_hash: failed_tx_hash,
            tokenization_request_id,
            sent_at,
            reason,
            ..
        }) = aggregate
        else {
            panic!("Expected Failed state, got {aggregate:?}");
        };

        assert_eq!(failed_symbol, symbol);
        assert_eq!(quantity, dec!(50.25));
        assert_eq!(failed_tx_hash, tx_hash);
        assert_eq!(
            tokenization_request_id,
            Some(TokenizationRequestId("REQ789".to_string()))
        );
        assert_eq!(reason, "Insufficient balance");
        assert!(sent_at > DateTime::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn test_cannot_fail_detection_before_sending() {
        let aggregate = Lifecycle::<EquityRedemption, Never>::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::FailDetection {
                    reason: "Cannot fail".to_string(),
                },
                &(),
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
                &(),
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
            tx_hash: TxHash::random(),
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
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
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
            tx_hash: TxHash::random(),
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
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
            sent_at: Utc::now(),
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
    fn test_apply_transition_rejects_tokens_sent_event() {
        let tokens_sent = EquityRedemption::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
            sent_at: Utc::now(),
        };

        let event = EquityRedemptionEvent::TokensSent {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: dec!(25.0),
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
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
