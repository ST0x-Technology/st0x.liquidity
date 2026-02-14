//! Aggregate modeling the lifecycle of redeeming tokenized
//! equities for underlying shares.
//!
//! Tracks the workflow from transferring tokens to Alpaca's
//! redemption wallet through to share delivery.
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
//! 1. **Send**: System initiates onchain transfer to
//!    Alpaca's redemption wallet
//! 2. **Detection**: Alpaca detects the transfer and
//!    returns `tokenization_request_id`
//! 3. **Processing**: Alpaca processes the redemption and credits the account
//! 4. **Completion**: System confirms redemption is complete
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - Failed state preserves context (tx_hash,
//!   tokenization_request_id) depending on when failure occurred
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_execution::Symbol;
use std::str::FromStr;

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};

use crate::tokenized_equity_mint::TokenizationRequestId;

/// Unique identifier for a redemption aggregate instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Attempted to detect redemption before sending tokens
    #[error("Cannot detect redemption: tokens not sent")]
    TokensNotSent,
    /// Attempted to complete before redemption was detected as pending
    #[error("Cannot complete: not in pending state")]
    NotPending,
    /// Attempted to reject before redemption was detected as pending
    #[error("Cannot reject: not in pending state")]
    NotPendingForRejection,
    /// Attempted to send tokens when redemption is already in progress
    #[error("Already started")]
    AlreadyStarted,
    /// Attempted to modify a completed redemption operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed redemption operation
    #[error("Already failed")]
    AlreadyFailed,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
impl EventSourced for EquityRedemption {
    type Id = RedemptionAggregateId;
    type Event = EquityRedemptionEvent;
    type Command = EquityRedemptionCommand;
    type Error = EquityRedemptionError;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "EquityRedemption";
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use EquityRedemptionEvent::*;
        match event {
            TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at,
            } => Some(Self::TokensSent {
                symbol: symbol.clone(),
                quantity: *quantity,
                redemption_wallet: *redemption_wallet,
                tx_hash: *tx_hash,
                sent_at: *sent_at,
            }),
            _ => None,
        }
    }

    fn evolve(event: &Self::Event, state: &Self) -> Result<Option<Self>, Self::Error> {
        use EquityRedemptionEvent::*;
        Ok(match event {
            TokensSent { .. } => None,

            Detected {
                tokenization_request_id,
                detected_at,
            } => state.try_apply_detected(tokenization_request_id, *detected_at),

            DetectionFailed { reason, failed_at } => {
                state.try_apply_detection_failed(reason, *failed_at)
            }

            Completed { completed_at } => state.try_apply_completed(*completed_at),

            RedemptionRejected {
                reason,
                rejected_at,
            } => state.try_apply_redemption_rejected(reason, *rejected_at),
        })
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            SendTokens {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
            } => Ok(vec![TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at: Utc::now(),
            }]),
            Detect { .. } | FailDetection { .. } => Err(EquityRedemptionError::TokensNotSent),
            Complete => Err(EquityRedemptionError::NotPending),
            RejectRedemption { .. } => Err(EquityRedemptionError::NotPendingForRejection),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use EquityRedemptionCommand::*;
        use EquityRedemptionEvent::*;
        match command {
            SendTokens { .. } => match self {
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                _ => Err(EquityRedemptionError::AlreadyStarted),
            },

            Detect {
                tokenization_request_id,
            } => match self {
                Self::TokensSent { .. } => Ok(vec![Detected {
                    tokenization_request_id,
                    detected_at: Utc::now(),
                }]),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                _ => Err(EquityRedemptionError::AlreadyCompleted),
            },

            FailDetection { reason } => match self {
                Self::TokensSent { .. } => Ok(vec![DetectionFailed {
                    reason,
                    failed_at: Utc::now(),
                }]),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Pending { .. } | Self::Completed { .. } => {
                    Err(EquityRedemptionError::AlreadyCompleted)
                }
            },

            Complete => match self {
                Self::TokensSent { .. } => Err(EquityRedemptionError::NotPending),
                Self::Pending { .. } => Ok(vec![Completed {
                    completed_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
            },

            RejectRedemption { reason } => match self {
                Self::TokensSent { .. } => Err(EquityRedemptionError::NotPendingForRejection),
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

impl EquityRedemption {
    fn try_apply_detected(
        &self,
        tokenization_request_id: &TokenizationRequestId,
        detected_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::TokensSent {
            symbol,
            quantity,
            tx_hash,
            sent_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Pending {
            symbol: symbol.clone(),
            quantity: *quantity,
            tx_hash: *tx_hash,
            tokenization_request_id: tokenization_request_id.clone(),
            sent_at: *sent_at,
            detected_at,
        })
    }

    fn try_apply_completed(&self, completed_at: DateTime<Utc>) -> Option<Self> {
        let Self::Pending {
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Completed {
            symbol: symbol.clone(),
            quantity: *quantity,
            tx_hash: *tx_hash,
            tokenization_request_id: tokenization_request_id.clone(),
            completed_at,
        })
    }

    fn try_apply_detection_failed(&self, reason: &str, failed_at: DateTime<Utc>) -> Option<Self> {
        let Self::TokensSent {
            symbol,
            quantity,
            tx_hash,
            sent_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            tx_hash: *tx_hash,
            tokenization_request_id: None,
            reason: reason.to_string(),
            sent_at: *sent_at,
            failed_at,
        })
    }

    fn try_apply_redemption_rejected(
        &self,
        reason: &str,
        rejected_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::Pending {
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            sent_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
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

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};

    use super::*;

    fn tokens_sent_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokensSent {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
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
    async fn send_tokens_from_uninitialized_produces_tokens_sent() {
        let events = TestHarness::<EquityRedemption>::with(())
            .given_no_previous_events()
            .when(EquityRedemptionCommand::SendTokens {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: dec!(50.25),
                redemption_wallet: Address::random(),
                tx_hash: TxHash::random(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::TokensSent { .. }
        ));
    }

    #[tokio::test]
    async fn detect_after_tokens_sent_produces_detected() {
        let events = TestHarness::<EquityRedemption>::with(())
            .given(vec![tokens_sent_event()])
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
        let events = TestHarness::<EquityRedemption>::with(())
            .given(vec![tokens_sent_event(), detected_event()])
            .when(EquityRedemptionCommand::Complete)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn complete_redemption_flow_end_to_end() {
        let store = TestStore::<EquityRedemption>::new(vec![], ());
        let id = RedemptionAggregateId::new("end-to-end");

        store
            .send(
                &id,
                EquityRedemptionCommand::SendTokens {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(50.25),
                    redemption_wallet: Address::random(),
                    tx_hash: TxHash::random(),
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

    #[tokio::test]
    async fn cannot_detect_before_sending_tokens() {
        let error = TestHarness::<EquityRedemption>::with(())
            .given_no_previous_events()
            .when(EquityRedemptionCommand::Detect {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::TokensNotSent)
        ));
    }

    #[tokio::test]
    async fn cannot_complete_before_pending() {
        let error = TestHarness::<EquityRedemption>::with(())
            .given_no_previous_events()
            .when(EquityRedemptionCommand::Complete)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotPending)
        ));
    }

    #[tokio::test]
    async fn fail_detection_from_tokens_sent_state() {
        let events = TestHarness::<EquityRedemption>::with(())
            .given(vec![tokens_sent_event()])
            .when(EquityRedemptionCommand::FailDetection {
                reason: "Alpaca timeout".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::DetectionFailed { .. }
        ));
    }

    #[tokio::test]
    async fn reject_redemption_from_pending_state() {
        let events = TestHarness::<EquityRedemption>::with(())
            .given(vec![tokens_sent_event(), detected_event()])
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "Insufficient balance".to_string(),
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
        let error = TestHarness::<EquityRedemption>::with(())
            .given(vec![tokens_sent_event()])
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "Cannot reject yet".to_string(),
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
        let tx_hash = TxHash::random();

        let entity = replay::<EquityRedemption>(vec![
            EquityRedemptionEvent::TokensSent {
                symbol: symbol.clone(),
                quantity: dec!(50.25),
                redemption_wallet: Address::random(),
                tx_hash,
                sent_at: Utc::now(),
            },
            EquityRedemptionEvent::Detected {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                detected_at: Utc::now(),
            },
            EquityRedemptionEvent::RedemptionRejected {
                reason: "Insufficient balance".to_string(),
                rejected_at: Utc::now(),
            },
        ])
        .unwrap();

        let EquityRedemption::Failed {
            symbol: failed_symbol,
            quantity,
            tx_hash: failed_tx_hash,
            tokenization_request_id,
            sent_at,
            reason,
            ..
        } = entity
        else {
            panic!("Expected Failed state, got {entity:?}");
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
    async fn cannot_fail_detection_before_sending() {
        let error = TestHarness::<EquityRedemption>::with(())
            .given_no_previous_events()
            .when(EquityRedemptionCommand::FailDetection {
                reason: "Cannot fail".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::TokensNotSent)
        ));
    }

    #[tokio::test]
    async fn cannot_reject_redemption_before_sending() {
        let error = TestHarness::<EquityRedemption>::with(())
            .given_no_previous_events()
            .when(EquityRedemptionCommand::RejectRedemption {
                reason: "Cannot reject".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(EquityRedemptionError::NotPendingForRejection)
        ));
    }

    #[test]
    fn test_evolve_detected_rejects_wrong_state() {
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

        let result = EquityRedemption::evolve(&event, &completed).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_completed_rejects_wrong_state() {
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

        let result = EquityRedemption::evolve(&event, &tokens_sent).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_detection_failed_rejects_non_tokens_sent_states() {
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

        let result = EquityRedemption::evolve(&event, &pending).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_redemption_rejected_rejects_non_pending_states() {
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

        let result = EquityRedemption::evolve(&event, &tokens_sent).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejects_tokens_sent_event_on_live_state() {
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

        let result = EquityRedemption::evolve(&event, &tokens_sent).unwrap();
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
    async fn send_tokens_when_tokens_already_sent_returns_already_started() {
        let store = TestStore::<EquityRedemption>::new(vec![], ());
        let id = RedemptionAggregateId::new("redemption-1");

        store
            .send(
                &id,
                EquityRedemptionCommand::SendTokens {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(10),
                    redemption_wallet: Address::random(),
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                EquityRedemptionCommand::SendTokens {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(10),
                    redemption_wallet: Address::random(),
                    tx_hash: TxHash::random(),
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
    async fn send_tokens_when_pending_returns_already_started() {
        let store = TestStore::<EquityRedemption>::new(vec![], ());
        let id = RedemptionAggregateId::new("redemption-1");

        store
            .send(
                &id,
                EquityRedemptionCommand::SendTokens {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(10),
                    redemption_wallet: Address::random(),
                    tx_hash: TxHash::random(),
                },
            )
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
                EquityRedemptionCommand::SendTokens {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: dec!(10),
                    redemption_wallet: Address::random(),
                    tx_hash: TxHash::random(),
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
