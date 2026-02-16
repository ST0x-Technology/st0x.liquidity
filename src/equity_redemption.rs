//! Aggregate modeling the lifecycle of redeeming tokenized
//! equities for underlying shares.
//!
//! Tracks the workflow from withdrawing tokens from the vault
//! through sending to Alpaca's redemption wallet to share delivery.
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

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::{info, warn};

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_execution::Symbol;

use crate::rebalancing::transfer::EquityTransferServices;
use crate::tokenization::Tokenizer;
use crate::tokenized_equity_mint::TokenizationRequestId;

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

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
    /// Vault lookup failed for the given token
    #[error("Token {0} not found in vault registry")]
    VaultNotFound(Address),
    /// Vault withdrawal transaction failed
    #[error("Vault withdraw failed")]
    VaultWithdrawFailed,
    /// Sending tokens to Alpaca redemption wallet failed
    #[error("Send for redemption failed")]
    SendForRedemptionFailed,
    /// Transaction failed with a known tx hash
    #[error("Transaction failed: {tx_hash}")]
    TransactionFailed { tx_hash: TxHash },
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
    /// Atomic command: withdraws from vault and sends to Alpaca.
    /// Emits VaultWithdrawn, then TokensSent on success.
    /// If vault withdraw succeeds but send fails, stays in VaultWithdrawn.
    Redeem {
        symbol: Symbol,
        quantity: Decimal,
        token: Address,
        amount: U256,
    },
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
impl EventSourced for EquityRedemption {
    type Id = RedemptionAggregateId;
    type Event = EquityRedemptionEvent;
    type Command = EquityRedemptionCommand;
    type Error = EquityRedemptionError;
    type Services = EquityTransferServices;

    const AGGREGATE_TYPE: &'static str = "EquityRedemption";
    const PROJECTION: Option<Table> = Some(Table("equity_redemption_view"));
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use EquityRedemptionEvent::*;
        match event {
            VaultWithdrawn {
                symbol,
                quantity,
                token,
                vault_withdraw_tx,
                withdrawn_at,
            } => Some(Self::VaultWithdrawn {
                symbol: symbol.clone(),
                quantity: *quantity,
                token: *token,
                vault_withdraw_tx: *vault_withdraw_tx,
                withdrawn_at: *withdrawn_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use EquityRedemptionEvent::*;

        Ok(match event {
            VaultWithdrawn { .. } => None,

            TransferFailed { tx_hash, failed_at } => {
                let Self::VaultWithdrawn {
                    symbol,
                    quantity,
                    vault_withdraw_tx,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    vault_withdraw_tx: Some(*vault_withdraw_tx),
                    redemption_tx: *tx_hash,
                    tokenization_request_id: None,
                    failed_at: *failed_at,
                })
            }

            TokensSent {
                redemption_wallet,
                redemption_tx,
                sent_at,
            } => {
                let Self::VaultWithdrawn {
                    symbol,
                    quantity,
                    token,
                    vault_withdraw_tx,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::TokensSent {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    token: *token,
                    vault_withdraw_tx: *vault_withdraw_tx,
                    redemption_wallet: *redemption_wallet,
                    redemption_tx: *redemption_tx,
                    sent_at: *sent_at,
                })
            }

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
                    vault_withdraw_tx,
                    redemption_tx,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    vault_withdraw_tx: Some(*vault_withdraw_tx),
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
                    vault_withdraw_tx: None,
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
                        warn!(%error, %token, "Vault lookup failed");
                        return Err(EquityRedemptionError::VaultNotFound(token));
                    }
                };

                info!(?vault_id, %token, %amount, "Withdrawing tokens from vault");

                let vault_withdraw_tx = match services
                    .raindex
                    .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
                    .await
                {
                    Ok(tx) => tx,
                    Err(error) => {
                        warn!(%error, %token, %amount, "Vault withdrawal failed");
                        return Err(EquityRedemptionError::VaultWithdrawFailed);
                    }
                };

                let now = Utc::now();
                let vault_withdrawn = VaultWithdrawn {
                    symbol,
                    quantity,
                    token,
                    vault_withdraw_tx,
                    withdrawn_at: now,
                };

                info!(%token, %amount, "Sending tokens for redemption");

                match Tokenizer::send_for_redemption(services.tokenizer.as_ref(), token, amount)
                    .await
                {
                    Ok(redemption_tx) => {
                        let redemption_wallet =
                            Tokenizer::redemption_wallet(services.tokenizer.as_ref());

                        Ok(vec![
                            vault_withdrawn,
                            TokensSent {
                                redemption_wallet,
                                redemption_tx,
                                sent_at: now,
                            },
                        ])
                    }
                    Err(error) => {
                        warn!(%error, %token, %amount, "Send for redemption failed");
                        Ok(vec![
                            vault_withdrawn,
                            TransferFailed {
                                tx_hash: None,
                                failed_at: now,
                            },
                        ])
                    }
                }
            }
            Detect { .. } | FailDetection { .. } | Complete | RejectRedemption { .. } => {
                Err(EquityRedemptionError::NotStarted)
            }
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
            Redeem { .. } => match self {
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
                Self::VaultWithdrawn { .. } => Err(EquityRedemptionError::TokensNotSent),
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            FailDetection { failure } => match self {
                Self::TokensSent { .. } => Ok(vec![DetectionFailed {
                    failure,
                    failed_at: Utc::now(),
                }]),
                Self::VaultWithdrawn { .. } => Err(EquityRedemptionError::TokensNotSent),
                Self::Pending { .. } => Err(EquityRedemptionError::AlreadyDetected),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
            },

            Complete => match self {
                Self::VaultWithdrawn { .. } | Self::TokensSent { .. } => {
                    Err(EquityRedemptionError::NotPending)
                }
                Self::Pending { .. } => Ok(vec![Completed {
                    completed_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(EquityRedemptionError::AlreadyCompleted),
                Self::Failed { .. } => Err(EquityRedemptionError::AlreadyFailed),
            },

            RejectRedemption { reason } => match self {
                Self::VaultWithdrawn { .. } | Self::TokensSent { .. } => {
                    Err(EquityRedemptionError::NotPendingForRejection)
                }
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
pub(crate) mod tests {
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore, replay};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::mock::MockTokenizer;

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
        }
    }

    fn vault_withdrawn_event() -> EquityRedemptionEvent {
        EquityRedemptionEvent::VaultWithdrawn {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(50.25),
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
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
    async fn redeem_from_uninitialized_produces_vault_withdrawn_and_tokens_sent() {
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

    #[tokio::test]
    async fn detect_after_tokens_sent_produces_detected() {
        let events = TestHarness::<EquityRedemption>::with(mock_services())
            .given(vec![vault_withdrawn_event(), tokens_sent_event()])
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
                vault_withdrawn_event(),
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
            .given(vec![vault_withdrawn_event(), tokens_sent_event()])
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
                vault_withdrawn_event(),
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
            .given(vec![vault_withdrawn_event(), tokens_sent_event()])
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
            EquityRedemptionEvent::VaultWithdrawn {
                symbol: symbol.clone(),
                quantity: dec!(50.25),
                token: Address::random(),
                vault_withdraw_tx: TxHash::random(),
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
            vault_withdraw_tx: TxHash::random(),
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
            vault_withdraw_tx: TxHash::random(),
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
