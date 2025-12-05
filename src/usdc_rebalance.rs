//! USDC Rebalance aggregate for cross-chain USDC transfers between Alpaca and Base.
//!
//! This module implements the CQRS-ES aggregate pattern for managing the asynchronous workflow
//! of rebalancing USDC between Alpaca (offchain) and Base (onchain) via Circle's Cross-Chain
//! Transfer Protocol (CCTP).
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//! (start) --Initiate--> Withdrawing --ConfirmWithdrawal--> WithdrawalComplete
//!                              |                                          |
//!                              v                                          v
//!                       WithdrawalFailed                          (future states)
//! ```
//!
//! Terminal states: `WithdrawalFailed`, `BridgingFailed`, `DepositFailed`, `DepositConfirmed`
//!
//! # Direction
//!
//! - `AlpacaToBase`: Withdraw from Alpaca → CCTP bridge → Deposit to Rain vault on Base
//! - `BaseToAlpaca`: Withdraw from Rain vault → CCTP bridge → Deposit to Alpaca

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};

use crate::alpaca_wallet::AlpacaTransferId;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::threshold::Usdc;

/// Unique identifier for a USDC rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UsdcRebalanceId(pub(crate) String);

impl UsdcRebalanceId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Reference to a transfer, either via Alpaca API or onchain transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TransferRef {
    AlpacaId(AlpacaTransferId),
    OnchainTx(TxHash),
}

/// Direction of the USDC rebalancing operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RebalanceDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

/// Errors that can occur during USDC rebalance operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UsdcRebalanceError {
    /// Attempted to initiate when already in progress
    #[error("Rebalancing has already been initiated")]
    AlreadyInitiated,
    /// Withdrawal has not been initiated yet
    #[error("Withdrawal has not been initiated")]
    WithdrawalNotInitiated,
    /// Withdrawal has already been confirmed or failed
    #[error("Withdrawal has already completed")]
    WithdrawalAlreadyCompleted,
    /// Withdrawal has not been confirmed yet
    #[error("Withdrawal has not been confirmed")]
    WithdrawalNotConfirmed,
    /// Bridging has not been initiated yet
    #[error("Bridging has not been initiated")]
    BridgingNotInitiated,
    /// Command not valid for current state
    #[error("Command {command} not valid for state {state}")]
    InvalidCommand { command: String, state: String },
    /// Lifecycle state error
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

/// Commands for the USDC rebalance aggregate.
#[derive(Debug, Clone)]
pub(crate) enum UsdcRebalanceCommand {
    Initiate {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
    },
    ConfirmWithdrawal,
    InitiateBridging {
        burn_tx: TxHash,
        cctp_nonce: u64,
    },
    ReceiveAttestation {
        attestation: Vec<u8>,
    },
    ConfirmBridging {
        mint_tx: TxHash,
    },
    InitiateDeposit {
        deposit: TransferRef,
    },
    ConfirmDeposit,
    FailWithdrawal {
        reason: String,
    },
    FailBridging {
        reason: String,
    },
    FailDeposit {
        reason: String,
    },
}

/// Events emitted by the USDC rebalance aggregate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalanceEvent {
    Initiated {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
    WithdrawalConfirmed {
        confirmed_at: DateTime<Utc>,
    },
    WithdrawalFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    BridgingInitiated {
        burn_tx_hash: TxHash,
        cctp_nonce: u64,
        burned_at: DateTime<Utc>,
    },
    BridgeAttestationReceived {
        attestation: Vec<u8>,
        attested_at: DateTime<Utc>,
    },
    Bridged {
        mint_tx_hash: TxHash,
        minted_at: DateTime<Utc>,
    },
    BridgingFailed {
        burn_tx_hash: Option<TxHash>,
        cctp_nonce: Option<u64>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    DepositInitiated {
        deposit_ref: TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    },
    DepositConfirmed {
        deposit_confirmed_at: DateTime<Utc>,
    },
    DepositFailed {
        deposit_ref: Option<TransferRef>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for UsdcRebalanceEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initiated { .. } => "UsdcRebalanceEvent::Initiated",
            Self::WithdrawalConfirmed { .. } => "UsdcRebalanceEvent::WithdrawalConfirmed",
            Self::WithdrawalFailed { .. } => "UsdcRebalanceEvent::WithdrawalFailed",
            Self::BridgingInitiated { .. } => "UsdcRebalanceEvent::BridgingInitiated",
            Self::BridgeAttestationReceived { .. } => {
                "UsdcRebalanceEvent::BridgeAttestationReceived"
            }
            Self::Bridged { .. } => "UsdcRebalanceEvent::Bridged",
            Self::BridgingFailed { .. } => "UsdcRebalanceEvent::BridgingFailed",
            Self::DepositInitiated { .. } => "UsdcRebalanceEvent::DepositInitiated",
            Self::DepositConfirmed { .. } => "UsdcRebalanceEvent::DepositConfirmed",
            Self::DepositFailed { .. } => "UsdcRebalanceEvent::DepositFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// USDC rebalance aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid states unrepresentable.
/// Each variant contains exactly the data valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UsdcRebalance {
    /// Withdrawal from source has been initiated
    Withdrawing {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
    /// Withdrawal from source has been confirmed, ready for bridging
    WithdrawalComplete {
        direction: RebalanceDirection,
        amount: Usdc,
        initiated_at: DateTime<Utc>,
        confirmed_at: DateTime<Utc>,
    },
    /// Withdrawal from source has failed (terminal state)
    WithdrawalFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// CCTP bridging has been initiated (burn transaction submitted)
    Bridging {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        cctp_nonce: u64,
        initiated_at: DateTime<Utc>,
        burned_at: DateTime<Utc>,
    },
    /// Circle attestation has been received, ready for minting on destination chain
    Attested {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        cctp_nonce: u64,
        attestation: Vec<u8>,
        initiated_at: DateTime<Utc>,
        attested_at: DateTime<Utc>,
    },
}

#[async_trait]
impl Aggregate for Lifecycle<UsdcRebalance, Never> {
    type Command = UsdcRebalanceCommand;
    type Event = UsdcRebalanceEvent;
    type Error = UsdcRebalanceError;
    type Services = ();

    fn aggregate_type() -> String {
        "UsdcRebalance".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, UsdcRebalance::apply_transition)
            .or_initialize(&event, UsdcRebalance::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            UsdcRebalanceCommand::Initiate {
                direction,
                amount,
                withdrawal,
            } => self.handle_initiate(direction, *amount, withdrawal),

            UsdcRebalanceCommand::ConfirmWithdrawal => self.handle_confirm_withdrawal(),

            UsdcRebalanceCommand::FailWithdrawal { reason } => self.handle_fail_withdrawal(reason),

            UsdcRebalanceCommand::InitiateBridging {
                burn_tx,
                cctp_nonce,
            } => self.handle_initiate_bridging(burn_tx, *cctp_nonce),

            UsdcRebalanceCommand::ReceiveAttestation { attestation } => {
                self.handle_receive_attestation(attestation)
            }

            _ => {
                // Other commands will be implemented in subsequent tasks
                Err(UsdcRebalanceError::InvalidCommand {
                    command: format!("{command:?}"),
                    state: format!("{self:?}"),
                })
            }
        }
    }
}

impl Lifecycle<UsdcRebalance, Never> {
    fn handle_initiate(
        &self,
        direction: &RebalanceDirection,
        amount: Usdc,
        withdrawal: &TransferRef,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Ok(vec![UsdcRebalanceEvent::Initiated {
                direction: direction.clone(),
                amount,
                withdrawal_ref: withdrawal.clone(),
                initiated_at: Utc::now(),
            }]),
            Ok(_) => Err(UsdcRebalanceError::AlreadyInitiated),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_confirm_withdrawal(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Err(UsdcRebalanceError::WithdrawalNotInitiated),
            Ok(UsdcRebalance::Withdrawing { .. }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. },
            ) => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail_withdrawal(
        &self,
        reason: &str,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Err(UsdcRebalanceError::WithdrawalNotInitiated),
            Ok(UsdcRebalance::Withdrawing { .. }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalFailed {
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. },
            ) => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_initiate_bridging(
        &self,
        burn_tx: &TxHash,
        cctp_nonce: u64,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(UsdcRebalance::Withdrawing { .. } | UsdcRebalance::WithdrawalFailed { .. }) => {
                Err(UsdcRebalanceError::WithdrawalNotConfirmed)
            }

            Ok(UsdcRebalance::WithdrawalComplete { .. }) => {
                Ok(vec![UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: *burn_tx,
                    cctp_nonce,
                    burned_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::Bridging { .. } | UsdcRebalance::Attested { .. }) => {
                Err(UsdcRebalanceError::InvalidCommand {
                    command: "InitiateBridging".to_string(),
                    state: "Bridging".to_string(),
                })
            }

            Err(e) => Err(e.into()),
        }
    }

    fn handle_receive_attestation(
        &self,
        attestation: &[u8],
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingNotInitiated),

            Ok(UsdcRebalance::Bridging { .. }) => {
                Ok(vec![UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: attestation.to_vec(),
                    attested_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::Attested { .. }) => Err(UsdcRebalanceError::InvalidCommand {
                command: "ReceiveAttestation".to_string(),
                state: "Attested".to_string(),
            }),

            Err(e) => Err(e.into()),
        }
    }
}

impl UsdcRebalance {
    /// Apply a transition event to an existing rebalance state.
    pub(crate) fn apply_transition(
        event: &UsdcRebalanceEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match (current, event) {
            (
                Self::Withdrawing {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
                UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            ) => Ok(Self::WithdrawalComplete {
                direction: direction.clone(),
                amount: *amount,
                initiated_at: *initiated_at,
                confirmed_at: *confirmed_at,
            }),

            (
                Self::Withdrawing {
                    direction,
                    amount,
                    withdrawal_ref,
                    initiated_at,
                },
                UsdcRebalanceEvent::WithdrawalFailed { reason, failed_at },
            ) => Ok(Self::WithdrawalFailed {
                direction: direction.clone(),
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            }),

            (
                Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    cctp_nonce,
                    burned_at,
                },
            ) => Ok(Self::Bridging {
                direction: direction.clone(),
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                cctp_nonce: *cctp_nonce,
                initiated_at: *initiated_at,
                burned_at: *burned_at,
            }),

            (
                Self::Bridging {
                    direction,
                    amount,
                    burn_tx_hash,
                    cctp_nonce,
                    initiated_at,
                    ..
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation,
                    attested_at,
                },
            ) => Ok(Self::Attested {
                direction: direction.clone(),
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                cctp_nonce: *cctp_nonce,
                attestation: attestation.clone(),
                initiated_at: *initiated_at,
                attested_at: *attested_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
            }),
        }
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(event: &UsdcRebalanceEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            UsdcRebalanceEvent::Initiated {
                direction,
                amount,
                withdrawal_ref,
                initiated_at,
            } => Ok(Self::Withdrawing {
                direction: direction.clone(),
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *initiated_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
        }
    }
}

impl View<Self> for Lifecycle<UsdcRebalance, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, UsdcRebalance::apply_transition)
            .or_initialize(&event.payload, UsdcRebalance::from_event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));

        let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected Initiated event");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*withdrawal_ref, TransferRef::AlpacaId(transfer_id));
    }

    #[tokio::test]
    async fn test_initiate_base_to_alpaca() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let tx_hash = Address::ZERO.into_word();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(500.50)),
                    withdrawal: TransferRef::OnchainTx(tx_hash),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));

        let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected Initiated event");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc(dec!(500.50)));
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        };
        aggregate.apply(event);

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(500.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::AlreadyInitiated
        ));
    }

    #[test]
    fn test_view_tracks_initiation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&envelope);

        let Lifecycle::Live(UsdcRebalance::Withdrawing {
            direction,
            amount,
            withdrawal_ref,
            initiated_at: view_initiated_at,
        }) = view
        else {
            panic!("Expected Live(Withdrawing) variant");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(view_initiated_at, initiated_at);
    }

    #[test]
    fn test_from_event_rejects_non_init_events() {
        let event = UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        };

        let result = UsdcRebalance::from_event(&event);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[test]
    fn test_apply_transition_rejects_initiated_event() {
        let current = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        };

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(500.00)),
            withdrawal_ref: TransferRef::OnchainTx(Address::ZERO.into_word()),
            initiated_at: Utc::now(),
        };

        let result = UsdcRebalance::apply_transition(&event, &current);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[tokio::test]
    async fn test_confirm_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::WithdrawalConfirmed { .. }
        ));

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::WithdrawalComplete {
            direction,
            amount,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected WithdrawalComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_fail_withdrawal_after_initiation() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Insufficient funds".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalFailed { reason, .. } = &events[0] else {
            panic!("Expected WithdrawalFailed event");
        };
        assert_eq!(reason, "Insufficient funds");

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::WithdrawalFailed {
            direction,
            amount,
            reason: state_reason,
            withdrawal_ref,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected WithdrawalFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_reason, "Insufficient funds");
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_fail_withdrawal_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Test failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_confirmed_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Late failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_failed_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Second failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[test]
    fn test_view_tracks_withdrawal_confirmation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::WithdrawalComplete {
            direction,
            amount,
            initiated_at: view_initiated_at,
            confirmed_at: view_confirmed_at,
        }) = view
        else {
            panic!("Expected WithdrawalComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_view_tracks_withdrawal_failure() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalFailed {
                reason: "Network error".to_string(),
                failed_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::WithdrawalFailed {
            direction,
            amount,
            withdrawal_ref,
            reason,
            initiated_at: view_initiated_at,
            failed_at: view_failed_at,
        }) = view
        else {
            panic!("Expected WithdrawalFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(reason, "Network error");
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_failed_at, failed_at);
    }

    #[tokio::test]
    async fn test_initiate_bridging_after_withdrawal_confirmed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at });

        let burn_tx_hash = Address::ZERO.into_word();
        let cctp_nonce = 12345u64;

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                    cctp_nonce,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: event_tx_hash,
            cctp_nonce: event_nonce,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingInitiated event");
        };

        assert_eq!(*event_tx_hash, burn_tx_hash);
        assert_eq!(*event_nonce, cctp_nonce);

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::Bridging {
            direction,
            amount,
            burn_tx_hash: state_tx_hash,
            cctp_nonce: state_nonce,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected Bridging state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_tx_hash, burn_tx_hash);
        assert_eq!(state_nonce, cctp_nonce);
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_before_withdrawal() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let burn_tx_hash = Address::ZERO.into_word();
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_while_withdrawing() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        let burn_tx_hash = Address::ZERO.into_word();
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_after_withdrawal_failed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Test failure".to_string(),
            failed_at: Utc::now(),
        });

        let burn_tx_hash = Address::ZERO.into_word();
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash = Address::ZERO.into_word();
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            cctp_nonce: 12345,
            burned_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                    cctp_nonce: 99999,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::InvalidCommand { .. }
        ));
    }

    #[test]
    fn test_view_tracks_bridging_initiation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let burn_tx_hash = Address::ZERO.into_word();
        let cctp_nonce = 12345u64;

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                cctp_nonce,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::Bridging {
            direction,
            amount,
            burn_tx_hash: view_tx_hash,
            cctp_nonce: view_nonce,
            initiated_at: view_initiated_at,
            burned_at: view_burned_at,
        }) = view
        else {
            panic!("Expected Bridging state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_tx_hash, burn_tx_hash);
        assert_eq!(view_nonce, cctp_nonce);
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_burned_at, burned_at);
    }

    #[tokio::test]
    async fn test_receive_attestation_after_bridging_initiated() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash = Address::ZERO.into_word();
        let cctp_nonce = 12345u64;

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            cctp_nonce,
            burned_at: Utc::now(),
        });

        let attestation = vec![0x01, 0x02, 0x03, 0x04];

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: attestation.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: event_attestation,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgeAttestationReceived event");
        };

        assert_eq!(*event_attestation, attestation);

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::Attested {
            direction,
            amount,
            burn_tx_hash: state_tx_hash,
            cctp_nonce: state_nonce,
            attestation: state_attestation,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected Attested state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_tx_hash, burn_tx_hash);
        assert_eq!(state_nonce, cctp_nonce);
        assert_eq!(state_attestation, attestation);
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_before_bridging() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_while_withdrawing() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_complete() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_failed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Test failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash = Address::ZERO.into_word();
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            cctp_nonce: 12345,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            attested_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x03, 0x04],
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::InvalidCommand { .. }
        ));
    }

    #[test]
    fn test_view_tracks_attestation_receipt() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let attested_at = Utc::now();
        let burn_tx_hash = Address::ZERO.into_word();
        let cctp_nonce = 12345u64;
        let attestation = vec![0x01, 0x02, 0x03, 0x04];

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                cctp_nonce,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 4,
            payload: UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: attestation.clone(),
                attested_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::Attested {
            direction,
            amount,
            burn_tx_hash: view_tx_hash,
            cctp_nonce: view_nonce,
            attestation: view_attestation,
            initiated_at: view_initiated_at,
            attested_at: view_attested_at,
        }) = view
        else {
            panic!("Expected Attested state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_tx_hash, burn_tx_hash);
        assert_eq!(view_nonce, cctp_nonce);
        assert_eq!(view_attestation, attestation);
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_attested_at, attested_at);
    }
}
