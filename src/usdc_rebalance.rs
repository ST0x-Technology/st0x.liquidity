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
//! (start) --Initiate--> WithdrawalInitiated --ConfirmWithdrawal--> WithdrawalConfirmed
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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::alpaca_wallet::AlpacaTransferId;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};

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
        amount: Decimal,
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
        amount: Decimal,
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
    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
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
        amount: Decimal,
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
}

impl UsdcRebalance {
    /// Apply a transition event to an existing rebalance state.
    pub(crate) fn apply_transition(
        event: &UsdcRebalanceEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        // Currently only WithdrawalInitiated state exists
        // Future tasks will add more state transitions
        Err(LifecycleError::Mismatch {
            state: format!("{current:?}"),
            event: event.event_type(),
        })
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(event: &UsdcRebalanceEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            UsdcRebalanceEvent::Initiated {
                direction,
                amount,
                withdrawal_ref,
                initiated_at,
            } => Ok(Self::WithdrawalInitiated {
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
                    amount: dec!(1000.00),
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
        assert_eq!(*amount, dec!(1000.00));
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
                    amount: dec!(500.50),
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
        assert_eq!(*amount, dec!(500.50));
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: dec!(1000.00),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        };
        aggregate.apply(event);

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: dec!(500.00),
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
            amount: dec!(1000.00),
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

        let Lifecycle::Live(UsdcRebalance::WithdrawalInitiated {
            direction,
            amount,
            withdrawal_ref,
            initiated_at: view_initiated_at,
        }) = view
        else {
            panic!("Expected Live(WithdrawalInitiated) variant");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, dec!(1000.00));
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
        let current = UsdcRebalance::WithdrawalInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: dec!(1000.00),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        };

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: dec!(500.00),
            withdrawal_ref: TransferRef::OnchainTx(Address::ZERO.into_word()),
            initiated_at: Utc::now(),
        };

        let result = UsdcRebalance::apply_transition(&event, &current);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }
}
