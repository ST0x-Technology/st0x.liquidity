use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::alpaca_wallet::AlpacaTransferId;

mod cmd;
mod event;
mod view;

pub(crate) use cmd::UsdcRebalanceCommand;
pub(crate) use event::UsdcRebalanceEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalance {
    NotStarted,
    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
}

impl Default for UsdcRebalance {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[async_trait]
impl Aggregate for UsdcRebalance {
    type Command = UsdcRebalanceCommand;
    type Event = UsdcRebalanceEvent;
    type Error = UsdcRebalanceError;
    type Services = ();

    fn aggregate_type() -> String {
        "UsdcRebalance".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self, command) {
            (
                Self::NotStarted,
                UsdcRebalanceCommand::Initiate {
                    direction,
                    amount,
                    withdrawal,
                },
            ) => Ok(vec![UsdcRebalanceEvent::Initiated {
                direction,
                amount,
                withdrawal_ref: withdrawal,
                initiated_at: Utc::now(),
            }]),

            (Self::WithdrawalInitiated { .. }, UsdcRebalanceCommand::Initiate { .. }) => {
                Err(UsdcRebalanceError::AlreadyInitiated)
            }

            (state, command) => Err(UsdcRebalanceError::InvalidStateTransition {
                from: format!("{state:?}"),
                command: format!("{command:?}"),
            }),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        if let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            initiated_at,
        } = event
        {
            *self = Self::WithdrawalInitiated {
                direction,
                amount,
                withdrawal_ref,
                initiated_at,
            };
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UsdcRebalanceId(pub(crate) String);

impl UsdcRebalanceId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TransferRef {
    AlpacaId(AlpacaTransferId),
    OnchainTx(TxHash),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RebalanceDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum UsdcRebalanceError {
    #[error("Rebalancing has already been initiated")]
    AlreadyInitiated,

    #[error("Invalid state transition from {from} with command {command}")]
    InvalidStateTransition { from: String, command: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let aggregate = UsdcRebalance::default();
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

        if let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        {
            assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
            assert_eq!(*amount, dec!(1000.00));
            assert_eq!(*withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        } else {
            panic!("Expected Initiated event");
        }
    }

    #[tokio::test]
    async fn test_initiate_base_to_alpaca() {
        let aggregate = UsdcRebalance::default();
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

        if let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        {
            assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
            assert_eq!(*amount, dec!(500.50));
            assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
        } else {
            panic!("Expected Initiated event");
        }
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let mut aggregate = UsdcRebalance::default();
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
        use cqrs_es::{EventEnvelope, View};
        use std::collections::HashMap;

        use view::UsdcRebalanceView;

        let mut view = UsdcRebalanceView::default();
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

        assert!(matches!(view, UsdcRebalanceView::Unavailable));

        view.update(&envelope);

        let UsdcRebalanceView::WithdrawalInitiated {
            direction,
            amount,
            withdrawal_ref,
            initiated_at: view_initiated_at,
        } = view
        else {
            panic!("Expected WithdrawalInitiated variant");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, dec!(1000.00));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(view_initiated_at, initiated_at);
    }
}
