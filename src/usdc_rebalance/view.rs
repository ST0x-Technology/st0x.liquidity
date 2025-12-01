use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::{RebalanceDirection, TransferRef, UsdcRebalance, UsdcRebalanceEvent};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalanceView {
    Unavailable,
    NotStarted,
    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
}

impl Default for UsdcRebalanceView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl View<UsdcRebalance> for UsdcRebalanceView {
    fn update(&mut self, event: &EventEnvelope<UsdcRebalance>) {
        if let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            initiated_at,
        } = &event.payload
        {
            self.handle_initiated(
                direction.clone(),
                *amount,
                withdrawal_ref.clone(),
                *initiated_at,
            );
        }
    }
}

impl UsdcRebalanceView {
    fn handle_initiated(
        &mut self,
        direction: RebalanceDirection,
        amount: Decimal,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    ) {
        match self {
            Self::Unavailable | Self::NotStarted => {
                *self = Self::WithdrawalInitiated {
                    direction,
                    amount,
                    withdrawal_ref,
                    initiated_at,
                };
            }
            Self::WithdrawalInitiated { .. } => {
                error!(
                    "Initiated event received but view is not in NotStarted/Unavailable state. Event ignored."
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca_wallet::AlpacaTransferId;
    use alloy::primitives::Address;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn test_view_update_from_initiated_event_alpaca_to_base() {
        let mut view = UsdcRebalanceView::default();
        let initiated_at = Utc::now();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

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

    #[test]
    fn test_view_update_from_initiated_event_base_to_alpaca() {
        let mut view = UsdcRebalanceView::default();
        let initiated_at = Utc::now();
        let tx_hash = Address::ZERO.into_word();

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: dec!(500.50),
            withdrawal_ref: TransferRef::OnchainTx(tx_hash),
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "rebalance-456".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

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

        assert_eq!(direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(amount, dec!(500.50));
        assert_eq!(withdrawal_ref, TransferRef::OnchainTx(tx_hash));
        assert_eq!(view_initiated_at, initiated_at);
    }

    #[test]
    fn test_view_from_not_started_state() {
        let mut view = UsdcRebalanceView::NotStarted;
        let initiated_at = Utc::now();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: dec!(1000.00),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "rebalance-789".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(
            view,
            UsdcRebalanceView::WithdrawalInitiated { .. }
        ));
    }
}
