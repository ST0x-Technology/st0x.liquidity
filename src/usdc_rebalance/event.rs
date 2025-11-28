use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::RebalanceDirection;
use crate::alpaca_wallet::AlpacaTransferId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalanceEvent {
    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        initiated_at: DateTime<Utc>,
    },
    AlpacaWithdrawalCompleted {
        transfer_id: AlpacaTransferId,
        completed_at: DateTime<Utc>,
    },
    RaindexWithdrawalCompleted {
        withdrawal_tx_hash: TxHash,
        completed_at: DateTime<Utc>,
    },
    AlpacaWithdrawalFailed {
        reference: Option<AlpacaTransferId>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    RaindexWithdrawalFailed {
        reference: Option<TxHash>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for UsdcRebalanceEvent {
    fn event_type(&self) -> String {
        match self {
            Self::WithdrawalInitiated { .. } => {
                "UsdcRebalanceEvent::WithdrawalInitiated".to_string()
            }
            Self::AlpacaWithdrawalCompleted { .. } => {
                "UsdcRebalanceEvent::AlpacaWithdrawalCompleted".to_string()
            }
            Self::RaindexWithdrawalCompleted { .. } => {
                "UsdcRebalanceEvent::RaindexWithdrawalCompleted".to_string()
            }
            Self::AlpacaWithdrawalFailed { .. } => {
                "UsdcRebalanceEvent::AlpacaWithdrawalFailed".to_string()
            }
            Self::RaindexWithdrawalFailed { .. } => {
                "UsdcRebalanceEvent::RaindexWithdrawalFailed".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
