use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::RebalanceDirection;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalanceEvent {
    WithdrawalInitiated {
        direction: RebalanceDirection,
        amount: Decimal,
        initiated_at: DateTime<Utc>,
    },
    Failed {
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
            Self::Failed { .. } => "UsdcRebalanceEvent::Failed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
