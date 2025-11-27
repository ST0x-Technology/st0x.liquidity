use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

use crate::tokenized_equity_mint::TokenizationRequestId;

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
    Completed {
        completed_at: DateTime<Utc>,
    },
    Failed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for EquityRedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::TokensSent { .. } => "EquityRedemptionEvent::TokensSent".to_string(),
            Self::Detected { .. } => "EquityRedemptionEvent::Detected".to_string(),
            Self::Completed { .. } => "EquityRedemptionEvent::Completed".to_string(),
            Self::Failed { .. } => "EquityRedemptionEvent::Failed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
