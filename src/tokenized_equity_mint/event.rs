use alloy::primitives::{Address, TxHash, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

use super::{IssuerRequestId, ReceiptId, TokenizationRequestId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedEquityMintEvent {
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    MintAccepted {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    TokensReceived {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    },
    MintCompleted {
        completed_at: DateTime<Utc>,
    },
    MintFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedEquityMintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::MintRequested { .. } => "TokenizedEquityMintEvent::MintRequested".to_string(),
            Self::MintAccepted { .. } => "TokenizedEquityMintEvent::MintAccepted".to_string(),
            Self::TokensReceived { .. } => "TokenizedEquityMintEvent::TokensReceived".to_string(),
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
            Self::MintFailed { .. } => "TokenizedEquityMintEvent::MintFailed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
