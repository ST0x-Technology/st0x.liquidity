use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{RebalanceDirection, TransferRef};

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
            Self::Initiated { .. } => "Initiated",
            Self::WithdrawalConfirmed { .. } => "WithdrawalConfirmed",
            Self::WithdrawalFailed { .. } => "WithdrawalFailed",
            Self::BridgingInitiated { .. } => "BridgingInitiated",
            Self::BridgeAttestationReceived { .. } => "BridgeAttestationReceived",
            Self::Bridged { .. } => "Bridged",
            Self::BridgingFailed { .. } => "BridgingFailed",
            Self::DepositInitiated { .. } => "DepositInitiated",
            Self::DepositConfirmed { .. } => "DepositConfirmed",
            Self::DepositFailed { .. } => "DepositFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
