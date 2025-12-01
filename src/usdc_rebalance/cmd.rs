use alloy::primitives::TxHash;
use rust_decimal::Decimal;

use super::{RebalanceDirection, TransferRef};

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
