use rust_decimal::Decimal;

use super::{RebalanceDirection, TransferRef};

#[derive(Debug, Clone)]
pub(crate) enum UsdcRebalanceCommand {
    InitiateWithdrawal {
        direction: RebalanceDirection,
        amount: Decimal,
    },
    CompleteWithdrawal {
        reference: TransferRef,
    },
    Fail {
        reason: String,
    },
}
