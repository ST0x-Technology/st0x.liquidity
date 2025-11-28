use rust_decimal::Decimal;

use super::RebalanceDirection;

#[derive(Debug, Clone)]
pub(crate) enum UsdcRebalanceCommand {
    InitiateWithdrawal {
        direction: RebalanceDirection,
        amount: Decimal,
    },
    Fail {
        reason: String,
    },
}
