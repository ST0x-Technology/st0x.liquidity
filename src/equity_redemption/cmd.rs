use alloy::primitives::{Address, TxHash};
use rust_decimal::Decimal;
use st0x_broker::Symbol;

use crate::tokenized_equity_mint::TokenizationRequestId;

#[derive(Debug, Clone)]
pub(crate) enum EquityRedemptionCommand {
    SendTokens {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
    },
    Detect {
        tokenization_request_id: TokenizationRequestId,
    },
    Complete,
    Fail {
        reason: String,
    },
}
