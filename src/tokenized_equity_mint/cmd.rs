use alloy::primitives::{Address, TxHash, U256};
use rust_decimal::Decimal;
use st0x_broker::Symbol;

use super::{IssuerRequestId, ReceiptId, TokenizationRequestId};

#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    RequestMint {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    AcknowledgeAcceptance {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    },
    ReceiveTokens {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
    },
    Finalize,
    Fail {
        reason: String,
    },
}
