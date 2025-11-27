use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

mod cmd;
mod event;

pub(crate) use cmd::TokenizedEquityMintCommand;
pub(crate) use event::TokenizedEquityMintEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssuerRequestId(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReceiptId(pub(crate) U256);

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum TokenizedEquityMintError {
    #[error("Cannot accept mint: not in requested state")]
    NotRequested,
    #[error("Cannot receive tokens: mint not accepted")]
    NotAccepted,
    #[error("Cannot finalize: tokens not received")]
    TokensNotReceived,
    #[error("Already completed")]
    AlreadyCompleted,
    #[error("Already failed")]
    AlreadyFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedEquityMint {
    NotStarted,
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },
    TokensReceived {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
    },
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for TokenizedEquityMint {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[async_trait]
impl Aggregate for TokenizedEquityMint {
    type Command = TokenizedEquityMintCommand;
    type Event = TokenizedEquityMintEvent;
    type Error = TokenizedEquityMintError;
    type Services = ();

    fn aggregate_type() -> String {
        "TokenizedEquityMint".to_string()
    }

    async fn handle(
        &self,
        _command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        todo!("Implement in Task 2")
    }

    fn apply(&mut self, _event: Self::Event) {
        todo!("Implement in Task 2")
    }
}
