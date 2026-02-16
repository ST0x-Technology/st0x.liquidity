//! Mint error type for equity transfer to market-making venue.

use thiserror::Error;

use st0x_event_sorcery::SendError;

use crate::tokenized_equity_mint::TokenizedEquityMint;

#[derive(Debug, Error)]
pub(crate) enum MintError {
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<TokenizedEquityMint>>),
}

impl From<SendError<TokenizedEquityMint>> for MintError {
    fn from(error: SendError<TokenizedEquityMint>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
