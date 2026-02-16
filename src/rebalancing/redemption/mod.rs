//! Redemption error type for equity transfer to hedging venue.

use thiserror::Error;

use st0x_event_sorcery::SendError;

use crate::equity_redemption::{EquityRedemption, RedemptionAggregateId};
use crate::onchain::raindex::RaindexError;
use crate::tokenization::{AlpacaTokenizationError, TokenizerError};

#[derive(Debug, Error)]
pub(crate) enum RedemptionError {
    #[error(transparent)]
    Send(#[from] SendError<EquityRedemption>),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error("Entity not found after command: {aggregate_id}")]
    EntityNotFound { aggregate_id: RedemptionAggregateId },
    #[error("Token send to Alpaca failed: {entity:?}")]
    SendFailed { entity: EquityRedemption },
    #[error("Unexpected entity: {entity:?}")]
    UnexpectedEntity { entity: EquityRedemption },
    #[error("Unexpected tokenization status: still pending after polling")]
    UnexpectedPendingStatus,
    #[error("Redemption was rejected by Alpaca")]
    Rejected,
}
