//! Mock implementation of the Tokenizer trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use reqwest::StatusCode;
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Mutex;

use super::{
    AlpacaTokenizationError, TokenizationRequest, TokenizationRequestStatus, Tokenizer,
    TokenizerError,
};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

/// Configurable outcome for mint request.
#[derive(Clone, Copy)]
pub(crate) enum MockMintRequestOutcome {
    Accepted,
    Rejected,
}

/// Configurable outcome for mint polling.
#[derive(Clone, Copy)]
pub(crate) enum MockMintPollOutcome {
    Completed,
    Rejected,
    Error,
}

pub(crate) struct MockTokenizer {
    redemption_wallet: Address,
    redemption_tx: TxHash,
    mint_request_outcome: MockMintRequestOutcome,
    mint_poll_outcome: MockMintPollOutcome,
    last_issuer_request_id: Mutex<Option<IssuerRequestId>>,
}

impl MockTokenizer {
    pub(crate) fn new() -> Self {
        Self {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            mint_request_outcome: MockMintRequestOutcome::Accepted,
            mint_poll_outcome: MockMintPollOutcome::Completed,
            last_issuer_request_id: Mutex::new(None),
        }
    }

    pub(crate) fn with_mint_request_outcome(mut self, outcome: MockMintRequestOutcome) -> Self {
        self.mint_request_outcome = outcome;
        self
    }

    pub(crate) fn with_mint_poll_outcome(mut self, outcome: MockMintPollOutcome) -> Self {
        self.mint_poll_outcome = outcome;
        self
    }

    pub(crate) fn with_redemption_tx(tx: TxHash) -> Self {
        Self {
            redemption_wallet: Address::random(),
            redemption_tx: tx,
            mint_request_outcome: MockMintRequestOutcome::Accepted,
            mint_poll_outcome: MockMintPollOutcome::Completed,
            last_issuer_request_id: Mutex::new(None),
        }
    }

    pub(crate) fn last_issuer_request_id(&self) -> Option<IssuerRequestId> {
        self.last_issuer_request_id.lock().unwrap().clone()
    }
}

#[async_trait]
impl Tokenizer for MockTokenizer {
    async fn request_mint(
        &self,
        _symbol: Symbol,
        _quantity: FractionalShares,
        _wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        *self.last_issuer_request_id.lock().unwrap() = Some(issuer_request_id);

        match self.mint_request_outcome {
            MockMintRequestOutcome::Accepted => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Pending,
            )),
            MockMintRequestOutcome::Rejected => {
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                    status: StatusCode::BAD_REQUEST,
                    message: "mock rejection".to_string(),
                }))
            }
        }
    }

    async fn poll_mint_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        match self.mint_poll_outcome {
            MockMintPollOutcome::Completed => Ok(TokenizationRequest::mock_completed()),
            MockMintPollOutcome::Rejected => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Rejected,
            )),
            MockMintPollOutcome::Error => Err(TokenizerError::Alpaca(
                AlpacaTokenizationError::PollTimeout {
                    elapsed: std::time::Duration::from_secs(60),
                },
            )),
        }
    }

    fn redemption_wallet(&self) -> Address {
        self.redemption_wallet
    }

    async fn send_for_redemption(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        Ok(self.redemption_tx)
    }

    async fn poll_for_redemption(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(TokenizationRequest::mock(
            TokenizationRequestStatus::Pending,
        ))
    }

    async fn poll_redemption_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(TokenizationRequest::mock(
            TokenizationRequestStatus::Completed,
        ))
    }
}
