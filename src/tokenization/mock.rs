//! Mock implementation of the Tokenizer trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use reqwest::StatusCode;
use st0x_execution::{FractionalShares, Symbol};

use super::{
    AlpacaTokenizationError, TokenizationRequest, TokenizationRequestStatus, Tokenizer,
    TokenizerError,
};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

/// Configurable outcome for detection polling.
#[derive(Clone, Copy)]
pub(crate) enum MockDetectionOutcome {
    Detected,
    Timeout,
    ApiError,
}

/// Configurable outcome for completion polling.
#[derive(Clone, Copy)]
pub(crate) enum MockCompletionOutcome {
    Completed,
    Rejected,
}

pub(crate) struct MockTokenizer {
    redemption_wallet: Address,
    redemption_tx: TxHash,
    detection_outcome: Option<MockDetectionOutcome>,
    completion_outcome: Option<MockCompletionOutcome>,
}

impl MockTokenizer {
    pub(crate) fn new() -> Self {
        Self {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            detection_outcome: None,
            completion_outcome: None,
        }
    }

    pub(crate) fn with_detection_outcome(mut self, outcome: MockDetectionOutcome) -> Self {
        self.detection_outcome = Some(outcome);
        self
    }

    pub(crate) fn with_completion_outcome(mut self, outcome: MockCompletionOutcome) -> Self {
        self.completion_outcome = Some(outcome);
        self
    }
}

#[async_trait]
impl Tokenizer for MockTokenizer {
    async fn request_mint(
        &self,
        _symbol: Symbol,
        _quantity: FractionalShares,
        _wallet: Address,
        _issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(TokenizationRequest::mock(
            TokenizationRequestStatus::Pending,
        ))
    }

    async fn poll_mint_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(TokenizationRequest::mock_completed())
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
        match self.detection_outcome {
            Some(MockDetectionOutcome::Detected) => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Pending,
            )),
            Some(MockDetectionOutcome::Timeout) => Err(TokenizerError::Alpaca(
                AlpacaTokenizationError::PollTimeout {
                    elapsed: std::time::Duration::from_secs(60),
                },
            )),
            Some(MockDetectionOutcome::ApiError) => {
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "mock error".to_string(),
                }))
            }
            None => unimplemented!("MockTokenizer::poll_for_redemption - no outcome configured"),
        }
    }

    async fn poll_redemption_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        match self.completion_outcome {
            Some(MockCompletionOutcome::Completed) => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Completed,
            )),
            Some(MockCompletionOutcome::Rejected) => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Rejected,
            )),
            None => {
                unimplemented!(
                    "MockTokenizer::poll_redemption_until_complete - no outcome configured"
                )
            }
        }
    }
}
