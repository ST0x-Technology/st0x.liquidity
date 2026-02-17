//! Mock implementation of the Tokenizer trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use reqwest::StatusCode;
use std::sync::Mutex;

use st0x_execution::{FractionalShares, Symbol};

use super::{
    AlpacaTokenizationError, TokenizationRequest, TokenizationRequestStatus, Tokenizer,
    TokenizerError,
};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

/// Configurable outcome for mint request.
#[derive(Clone, Copy)]
pub(crate) enum MockMintRequestOutcome {
    Pending,
    Rejected,
    ApiError,
}

/// Configurable outcome for mint completion polling.
#[derive(Clone, Copy)]
pub(crate) enum MockMintPollOutcome {
    Completed,
    Rejected,
    Pending,
    PollError,
}

/// Configurable outcome for redemption detection polling.
#[derive(Clone, Copy)]
pub(crate) enum MockDetectionOutcome {
    Detected,
    Timeout,
    ApiError,
}

/// Configurable outcome for redemption completion polling.
#[derive(Clone, Copy)]
pub(crate) enum MockCompletionOutcome {
    Completed,
    Rejected,
    Pending,
}

pub(crate) struct MockTokenizer {
    redemption_wallet: Address,
    redemption_tx: TxHash,
    mint_request_outcome: MockMintRequestOutcome,
    mint_poll_outcome: MockMintPollOutcome,
    detection_outcome: Option<MockDetectionOutcome>,
    completion_outcome: Option<MockCompletionOutcome>,
    should_fail_send: bool,
    last_issuer_request_id: Mutex<Option<IssuerRequestId>>,
}

impl MockTokenizer {
    pub(crate) fn new() -> Self {
        Self {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            mint_request_outcome: MockMintRequestOutcome::Pending,
            mint_poll_outcome: MockMintPollOutcome::Completed,
            detection_outcome: None,
            completion_outcome: None,
            should_fail_send: false,
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

    pub(crate) fn with_detection_outcome(mut self, outcome: MockDetectionOutcome) -> Self {
        self.detection_outcome = Some(outcome);
        self
    }

    pub(crate) fn with_completion_outcome(mut self, outcome: MockCompletionOutcome) -> Self {
        self.completion_outcome = Some(outcome);
        self
    }

    pub(crate) fn with_send_failure(mut self) -> Self {
        self.should_fail_send = true;
        self
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
            MockMintRequestOutcome::Pending => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Pending,
            )),
            MockMintRequestOutcome::Rejected => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Rejected,
            )),
            MockMintRequestOutcome::ApiError => {
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "mock mint request error".to_string(),
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
            MockMintPollOutcome::Pending => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Pending,
            )),
            MockMintPollOutcome::PollError => Err(TokenizerError::Alpaca(
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
        if self.should_fail_send {
            return Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "mock send_for_redemption failure".to_string(),
            }));
        }
        Ok(self.redemption_tx)
    }

    async fn poll_for_redemption(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        match self.detection_outcome {
            Some(MockDetectionOutcome::Detected) | None => Ok(TokenizationRequest::mock(
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
                    message: "mock detection API error".to_string(),
                }))
            }
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
            Some(MockCompletionOutcome::Pending) => Ok(TokenizationRequest::mock(
                TokenizationRequestStatus::Pending,
            )),
            None => {
                unimplemented!(
                    "MockTokenizer::poll_redemption_until_complete - no outcome configured"
                )
            }
        }
    }
}
