//! Mock implementation of the Tokenizer trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use rain_math_float::Float;
use reqwest::StatusCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use st0x_evm::{EvmError, NODE_SYNC_MAX_ATTEMPTS};
use st0x_execution::{FractionalShares, Symbol};

use super::{
    AlpacaTokenizationError, IssuerRequestId, MintVerificationError, TokenizationRequest,
    TokenizationRequestId, TokenizationRequestStatus, Tokenizer, TokenizerError,
};
use crate::alpaca::AlpacaApiErrorMessage;

/// Configurable outcome for mint request.
#[derive(Clone, Copy)]
pub enum MockMintRequestOutcome {
    Pending,
    Rejected,
    ApiError,
}

/// Configurable outcome for mint completion polling.
#[derive(Clone, Copy)]
pub enum MockMintPollOutcome {
    Completed,
    Rejected,
    Pending,
    PollError,
}

/// Configurable outcome for redemption detection polling.
#[derive(Clone, Copy)]
pub enum MockDetectionOutcome {
    Detected,
    Timeout,
    ApiError,
}

/// Configurable outcome for redemption completion polling.
#[derive(Clone, Copy)]
pub enum MockCompletionOutcome {
    Completed,
    Rejected,
    Pending,
}

/// Configurable outcome for mint verification.
#[derive(Clone, Copy)]
pub enum MockVerificationOutcome {
    Success,
    ReceiptNotFound,
    TransactionReverted,
    NoMatchingTransfer,
    InsufficientTransferAmount,
}

#[derive(Clone)]
enum MockTokenSymbolBehavior {
    /// Use the default token_symbol from `mock_completed()`.
    Default,
    /// Force token_symbol to None.
    None,
    /// Override token_symbol with a specific value.
    Override(String),
}

/// Configurable outcome for `send_for_redemption`.
#[derive(Clone, Copy)]
enum MockSendOutcome {
    Succeed,
    ApiError,
}

/// Configurable outcome for `wait_for_block`.
#[derive(Clone, Copy)]
enum MockWaitForBlockOutcome {
    Succeed,
    /// Fail with `NodeBehindRequiredBlock` (budget exhausted while behind).
    NodeBehind,
}

/// Configurable outcome for `list_pending_requests`.
#[derive(Clone, Copy)]
enum MockListPendingOutcome {
    Succeed,
    ApiError,
}

pub struct MockTokenizer {
    redemption_wallet: Option<Address>,
    redemption_tx: TxHash,
    mint_request_outcome: MockMintRequestOutcome,
    mint_poll_outcome: MockMintPollOutcome,
    detection_outcome: Option<MockDetectionOutcome>,
    completion_outcome: Option<MockCompletionOutcome>,
    verification_outcome: MockVerificationOutcome,
    send_outcome: MockSendOutcome,
    wait_for_block_outcome: MockWaitForBlockOutcome,
    wait_for_block_calls: Mutex<Vec<u64>>,
    list_pending_outcome: MockListPendingOutcome,
    last_issuer_request_id: Mutex<Option<IssuerRequestId>>,
    /// Total number of tokenizer method calls made (across all methods).
    call_count: AtomicUsize,
    pending_requests: Vec<TokenizationRequest>,
    /// Override the token_symbol in completed mint responses.
    token_symbol_behavior: MockTokenSymbolBehavior,
    /// Override fees in completed mint responses.
    fees_override: Option<Float>,
}

impl MockTokenizer {
    pub fn new() -> Self {
        Self {
            redemption_wallet: Some(Address::random()),
            redemption_tx: TxHash::random(),
            mint_request_outcome: MockMintRequestOutcome::Pending,
            mint_poll_outcome: MockMintPollOutcome::Completed,
            detection_outcome: None,
            completion_outcome: Some(MockCompletionOutcome::Completed),
            verification_outcome: MockVerificationOutcome::Success,
            send_outcome: MockSendOutcome::Succeed,
            wait_for_block_outcome: MockWaitForBlockOutcome::Succeed,
            wait_for_block_calls: Mutex::new(Vec::new()),
            list_pending_outcome: MockListPendingOutcome::Succeed,
            last_issuer_request_id: Mutex::new(None),
            call_count: AtomicUsize::new(0),
            token_symbol_behavior: MockTokenSymbolBehavior::Default,
            fees_override: None,
            pending_requests: Vec::new(),
        }
    }

    /// Returns the total number of tokenizer method calls made since construction.
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn with_mint_request_outcome(mut self, outcome: MockMintRequestOutcome) -> Self {
        self.mint_request_outcome = outcome;
        self
    }

    #[must_use]
    pub fn with_mint_poll_outcome(mut self, outcome: MockMintPollOutcome) -> Self {
        self.mint_poll_outcome = outcome;
        self
    }

    #[must_use]
    pub fn with_detection_outcome(mut self, outcome: MockDetectionOutcome) -> Self {
        self.detection_outcome = Some(outcome);
        self
    }

    #[must_use]
    pub fn with_completion_outcome(mut self, outcome: MockCompletionOutcome) -> Self {
        self.completion_outcome = Some(outcome);
        self
    }

    #[must_use]
    pub fn with_verification_outcome(mut self, outcome: MockVerificationOutcome) -> Self {
        self.verification_outcome = outcome;
        self
    }

    #[must_use]
    pub fn with_send_failure(mut self) -> Self {
        self.send_outcome = MockSendOutcome::ApiError;
        self
    }

    /// Makes `wait_for_block` fail with `NodeBehindRequiredBlock` (budget
    /// exhausted while the node stayed behind the required block).
    #[must_use]
    pub fn failing_wait_for_block(mut self) -> Self {
        self.wait_for_block_outcome = MockWaitForBlockOutcome::NodeBehind;
        self
    }

    /// Returns the block numbers passed to `wait_for_block`, in call order.
    pub fn wait_for_block_calls(&self) -> Vec<u64> {
        self.wait_for_block_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn with_list_pending_failure(mut self) -> Self {
        self.list_pending_outcome = MockListPendingOutcome::ApiError;
        self
    }

    #[must_use]
    pub fn with_pending_requests(mut self, requests: Vec<TokenizationRequest>) -> Self {
        self.pending_requests = requests;
        self
    }

    #[must_use]
    pub fn with_token_symbol_override(mut self, symbol: impl Into<String>) -> Self {
        self.token_symbol_behavior = MockTokenSymbolBehavior::Override(symbol.into());
        self
    }

    #[must_use]
    pub fn with_no_token_symbol(mut self) -> Self {
        self.token_symbol_behavior = MockTokenSymbolBehavior::None;
        self
    }

    #[must_use]
    pub fn with_no_redemption_wallet(mut self) -> Self {
        self.redemption_wallet = None;
        self
    }

    #[must_use]
    pub fn with_fees(mut self, fees: Float) -> Self {
        self.fees_override = Some(fees);
        self
    }

    pub fn last_issuer_request_id(&self) -> Option<IssuerRequestId> {
        self.last_issuer_request_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Default for MockTokenizer {
    fn default() -> Self {
        Self::new()
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
        self.call_count.fetch_add(1, Ordering::Relaxed);
        *self
            .last_issuer_request_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(issuer_request_id);

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
                    message: AlpacaApiErrorMessage::from_response(
                        "mock mint request error".to_string(),
                    ),
                }))
            }
        }
    }

    async fn poll_mint_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        match self.mint_poll_outcome {
            MockMintPollOutcome::Completed => {
                let mut request = TokenizationRequest::mock_completed();
                match &self.token_symbol_behavior {
                    MockTokenSymbolBehavior::Default => {}
                    MockTokenSymbolBehavior::None => request.token_symbol = None,
                    MockTokenSymbolBehavior::Override(symbol) => {
                        request.token_symbol = Some(symbol.clone());
                    }
                }
                if let Some(fees) = &self.fees_override {
                    request.fees = Some(*fees);
                }
                Ok(request)
            }
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

    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.pending_requests
            .iter()
            .find(|request| &request.id == id)
            .cloned()
            .ok_or_else(|| {
                TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id: id.clone() })
            })
    }

    fn redemption_wallet(&self) -> Option<Address> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.redemption_wallet
    }

    async fn wait_for_block(&self, block: u64) -> Result<(), EvmError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.wait_for_block_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(block);

        match self.wait_for_block_outcome {
            MockWaitForBlockOutcome::Succeed => Ok(()),
            MockWaitForBlockOutcome::NodeBehind => Err(EvmError::NodeBehindRequiredBlock {
                observed_tip: 0,
                required_block: block,
                attempts: NODE_SYNC_MAX_ATTEMPTS,
            }),
        }
    }

    async fn send_for_redemption(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        match self.send_outcome {
            MockSendOutcome::ApiError => {
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: AlpacaApiErrorMessage::from_response(
                        "mock send_for_redemption failure".to_string(),
                    ),
                }))
            }
            MockSendOutcome::Succeed => Ok(self.redemption_tx),
        }
    }

    async fn poll_for_redemption(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
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
                    message: AlpacaApiErrorMessage::from_response(
                        "mock detection API error".to_string(),
                    ),
                }))
            }
        }
    }

    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .pending_requests
            .iter()
            .find(|request| request.tx_hash.as_ref() == Some(tx_hash))
            .cloned())
    }

    async fn poll_redemption_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
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

    async fn verify_mint_tx(
        &self,
        tx_hash: TxHash,
        token_address: Address,
        wallet: Address,
        expected_amount: U256,
    ) -> Result<(), MintVerificationError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        match self.verification_outcome {
            MockVerificationOutcome::Success => Ok(()),
            MockVerificationOutcome::ReceiptNotFound => {
                Err(MintVerificationError::ReceiptNotFound { tx_hash })
            }
            MockVerificationOutcome::TransactionReverted => {
                Err(MintVerificationError::TransactionReverted { tx_hash })
            }
            MockVerificationOutcome::NoMatchingTransfer => {
                Err(MintVerificationError::NoMatchingTransfer {
                    tx_hash,
                    wallet,
                    token: token_address,
                })
            }
            MockVerificationOutcome::InsufficientTransferAmount => {
                Err(MintVerificationError::InsufficientTransferAmount {
                    tx_hash,
                    expected: expected_amount,
                    actual: U256::ZERO,
                })
            }
        }
    }

    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        match self.list_pending_outcome {
            MockListPendingOutcome::ApiError => {
                return Err(TokenizerError::Alpaca(AlpacaTokenizationError::ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: AlpacaApiErrorMessage::from_response(
                        "mock list_pending_requests failure".to_string(),
                    ),
                }));
            }
            MockListPendingOutcome::Succeed => {}
        }

        Ok(self
            .pending_requests
            .iter()
            .filter(|request| match request.status {
                TokenizationRequestStatus::Pending => true,
                TokenizationRequestStatus::Completed | TokenizationRequestStatus::Rejected => false,
            })
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{MockTokenizer, TokenizationRequestStatus, Tokenizer};

    #[tokio::test]
    async fn default_mock_tokenizer_completes_redemption_polling() {
        let tokenizer = MockTokenizer::new();
        let request = tokenizer
            .poll_redemption_until_complete(&super::super::tokenization_request_id(
                "mock-redemption",
            ))
            .await
            .unwrap();

        assert_eq!(request.status, TokenizationRequestStatus::Completed);
    }
}
