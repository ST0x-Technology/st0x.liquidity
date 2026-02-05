//! Mock implementation of the Tokenizer trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::{FractionalShares, Symbol};

use super::{TokenizationRequest, Tokenizer, TokenizerError};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

pub(crate) struct MockTokenizer {
    // TODO: Add mock configuration fields as needed
}

impl MockTokenizer {
    pub(crate) fn new() -> Self {
        Self {}
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
        todo!("MockTokenizer::request_mint")
    }

    async fn poll_mint_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        todo!("MockTokenizer::poll_mint_until_complete")
    }

    fn redemption_wallet(&self) -> Address {
        todo!("MockTokenizer::redemption_wallet")
    }

    async fn send_for_redemption(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        todo!("MockTokenizer::send_for_redemption")
    }

    async fn poll_for_redemption(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        todo!("MockTokenizer::poll_for_redemption")
    }

    async fn poll_redemption_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        todo!("MockTokenizer::poll_redemption_until_complete")
    }
}
