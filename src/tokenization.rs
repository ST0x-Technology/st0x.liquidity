//! Tokenization abstraction for converting between offchain shares and onchain tokens.
//!
//! This module provides the `Tokenizer` trait that abstracts tokenization operations,
//! allowing different implementations (Alpaca, mock, etc.) to be used interchangeably.

pub(crate) mod alpaca;

#[cfg(test)]
pub(crate) mod mock;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::{FractionalShares, Symbol};

pub(crate) use alpaca::{
    AlpacaTokenizationError, AlpacaTokenizationService, TokenizationRequest,
    TokenizationRequestStatus,
};

use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

/// Error type for Tokenizer operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizerError {
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
}

/// Abstraction for equity tokenization operations.
///
/// Implementations handle tokenization API calls for:
/// - Minting: converting offchain shares to onchain tokens
/// - Redemption: converting onchain tokens back to offchain shares
#[async_trait]
pub(crate) trait Tokenizer: Send + Sync {
    /// Request a mint operation to convert offchain shares to onchain tokens.
    async fn request_mint(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Poll a mint request until it reaches a terminal state.
    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Returns the redemption wallet address where tokens should be sent.
    fn redemption_wallet(&self) -> Address;

    /// Send tokens to the redemption wallet to initiate redemption.
    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, TokenizerError>;

    /// Poll until the tokenization provider detects the redemption transfer.
    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Poll a redemption request until it reaches a terminal state.
    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;
}
