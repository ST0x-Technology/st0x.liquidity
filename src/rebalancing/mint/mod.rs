//! Mint operations for tokenized equity.
//!
//! This module provides the trait and implementations for minting tokenized equities
//! through the Alpaca tokenization API.

pub(crate) mod manager;
#[cfg(test)]
pub(crate) mod mock;

use alloy::primitives::Address;
use async_trait::async_trait;
use cqrs_es::AggregateError;
use st0x_execution::Symbol;
use thiserror::Error;

use crate::alpaca_tokenization::AlpacaTokenizationError;
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizedEquityMintError};

#[derive(Debug, Error)]
pub(crate) enum MintError {
    #[error("Alpaca API error: {0}")]
    Alpaca(#[from] AlpacaTokenizationError),

    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<TokenizedEquityMintError>),

    #[error("Mint request was rejected by Alpaca")]
    Rejected,

    #[error("Missing issuer_request_id in Alpaca response")]
    MissingIssuerRequestId,

    #[error("Missing tx_hash in completed Alpaca response")]
    MissingTxHash,

    #[error("U256 parse error: {0}")]
    U256Parse(#[from] alloy::primitives::ruint::ParseError),

    #[error("Quantity {0} has more than 18 decimal places")]
    PrecisionLoss(FractionalShares),

    #[error("Decimal overflow when scaling {0} to 18 decimals")]
    DecimalOverflow(FractionalShares),
}

/// Trait for executing mint operations.
#[async_trait]
pub(crate) trait Mint: Send + Sync {
    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError>;
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use super::*;
    use crate::rebalancing::mint::mock::MockMint;

    #[test]
    fn mock_mint_tracks_call_count() {
        let mock = MockMint::new();
        assert_eq!(mock.calls(), 0);
    }

    #[tokio::test]
    async fn mock_mint_increments_on_execute() {
        let mock = Arc::new(MockMint::new());

        mock.execute_mint(
            &IssuerRequestId::new("test-id"),
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(dec!(100)),
            address!("0x1234567890123456789012345678901234567890"),
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn mock_mint_captures_last_call_parameters() {
        let mock = Arc::new(MockMint::new());
        let symbol = Symbol::new("TSLA").unwrap();
        let quantity = FractionalShares::new(dec!(50.5));
        let wallet = address!("0xabcdef0123456789abcdef0123456789abcdef01");

        mock.execute_mint(
            &IssuerRequestId::new("req-123"),
            symbol.clone(),
            quantity,
            wallet,
        )
        .await
        .unwrap();

        let last = mock.last_call().unwrap();
        assert_eq!(last.symbol, symbol);
        assert_eq!(last.quantity, quantity);
        assert_eq!(last.wallet, wallet);
    }

    #[tokio::test]
    async fn mock_mint_returns_configured_error() {
        let mock = Arc::new(MockMint::failing());

        let result = mock
            .execute_mint(
                &IssuerRequestId::new("test"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(10)),
                Address::ZERO,
            )
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));
    }
}
