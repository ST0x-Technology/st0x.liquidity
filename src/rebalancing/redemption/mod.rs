//! Redemption operations for tokenized equity.
//!
//! This module provides the trait and implementations for redeeming
//! tokenized equities through the Alpaca tokenization API.

pub(crate) mod manager;
#[cfg(test)]
pub(crate) mod mock;
pub(crate) mod service;

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use thiserror::Error;

use st0x_execution::{FractionalShares, Symbol};

use st0x_event_sorcery::SendError;

use crate::equity_redemption::{EquityRedemption, RedemptionAggregateId};
use crate::onchain::raindex::RaindexError;
use crate::tokenization::AlpacaTokenizationError;

#[derive(Debug, Error)]
pub(crate) enum RedemptionError {
    #[error(transparent)]
    Send(#[from] SendError<EquityRedemption>),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error("Token {token} not found in vault registry")]
    VaultNotFound { token: Address },
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

/// Trait for executing redemption operations.
#[async_trait]
pub(crate) trait Redeem: Send + Sync {
    async fn execute_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError>;
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use super::*;
    use crate::rebalancing::redemption::mock::MockRedeem;

    #[test]
    fn mock_redeem_tracks_call_count() {
        let mock = MockRedeem::new();
        assert_eq!(mock.calls(), 0);
    }

    #[tokio::test]
    async fn mock_redeem_increments_on_execute() {
        let mock = Arc::new(MockRedeem::new());

        mock.execute_redemption(
            &RedemptionAggregateId::new("agg-1"),
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(dec!(100)),
            address!("0x1234567890123456789012345678901234567890"),
            U256::from(100_000_000_000_000_000_000_u128),
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn mock_redeem_captures_last_call_parameters() {
        let mock = Arc::new(MockRedeem::new());
        let symbol = Symbol::new("TSLA").unwrap();
        let quantity = FractionalShares::new(dec!(50.5));
        let token = address!("0xabcdef0123456789abcdef0123456789abcdef01");
        let amount = U256::from(50_500_000_000_000_000_000_u128);
        let aggregate_id = RedemptionAggregateId::new("agg-123");

        mock.execute_redemption(&aggregate_id, symbol.clone(), quantity, token, amount)
            .await
            .unwrap();

        let last = mock.last_call().unwrap();
        assert_eq!(last.aggregate_id, aggregate_id);
        assert_eq!(last.symbol, symbol);
        assert_eq!(last.quantity, quantity);
        assert_eq!(last.token, token);
        assert_eq!(last.amount, amount);
    }

    #[tokio::test]
    async fn mock_redeem_returns_configured_error() {
        let mock = Arc::new(MockRedeem::failing());

        let result = mock
            .execute_redemption(
                &RedemptionAggregateId::new("agg-fail"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(10)),
                Address::ZERO,
                U256::ZERO,
            )
            .await;

        assert!(matches!(result, Err(RedemptionError::Rejected)));
    }
}
