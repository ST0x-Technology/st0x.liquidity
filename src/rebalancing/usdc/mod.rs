//! USDC rebalancing operations between Alpaca and Base.
//!
//! This module provides the trait and implementations for rebalancing USDC
//! between the Alpaca wallet and the Base chain vault.

mod manager;
#[cfg(test)]
pub(crate) mod mock;

pub(crate) use manager::UsdcRebalanceManager;

use async_trait::async_trait;
use cqrs_es::AggregateError;
use thiserror::Error;

use alloy::primitives::ruint::FromUintError;

use crate::alpaca_wallet::AlpacaWalletError;
use crate::cctp::CctpError;
use crate::onchain::vault::VaultError;
use crate::threshold::Usdc;
use crate::usdc_rebalance::{UsdcRebalanceError, UsdcRebalanceId};
use st0x_execution::AlpacaBrokerApiError;

#[derive(Debug, Error)]
pub(crate) enum UsdcRebalanceManagerError {
    #[error("Alpaca wallet error: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("CCTP bridge error: {0}")]
    Cctp(#[from] CctpError),
    #[error("Vault error: {0}")]
    Vault(#[from] VaultError),
    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<UsdcRebalanceError>),
    #[error("Withdrawal failed with terminal status: {status}")]
    WithdrawalFailed { status: String },
    #[error("Deposit failed with terminal status: {status}")]
    DepositFailed { status: String },
    #[error("Invalid amount: {0}")]
    InvalidAmount(String),
    #[error("Arithmetic overflow: {0}")]
    ArithmeticOverflow(String),
    #[error("U256 parse error: {0}")]
    U256Parse(#[from] alloy::primitives::ruint::ParseError),
    #[error("U256 to u128 conversion error: {0}")]
    U256ToU128(#[from] FromUintError<u128>),
    #[error("Conversion order {order_id} filled but filled_quantity is missing")]
    MissingFilledQuantity { order_id: uuid::Uuid },
}

/// Trait for executing USDC rebalance operations.
#[async_trait]
pub(crate) trait UsdcRebalance: Send + Sync {
    /// Executes a rebalance from Alpaca wallet to Base vault.
    async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError>;

    /// Executes a rebalance from Base vault to Alpaca wallet.
    async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError>;
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use super::*;
    use crate::rebalancing::usdc::mock::MockUsdcRebalance;

    #[test]
    fn mock_tracks_call_counts() {
        let mock = MockUsdcRebalance::new();
        assert_eq!(mock.alpaca_to_base_calls(), 0);
        assert_eq!(mock.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn mock_alpaca_to_base_increments_count() {
        let mock = Arc::new(MockUsdcRebalance::new());

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("id-1"), Usdc(dec!(1000)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 1);
        assert_eq!(mock.base_to_alpaca_calls(), 0);
    }

    #[tokio::test]
    async fn mock_base_to_alpaca_increments_count() {
        let mock = Arc::new(MockUsdcRebalance::new());

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("id-2"), Usdc(dec!(2000)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 0);
        assert_eq!(mock.base_to_alpaca_calls(), 1);
    }

    #[tokio::test]
    async fn mock_captures_last_alpaca_to_base_call() {
        let mock = Arc::new(MockUsdcRebalance::new());
        let amount = Usdc(dec!(5000.50));

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("atb-123"), amount)
            .await
            .unwrap();

        let last = mock.last_alpaca_to_base_call().unwrap();
        assert_eq!(last.id, "atb-123");
        assert_eq!(last.amount, amount);
    }

    #[tokio::test]
    async fn mock_captures_last_base_to_alpaca_call() {
        let mock = Arc::new(MockUsdcRebalance::new());
        let amount = Usdc(dec!(7500.25));

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("bta-456"), amount)
            .await
            .unwrap();

        let last = mock.last_base_to_alpaca_call().unwrap();
        assert_eq!(last.id, "bta-456");
        assert_eq!(last.amount, amount);
    }

    #[tokio::test]
    async fn failing_mock_returns_error_for_alpaca_to_base() {
        let mock = Arc::new(MockUsdcRebalance::failing_alpaca_to_base());

        let result = mock
            .execute_alpaca_to_base(&UsdcRebalanceId::new("fail"), Usdc(dec!(100)))
            .await;

        assert!(matches!(
            result,
            Err(UsdcRebalanceManagerError::WithdrawalFailed { .. })
        ));
    }

    #[tokio::test]
    async fn failing_mock_returns_error_for_base_to_alpaca() {
        let mock = Arc::new(MockUsdcRebalance::failing_base_to_alpaca());

        let result = mock
            .execute_base_to_alpaca(&UsdcRebalanceId::new("fail"), Usdc(dec!(100)))
            .await;

        assert!(matches!(
            result,
            Err(UsdcRebalanceManagerError::DepositFailed { .. })
        ));
    }
}
