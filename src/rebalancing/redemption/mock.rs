//! Mock implementation of the Redeem trait for testing.

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use st0x_execution::Symbol;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{Redeem, RedemptionError};
use crate::equity_redemption::RedemptionAggregateId;
use crate::shares::FractionalShares;

/// Parameters captured from the last `execute_redemption` call.
#[derive(Debug, Clone)]
pub(crate) struct RedeemCall {
    pub(crate) aggregate_id: RedemptionAggregateId,
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
    pub(crate) token: Address,
    pub(crate) amount: U256,
}

/// Mock implementation of the Redeem trait for testing.
///
/// Tracks call counts and optionally captures parameters for verification.
pub(crate) struct MockRedeem {
    call_count: AtomicUsize,
    should_fail: AtomicBool,
    last_call: Mutex<Option<RedeemCall>>,
}

impl MockRedeem {
    /// Creates a new mock that succeeds on all calls.
    pub(crate) fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            should_fail: AtomicBool::new(false),
            last_call: Mutex::new(None),
        }
    }

    /// Creates a new mock that always returns `RedemptionError::Rejected`.
    pub(crate) fn failing() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            should_fail: AtomicBool::new(true),
            last_call: Mutex::new(None),
        }
    }

    /// Returns the number of times `execute_redemption` was called.
    pub(crate) fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    /// Returns the parameters from the last call, if any.
    pub(crate) fn last_call(&self) -> Option<RedeemCall> {
        self.last_call.lock().unwrap().clone()
    }
}

#[async_trait]
impl Redeem for MockRedeem {
    async fn execute_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        *self.last_call.lock().unwrap() = Some(RedeemCall {
            aggregate_id: aggregate_id.clone(),
            symbol,
            quantity,
            token,
            amount,
        });

        if self.should_fail.load(Ordering::SeqCst) {
            return Err(RedemptionError::Rejected);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn new_mock_starts_with_zero_calls() {
        let mock = MockRedeem::new();
        assert_eq!(mock.calls(), 0);
    }

    #[test]
    fn new_mock_has_no_last_call() {
        let mock = MockRedeem::new();
        assert!(mock.last_call().is_none());
    }

    #[tokio::test]
    async fn execute_redemption_increments_call_count() {
        let mock = MockRedeem::new();

        mock.execute_redemption(
            &RedemptionAggregateId::new("agg-1"),
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(dec!(10)),
            Address::ZERO,
            U256::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 1);

        mock.execute_redemption(
            &RedemptionAggregateId::new("agg-2"),
            Symbol::new("TSLA").unwrap(),
            FractionalShares::new(dec!(20)),
            Address::ZERO,
            U256::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    async fn execute_redemption_captures_parameters() {
        let mock = MockRedeem::new();
        let token = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        let amount = U256::from(999_000_000u64);
        let aggregate_id = RedemptionAggregateId::new("my-agg-id");

        mock.execute_redemption(
            &aggregate_id,
            Symbol::new("GOOG").unwrap(),
            FractionalShares::new(dec!(123.456)),
            token,
            amount,
        )
        .await
        .unwrap();

        let call = mock.last_call().unwrap();
        assert_eq!(call.aggregate_id, aggregate_id);
        assert_eq!(call.symbol, Symbol::new("GOOG").unwrap());
        assert_eq!(call.quantity, FractionalShares::new(dec!(123.456)));
        assert_eq!(call.token, token);
        assert_eq!(call.amount, amount);
    }

    #[tokio::test]
    async fn failing_mock_returns_rejected_error() {
        let mock = MockRedeem::failing();

        let result = mock
            .execute_redemption(
                &RedemptionAggregateId::new("fail-test"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(1)),
                Address::ZERO,
                U256::ZERO,
            )
            .await;

        assert!(matches!(result, Err(RedemptionError::Rejected)));
    }

    #[tokio::test]
    async fn failing_mock_still_increments_call_count() {
        let mock = MockRedeem::failing();

        let _ = mock
            .execute_redemption(
                &RedemptionAggregateId::new("x"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(1)),
                Address::ZERO,
                U256::ZERO,
            )
            .await;

        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn failing_mock_still_captures_last_call() {
        let mock = MockRedeem::failing();
        let aggregate_id = RedemptionAggregateId::new("captured-agg");

        let _ = mock
            .execute_redemption(
                &aggregate_id,
                Symbol::new("NVDA").unwrap(),
                FractionalShares::new(dec!(50)),
                Address::ZERO,
                U256::from(12345u64),
            )
            .await;

        let call = mock.last_call().unwrap();
        assert_eq!(call.aggregate_id, aggregate_id);
        assert_eq!(call.symbol, Symbol::new("NVDA").unwrap());
        assert_eq!(call.amount, U256::from(12345u64));
    }
}
