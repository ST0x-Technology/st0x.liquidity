//! Mock implementation of the UsdcRebalance trait for testing.

use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{UsdcRebalance, UsdcRebalanceManagerError};
use crate::threshold::Usdc;
use crate::usdc_rebalance::UsdcRebalanceId;

/// Parameters captured from an `execute_alpaca_to_base` call.
#[derive(Debug, Clone)]
pub(crate) struct AlpacaToBaseCall {
    pub(crate) id: String,
    pub(crate) amount: Usdc,
}

/// Parameters captured from an `execute_base_to_alpaca` call.
#[derive(Debug, Clone)]
pub(crate) struct BaseToAlpacaCall {
    pub(crate) id: String,
    pub(crate) amount: Usdc,
}

/// Mock implementation of the UsdcRebalance trait for testing.
///
/// Tracks call counts and optionally captures parameters for verification.
pub(crate) struct MockUsdcRebalance {
    alpaca_to_base_count: AtomicUsize,
    base_to_alpaca_count: AtomicUsize,
    fail_alpaca_to_base: AtomicBool,
    fail_base_to_alpaca: AtomicBool,
    last_alpaca_to_base: Mutex<Option<AlpacaToBaseCall>>,
    last_base_to_alpaca: Mutex<Option<BaseToAlpacaCall>>,
}

impl MockUsdcRebalance {
    /// Creates a new mock that succeeds on all calls.
    pub(crate) fn new() -> Self {
        Self {
            alpaca_to_base_count: AtomicUsize::new(0),
            base_to_alpaca_count: AtomicUsize::new(0),
            fail_alpaca_to_base: AtomicBool::new(false),
            fail_base_to_alpaca: AtomicBool::new(false),
            last_alpaca_to_base: Mutex::new(None),
            last_base_to_alpaca: Mutex::new(None),
        }
    }

    /// Creates a mock that fails `execute_alpaca_to_base` calls.
    pub(crate) fn failing_alpaca_to_base() -> Self {
        Self {
            alpaca_to_base_count: AtomicUsize::new(0),
            base_to_alpaca_count: AtomicUsize::new(0),
            fail_alpaca_to_base: AtomicBool::new(true),
            fail_base_to_alpaca: AtomicBool::new(false),
            last_alpaca_to_base: Mutex::new(None),
            last_base_to_alpaca: Mutex::new(None),
        }
    }

    /// Creates a mock that fails `execute_base_to_alpaca` calls.
    pub(crate) fn failing_base_to_alpaca() -> Self {
        Self {
            alpaca_to_base_count: AtomicUsize::new(0),
            base_to_alpaca_count: AtomicUsize::new(0),
            fail_alpaca_to_base: AtomicBool::new(false),
            fail_base_to_alpaca: AtomicBool::new(true),
            last_alpaca_to_base: Mutex::new(None),
            last_base_to_alpaca: Mutex::new(None),
        }
    }

    /// Returns the number of `execute_alpaca_to_base` calls.
    pub(crate) fn alpaca_to_base_calls(&self) -> usize {
        self.alpaca_to_base_count.load(Ordering::SeqCst)
    }

    /// Returns the number of `execute_base_to_alpaca` calls.
    pub(crate) fn base_to_alpaca_calls(&self) -> usize {
        self.base_to_alpaca_count.load(Ordering::SeqCst)
    }

    /// Returns the parameters from the last `execute_alpaca_to_base` call.
    pub(crate) fn last_alpaca_to_base_call(&self) -> Option<AlpacaToBaseCall> {
        self.last_alpaca_to_base.lock().unwrap().clone()
    }

    /// Returns the parameters from the last `execute_base_to_alpaca` call.
    pub(crate) fn last_base_to_alpaca_call(&self) -> Option<BaseToAlpacaCall> {
        self.last_base_to_alpaca.lock().unwrap().clone()
    }
}

#[async_trait]
impl UsdcRebalance for MockUsdcRebalance {
    async fn execute_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        self.alpaca_to_base_count.fetch_add(1, Ordering::SeqCst);

        *self.last_alpaca_to_base.lock().unwrap() = Some(AlpacaToBaseCall {
            id: id.0.clone(),
            amount,
        });

        if self.fail_alpaca_to_base.load(Ordering::SeqCst) {
            return Err(UsdcRebalanceManagerError::WithdrawalFailed {
                status: "mock_failure".to_string(),
            });
        }

        Ok(())
    }

    async fn execute_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcRebalanceManagerError> {
        self.base_to_alpaca_count.fetch_add(1, Ordering::SeqCst);

        *self.last_base_to_alpaca.lock().unwrap() = Some(BaseToAlpacaCall {
            id: id.0.clone(),
            amount,
        });

        if self.fail_base_to_alpaca.load(Ordering::SeqCst) {
            return Err(UsdcRebalanceManagerError::DepositFailed {
                status: "mock_failure".to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn new_mock_starts_with_zero_counts() {
        let mock = MockUsdcRebalance::new();
        assert_eq!(mock.alpaca_to_base_calls(), 0);
        assert_eq!(mock.base_to_alpaca_calls(), 0);
    }

    #[test]
    fn new_mock_has_no_last_calls() {
        let mock = MockUsdcRebalance::new();
        assert!(mock.last_alpaca_to_base_call().is_none());
        assert!(mock.last_base_to_alpaca_call().is_none());
    }

    #[tokio::test]
    async fn alpaca_to_base_increments_count() {
        let mock = MockUsdcRebalance::new();

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("id-1"), Usdc(dec!(100)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 1);

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("id-2"), Usdc(dec!(200)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 2);
    }

    #[tokio::test]
    async fn base_to_alpaca_increments_count() {
        let mock = MockUsdcRebalance::new();

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("id-1"), Usdc(dec!(100)))
            .await
            .unwrap();

        assert_eq!(mock.base_to_alpaca_calls(), 1);

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("id-2"), Usdc(dec!(200)))
            .await
            .unwrap();

        assert_eq!(mock.base_to_alpaca_calls(), 2);
    }

    #[tokio::test]
    async fn alpaca_to_base_captures_parameters() {
        let mock = MockUsdcRebalance::new();

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("capture-atb"), Usdc(dec!(999.99)))
            .await
            .unwrap();

        let call = mock.last_alpaca_to_base_call().unwrap();
        assert_eq!(call.id, "capture-atb");
        assert_eq!(call.amount, Usdc(dec!(999.99)));
    }

    #[tokio::test]
    async fn base_to_alpaca_captures_parameters() {
        let mock = MockUsdcRebalance::new();

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("capture-bta"), Usdc(dec!(1234.56)))
            .await
            .unwrap();

        let call = mock.last_base_to_alpaca_call().unwrap();
        assert_eq!(call.id, "capture-bta");
        assert_eq!(call.amount, Usdc(dec!(1234.56)));
    }

    #[tokio::test]
    async fn failing_alpaca_to_base_returns_error() {
        let mock = MockUsdcRebalance::failing_alpaca_to_base();

        let result = mock
            .execute_alpaca_to_base(&UsdcRebalanceId::new("fail"), Usdc(dec!(1)))
            .await;

        assert!(matches!(
            result,
            Err(UsdcRebalanceManagerError::WithdrawalFailed { .. })
        ));
    }

    #[tokio::test]
    async fn failing_base_to_alpaca_returns_error() {
        let mock = MockUsdcRebalance::failing_base_to_alpaca();

        let result = mock
            .execute_base_to_alpaca(&UsdcRebalanceId::new("fail"), Usdc(dec!(1)))
            .await;

        assert!(matches!(
            result,
            Err(UsdcRebalanceManagerError::DepositFailed { .. })
        ));
    }

    #[tokio::test]
    async fn failing_mock_still_increments_count() {
        let mock = MockUsdcRebalance::failing_alpaca_to_base();

        let _ = mock
            .execute_alpaca_to_base(&UsdcRebalanceId::new("x"), Usdc(dec!(1)))
            .await;

        assert_eq!(mock.alpaca_to_base_calls(), 1);
    }

    #[tokio::test]
    async fn failing_mock_still_captures_last_call() {
        let mock = MockUsdcRebalance::failing_alpaca_to_base();

        let _ = mock
            .execute_alpaca_to_base(&UsdcRebalanceId::new("captured"), Usdc(dec!(42)))
            .await;

        let call = mock.last_alpaca_to_base_call().unwrap();
        assert_eq!(call.id, "captured");
        assert_eq!(call.amount, Usdc(dec!(42)));
    }

    #[tokio::test]
    async fn operations_are_independent() {
        let mock = MockUsdcRebalance::new();

        mock.execute_alpaca_to_base(&UsdcRebalanceId::new("atb"), Usdc(dec!(100)))
            .await
            .unwrap();

        mock.execute_base_to_alpaca(&UsdcRebalanceId::new("bta"), Usdc(dec!(200)))
            .await
            .unwrap();

        assert_eq!(mock.alpaca_to_base_calls(), 1);
        assert_eq!(mock.base_to_alpaca_calls(), 1);

        let atb = mock.last_alpaca_to_base_call().unwrap();
        let bta = mock.last_base_to_alpaca_call().unwrap();

        assert_eq!(atb.id, "atb");
        assert_eq!(bta.id, "bta");
    }
}
