//! Mock implementation of the Mint trait for testing.

use alloy::primitives::Address;
use async_trait::async_trait;
use st0x_execution::Symbol;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{Mint, MintError};
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::IssuerRequestId;

/// Parameters captured from the last `execute_mint` call.
#[derive(Debug, Clone)]
pub(crate) struct MintCall {
    pub(crate) issuer_request_id: String,
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
    pub(crate) wallet: Address,
}

/// Mock implementation of the Mint trait for testing.
///
/// Tracks call counts and optionally captures parameters for verification.
pub(crate) struct MockMint {
    call_count: AtomicUsize,
    should_fail: AtomicBool,
    last_call: Mutex<Option<MintCall>>,
}

impl MockMint {
    /// Creates a new mock that succeeds on all calls.
    pub(crate) fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            should_fail: AtomicBool::new(false),
            last_call: Mutex::new(None),
        }
    }

    /// Creates a new mock that always returns `MintError::Rejected`.
    pub(crate) fn failing() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            should_fail: AtomicBool::new(true),
            last_call: Mutex::new(None),
        }
    }

    /// Returns the number of times `execute_mint` was called.
    pub(crate) fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    /// Returns the parameters from the last call, if any.
    pub(crate) fn last_call(&self) -> Option<MintCall> {
        self.last_call.lock().unwrap().clone()
    }
}

#[async_trait]
impl Mint for MockMint {
    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        *self.last_call.lock().unwrap() = Some(MintCall {
            issuer_request_id: issuer_request_id.0.clone(),
            symbol,
            quantity,
            wallet,
        });

        if self.should_fail.load(Ordering::SeqCst) {
            return Err(MintError::Rejected);
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
        let mock = MockMint::new();
        assert_eq!(mock.calls(), 0);
    }

    #[test]
    fn new_mock_has_no_last_call() {
        let mock = MockMint::new();
        assert!(mock.last_call().is_none());
    }

    #[tokio::test]
    async fn execute_mint_increments_call_count() {
        let mock = MockMint::new();

        mock.execute_mint(
            &IssuerRequestId::new("id-1"),
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(dec!(10)),
            Address::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 1);

        mock.execute_mint(
            &IssuerRequestId::new("id-2"),
            Symbol::new("TSLA").unwrap(),
            FractionalShares::new(dec!(20)),
            Address::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    async fn execute_mint_captures_parameters() {
        let mock = MockMint::new();
        let wallet = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        mock.execute_mint(
            &IssuerRequestId::new("req-abc"),
            Symbol::new("GOOG").unwrap(),
            FractionalShares::new(dec!(123.456)),
            wallet,
        )
        .await
        .unwrap();

        let call = mock.last_call().unwrap();
        assert_eq!(call.issuer_request_id, "req-abc");
        assert_eq!(call.symbol, Symbol::new("GOOG").unwrap());
        assert_eq!(call.quantity, FractionalShares::new(dec!(123.456)));
        assert_eq!(call.wallet, wallet);
    }

    #[tokio::test]
    async fn failing_mock_returns_rejected_error() {
        let mock = MockMint::failing();

        let result = mock
            .execute_mint(
                &IssuerRequestId::new("fail-test"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(1)),
                Address::ZERO,
            )
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));
    }

    #[tokio::test]
    async fn failing_mock_still_increments_call_count() {
        let mock = MockMint::failing();

        let _ = mock
            .execute_mint(
                &IssuerRequestId::new("x"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(1)),
                Address::ZERO,
            )
            .await;

        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn failing_mock_still_captures_last_call() {
        let mock = MockMint::failing();

        let _ = mock
            .execute_mint(
                &IssuerRequestId::new("captured"),
                Symbol::new("NVDA").unwrap(),
                FractionalShares::new(dec!(50)),
                Address::ZERO,
            )
            .await;

        let call = mock.last_call().unwrap();
        assert_eq!(call.issuer_request_id, "captured");
        assert_eq!(call.symbol, Symbol::new("NVDA").unwrap());
    }
}
