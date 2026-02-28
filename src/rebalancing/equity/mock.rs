//! Mock implementation of CrossVenueTransfer for equity, used in testing.

use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use st0x_execution::{FractionalShares, Symbol};

use super::{Equity, MintError, RedemptionError};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};

/// Parameters captured from a hedging-to-market-making (mint) call.
#[derive(Debug, Clone)]
pub(crate) struct MintCall {
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
}

/// Parameters captured from a market-making-to-hedging (redemption) call.
#[derive(Debug, Clone)]
pub(crate) struct RedemptionCall {
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
}

/// Mock equity cross-venue transfer for testing.
///
/// Tracks call counts and captures parameters for verification.
pub(crate) struct MockCrossVenueEquityTransfer {
    mint_count: AtomicUsize,
    redeem_count: AtomicUsize,
    last_mint: Mutex<Option<MintCall>>,
    last_redeem: Mutex<Option<RedemptionCall>>,
}

impl MockCrossVenueEquityTransfer {
    pub(crate) fn new() -> Self {
        Self {
            mint_count: AtomicUsize::new(0),
            redeem_count: AtomicUsize::new(0),
            last_mint: Mutex::new(None),
            last_redeem: Mutex::new(None),
        }
    }

    pub(crate) fn mint_calls(&self) -> usize {
        self.mint_count.load(Ordering::SeqCst)
    }

    pub(crate) fn redeem_calls(&self) -> usize {
        self.redeem_count.load(Ordering::SeqCst)
    }

    pub(crate) fn last_mint_call(&self) -> Option<MintCall> {
        self.last_mint.lock().unwrap().clone()
    }

    pub(crate) fn last_redeem_call(&self) -> Option<RedemptionCall> {
        self.last_redeem.lock().unwrap().clone()
    }
}

/// Hedging -> Market-Making (mint direction).
#[async_trait]
impl CrossVenueTransfer<HedgingVenue, MarketMakingVenue> for MockCrossVenueEquityTransfer {
    type Asset = Equity;
    type Error = MintError;

    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        self.mint_count.fetch_add(1, Ordering::SeqCst);

        *self.last_mint.lock().unwrap() = Some(MintCall {
            symbol: asset.symbol,
            quantity: asset.quantity,
        });

        Ok(())
    }
}

/// Market-Making -> Hedging (redemption direction).
#[async_trait]
impl CrossVenueTransfer<MarketMakingVenue, HedgingVenue> for MockCrossVenueEquityTransfer {
    type Asset = Equity;
    type Error = RedemptionError;

    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        self.redeem_count.fetch_add(1, Ordering::SeqCst);

        *self.last_redeem.lock().unwrap() = Some(RedemptionCall {
            symbol: asset.symbol,
            quantity: asset.quantity,
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use st0x_exact_decimal::ExactDecimal;

    use super::*;

    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    #[test]
    fn new_mock_starts_with_zero_counts() {
        let mock = MockCrossVenueEquityTransfer::new();
        assert_eq!(mock.mint_calls(), 0);
        assert_eq!(mock.redeem_calls(), 0);
    }

    #[test]
    fn new_mock_has_no_last_calls() {
        let mock = MockCrossVenueEquityTransfer::new();
        assert!(mock.last_mint_call().is_none());
        assert!(mock.last_redeem_call().is_none());
    }

    #[tokio::test]
    async fn mint_increments_count() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(ed("100")),
            },
        )
        .await
        .unwrap();

        assert_eq!(mock.mint_calls(), 1);
        assert_eq!(mock.redeem_calls(), 0);
    }

    #[tokio::test]
    async fn redeem_increments_count() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(ed("50")),
            },
        )
        .await
        .unwrap();

        assert_eq!(mock.mint_calls(), 0);
        assert_eq!(mock.redeem_calls(), 1);
    }

    #[tokio::test]
    async fn mint_captures_parameters() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("TSLA").unwrap(),
                quantity: FractionalShares::new(ed("42.5")),
            },
        )
        .await
        .unwrap();

        let call = mock.last_mint_call().unwrap();
        assert_eq!(call.symbol, Symbol::new("TSLA").unwrap());
        assert_eq!(call.quantity, FractionalShares::new(ed("42.5")));
    }

    #[tokio::test]
    async fn redeem_captures_parameters() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("GOOG").unwrap(),
                quantity: FractionalShares::new(ed("7.25")),
            },
        )
        .await
        .unwrap();

        let call = mock.last_redeem_call().unwrap();
        assert_eq!(call.symbol, Symbol::new("GOOG").unwrap());
        assert_eq!(call.quantity, FractionalShares::new(ed("7.25")));
    }

    #[tokio::test]
    async fn operations_are_independent() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(ed("10")),
            },
        )
        .await
        .unwrap();

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("TSLA").unwrap(),
                quantity: FractionalShares::new(ed("20")),
            },
        )
        .await
        .unwrap();

        assert_eq!(mock.mint_calls(), 1);
        assert_eq!(mock.redeem_calls(), 1);

        let mint = mock.last_mint_call().unwrap();
        let redeem = mock.last_redeem_call().unwrap();

        assert_eq!(mint.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(redeem.symbol, Symbol::new("TSLA").unwrap());
    }
}
