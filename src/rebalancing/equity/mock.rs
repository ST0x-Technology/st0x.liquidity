//! Mock implementation of CrossVenueTransfer for equity, used in testing.

use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use st0x_execution::{FractionalShares, Symbol};

use super::{Equity, RedemptionError};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};

/// Parameters captured from a market-making-to-hedging (redemption) call.
#[derive(Debug, Clone)]
pub(crate) struct RedemptionCall {
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
}

/// Mock equity cross-venue transfer for testing the redemption direction.
///
/// Tracks call counts and captures parameters for verification. The mint
/// direction (hedging -> market-making) is driven by the
/// `TransferEquityToMarketMaking` apalis job, not this trait, so the mock
/// only implements redemption.
pub(crate) struct MockCrossVenueEquityTransfer {
    redeem_count: AtomicUsize,
    last_redeem: Mutex<Option<RedemptionCall>>,
}

impl MockCrossVenueEquityTransfer {
    pub(crate) fn new() -> Self {
        Self {
            redeem_count: AtomicUsize::new(0),
            last_redeem: Mutex::new(None),
        }
    }

    pub(crate) fn redeem_calls(&self) -> usize {
        self.redeem_count.load(Ordering::SeqCst)
    }

    pub(crate) fn last_redeem_call(&self) -> Option<RedemptionCall> {
        self.last_redeem.lock().unwrap().clone()
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
    use super::*;
    use st0x_float_macro::float;

    #[test]
    fn new_mock_starts_with_zero_counts() {
        let mock = MockCrossVenueEquityTransfer::new();
        assert_eq!(mock.redeem_calls(), 0);
        assert!(mock.last_redeem_call().is_none());
    }

    #[tokio::test]
    async fn redeem_increments_count_and_captures_parameters() {
        let mock = MockCrossVenueEquityTransfer::new();

        CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &mock,
            Equity {
                symbol: Symbol::new("GOOG").unwrap(),
                quantity: FractionalShares::new(float!(7.25)),
            },
        )
        .await
        .unwrap();

        assert_eq!(mock.redeem_calls(), 1);

        let call = mock.last_redeem_call().unwrap();
        assert_eq!(call.symbol, Symbol::new("GOOG").unwrap());
        assert_eq!(call.quantity, FractionalShares::new(float!(7.25)));
    }
}
