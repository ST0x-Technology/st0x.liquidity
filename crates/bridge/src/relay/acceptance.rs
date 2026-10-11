//! Whether a quote's amounts are good enough to deposit against, in checked
//! integer basis-point arithmetic.

use alloy::primitives::U256;

/// Basis points out of 10,000: a slippage or loss bound. Never above 10,000,
/// so `10_000 - bps` cannot underflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BasisPoints(pub(super) u16);

impl BasisPoints {
    const SCALE: u16 = 10_000;

    pub fn new(bps: u16) -> Result<Self, BasisPointsOutOfRange> {
        if bps > Self::SCALE {
            return Err(BasisPointsOutOfRange { bps });
        }

        Ok(Self(bps))
    }

    /// `amount * (10_000 - self) / 10_000`, rounded as `rounding` says.
    fn keep(self, amount: U256, rounding: Rounding) -> Result<U256, QuoteAcceptanceError> {
        let Self(bps) = self;
        let scale = U256::from(Self::SCALE);

        let scaled = amount
            .checked_mul(U256::from(Self::SCALE - bps))
            .ok_or(QuoteAcceptanceError::Overflow { amount, bps: self })?;

        Ok(match rounding {
            Rounding::Down => scaled / scale,
            Rounding::Up => scaled.div_ceil(scale),
        })
    }
}

/// Which way a bound rounds off the exact basis-point value.
#[derive(Debug, Clone, Copy)]
enum Rounding {
    /// The loss bound: errs towards refusing the quote.
    Up,
    /// The slippage floor: Relay rounds its own `minimumAmount` down, so a
    /// floor rounded up would refuse Relay's honest minimum.
    Down,
}

impl std::fmt::Display for BasisPoints {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(bps) = self;
        write!(formatter, "{bps} bps")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{bps} basis points is above 10000")]
pub struct BasisPointsOutOfRange {
    pub bps: u16,
}

/// What a quote must clear before we deposit against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteBounds {
    /// Most of the input the expected output may lose, fees included.
    pub max_loss: BasisPoints,
    /// The smallest amount the next leg accepts (an Alpaca deposit minimum),
    /// in the destination stable's smallest unit.
    pub downstream_minimum: U256,
}

/// A quote's amounts, all in the stables' smallest unit (6 decimals on both
/// ends of the Robinhood corridor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteAmounts {
    pub amount_in: U256,
    pub expected_out: U256,
    /// Relay's `minimumAmount`: below it the order is refunded, not filled.
    pub minimum_out: U256,
    /// The slippage the quote was requested at: Relay's minimum must not sit
    /// below the expected output less this much.
    pub slippage: BasisPoints,
}

impl QuoteAmounts {
    /// Accepts the quote only if every bound holds.
    pub fn accept(&self, bounds: &QuoteBounds) -> Result<(), QuoteAcceptanceError> {
        let Self {
            amount_in,
            expected_out,
            minimum_out,
            slippage,
        } = *self;

        if minimum_out > expected_out {
            return Err(QuoteAcceptanceError::MinimumAboveExpected {
                minimum: minimum_out,
                expected: expected_out,
            });
        }

        let loss_bound = bounds.max_loss.keep(amount_in, Rounding::Up)?;
        if expected_out < loss_bound {
            return Err(QuoteAcceptanceError::QuoteLossExceedsBound {
                expected: expected_out,
                bound: loss_bound,
            });
        }

        let floor = slippage.keep(expected_out, Rounding::Down)?;
        if minimum_out < floor {
            return Err(QuoteAcceptanceError::QuoteFloorBelowBound {
                minimum: minimum_out,
                floor,
            });
        }

        if minimum_out < bounds.downstream_minimum {
            return Err(QuoteAcceptanceError::QuoteBelowDownstreamMinimum {
                minimum: minimum_out,
                downstream_minimum: bounds.downstream_minimum,
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QuoteAcceptanceError {
    #[error("quote minimum {minimum} is above its expected output {expected}")]
    MinimumAboveExpected { minimum: U256, expected: U256 },
    #[error("expected output {expected} is below the loss bound {bound}")]
    QuoteLossExceedsBound { expected: U256, bound: U256 },
    #[error("Relay's minimum {minimum} is below our slippage floor {floor}")]
    QuoteFloorBelowBound { minimum: U256, floor: U256 },
    #[error("quote minimum {minimum} is below the next leg's minimum {downstream_minimum}")]
    QuoteBelowDownstreamMinimum {
        minimum: U256,
        downstream_minimum: U256,
    },
    #[error("{amount} scaled by {bps} overflows U256")]
    Overflow { amount: U256, bps: BasisPoints },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U512;
    use proptest::prelude::*;

    use super::*;

    fn bps(value: u16) -> BasisPoints {
        BasisPoints::new(value).unwrap()
    }

    /// The funded test quote: 5 USDG in, 4.763755 USDC expected, 4.749464
    /// minimum at 30 bps slippage.
    fn funded_amounts() -> QuoteAmounts {
        QuoteAmounts {
            amount_in: U256::from(5_000_000),
            expected_out: U256::from(4_763_755),
            minimum_out: U256::from(4_749_464),
            slippage: bps(30),
        }
    }

    fn bounds(max_loss: u16, downstream_minimum: u64) -> QuoteBounds {
        QuoteBounds {
            max_loss: bps(max_loss),
            downstream_minimum: U256::from(downstream_minimum),
        }
    }

    #[test]
    fn basis_points_above_ten_thousand_are_refused() {
        assert_eq!(
            BasisPoints::new(10_001).unwrap_err(),
            BasisPointsOutOfRange { bps: 10_001 }
        );
        assert_eq!(BasisPoints::new(10_000).unwrap(), BasisPoints(10_000));
    }

    #[test]
    fn funded_quote_is_accepted_within_its_bounds() {
        funded_amounts().accept(&bounds(500, 1_000_000)).unwrap();
    }

    /// The fixed relayer fee is 4.7% of a 5 dollar transfer.
    #[test]
    fn loss_above_the_bound_is_refused() {
        let error = funded_amounts().accept(&bounds(50, 1_000_000)).unwrap_err();

        assert_eq!(
            error,
            QuoteAcceptanceError::QuoteLossExceedsBound {
                expected: U256::from(4_763_755),
                bound: U256::from(4_975_000),
            }
        );
    }

    /// The floor is checked at the slippage the quote was requested at.
    #[test]
    fn relay_minimum_below_our_floor_is_refused() {
        let amounts = QuoteAmounts {
            slippage: bps(10),
            ..funded_amounts()
        };

        assert_eq!(
            amounts.accept(&bounds(500, 1_000_000)).unwrap_err(),
            QuoteAcceptanceError::QuoteFloorBelowBound {
                minimum: U256::from(4_749_464),
                floor: U256::from(4_758_991),
            }
        );
    }

    /// 30 bps off 4,763,755 is 4,749,463.735: the floor rounds down, so
    /// 4,749,463 is accepted and one unit less is refused.
    #[test]
    fn slippage_floor_rounds_down() {
        let at_floor = QuoteAmounts {
            minimum_out: U256::from(4_749_463),
            ..funded_amounts()
        };
        at_floor.accept(&bounds(500, 0)).unwrap();

        let below_floor = QuoteAmounts {
            minimum_out: U256::from(4_749_462),
            ..funded_amounts()
        };
        assert_eq!(
            below_floor.accept(&bounds(500, 0)).unwrap_err(),
            QuoteAcceptanceError::QuoteFloorBelowBound {
                minimum: U256::from(4_749_462),
                floor: U256::from(4_749_463),
            }
        );
    }

    /// 475 bps off 5,001,318 is 4,763,755.395: the loss bound rounds up past
    /// the expected output.
    #[test]
    fn loss_bound_rounds_up() {
        let amounts = QuoteAmounts {
            amount_in: U256::from(5_001_318),
            ..funded_amounts()
        };

        assert_eq!(
            amounts.accept(&bounds(475, 0)).unwrap_err(),
            QuoteAcceptanceError::QuoteLossExceedsBound {
                expected: U256::from(4_763_755),
                bound: U256::from(4_763_756),
            }
        );
    }

    #[test]
    fn minimum_below_the_downstream_minimum_is_refused() {
        let error = funded_amounts()
            .accept(&bounds(500, 5_000_000))
            .unwrap_err();

        assert_eq!(
            error,
            QuoteAcceptanceError::QuoteBelowDownstreamMinimum {
                minimum: U256::from(4_749_464),
                downstream_minimum: U256::from(5_000_000),
            }
        );
    }

    #[test]
    fn minimum_above_expected_is_refused() {
        let amounts = QuoteAmounts {
            minimum_out: U256::from(4_763_756),
            ..funded_amounts()
        };

        assert_eq!(
            amounts.accept(&bounds(500, 0)).unwrap_err(),
            QuoteAcceptanceError::MinimumAboveExpected {
                minimum: U256::from(4_763_756),
                expected: U256::from(4_763_755),
            }
        );
    }

    #[test]
    fn huge_amount_overflows_into_a_typed_error() {
        let amounts = QuoteAmounts {
            amount_in: U256::MAX,
            expected_out: U256::MAX,
            minimum_out: U256::MAX,
            slippage: bps(30),
        };

        assert_eq!(
            amounts.accept(&bounds(50, 0)).unwrap_err(),
            QuoteAcceptanceError::Overflow {
                amount: U256::MAX,
                bps: bps(50),
            }
        );
    }

    fn any_u256() -> impl Strategy<Value = U256> {
        prop_oneof![
            any::<u64>().prop_map(U256::from),
            any::<[u64; 4]>().prop_map(U256::from_limbs),
        ]
    }

    fn widened_product(amount: U256, bps: u16) -> U512 {
        U512::from(amount) * U512::from(10_000 - bps)
    }

    proptest! {
        #[test]
        fn bps_math_never_overflows(
            amount_in in any_u256(),
            expected_out in any_u256(),
            minimum_out in any_u256(),
            max_loss in 0..=10_000_u16,
            slippage in 0..=10_000_u16,
            downstream_minimum in any_u256(),
        ) {
            let amounts =
                QuoteAmounts { amount_in, expected_out, minimum_out, slippage: bps(slippage) };
            let bounds = QuoteBounds { max_loss: bps(max_loss), downstream_minimum };

            if let Err(QuoteAcceptanceError::Overflow { amount, bps }) = amounts.accept(&bounds) {
                let BasisPoints(bps) = bps;
                prop_assert!(widened_product(amount, bps) > U512::from(U256::MAX));
            }
        }

        #[test]
        fn accepted_quotes_have_minimum_out_at_most_expected_out(
            amount_in in any_u256(),
            expected_out in any_u256(),
            minimum_out in any_u256(),
            max_loss in 0..=10_000_u16,
            slippage in 0..=10_000_u16,
            downstream_minimum in any_u256(),
        ) {
            let amounts =
                QuoteAmounts { amount_in, expected_out, minimum_out, slippage: bps(slippage) };
            let bounds = QuoteBounds { max_loss: bps(max_loss), downstream_minimum };

            if amounts.accept(&bounds).is_ok() {
                prop_assert!(minimum_out <= expected_out);
                let scale = U512::from(10_000);
                prop_assert!(
                    U512::from(expected_out) >= widened_product(amount_in, max_loss).div_ceil(scale)
                );
                prop_assert!(
                    U512::from(minimum_out) >= widened_product(expected_out, slippage) / scale
                );
                prop_assert!(minimum_out >= downstream_minimum);
            }
        }

        #[test]
        fn quotes_within_bounds_are_accepted(
            amount_in in any::<u64>(),
            max_loss in 0..=10_000_u16,
            slippage in 0..=10_000_u16,
            loss_share in 0..=10_000_u16,
            slippage_share in 0..=10_000_u16,
        ) {
            let loss = u16::try_from(u32::from(max_loss) * u32::from(loss_share) / 10_000).unwrap();
            let slip =
                u16::try_from(u32::from(slippage) * u32::from(slippage_share) / 10_000).unwrap();
            let expected_out = bps(loss).keep(U256::from(amount_in), Rounding::Up).unwrap();
            let minimum_out = bps(slip).keep(expected_out, Rounding::Down).unwrap();

            let amounts = QuoteAmounts {
                amount_in: U256::from(amount_in),
                expected_out,
                minimum_out,
                slippage: bps(slippage),
            };
            let bounds = QuoteBounds { max_loss: bps(max_loss), downstream_minimum: minimum_out };

            prop_assert_eq!(amounts.accept(&bounds), Ok(()));
            prop_assert!(minimum_out <= expected_out);
        }
    }
}
