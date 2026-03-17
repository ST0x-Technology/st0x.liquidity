//! Execution threshold configuration for position management.
//!
//! Determines the minimum position imbalance (whole-share or
//! dollar-value based) required before placing an offsetting
//! broker order.

use rain_math_float::FloatError;
use serde::{Deserialize, Serialize};

use st0x_execution::{FractionalShares, Positive};
use st0x_finance::{HasZero, Usdc};

/// Threshold configuration that determines when to trigger offchain execution.
///
/// `PartialEq`/`Eq` required by `cqrs_es::DomainEvent` since this appears
/// in `PositionEvent`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionThreshold {
    Shares(Positive<FractionalShares>),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    pub(crate) fn shares(value: Positive<FractionalShares>) -> Self {
        Self::Shares(value)
    }

    pub(crate) fn dollar_value(value: Usdc) -> Result<Self, InvalidThresholdError> {
        if value.is_negative().map_err(InvalidThresholdError::Float)? {
            return Err(InvalidThresholdError::NegativeDollarValue(value));
        }

        if value.is_zero().map_err(InvalidThresholdError::Float)? {
            return Err(InvalidThresholdError::ZeroDollarValue);
        }

        Ok(Self::DollarValue(value))
    }

    #[cfg(test)]
    pub(crate) fn whole_share() -> Self {
        Self::Shares(Positive::new(FractionalShares::new(st0x_float_macro::float!(1))).unwrap())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidThresholdError {
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Dollar threshold cannot be zero")]
    ZeroDollarValue,
    #[error("Float operation failed: {0}")]
    Float(FloatError),
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rain_math_float::Float;

    use super::*;
    use st0x_float_macro::float;

    fn arb_float() -> impl Strategy<Value = Float> {
        (any::<i64>(), 0u32..=10).prop_filter_map(
            "Float::parse must succeed",
            |(mantissa, scale)| {
                let divisor = 10i64.checked_pow(scale).unwrap_or(1);
                let integer_part = mantissa / divisor;
                let frac_part = (mantissa % divisor).unsigned_abs();

                let value_str = format!(
                    "{integer_part}.{frac_part:0>width$}",
                    width = scale as usize
                );
                Float::parse(value_str).ok()
            },
        )
    }

    proptest! {
        #[test]
        fn usdc_is_zero_matches_float_is_zero(decimal in arb_float()) {
            let usdc = Usdc::new(decimal);
            let is_zero = decimal.is_zero().map_err(|error| {
                TestCaseError::Fail(format!("is_zero() failed: {error}").into())
            })?;

            let usdc_is_zero = usdc.is_zero().map_err(|error| {
                TestCaseError::Fail(format!("usdc.is_zero() failed: {error}").into())
            })?;

            prop_assert_eq!(usdc_is_zero, is_zero);
        }

        #[test]
        fn usdc_is_negative_matches_float_is_negative(decimal in arb_float()) {
            let usdc = Usdc::new(decimal);
            let zero = Float::zero().map_err(|error| {
                TestCaseError::Fail(format!("Float::zero() failed: {error}").into())
            })?;
            let is_negative = decimal.lt(zero).map_err(|error| {
                TestCaseError::Fail(format!("lt() failed: {error}").into())
            })?;

            let usdc_is_negative = usdc.is_negative().map_err(|error| {
                TestCaseError::Fail(format!("usdc.is_negative() failed: {error}").into())
            })?;

            prop_assert_eq!(usdc_is_negative, is_negative);
        }
    }

    #[test]
    fn shares_threshold_accepts_positive() {
        let threshold =
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(1))).unwrap());
        assert!(matches!(threshold, ExecutionThreshold::Shares(_)));
    }

    #[test]
    fn dollar_threshold_rejects_zero() {
        let result = ExecutionThreshold::dollar_value(Usdc::new(Float::zero().unwrap()));
        assert!(matches!(
            result.unwrap_err(),
            InvalidThresholdError::ZeroDollarValue
        ));
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc::new(float!(-1));
        let result = ExecutionThreshold::dollar_value(negative);
        assert!(matches!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(_)
        ));
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        ExecutionThreshold::dollar_value(Usdc::new(float!(1))).unwrap();
    }

    #[test]
    fn usdc_add_succeeds() {
        let smaller = Usdc::new(float!(1));
        let larger = Usdc::new(float!(2));

        let result = (smaller + larger).unwrap();

        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn usdc_sub_succeeds() {
        let larger = Usdc::new(float!(5));
        let smaller = Usdc::new(float!(2));

        let result = (larger - smaller).unwrap();

        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn usdc_zero_constant() {
        assert!(Usdc::ZERO.is_zero().unwrap());
    }

    #[test]
    fn usdc_into_float_extracts_inner_value() {
        let usdc = Usdc::new(float!(42));
        let float: Float = usdc.into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn usdc_mul_float_succeeds() {
        let usdc = Usdc::new(float!(100));
        let ratio = float!(0.5);

        let result = (usdc * ratio).unwrap();

        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn from_cents_converts_positive_cents_to_dollars() {
        let usdc = Usdc::from_cents(12345).unwrap();
        assert!(usdc.inner().eq(float!(123.45)).unwrap());
    }

    #[test]
    fn from_cents_converts_negative_cents_to_dollars() {
        let usdc = Usdc::from_cents(-500).unwrap();
        assert!(usdc.inner().eq(float!(-5)).unwrap());
    }

    #[test]
    fn from_cents_converts_zero() {
        let usdc = Usdc::from_cents(0).unwrap();
        assert!(usdc.is_zero().unwrap());
    }

    #[test]
    fn from_cents_handles_large_values() {
        let usdc = Usdc::from_cents(i64::MAX).unwrap();
        let expected = (float!(i64::MAX) / float!(100)).unwrap();
        assert!(usdc.inner().eq(expected).unwrap());
    }
}
