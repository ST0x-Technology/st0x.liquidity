//! Execution threshold configuration for position management.
//!
//! Determines the minimum position imbalance (whole-share or
//! dollar-value based) required before placing an offsetting
//! broker order.

use alloy::primitives::U256;
use rain_math_float::FloatError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::{Add, Mul, Sub};
use std::str::FromStr;

use st0x_exact_decimal::ExactDecimal;
use st0x_execution::{FractionalShares, HasZero, Positive};

/// A USDC dollar amount used for threshold configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct Usdc(pub(crate) ExactDecimal);

impl FromStr for Usdc {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ExactDecimal::parse(value).map(Self)
    }
}

impl Display for Usdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl HasZero for Usdc {
    const ZERO: Self = Self(ExactDecimal::zero());
}

impl Usdc {
    /// Creates a Usdc amount from cents (e.g., 12345 cents = $123.45).
    pub(crate) fn from_cents(cents: i64) -> Option<Self> {
        let cents_ed = ExactDecimal::parse(&cents.to_string()).ok()?;
        let hundred = ExactDecimal::parse("100").ok()?;

        (cents_ed / hundred).ok().map(Self)
    }

    /// Converts to U256 with 6 decimal places (USDC standard).
    ///
    /// Returns an error for negative values or overflow during scaling.
    pub fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
        if self.0.is_negative().map_err(UsdcConversionError::Float)? {
            return Err(UsdcConversionError::NegativeValue(self.0));
        }

        self.0
            .to_fixed_decimal_lossy(6)
            .map(|(fixed, _lossless)| fixed)
            .map_err(UsdcConversionError::Float)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UsdcConversionError {
    #[error("USDC amount cannot be negative: {0}")]
    NegativeValue(ExactDecimal),
    #[error("Float operation failed: {0}")]
    Float(FloatError),
}

impl From<Usdc> for ExactDecimal {
    fn from(value: Usdc) -> Self {
        value.0
    }
}

impl Mul<ExactDecimal> for Usdc {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: ExactDecimal) -> Self::Output {
        (self.0 * rhs).map(Self)
    }
}

impl Add for Usdc {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl Sub for Usdc {
    type Output = Result<Self, FloatError>;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).map(Self)
    }
}

/// Threshold configuration that determines when to trigger offchain execution.
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
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeDollarValue(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroDollarValue);
        }

        Ok(Self::DollarValue(value))
    }

    #[cfg(test)]
    pub(crate) fn whole_share() -> Self {
        Self::Shares(
            Positive::new(FractionalShares::new(ExactDecimal::parse("1").unwrap())).unwrap(),
        )
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidThresholdError {
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Dollar threshold cannot be zero")]
    ZeroDollarValue,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    fn arb_exact_decimal() -> impl Strategy<Value = ExactDecimal> {
        (any::<i64>(), 0u32..=10).prop_filter_map(
            "ExactDecimal::parse must succeed",
            |(mantissa, scale)| {
                let divisor = 10i64.checked_pow(scale).unwrap_or(1);
                let integer_part = mantissa / divisor;
                let frac_part = (mantissa % divisor).unsigned_abs();

                let value_str = format!(
                    "{integer_part}.{frac_part:0>width$}",
                    width = scale as usize
                );
                ExactDecimal::parse(&value_str).ok()
            },
        )
    }

    proptest! {
        #[test]
        fn usdc_is_zero_matches_exact_decimal_is_zero(decimal in arb_exact_decimal()) {
            let usdc = Usdc(decimal);
            let is_zero = decimal.is_zero().map_err(|error| {
                TestCaseError::Fail(format!("is_zero() failed: {error}").into())
            })?;

            prop_assert_eq!(usdc.is_zero(), is_zero);
        }

        #[test]
        fn usdc_is_negative_matches_exact_decimal_is_negative(decimal in arb_exact_decimal()) {
            let usdc = Usdc(decimal);
            let is_negative = decimal.is_negative().map_err(|error| {
                TestCaseError::Fail(format!("is_negative() failed: {error}").into())
            })?;

            prop_assert_eq!(usdc.is_negative(), is_negative);
        }
    }

    #[test]
    fn whole_share_matches_smart_constructor() {
        let from_whole_share = ExecutionThreshold::whole_share();
        let from_constructor =
            ExecutionThreshold::Shares(Positive::new(FractionalShares::new(ed("1"))).unwrap());
        assert_eq!(from_whole_share, from_constructor);
    }

    #[test]
    fn shares_threshold_accepts_positive() {
        let threshold =
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(ed("1"))).unwrap());
        assert!(matches!(threshold, ExecutionThreshold::Shares(_)));
    }

    #[test]
    fn dollar_threshold_rejects_zero() {
        let result = ExecutionThreshold::dollar_value(Usdc(ExactDecimal::zero()));
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroDollarValue);
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc(ed("-1"));
        let result = ExecutionThreshold::dollar_value(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(negative)
        );
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        ExecutionThreshold::dollar_value(Usdc(ed("1"))).unwrap();
    }

    #[test]
    fn usdc_add_succeeds() {
        let smaller = Usdc(ed("1"));
        let larger = Usdc(ed("2"));

        let result = (smaller + larger).unwrap();

        assert_eq!(result.0, ed("3"));
    }

    #[test]
    fn usdc_sub_succeeds() {
        let larger = Usdc(ed("5"));
        let smaller = Usdc(ed("2"));

        let result = (larger - smaller).unwrap();

        assert_eq!(result.0, ed("3"));
    }

    #[test]
    fn usdc_zero_constant() {
        assert!(Usdc::ZERO.is_zero());
    }

    #[test]
    fn usdc_into_exact_decimal_extracts_inner_value() {
        let usdc = Usdc(ed("42"));
        let exact: ExactDecimal = usdc.into();
        assert_eq!(exact, ed("42"));
    }

    #[test]
    fn usdc_mul_exact_decimal_succeeds() {
        let usdc = Usdc(ed("100"));
        let ratio = ed("0.5");

        let result = (usdc * ratio).unwrap();

        assert_eq!(result.0, ed("50"));
    }

    #[test]
    fn from_cents_converts_positive_cents_to_dollars() {
        let usdc = Usdc::from_cents(12345).unwrap();
        assert_eq!(usdc.0, ed("123.45"));
    }

    #[test]
    fn from_cents_converts_negative_cents_to_dollars() {
        let usdc = Usdc::from_cents(-500).unwrap();
        assert_eq!(usdc.0, ed("-5"));
    }

    #[test]
    fn from_cents_converts_zero() {
        let usdc = Usdc::from_cents(0).unwrap();
        assert!(usdc.is_zero());
    }

    #[test]
    fn from_cents_handles_large_values() {
        let usdc = Usdc::from_cents(i64::MAX).unwrap();
        let expected = (ed(&i64::MAX.to_string()) / ed("100")).unwrap();
        assert_eq!(usdc.0, expected);
    }
}
