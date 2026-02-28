//! Execution threshold configuration for position management.
//!
//! Determines the minimum position imbalance (whole-share or
//! dollar-value based) required before placing an offsetting
//! broker order.

use alloy::primitives::{B256, U256};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::{Add, Mul, Sub};
use std::str::FromStr;

use st0x_execution::{FractionalShares, HasZero, Positive};

use crate::float_serde::{
    deserialize_float_from_number_or_string, format_float, serialize_float_as_string,
};

/// A USDC dollar amount used for threshold configuration.
#[derive(Debug, Clone, Copy)]
pub struct Usdc(pub(crate) Float);

/// Required by `cqrs_es::DomainEvent` since Usdc appears in event types.
impl PartialEq for Usdc {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}

impl Eq for Usdc {}

impl Serialize for Usdc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_float_as_string(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Usdc {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_float_from_number_or_string(deserializer).map(Self)
    }
}

impl FromStr for Usdc {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Float::parse(value.to_string()).map(Self)
    }
}

impl Display for Usdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_float(&self.0))
    }
}

impl HasZero for Usdc {
    const ZERO: Self = Self(Float::from_raw(B256::ZERO));

    fn is_zero(&self) -> Result<bool, FloatError> {
        self.0.is_zero()
    }

    fn is_negative(&self) -> Result<bool, FloatError> {
        self.0.lt(Float::zero()?)
    }
}

impl Usdc {
    /// Creates a Usdc amount from cents (e.g., 12345 cents = $123.45).
    pub(crate) fn from_cents(cents: i64) -> Option<Self> {
        let cents_float = Float::parse(cents.to_string()).ok()?;
        let hundred = Float::parse("100".to_string()).ok()?;

        (cents_float / hundred).ok().map(Self)
    }

    /// Converts to U256 with 6 decimal places (USDC standard).
    ///
    /// Returns an error for negative values or overflow during scaling.
    pub(crate) fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
        if self
            .0
            .lt(Float::zero()?)
            .map_err(UsdcConversionError::Float)?
        {
            return Err(UsdcConversionError::NegativeValue(self.0));
        }

        self.0
            .to_fixed_decimal(6)
            .map_err(UsdcConversionError::Float)
    }

    /// Fallible equality comparison.
    pub fn eq(&self, other: &Self) -> Result<bool, FloatError> {
        self.0.eq(other.0)
    }

    /// Fallible less-than comparison.
    pub fn lt(&self, other: &Self) -> Result<bool, FloatError> {
        self.0.lt(other.0)
    }

    /// Fallible greater-than comparison.
    pub fn gt(&self, other: &Self) -> Result<bool, FloatError> {
        self.0.gt(other.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UsdcConversionError {
    #[error("USDC amount cannot be negative: {0:?}")]
    NegativeValue(Float),
    #[error("Float operation failed: {0}")]
    Float(#[from] FloatError),
    #[error("failed to parse U256: {0}")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

impl From<Usdc> for Float {
    fn from(value: Usdc) -> Self {
        value.0
    }
}

impl Mul<Float> for Usdc {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Float) -> Self::Output {
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
        Self::Shares(
            Positive::new(FractionalShares::new(
                Float::parse("1".to_string()).unwrap(),
            ))
            .unwrap(),
        )
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

    use super::*;

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
            let usdc = Usdc(decimal);
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
            let usdc = Usdc(decimal);
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
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!("1"))).unwrap());
        assert!(matches!(threshold, ExecutionThreshold::Shares(_)));
    }

    #[test]
    fn dollar_threshold_rejects_zero() {
        let result = ExecutionThreshold::dollar_value(Usdc(Float::zero().unwrap()));
        assert!(matches!(
            result.unwrap_err(),
            InvalidThresholdError::ZeroDollarValue
        ));
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc(float!("-1"));
        let result = ExecutionThreshold::dollar_value(negative);
        assert!(matches!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(_)
        ));
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        ExecutionThreshold::dollar_value(Usdc(float!("1"))).unwrap();
    }

    #[test]
    fn usdc_add_succeeds() {
        let smaller = Usdc(float!("1"));
        let larger = Usdc(float!("2"));

        let result = (smaller + larger).unwrap();

        assert!(result.0.eq(float!("3")).unwrap());
    }

    #[test]
    fn usdc_sub_succeeds() {
        let larger = Usdc(float!("5"));
        let smaller = Usdc(float!("2"));

        let result = (larger - smaller).unwrap();

        assert!(result.0.eq(float!("3")).unwrap());
    }

    #[test]
    fn usdc_zero_constant() {
        assert!(Usdc::ZERO.is_zero().unwrap());
    }

    #[test]
    fn usdc_into_float_extracts_inner_value() {
        let usdc = Usdc(float!("42"));
        let float: Float = usdc.into();
        assert!(float.eq(float!("42")).unwrap());
    }

    #[test]
    fn usdc_mul_float_succeeds() {
        let usdc = Usdc(float!("100"));
        let ratio = float!("0.5");

        let result = (usdc * ratio).unwrap();

        assert!(result.0.eq(float!("50")).unwrap());
    }

    #[test]
    fn from_cents_converts_positive_cents_to_dollars() {
        let usdc = Usdc::from_cents(12345).unwrap();
        assert!(usdc.0.eq(float!("123.45")).unwrap());
    }

    #[test]
    fn from_cents_converts_negative_cents_to_dollars() {
        let usdc = Usdc::from_cents(-500).unwrap();
        assert!(usdc.0.eq(float!("-5")).unwrap());
    }

    #[test]
    fn from_cents_converts_zero() {
        let usdc = Usdc::from_cents(0).unwrap();
        assert!(usdc.is_zero().unwrap());
    }

    #[test]
    fn from_cents_handles_large_values() {
        let usdc = Usdc::from_cents(i64::MAX).unwrap();
        let expected = (float!(i64::MAX) / float!("100")).unwrap();
        assert!(usdc.0.eq(expected).unwrap());
    }
}
