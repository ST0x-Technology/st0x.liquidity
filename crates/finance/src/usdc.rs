//! USDC dollar amount newtype with checked arithmetic.

use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use st0x_float_serde::{
    deserialize_float_from_number_or_string, format_float_with_fallback, serialize_float_as_string,
};

use crate::HasZero;

/// A USDC dollar amount.
#[derive(Debug, Clone, Copy)]
pub struct Usdc(Float);

impl Usdc {
    #[must_use]
    pub fn new(value: Float) -> Self {
        Self(value)
    }

    pub fn inner(self) -> Float {
        self.0
    }

    /// Creates a Usdc amount from cents (e.g., 12345 cents = $123.45).
    pub fn from_cents(cents: i64) -> Option<Self> {
        let cents_float = Float::parse(cents.to_string()).ok()?;
        let hundred = Float::parse("100".to_string()).ok()?;
        (cents_float / hundred).ok().map(Self)
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

impl HasZero for Usdc {
    const ZERO: Self = Self(float!(0));

    fn is_zero(&self) -> Result<bool, FloatError> {
        self.0.is_zero()
    }

    fn is_negative(&self) -> Result<bool, FloatError> {
        self.0.lt(Float::zero()?)
    }
}

impl From<Usdc> for Float {
    fn from(value: Usdc) -> Self {
        value.0
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
        write!(f, "{}", format_float_with_fallback(&self.0))
    }
}

/// Required by `cqrs_es::DomainEvent` since Usdc appears in event types.
impl PartialEq for Usdc {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}

impl Eq for Usdc {}

impl PartialOrd for Usdc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let lt = self.0.lt(other.0).ok()?;
        if lt {
            Some(std::cmp::Ordering::Less)
        } else if self.0.eq(other.0).ok()? {
            Some(std::cmp::Ordering::Equal)
        } else {
            Some(std::cmp::Ordering::Greater)
        }
    }
}

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

mod alloy_support {
    use alloy_primitives::U256;
    use rain_math_float::{Float, FloatError};

    use super::Usdc;

    #[derive(Debug, thiserror::Error)]
    pub enum UsdcConversionError {
        #[error("USDC amount cannot be negative: {0:?}")]
        NegativeValue(Float),
        #[error("Float operation failed: {0}")]
        Float(#[from] FloatError),
    }

    impl Usdc {
        /// Converts to U256 with 6 decimal places (USDC standard).
        ///
        /// # Errors
        ///
        /// Returns [`UsdcConversionError::NegativeValue`] if the USDC
        /// amount is negative, or [`UsdcConversionError::Float`] if
        /// the Float operation fails.
        pub fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
            if self
                .inner()
                .lt(Float::zero()?)
                .map_err(UsdcConversionError::Float)?
            {
                return Err(UsdcConversionError::NegativeValue(self.inner()));
            }

            self.inner()
                .to_fixed_decimal(6)
                .map_err(UsdcConversionError::Float)
        }
    }
}

pub use alloy_support::UsdcConversionError;
use st0x_float_macro::float;

impl std::ops::Mul<Float> for Usdc {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Float) -> Self::Output {
        (self.0 * rhs).map(Self)
    }
}

impl std::ops::Add for Usdc {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl std::ops::Sub for Usdc {
    type Output = Result<Self, FloatError>;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn add_succeeds() {
        let result = (Usdc::new(float!(1)) + Usdc::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn sub_succeeds() {
        let result = (Usdc::new(float!(5)) - Usdc::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn zero_constant() {
        assert!(Usdc::ZERO.is_zero().unwrap());
    }

    #[test]
    fn into_float_extracts_inner_value() {
        let float: Float = Usdc::new(float!(42)).into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn mul_float_succeeds() {
        let result = (Usdc::new(float!(100)) * float!(0.5)).unwrap();
        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn from_cents_converts_positive() {
        let usdc = Usdc::from_cents(12345).unwrap();
        assert!(usdc.inner().eq(float!(123.45)).unwrap());
    }

    #[test]
    fn from_cents_converts_negative() {
        let usdc = Usdc::from_cents(-500).unwrap();
        assert!(usdc.inner().eq(float!(-5)).unwrap());
    }

    #[test]
    fn from_cents_converts_zero() {
        let usdc = Usdc::from_cents(0).unwrap();
        assert!(usdc.is_zero().unwrap());
    }

    #[test]
    fn serde_roundtrip() {
        let usdc = Usdc::new(float!(42.5));
        let json = serde_json::to_string(&usdc).unwrap();
        let roundtripped: Usdc = serde_json::from_str(&json).unwrap();
        assert_eq!(usdc, roundtripped);
    }

    use alloy_primitives::U256;

    #[test]
    fn to_u256_whole_dollars_convert_correctly() {
        let usdc = Usdc::new(float!(100));
        assert_eq!(
            usdc.to_u256_6_decimals().unwrap(),
            U256::from(100_000_000u64)
        );
    }

    #[test]
    fn to_u256_negative_value_returns_error() {
        let usdc = Usdc::new(float!(-1));
        let error = usdc.to_u256_6_decimals().unwrap_err();
        assert!(matches!(error, UsdcConversionError::NegativeValue(_)));
    }

    #[test]
    fn to_u256_zero_converts_to_zero() {
        assert_eq!(Usdc::ZERO.to_u256_6_decimals().unwrap(), U256::ZERO);
    }
}
