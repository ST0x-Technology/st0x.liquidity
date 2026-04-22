//! USD dollar amount newtype with checked arithmetic.
//!
//! Unlike `Usdc` (which represents onchain USDC amounts and supports
//! alloy/U256 conversions), `Usd` represents offchain US dollar amounts
//! (e.g., brokerage cash balances) and has no onchain representation.

use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;
use st0x_float_serde::{
    deserialize_float_from_number_or_string, format_float_with_fallback, serialize_float_as_string,
};
use std::fmt::Display;
use std::str::FromStr;

use crate::HasZero;

/// An offchain US dollar amount.
#[derive(Clone, Copy)]
pub struct Usd(Float);

impl std::fmt::Debug for Usd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Usd({})", format_float_with_fallback(&self.0))
    }
}

impl Usd {
    #[must_use]
    pub fn new(value: Float) -> Self {
        Self(value)
    }

    pub fn inner(self) -> Float {
        self.0
    }

    /// Creates a Usd amount from cents (e.g., 12345 cents = $123.45).
    pub fn from_cents(cents: i64) -> Option<Self> {
        let cents_float = Float::parse(cents.to_string()).ok()?;
        let hundred = Float::parse("100".to_string()).ok()?;
        (cents_float / hundred).ok().map(Self)
    }

    /// Converts to cents exactly.
    /// Returns `None` if the value overflows `i64` or has sub-cent precision.
    pub fn to_cents(self) -> Option<i64> {
        let hundred = Float::parse("100".to_string()).ok()?;
        let scaled = (self.0 * hundred).ok()?;
        let frac = scaled.frac().ok()?;
        let frac_is_zero = frac.is_zero().ok()?;

        if !frac_is_zero {
            return None;
        }

        let formatted = scaled.format().ok()?;
        let integer_str = formatted.split('.').next().unwrap_or(&formatted);
        integer_str.parse::<i64>().ok()
    }

    /// Fallible equality comparison.
    pub fn eq(&self, other: &Self) -> Result<bool, FloatError> {
        self.0.eq(other.0)
    }

    /// Fallible less-than comparison.
    pub fn lt(&self, other: &Self) -> Result<bool, FloatError> {
        self.0.lt(other.0)
    }
}

impl HasZero for Usd {
    const ZERO: Self = Self(float!(0));

    fn is_zero(&self) -> Result<bool, FloatError> {
        self.0.is_zero()
    }

    fn is_negative(&self) -> Result<bool, FloatError> {
        self.0.lt(Float::zero()?)
    }
}

impl From<Usd> for Float {
    fn from(value: Usd) -> Self {
        value.0
    }
}

impl FromStr for Usd {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Float::parse(value.to_string()).map(Self)
    }
}

impl Display for Usd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_float_with_fallback(&self.0))
    }
}

/// Required by `cqrs_es::DomainEvent` since Usd appears in event types.
impl PartialEq for Usd {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}

impl Eq for Usd {}

impl PartialOrd for Usd {
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

impl Serialize for Usd {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_float_as_string(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Usd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_float_from_number_or_string(deserializer).map(Self)
    }
}

impl std::ops::Mul<Float> for Usd {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Float) -> Self::Output {
        (self.0 * rhs).map(Self)
    }
}

impl std::ops::Add for Usd {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl std::ops::Sub for Usd {
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
        let result = (Usd::new(float!(1)) + Usd::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn sub_succeeds() {
        let result = (Usd::new(float!(5)) - Usd::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn zero_constant() {
        assert!(Usd::ZERO.is_zero().unwrap());
    }

    #[test]
    fn into_float_extracts_inner_value() {
        let float: Float = Usd::new(float!(42)).into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn mul_float_succeeds() {
        let result = (Usd::new(float!(100)) * float!(0.5)).unwrap();
        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn from_cents_converts_positive() {
        let usd = Usd::from_cents(12345).unwrap();
        assert!(usd.inner().eq(float!(123.45)).unwrap());
    }

    #[test]
    fn from_cents_converts_negative() {
        let usd = Usd::from_cents(-500).unwrap();
        assert!(usd.inner().eq(float!(-5)).unwrap());
    }

    #[test]
    fn from_cents_converts_zero() {
        let usd = Usd::from_cents(0).unwrap();
        assert!(usd.is_zero().unwrap());
    }

    #[test]
    fn to_cents_converts_whole_dollars() {
        let usd = Usd::new(float!(500));
        assert_eq!(usd.to_cents().unwrap(), 50_000);
    }

    #[test]
    fn to_cents_converts_dollars_and_cents() {
        let usd = Usd::new(float!(123.45));
        assert_eq!(usd.to_cents().unwrap(), 12_345);
    }

    #[test]
    fn to_cents_rejects_sub_cent_precision() {
        let usd = Usd::new(float!(99.999));
        assert_eq!(usd.to_cents(), None);
    }

    #[test]
    fn to_cents_converts_zero() {
        assert_eq!(Usd::ZERO.to_cents().unwrap(), 0);
    }

    #[test]
    fn serde_roundtrip() {
        let usd = Usd::new(float!(42.5));
        let json = serde_json::to_string(&usd).unwrap();
        let roundtripped: Usd = serde_json::from_str(&json).unwrap();
        assert_eq!(usd, roundtripped);
    }

    #[test]
    fn debug_formats_as_decimal() {
        let usd = Usd::new(float!(123.45));
        let output = format!("{usd:?}");
        assert_eq!(output, "Usd(123.45)");
    }

    #[test]
    fn display_formats_as_decimal() {
        let usd = Usd::new(float!(123.45));
        let output = format!("{usd}");
        assert_eq!(output, "123.45");
    }
}
