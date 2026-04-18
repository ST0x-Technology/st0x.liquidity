//! Fractional share quantity newtype with checked arithmetic.

use rain_math_float::{Float, FloatError};
use serde::Serialize;
use st0x_float_macro::float;
use st0x_float_serde::{
    deserialize_float_from_number_or_string, format_float_with_fallback, serialize_float_as_string,
};
use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;

use crate::HasZero;

/// Fractional share quantity newtype wrapper.
///
/// Represents share quantities that can include fractional amounts (e.g., 1.212 shares).
/// Can be negative (for position tracking). Use `Positive<FractionalShares>` when
/// strictly positive values are required (e.g., order quantities).
#[derive(Clone, Copy)]
pub struct FractionalShares(Float);

impl std::fmt::Debug for FractionalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FractionalShares({})",
            format_float_with_fallback(&self.0)
        )
    }
}

impl HasZero for FractionalShares {
    const ZERO: Self = Self(float!(0));

    fn is_zero(&self) -> Result<bool, FloatError> {
        self.0.is_zero()
    }

    fn is_negative(&self) -> Result<bool, FloatError> {
        self.0.lt(Self::ZERO.0)
    }
}

impl From<FractionalShares> for Float {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl FractionalShares {
    pub const ZERO: Self = Self(float!(0));

    pub fn new(value: Float) -> Self {
        Self(value)
    }

    pub fn inner(self) -> Float {
        self.0
    }

    pub fn abs(self) -> Result<Self, FloatError> {
        self.0.abs().map(Self)
    }

    /// Returns true if this represents a whole number of shares (no fractional part).
    pub fn is_whole(self) -> Result<bool, FloatError> {
        let frac = self.0.frac()?;
        frac.is_zero()
    }
}

impl PartialEq for FractionalShares {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}

impl Eq for FractionalShares {}

impl PartialOrd for FractionalShares {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let lt = self.0.lt(other.0).ok()?;
        if lt {
            Some(Ordering::Less)
        } else if self.0.eq(other.0).ok()? {
            Some(Ordering::Equal)
        } else {
            Some(Ordering::Greater)
        }
    }
}

impl Serialize for FractionalShares {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_float_as_string(&self.0, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FractionalShares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_float_from_number_or_string(deserializer).map(Self)
    }
}

impl std::ops::Add for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).map(Self)
    }
}

impl std::ops::Mul<Float> for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Float) -> Self::Output {
        (self.0 * rhs).map(Self)
    }
}

impl FromStr for FractionalShares {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Float::parse(value.to_string()).map(Self)
    }
}

impl Display for FractionalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_float_with_fallback(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn add_succeeds() {
        let result = (FractionalShares::new(float!(1)) + FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn sub_succeeds() {
        let result = (FractionalShares::new(float!(5)) - FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn abs_returns_absolute_value() {
        let result = FractionalShares::new(float!(-1)).abs().unwrap();
        assert!(result.inner().eq(float!(1)).unwrap());
    }

    #[test]
    fn into_float_extracts_inner_value() {
        let float: Float = FractionalShares::new(float!(42)).into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn mul_float_succeeds() {
        let result = (FractionalShares::new(float!(100)) * float!(0.5)).unwrap();
        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn is_whole_returns_true_for_whole_numbers() {
        assert!(FractionalShares::new(float!(1)).is_whole().unwrap());
        assert!(FractionalShares::new(float!(42)).is_whole().unwrap());
    }

    #[test]
    fn is_whole_returns_false_for_fractional_values() {
        assert!(!FractionalShares::new(float!(1.5)).is_whole().unwrap());
        assert!(!FractionalShares::new(float!(0.001)).is_whole().unwrap());
    }

    #[test]
    fn zero_constant_is_zero() {
        assert!(FractionalShares::ZERO.is_zero().unwrap());
    }

    #[test]
    fn serde_roundtrip() {
        let shares = FractionalShares::new(float!(42.5));
        let json = serde_json::to_string(&shares).unwrap();
        let roundtripped: FractionalShares = serde_json::from_str(&json).unwrap();
        assert_eq!(shares, roundtripped);
    }

    #[test]
    fn debug_formats_as_decimal() {
        let shares = FractionalShares::new(float!(42.5));
        let output = format!("{shares:?}");
        assert_eq!(output, "FractionalShares(42.5)");
    }

    #[test]
    fn display_formats_as_decimal() {
        let shares = FractionalShares::new(float!(42.5));
        let output = format!("{shares}");
        assert_eq!(output, "42.5");
    }
}
