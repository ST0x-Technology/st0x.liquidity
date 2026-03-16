//! Fractional share quantity newtype with checked arithmetic.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::{ArithmeticError, ArithmeticOperation, HasZero};

/// Fractional share quantity newtype wrapper.
///
/// Represents share quantities that can include fractional amounts (e.g., 1.212 shares).
/// Can be negative (for position tracking). Use `Positive<FractionalShares>` when
/// strictly positive values are required (e.g., order quantities).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct FractionalShares(Decimal);

impl HasZero for FractionalShares {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<FractionalShares> for Decimal {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl FractionalShares {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn inner(self) -> Decimal {
        self.0
    }

    #[must_use]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    /// Returns true if this represents a whole number of shares (no fractional part).
    #[must_use]
    pub fn is_whole(self) -> bool {
        self.0.fract().is_zero()
    }
}

impl std::ops::Add for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0.checked_add(rhs.0).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Add,
            lhs: self,
            rhs,
        })
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0.checked_sub(rhs.0).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Sub,
            lhs: self,
            rhs,
        })
    }
}

impl std::ops::Mul<Decimal> for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0.checked_mul(rhs).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Mul,
            lhs: self,
            rhs: Self(rhs),
        })
    }
}

impl FromStr for FractionalShares {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

impl Display for FractionalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for FractionalShares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <Decimal as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self::new(value))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn add_succeeds() {
        let a = FractionalShares::new(Decimal::ONE);
        let b = FractionalShares::new(Decimal::TWO);
        let result = (a + b).unwrap();
        assert_eq!(result.inner(), Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let a = FractionalShares::new(Decimal::from(5));
        let b = FractionalShares::new(Decimal::TWO);
        let result = (a - b).unwrap();
        assert_eq!(result.inner(), Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = FractionalShares::new(Decimal::MAX);
        let one = FractionalShares::new(Decimal::ONE);
        let err = (max + one).unwrap_err();
        assert_eq!(err.operation, ArithmeticOperation::Add);
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = FractionalShares::new(Decimal::MIN);
        let one = FractionalShares::new(Decimal::ONE);
        let err = (min - one).unwrap_err();
        assert_eq!(err.operation, ArithmeticOperation::Sub);
        assert_eq!(err.lhs, min);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn abs_returns_absolute_value() {
        let negative = FractionalShares::new(Decimal::NEGATIVE_ONE);
        assert_eq!(negative.abs().inner(), Decimal::ONE);
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let shares = FractionalShares::new(Decimal::from(42));
        let decimal: Decimal = shares.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let shares = FractionalShares::new(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5
        let result = (shares * ratio).unwrap();
        assert_eq!(result.inner(), Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = FractionalShares::new(Decimal::MAX);
        let two = Decimal::TWO;
        let err = (max * two).unwrap_err();
        assert_eq!(err.operation, ArithmeticOperation::Mul);
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, FractionalShares::new(two));
    }

    #[test]
    fn is_whole_returns_true_for_whole_numbers() {
        assert!(FractionalShares::new(Decimal::from(1)).is_whole());
        assert!(FractionalShares::new(dec!(42.0)).is_whole());
    }

    #[test]
    fn is_whole_returns_false_for_fractional_values() {
        assert!(!FractionalShares::new(dec!(1.5)).is_whole());
        assert!(!FractionalShares::new(dec!(0.001)).is_whole());
    }

    proptest! {
        #[test]
        fn construction_preserves_value(
            mantissa in i64::MIN..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);
            prop_assert_eq!(shares.inner(), decimal);
        }

        #[test]
        fn is_whole_matches_fract_is_zero(
            mantissa in i64::MIN..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);
            prop_assert_eq!(shares.is_whole(), decimal.fract().is_zero());
        }

        #[test]
        fn serde_roundtrip(
            mantissa in i64::MIN..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);
            let json = serde_json::to_string(&shares).unwrap();
            let roundtripped: FractionalShares = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(shares, roundtripped);
        }
    }
}
