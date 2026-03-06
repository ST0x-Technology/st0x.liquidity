//! USDC dollar amount newtype with checked arithmetic.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::{ArithmeticError, HasZero};

/// A USDC dollar amount.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Usdc(Decimal);

impl Usdc {
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub fn inner(self) -> Decimal {
        self.0
    }

    /// Creates a Usdc amount from cents (e.g., 12345 cents = $123.45).
    pub fn from_cents(cents: i64) -> Option<Self> {
        Decimal::from(cents).checked_div(dec!(100)).map(Self)
    }

    pub fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    pub fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }
}

impl HasZero for Usdc {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<Usdc> for Decimal {
    fn from(value: Usdc) -> Self {
        value.0
    }
}

impl FromStr for Usdc {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

impl Display for Usdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Mul<Decimal> for Usdc {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0
            .checked_mul(rhs)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "*".to_string(),
                lhs: self,
                rhs: Self(rhs),
            })
    }
}

impl std::ops::Add for Usdc {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "+".to_string(),
                lhs: self,
                rhs,
            })
    }
}

impl std::ops::Sub for Usdc {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "-".to_string(),
                lhs: self,
                rhs,
            })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    use super::*;

    fn arb_decimal() -> impl Strategy<Value = Decimal> {
        (any::<i64>(), 0u32..=10).prop_map(|(mantissa, scale)| Decimal::new(mantissa, scale))
    }

    proptest! {
        #[test]
        fn is_zero_matches_decimal(decimal in arb_decimal()) {
            let usdc = Usdc(decimal);
            prop_assert_eq!(usdc.is_zero(), decimal.is_zero());
        }

        #[test]
        fn is_negative_matches_decimal(decimal in arb_decimal()) {
            let usdc = Usdc(decimal);
            prop_assert_eq!(usdc.is_negative(), decimal.is_sign_negative());
        }

        #[test]
        fn serde_roundtrip(decimal in arb_decimal()) {
            let usdc = Usdc(decimal);
            let json = serde_json::to_string(&usdc).unwrap();
            let roundtripped: Usdc = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(usdc, roundtripped);
        }
    }

    #[test]
    fn add_succeeds() {
        let result = (Usdc(Decimal::ONE) + Usdc(Decimal::TWO)).unwrap();
        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let result = (Usdc(Decimal::from(5)) - Usdc(Decimal::TWO)).unwrap();
        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = Usdc(Decimal::MAX);
        let one = Usdc(Decimal::ONE);
        let error = (max + one).unwrap_err();
        assert_eq!(error.operation, "+");
        assert_eq!(error.lhs, max);
        assert_eq!(error.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = Usdc(Decimal::MIN);
        let one = Usdc(Decimal::ONE);
        let error = (min - one).unwrap_err();
        assert_eq!(error.operation, "-");
        assert_eq!(error.lhs, min);
        assert_eq!(error.rhs, one);
    }

    #[test]
    fn zero_constant() {
        assert!(Usdc::ZERO.is_zero());
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let usdc = Usdc(Decimal::from(42));
        let decimal: Decimal = usdc.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let usdc = Usdc(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5
        let result = (usdc * ratio).unwrap();
        assert_eq!(result.0, Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = Usdc(Decimal::MAX);
        let two = Decimal::TWO;
        let error = (max * two).unwrap_err();
        assert_eq!(error.operation, "*");
        assert_eq!(error.lhs, max);
        assert_eq!(error.rhs, Usdc(two));
    }

    #[test]
    fn from_cents_converts_positive() {
        let usdc = Usdc::from_cents(12345).unwrap();
        assert_eq!(usdc.0, Decimal::new(12345, 2));
    }

    #[test]
    fn from_cents_converts_negative() {
        let usdc = Usdc::from_cents(-500).unwrap();
        assert_eq!(usdc.0, Decimal::new(-5, 0));
    }

    #[test]
    fn from_cents_converts_zero() {
        let usdc = Usdc::from_cents(0).unwrap();
        assert!(usdc.is_zero());
    }

    #[test]
    fn from_cents_handles_large_values() {
        let usdc = Usdc::from_cents(i64::MAX).unwrap();
        assert_eq!(usdc.0, Decimal::from(i64::MAX) / Decimal::from(100));
    }
}
