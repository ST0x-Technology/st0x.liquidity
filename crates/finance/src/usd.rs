//! USD dollar amount newtype with checked arithmetic.
//!
//! Unlike `Usdc` (which represents onchain USDC amounts and supports
//! alloy/U256 conversions), `Usd` represents offchain US dollar amounts
//! (e.g., brokerage cash balances) and has no onchain representation.

use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::{ArithmeticError, ArithmeticOperation, HasZero};

/// An offchain US dollar amount.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Usd(Decimal);

impl Usd {
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn inner(self) -> Decimal {
        self.0
    }

    /// Creates a Usd amount from cents (e.g., 12345 cents = $123.45).
    #[must_use]
    pub fn from_cents(cents: i64) -> Option<Self> {
        Decimal::from(cents).checked_div(dec!(100)).map(Self)
    }

    /// Converts to cents exactly.
    /// Returns `None` if the value has sub-cent precision or overflows `i64`.
    #[must_use]
    pub fn to_cents(self) -> Option<i64> {
        let scaled = self.0.checked_mul(dec!(100))?;

        if scaled.fract() != Decimal::ZERO {
            return None;
        }

        scaled.to_i64()
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }
}

impl HasZero for Usd {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<Usd> for Decimal {
    fn from(value: Usd) -> Self {
        value.0
    }
}

impl FromStr for Usd {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

impl Display for Usd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Mul<Decimal> for Usd {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0.checked_mul(rhs).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Mul,
            lhs: self,
            rhs: Self(rhs),
        })
    }
}

impl std::ops::Add for Usd {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0.checked_add(rhs.0).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Add,
            lhs: self,
            rhs,
        })
    }
}

impl std::ops::Sub for Usd {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0.checked_sub(rhs.0).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Sub,
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
            let usd = Usd(decimal);
            prop_assert_eq!(usd.is_zero(), decimal.is_zero());
        }

        #[test]
        fn is_negative_matches_decimal(decimal in arb_decimal()) {
            let usd = Usd(decimal);
            prop_assert_eq!(usd.is_negative(), decimal.is_sign_negative());
        }

        #[test]
        fn serde_roundtrip(decimal in arb_decimal()) {
            let usd = Usd(decimal);
            let json = serde_json::to_string(&usd).unwrap();
            let roundtripped: Usd = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(usd, roundtripped);
        }
    }

    #[test]
    fn add_succeeds() {
        let result = (Usd(Decimal::ONE) + Usd(Decimal::TWO)).unwrap();
        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let result = (Usd(Decimal::from(5)) - Usd(Decimal::TWO)).unwrap();
        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = Usd(Decimal::MAX);
        let one = Usd(Decimal::ONE);
        let error = (max + one).unwrap_err();
        assert_eq!(error.operation, ArithmeticOperation::Add);
        assert_eq!(error.lhs, max);
        assert_eq!(error.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = Usd(Decimal::MIN);
        let one = Usd(Decimal::ONE);
        let error = (min - one).unwrap_err();
        assert_eq!(error.operation, ArithmeticOperation::Sub);
        assert_eq!(error.lhs, min);
        assert_eq!(error.rhs, one);
    }

    #[test]
    fn zero_constant() {
        assert!(Usd::ZERO.is_zero());
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let usd = Usd(Decimal::from(42));
        let decimal: Decimal = usd.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let usd = Usd(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5
        let result = (usd * ratio).unwrap();
        assert_eq!(result.0, Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = Usd(Decimal::MAX);
        let two = Decimal::TWO;
        let error = (max * two).unwrap_err();
        assert_eq!(error.operation, ArithmeticOperation::Mul);
        assert_eq!(error.lhs, max);
        assert_eq!(error.rhs, Usd(two));
    }

    #[test]
    fn from_cents_converts_positive() {
        let usd = Usd::from_cents(12345).unwrap();
        assert_eq!(usd.0, Decimal::new(12345, 2));
    }

    #[test]
    fn from_cents_converts_negative() {
        let usd = Usd::from_cents(-500).unwrap();
        assert_eq!(usd.0, Decimal::new(-5, 0));
    }

    #[test]
    fn from_cents_converts_zero() {
        let usd = Usd::from_cents(0).unwrap();
        assert!(usd.is_zero());
    }

    #[test]
    fn from_cents_handles_large_values() {
        let usd = Usd::from_cents(i64::MAX).unwrap();
        assert_eq!(usd.0, Decimal::from(i64::MAX) / Decimal::from(100));
    }

    #[test]
    fn to_cents_converts_whole_dollars() {
        let usd = Usd(Decimal::from(500));
        assert_eq!(usd.to_cents().unwrap(), 50_000);
    }

    #[test]
    fn to_cents_converts_dollars_and_cents() {
        let usd = Usd(Decimal::new(12345, 2)); // $123.45
        assert_eq!(usd.to_cents().unwrap(), 12_345);
    }

    #[test]
    fn to_cents_rejects_sub_cent_precision() {
        let usd = Usd(Decimal::new(99999, 3)); // $99.999
        assert_eq!(usd.to_cents(), None);
    }

    #[test]
    fn to_cents_roundtrips_from_cents() {
        let original_cents = 25_000_000i64; // $250,000.00
        let usd = Usd::from_cents(original_cents).unwrap();
        assert_eq!(usd.to_cents().unwrap(), original_cents);
    }

    #[test]
    fn to_cents_converts_zero() {
        assert_eq!(Usd::ZERO.to_cents().unwrap(), 0);
    }

    #[test]
    fn to_cents_returns_none_on_checked_mul_overflow() {
        let usd = Usd(Decimal::MAX);
        assert_eq!(usd.to_cents(), None);
    }

    #[test]
    fn to_cents_returns_none_on_i64_overflow() {
        // i64::MAX cents = $92_233_720_368_547_758.07
        // Use a value that fits in Decimal * 100 but exceeds i64 range.
        let exceeds_i64 = Decimal::from(i64::MAX) + Decimal::ONE;
        let usd = Usd(exceeds_i64);
        assert_eq!(usd.to_cents(), None);
    }
}
