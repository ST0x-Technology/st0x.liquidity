//! USDC dollar amount newtype with checked arithmetic.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::{ArithmeticError, ArithmeticOperation, HasZero};

/// A USDC dollar amount.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Usdc(Decimal);

impl Usdc {
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn inner(self) -> Decimal {
        self.0
    }

    /// Creates a Usdc amount from cents (e.g., 12345 cents = $123.45).
    #[must_use]
    pub fn from_cents(cents: i64) -> Option<Self> {
        Decimal::from(cents).checked_div(dec!(100)).map(Self)
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

#[cfg(feature = "alloy")]
mod alloy_support {
    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use super::Usdc;

    /// 10^6 scale factor for USDC (6 decimals).
    const USDC_DECIMAL_SCALE: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

    #[derive(Debug, thiserror::Error)]
    pub enum UsdcConversionError {
        #[error("USDC amount cannot be negative: {0}")]
        NegativeValue(Usdc),
        #[error("overflow when scaling USDC to 6 decimals")]
        Overflow,
        #[error(
            "USDC amount has more than 6 decimal places \
             (would lose precision): {0}"
        )]
        TooManyDecimals(Usdc),
        #[error("failed to parse U256: {0}")]
        ParseError(#[from] alloy_primitives::ruint::ParseError),
    }

    impl Usdc {
        /// Converts to U256 with 6 decimal places (USDC standard).
        ///
        /// Returns an error for negative values or overflow during scaling.
        ///
        /// # Errors
        ///
        /// Returns [`UsdcConversionError::NegativeValue`] if the USDC
        /// amount is negative, [`UsdcConversionError::Overflow`] if
        /// scaling overflows, or
        /// [`UsdcConversionError::TooManyDecimals`] if the value has
        /// more than 6 decimal places.
        pub fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
            if self.inner().is_sign_negative() {
                return Err(UsdcConversionError::NegativeValue(self));
            }

            let scaled = self
                .inner()
                .checked_mul(USDC_DECIMAL_SCALE)
                .ok_or(UsdcConversionError::Overflow)?;

            if scaled.fract() != Decimal::ZERO {
                return Err(UsdcConversionError::TooManyDecimals(self));
            }

            Ok(U256::from_str_radix(&scaled.trunc().to_string(), 10)?)
        }
    }
}

#[cfg(feature = "alloy")]
pub use alloy_support::UsdcConversionError;

impl std::ops::Mul<Decimal> for Usdc {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0.checked_mul(rhs).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Mul,
            lhs: self,
            rhs: Self(rhs),
        })
    }
}

impl std::ops::Add for Usdc {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0.checked_add(rhs.0).map(Self).ok_or(ArithmeticError {
            operation: ArithmeticOperation::Add,
            lhs: self,
            rhs,
        })
    }
}

impl std::ops::Sub for Usdc {
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
        assert_eq!(error.operation, ArithmeticOperation::Add);
        assert_eq!(error.lhs, max);
        assert_eq!(error.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = Usdc(Decimal::MIN);
        let one = Usdc(Decimal::ONE);
        let error = (min - one).unwrap_err();
        assert_eq!(error.operation, ArithmeticOperation::Sub);
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
        assert_eq!(error.operation, ArithmeticOperation::Mul);
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

    #[cfg(feature = "alloy")]
    use alloy_primitives::U256;

    #[cfg(feature = "alloy")]
    #[test]
    fn to_u256_whole_dollars_convert_correctly() {
        let usdc = Usdc(Decimal::from(100));
        // 100 USDC = 100_000_000 in 6-decimal representation
        assert_eq!(
            usdc.to_u256_6_decimals().unwrap(),
            U256::from(100_000_000u64)
        );
    }

    #[cfg(feature = "alloy")]
    #[test]
    fn to_u256_six_decimal_places_convert_exactly() {
        // 1.123456 USDC
        let usdc = Usdc(Decimal::new(1_123_456, 6));
        assert_eq!(usdc.to_u256_6_decimals().unwrap(), U256::from(1_123_456u64));
    }

    #[cfg(feature = "alloy")]
    #[test]
    fn to_u256_seven_decimal_places_returns_too_many_decimals() {
        // 1.1234567 has 7 decimal places -> excess precision
        let usdc = Usdc(Decimal::new(11_234_567, 7));
        let error = usdc.to_u256_6_decimals().unwrap_err();
        assert!(matches!(error, UsdcConversionError::TooManyDecimals(_)));
    }

    #[cfg(feature = "alloy")]
    #[test]
    fn to_u256_negative_value_returns_error() {
        let usdc = Usdc(Decimal::new(-1, 0));
        let error = usdc.to_u256_6_decimals().unwrap_err();
        assert!(matches!(error, UsdcConversionError::NegativeValue(_)));
    }

    #[cfg(feature = "alloy")]
    #[test]
    fn to_u256_zero_converts_to_zero() {
        assert_eq!(Usdc::ZERO.to_u256_6_decimals().unwrap(), U256::ZERO);
    }
}
