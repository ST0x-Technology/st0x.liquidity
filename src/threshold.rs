//! Execution threshold configuration for position management.

use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use st0x_execution::{FractionalShares, Positive};

use crate::shares::{ArithmeticError, HasZero};

/// A USDC dollar amount used for threshold configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Usdc(pub(crate) Decimal);

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

impl HasZero for Usdc {
    const ZERO: Self = Self(Decimal::ZERO);
}

/// 10^6 scale factor for USDC (6 decimals).
const USDC_DECIMAL_SCALE: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

impl Usdc {
    /// Converts to U256 with 6 decimal places (USDC standard).
    ///
    /// Returns an error for negative values or overflow during scaling.
    pub fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
        if self.0.is_sign_negative() {
            return Err(UsdcConversionError::NegativeValue(self.0));
        }

        let scaled = self
            .0
            .checked_mul(USDC_DECIMAL_SCALE)
            .ok_or(UsdcConversionError::Overflow)?;

        U256::from_str_radix(&scaled.trunc().to_string(), 10)
            .map_err(UsdcConversionError::ParseError)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UsdcConversionError {
    #[error("USDC amount cannot be negative: {0}")]
    NegativeValue(Decimal),
    #[error("overflow when scaling USDC to 6 decimals")]
    Overflow,
    #[error("failed to parse U256: {0}")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

impl From<Usdc> for Decimal {
    fn from(value: Usdc) -> Self {
        value.0
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

/// Threshold configuration that determines when to trigger offchain execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ExecutionThreshold {
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
        Self::Shares(Positive::new(FractionalShares::new(rust_decimal::Decimal::ONE)).unwrap())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum InvalidThresholdError {
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Dollar threshold cannot be zero")]
    ZeroDollarValue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn whole_share_matches_smart_constructor() {
        let from_whole_share = ExecutionThreshold::whole_share();
        let from_constructor = ExecutionThreshold::Shares(Positive::<FractionalShares>::ONE);
        assert_eq!(from_whole_share, from_constructor);
    }

    #[test]
    fn shares_threshold_accepts_positive() {
        let threshold = ExecutionThreshold::shares(Positive::<FractionalShares>::ONE);
        assert!(matches!(threshold, ExecutionThreshold::Shares(_)));
    }

    #[test]
    fn dollar_threshold_rejects_zero() {
        let result = ExecutionThreshold::dollar_value(Usdc(Decimal::ZERO));
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroDollarValue);
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc(Decimal::NEGATIVE_ONE);
        let result = ExecutionThreshold::dollar_value(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(negative)
        );
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        let result = ExecutionThreshold::dollar_value(Usdc(Decimal::ONE));
        assert!(result.is_ok());
    }

    #[test]
    fn usdc_add_succeeds() {
        let a = Usdc(Decimal::ONE);
        let b = Usdc(Decimal::TWO);

        let result = (a + b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn usdc_sub_succeeds() {
        let a = Usdc(Decimal::from(5));
        let b = Usdc(Decimal::TWO);

        let result = (a - b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn usdc_add_overflow_returns_error() {
        let max = Usdc(Decimal::MAX);
        let one = Usdc(Decimal::ONE);

        let result = max + one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "+");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn usdc_sub_overflow_returns_error() {
        let min = Usdc(Decimal::MIN);
        let one = Usdc(Decimal::ONE);

        let result = min - one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "-");
        assert_eq!(err.lhs, min);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn usdc_zero_constant() {
        assert!(Usdc::ZERO.is_zero());
    }

    #[test]
    fn usdc_into_decimal_extracts_inner_value() {
        let usdc = Usdc(Decimal::from(42));
        let decimal: Decimal = usdc.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn usdc_mul_decimal_succeeds() {
        let usdc = Usdc(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5

        let result = (usdc * ratio).unwrap();

        assert_eq!(result.0, Decimal::from(50));
    }

    #[test]
    fn usdc_mul_decimal_overflow_returns_error() {
        let max = Usdc(Decimal::MAX);
        let two = Decimal::TWO;

        let err = (max * two).unwrap_err();

        assert_eq!(err.operation, "*");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, Usdc(two));
    }
}
