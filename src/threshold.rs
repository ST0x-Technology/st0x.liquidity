//! Execution threshold configuration for position management.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::shares::{ArithmeticError, FractionalShares, HasZero};

/// A USDC dollar amount used for threshold configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Usdc(pub(crate) Decimal);

impl HasZero for Usdc {
    const ZERO: Self = Self(Decimal::ZERO);
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
    Shares(FractionalShares),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    #[cfg(test)]
    pub(crate) fn shares(value: FractionalShares) -> Result<Self, InvalidThresholdError> {
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeShares(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroShares);
        }

        Ok(Self::Shares(value))
    }

    #[cfg(test)]
    pub(crate) fn dollar_value(value: Usdc) -> Result<Self, InvalidThresholdError> {
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeDollarValue(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroDollarValue);
        }

        Ok(Self::DollarValue(value))
    }

    pub(crate) fn whole_share() -> Self {
        Self::Shares(FractionalShares::ONE)
    }
}

#[cfg(test)]
#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum InvalidThresholdError {
    #[error("Shares threshold cannot be negative: {0:?}")]
    NegativeShares(FractionalShares),
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Shares threshold cannot be zero")]
    ZeroShares,
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
        let from_constructor = ExecutionThreshold::Shares(FractionalShares::ONE);
        assert_eq!(from_whole_share, from_constructor);
    }

    #[test]
    fn shares_threshold_rejects_zero() {
        let result = ExecutionThreshold::shares(FractionalShares::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroShares);
    }

    #[test]
    fn shares_threshold_rejects_negative() {
        let negative = FractionalShares(Decimal::NEGATIVE_ONE);
        let result = ExecutionThreshold::shares(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeShares(negative)
        );
    }

    #[test]
    fn shares_threshold_accepts_positive() {
        let result = ExecutionThreshold::shares(FractionalShares::ONE);
        assert!(result.is_ok());
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
