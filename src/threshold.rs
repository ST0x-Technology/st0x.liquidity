//! Execution threshold configuration for position management.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::shares::FractionalShares;

/// A USDC dollar amount used for threshold configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Usdc(pub(crate) Decimal);

impl Usdc {
    pub(crate) fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    pub(crate) fn is_zero(self) -> bool {
        self.0.is_zero()
    }
}

/// Threshold configuration that determines when to trigger offchain execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ExecutionThreshold {
    Shares(FractionalShares),
    DollarValue(Usdc),
}

impl ExecutionThreshold {
    pub(crate) fn shares(value: FractionalShares) -> Result<Self, InvalidThresholdError> {
        if value.is_negative() {
            return Err(InvalidThresholdError::NegativeShares(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::ZeroShares);
        }

        Ok(Self::Shares(value))
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

    pub(crate) fn whole_share() -> Self {
        Self::Shares(FractionalShares::ONE)
    }
}

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
}
