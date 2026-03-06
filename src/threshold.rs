//! Execution threshold configuration for position management.
//!
//! Determines the minimum position imbalance (whole-share or
//! dollar-value based) required before placing an offsetting
//! broker order.

use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use st0x_execution::{FractionalShares, Positive};
use st0x_finance::Usdc;

/// 10^6 scale factor for USDC (6 decimals).
const USDC_DECIMAL_SCALE: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

/// Extension trait for U256 conversions on `Usdc`.
///
/// These depend on alloy types and therefore live here
/// rather than the leaf st0x-finance crate.
pub(crate) trait UsdcBlockchain {
    /// Converts to U256 with 6 decimal places (USDC standard).
    ///
    /// Returns an error for negative values or overflow during scaling.
    fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError>;
}

impl UsdcBlockchain for Usdc {
    fn to_u256_6_decimals(self) -> Result<U256, UsdcConversionError> {
        let inner = self.inner();

        if inner.is_sign_negative() {
            return Err(UsdcConversionError::NegativeValue(inner));
        }

        let scaled = inner
            .checked_mul(USDC_DECIMAL_SCALE)
            .ok_or(UsdcConversionError::Overflow)?;

        Ok(U256::from_str_radix(&scaled.trunc().to_string(), 10)?)
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
        Self::Shares(Positive::new(FractionalShares::new(Decimal::ONE)).unwrap())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidThresholdError {
    #[error("Dollar threshold cannot be negative: {0:?}")]
    NegativeDollarValue(Usdc),
    #[error("Dollar threshold cannot be zero")]
    ZeroDollarValue,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

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
        let result = ExecutionThreshold::dollar_value(Usdc::new(Decimal::ZERO));
        assert_eq!(result.unwrap_err(), InvalidThresholdError::ZeroDollarValue);
    }

    #[test]
    fn dollar_threshold_rejects_negative() {
        let negative = Usdc::new(Decimal::NEGATIVE_ONE);
        let result = ExecutionThreshold::dollar_value(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::NegativeDollarValue(negative)
        );
    }

    #[test]
    fn dollar_threshold_accepts_positive() {
        ExecutionThreshold::dollar_value(Usdc::new(Decimal::ONE)).unwrap();
    }
}
