//! Ratio type for converting between wrapped and underlying token amounts.
//!
//! ERC-4626 vaults track a ratio of assets (underlying tokens) to shares (wrapped tokens).
//! This ratio starts at 1:1 but increases over time as the vault accrues value from
//! stock splits, dividends, etc.

use alloy::primitives::U256;

use st0x_execution::{FractionalShares, SharesConversionError};

/// One unit in ratio representation (10^18).
pub const RATIO_ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Ratio of underlying tokens per wrapped token.
///
/// Represents how many underlying tokens you receive for each wrapped token,
/// with 18 decimal places of precision.
/// A ratio of 1.0 means 1 wrapped token = 1 underlying token.
/// A ratio of 1.05 means 1 wrapped token = 1.05 underlying tokens (5% appreciation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnderlyingPerWrapped {
    /// Underlying tokens per wrapped share with 18 decimals precision.
    /// Obtained from vault's `convertToAssets(10^18)`.
    ratio: U256,
}

#[derive(Debug, thiserror::Error)]
pub enum RatioError {
    #[error("Division by zero: ratio is zero")]
    DivisionByZero,
    #[error("Arithmetic overflow during conversion")]
    Overflow,
    #[error("Shares conversion error: {0}")]
    SharesConversion(#[from] SharesConversionError),
}

impl UnderlyingPerWrapped {
    /// Creates a new ratio from underlying-per-wrapped value.
    pub fn new(ratio: U256) -> Result<Self, RatioError> {
        if ratio.is_zero() {
            return Err(RatioError::DivisionByZero);
        }

        Ok(Self { ratio })
    }

    /// Converts wrapped token amount to underlying amount.
    ///
    /// Formula: underlying = wrapped * ratio / 10^18
    pub fn to_underlying(self, wrapped: U256) -> Result<U256, RatioError> {
        let numerator = wrapped
            .checked_mul(self.ratio)
            .ok_or(RatioError::Overflow)?;

        Ok(numerator / RATIO_ONE)
    }

    /// Converts wrapped FractionalShares to underlying FractionalShares.
    pub fn to_underlying_fractional(
        self,
        wrapped: FractionalShares,
    ) -> Result<FractionalShares, RatioError> {
        let wrapped_u256 = wrapped.to_u256_18_decimals()?;
        let underlying_u256 = self.to_underlying(wrapped_u256)?;
        let underlying = FractionalShares::from_u256_18_decimals(underlying_u256)?;

        Ok(underlying)
    }
}

#[cfg(test)]
mod tests {
    use st0x_exact_decimal::ExactDecimal;

    use super::*;

    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    #[test]
    fn one_to_one_ratio_converts_identity() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let amount = U256::from(1000u64);

        let underlying = ratio.to_underlying(amount).unwrap();
        assert_eq!(underlying, amount);
    }

    #[test]
    fn ratio_1_05_converts_correctly() {
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        let wrapped = U256::from(100u64);
        let underlying = ratio.to_underlying(wrapped).unwrap();
        assert_eq!(underlying, U256::from(105u64));
    }

    #[test]
    fn zero_ratio_fails() {
        let result = UnderlyingPerWrapped::new(U256::ZERO);
        assert!(matches!(result.unwrap_err(), RatioError::DivisionByZero));
    }

    #[test]
    fn fractional_one_to_one_converts_identity() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let wrapped = FractionalShares::new(ed("100"));

        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), ed("100"));
    }

    #[test]
    fn fractional_1_05_ratio_converts_correctly() {
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        let wrapped = FractionalShares::new(ed("100"));
        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), ed("105"));
    }

    #[test]
    fn fractional_zero_converts_to_zero() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let underlying = ratio
            .to_underlying_fractional(FractionalShares::ZERO)
            .unwrap();

        assert_eq!(underlying, FractionalShares::ZERO);
    }
}
