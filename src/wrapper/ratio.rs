//! Ratio type for converting between wrapped and underlying token amounts.
//!
//! ERC-4626 vaults track a ratio of assets (underlying tokens) to shares (wrapped tokens).
//! This ratio starts at 1:1 but increases over time as the vault accrues value from
//! stock splits, dividends, etc.

use alloy::primitives::U256;

use crate::shares::{FractionalShares, SharesConversionError};

/// One unit in ratio representation (10^18).
pub(crate) const RATIO_ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Ratio of underlying tokens per wrapped token.
///
/// Represents how many underlying tokens you receive for each wrapped token,
/// with 18 decimal places of precision.
/// A ratio of 1.0 means 1 wrapped token = 1 underlying token.
/// A ratio of 1.05 means 1 wrapped token = 1.05 underlying tokens (5% appreciation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnderlyingPerWrapped {
    /// Underlying tokens per wrapped share with 18 decimals precision.
    /// Obtained from vault's `convertToAssets(10^18)`.
    ratio: U256,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RatioError {
    #[error("Division by zero: ratio is zero")]
    DivisionByZero,
    #[error("Arithmetic overflow during conversion")]
    Overflow,
    #[error("Shares conversion error: {0}")]
    SharesConversion(#[from] SharesConversionError),
}

impl UnderlyingPerWrapped {
    /// Creates a new ratio from underlying-per-wrapped value.
    ///
    /// # Arguments
    ///
    /// * `ratio` - The number of underlying tokens per wrapped share,
    ///   with 18 decimals of precision (e.g., 1_000_000_000_000_000_000 = 1.0).
    pub(crate) fn new(ratio: U256) -> Result<Self, RatioError> {
        if ratio.is_zero() {
            return Err(RatioError::DivisionByZero);
        }

        Ok(Self { ratio })
    }

    /// Converts wrapped token amount to underlying amount.
    ///
    /// Formula: underlying = wrapped * ratio / 10^18
    pub(crate) fn to_underlying(self, wrapped: U256) -> Result<U256, RatioError> {
        let numerator = wrapped
            .checked_mul(self.ratio)
            .ok_or(RatioError::Overflow)?;

        Ok(numerator / RATIO_ONE)
    }

    /// Converts wrapped FractionalShares to underlying FractionalShares.
    ///
    /// Handles the conversion chain: FractionalShares -> U256 -> apply ratio -> FractionalShares.
    pub(crate) fn to_underlying_fractional(
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
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn one_to_one_ratio_converts_identity() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let amount = U256::from(1000u64);

        let underlying = ratio.to_underlying(amount).unwrap();
        assert_eq!(underlying, amount);
    }

    #[test]
    fn ratio_1_05_converts_correctly() {
        // 1.05 ratio = 1_050_000_000_000_000_000
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        // 100 wrapped should give 105 underlying
        let wrapped = U256::from(100u64);
        let underlying = ratio.to_underlying(wrapped).unwrap();
        assert_eq!(underlying, U256::from(105u64));
    }

    #[test]
    fn ratio_2_0_post_split_converts_correctly() {
        // 2.0 ratio = 2_000_000_000_000_000_000 (post 2:1 split)
        let assets_per_share = U256::from(2_000_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        // 50 wrapped should give 100 underlying
        let wrapped = U256::from(50u64);
        let underlying = ratio.to_underlying(wrapped).unwrap();
        assert_eq!(underlying, U256::from(100u64));
    }

    #[test]
    fn zero_amount_converts_to_zero() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let underlying = ratio.to_underlying(U256::ZERO).unwrap();
        assert_eq!(underlying, U256::ZERO);
    }

    #[test]
    fn zero_ratio_fails() {
        let result = UnderlyingPerWrapped::new(U256::ZERO);
        assert!(matches!(result.unwrap_err(), RatioError::DivisionByZero));
    }

    #[test]
    fn large_amounts_dont_overflow() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        // Test with a reasonably large amount (not U256::MAX which would overflow)
        let large_amount = U256::from(10u64).pow(U256::from(30u64));

        let underlying = ratio.to_underlying(large_amount).unwrap();
        assert_eq!(underlying, large_amount);
    }

    #[test]
    fn fractional_one_to_one_converts_identity() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let wrapped = FractionalShares::new(dec!(100));

        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), dec!(100));
    }

    #[test]
    fn fractional_1_05_ratio_converts_correctly() {
        // 1.05 ratio = 1_050_000_000_000_000_000
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        // 100 wrapped should give 105 underlying
        let wrapped = FractionalShares::new(dec!(100));
        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), dec!(105));
    }

    #[test]
    fn fractional_2_0_post_split_converts_correctly() {
        // 2.0 ratio = 2_000_000_000_000_000_000 (post 2:1 split)
        let assets_per_share = U256::from(2_000_000_000_000_000_000u64);
        let ratio = UnderlyingPerWrapped::new(assets_per_share).unwrap();

        // 50 wrapped should give 100 underlying
        let wrapped = FractionalShares::new(dec!(50));
        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), dec!(100));
    }

    #[test]
    fn fractional_zero_converts_to_zero() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let underlying = ratio
            .to_underlying_fractional(FractionalShares::ZERO)
            .unwrap();

        assert_eq!(underlying, FractionalShares::ZERO);
    }

    #[test]
    fn fractional_conversion_handles_small_values() {
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let wrapped = FractionalShares::new(Decimal::new(1, 6)); // 0.000001

        let underlying = ratio.to_underlying_fractional(wrapped).unwrap();

        assert_eq!(underlying.inner(), Decimal::new(1, 6));
    }
}
