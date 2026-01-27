//! VaultRatio type for converting between wrapped and unwrapped token amounts.
//!
//! ERC-4626 vaults track a ratio of assets (underlying tokens) to shares (wrapped tokens).
//! This ratio starts at 1:1 but increases over time as the vault accrues value from
//! stock splits, dividends, etc.

use alloy::primitives::U256;

use crate::shares::{FractionalShares, SharesConversionError};

/// One unit in ratio representation (10^18).
const RATIO_ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Conversion ratio between wrapped and unwrapped tokens.
///
/// Represents `assets_per_share` with 18 decimal places of precision.
/// A ratio of 1.0 means 1 wrapped token = 1 unwrapped token.
/// A ratio of 1.05 means 1 wrapped token = 1.05 unwrapped tokens (5% appreciation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VaultRatio {
    /// Assets per share with 18 decimals precision.
    /// Obtained from vault's `convertToAssets(10^18)`.
    assets_per_share: U256,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultRatioError {
    #[error("Division by zero: assets_per_share is zero")]
    DivisionByZero,

    #[error("Arithmetic overflow during conversion")]
    Overflow,

    #[error("Shares conversion error: {0}")]
    SharesConversion(#[from] SharesConversionError),
}

impl VaultRatio {
    /// Creates a new VaultRatio from assets_per_share value.
    ///
    /// # Arguments
    ///
    /// * `assets_per_share` - The number of underlying assets per wrapped share,
    ///   with 18 decimals of precision (e.g., 1_000_000_000_000_000_000 = 1.0).
    pub(crate) fn new(assets_per_share: U256) -> Result<Self, VaultRatioError> {
        if assets_per_share.is_zero() {
            return Err(VaultRatioError::DivisionByZero);
        }

        Ok(Self { assets_per_share })
    }

    /// Creates a 1:1 ratio (used when no wrapping is needed).
    pub(crate) fn one_to_one() -> Self {
        Self {
            assets_per_share: RATIO_ONE,
        }
    }

    /// Converts wrapped token amount to unwrapped-equivalent amount.
    ///
    /// Formula: unwrapped = wrapped * assets_per_share / 10^18
    pub(crate) fn wrapped_to_unwrapped(&self, wrapped: U256) -> Result<U256, VaultRatioError> {
        let numerator = wrapped
            .checked_mul(self.assets_per_share)
            .ok_or(VaultRatioError::Overflow)?;

        Ok(numerator / RATIO_ONE)
    }

    /// Converts wrapped FractionalShares to unwrapped-equivalent FractionalShares.
    ///
    /// Handles the conversion chain: FractionalShares -> U256 -> apply ratio -> FractionalShares.
    pub(crate) fn wrapped_to_unwrapped_fractional(
        &self,
        wrapped: FractionalShares,
    ) -> Result<FractionalShares, VaultRatioError> {
        let wrapped_u256 = wrapped.to_u256_18_decimals()?;
        let unwrapped_u256 = self.wrapped_to_unwrapped(wrapped_u256)?;
        let unwrapped = FractionalShares::from_u256_18_decimals(unwrapped_u256)?;

        Ok(unwrapped)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::shares::HasZero;

    #[test]
    fn one_to_one_ratio_converts_identity() {
        let ratio = VaultRatio::one_to_one();
        let amount = U256::from(1000u64);

        let unwrapped = ratio.wrapped_to_unwrapped(amount).unwrap();
        assert_eq!(unwrapped, amount);
    }

    #[test]
    fn ratio_1_05_converts_correctly() {
        // 1.05 ratio = 1_050_000_000_000_000_000
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = VaultRatio::new(assets_per_share).unwrap();

        // 100 wrapped should give 105 unwrapped
        let wrapped = U256::from(100u64);
        let unwrapped = ratio.wrapped_to_unwrapped(wrapped).unwrap();
        assert_eq!(unwrapped, U256::from(105u64));
    }

    #[test]
    fn ratio_2_0_post_split_converts_correctly() {
        // 2.0 ratio = 2_000_000_000_000_000_000 (post 2:1 split)
        let assets_per_share = U256::from(2_000_000_000_000_000_000u64);
        let ratio = VaultRatio::new(assets_per_share).unwrap();

        // 50 wrapped should give 100 unwrapped
        let wrapped = U256::from(50u64);
        let unwrapped = ratio.wrapped_to_unwrapped(wrapped).unwrap();
        assert_eq!(unwrapped, U256::from(100u64));
    }

    #[test]
    fn zero_amount_converts_to_zero() {
        let ratio = VaultRatio::one_to_one();

        let unwrapped = ratio.wrapped_to_unwrapped(U256::ZERO).unwrap();
        assert_eq!(unwrapped, U256::ZERO);
    }

    #[test]
    fn zero_ratio_fails() {
        let result = VaultRatio::new(U256::ZERO);
        assert!(matches!(
            result.unwrap_err(),
            VaultRatioError::DivisionByZero
        ));
    }

    #[test]
    fn large_amounts_dont_overflow() {
        let ratio = VaultRatio::one_to_one();

        // Test with a reasonably large amount (not U256::MAX which would overflow)
        let large_amount = U256::from(10u64).pow(U256::from(30u64));

        let unwrapped = ratio.wrapped_to_unwrapped(large_amount).unwrap();
        assert_eq!(unwrapped, large_amount);
    }

    #[test]
    fn fractional_one_to_one_converts_identity() {
        let ratio = VaultRatio::one_to_one();
        let wrapped = FractionalShares(dec!(100));

        let unwrapped = ratio.wrapped_to_unwrapped_fractional(wrapped).unwrap();

        assert_eq!(unwrapped.0, dec!(100));
    }

    #[test]
    fn fractional_1_05_ratio_converts_correctly() {
        // 1.05 ratio = 1_050_000_000_000_000_000
        let assets_per_share = U256::from(1_050_000_000_000_000_000u64);
        let ratio = VaultRatio::new(assets_per_share).unwrap();

        // 100 wrapped should give 105 unwrapped
        let wrapped = FractionalShares(dec!(100));
        let unwrapped = ratio.wrapped_to_unwrapped_fractional(wrapped).unwrap();

        assert_eq!(unwrapped.0, dec!(105));
    }

    #[test]
    fn fractional_2_0_post_split_converts_correctly() {
        // 2.0 ratio = 2_000_000_000_000_000_000 (post 2:1 split)
        let assets_per_share = U256::from(2_000_000_000_000_000_000u64);
        let ratio = VaultRatio::new(assets_per_share).unwrap();

        // 50 wrapped should give 100 unwrapped
        let wrapped = FractionalShares(dec!(50));
        let unwrapped = ratio.wrapped_to_unwrapped_fractional(wrapped).unwrap();

        assert_eq!(unwrapped.0, dec!(100));
    }

    #[test]
    fn fractional_zero_converts_to_zero() {
        let ratio = VaultRatio::one_to_one();

        let unwrapped = ratio
            .wrapped_to_unwrapped_fractional(FractionalShares::ZERO)
            .unwrap();

        assert_eq!(unwrapped, FractionalShares::ZERO);
    }

    #[test]
    fn fractional_conversion_handles_small_values() {
        let ratio = VaultRatio::one_to_one();
        let wrapped = FractionalShares(Decimal::new(1, 6)); // 0.000001

        let unwrapped = ratio.wrapped_to_unwrapped_fractional(wrapped).unwrap();

        assert_eq!(unwrapped.0, Decimal::new(1, 6));
    }
}
