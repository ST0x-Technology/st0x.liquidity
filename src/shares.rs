use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct FractionalShares(pub(crate) Decimal);

impl FromStr for FractionalShares {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

impl Display for FractionalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub struct ArithmeticError<T> {
    pub operation: String,
    pub lhs: T,
    pub rhs: T,
}

pub(crate) trait HasZero: PartialOrd + Sized {
    const ZERO: Self;

    fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }

    fn is_negative(&self) -> bool {
        self < &Self::ZERO
    }
}

impl HasZero for FractionalShares {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<FractionalShares> for Decimal {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl std::ops::Mul<Decimal> for FractionalShares {
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

/// 10^18 scale factor for tokenized equity decimal conversion.
///
/// Tokenized equities use 18 decimals (unlike USDC which uses 6).
/// This equals 1,000,000,000,000,000,000 (one quintillion).
/// The `from_parts` representation: lo=2_808_348_672, mid=232_830_643, hi=0, negative=false, scale=0.
/// Verified by `tokenized_equity_scale_equals_10_pow_18` test.
const TOKENIZED_EQUITY_SCALE: Decimal =
    Decimal::from_parts(2_808_348_672, 232_830_643, 0, false, 0);

impl FractionalShares {
    pub(crate) const ONE: Self = Self(Decimal::ONE);

    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Converts the whole number part to u64.
    ///
    /// Returns an error if the value is negative, has fractional parts, or exceeds u64::MAX.
    pub(crate) fn try_into_u64(self) -> Result<u64, SharesConversionError> {
        if self.0.is_sign_negative() {
            return Err(SharesConversionError::NegativeValue(self.0));
        }

        if self.0.fract() != Decimal::ZERO {
            return Err(SharesConversionError::FractionalValue(self.0));
        }

        Ok(self.0.to_string().parse::<u64>()?)
    }

    /// Converts to U256 with 18 decimal places (standard ERC20 decimals).
    ///
    /// Returns an error for negative values, underflow (values < 1e-18),
    /// or overflow during scaling.
    pub(crate) fn to_u256_18_decimals(self) -> Result<U256, SharesConversionError> {
        if self.0.is_sign_negative() {
            return Err(SharesConversionError::NegativeValue(self.0));
        }

        if self.0.is_zero() {
            return Ok(U256::ZERO);
        }

        let scaled = self
            .0
            .checked_mul(TOKENIZED_EQUITY_SCALE)
            .ok_or(SharesConversionError::Overflow)?;

        let truncated = scaled.trunc();

        if truncated.is_zero() {
            return Err(SharesConversionError::Underflow(self.0));
        }

        U256::from_str_radix(&truncated.to_string(), 10).map_err(SharesConversionError::ParseError)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SharesConversionError {
    #[error("shares value cannot be negative: {0}")]
    NegativeValue(Decimal),
    #[error("shares value has fractional component: {0}")]
    FractionalValue(Decimal),
    #[error("shares value too small to represent with 18 decimals: {0}")]
    Underflow(Decimal),
    #[error("shares value exceeds u64::MAX: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("overflow when scaling shares to 18 decimals")]
    Overflow,
    #[error("failed to parse U256: {0}")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

impl std::ops::Add for FractionalShares {
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

impl std::ops::Sub for FractionalShares {
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
    use std::str::FromStr;

    use super::*;

    #[test]
    fn tokenized_equity_scale_equals_10_pow_18() {
        // Verify that TOKENIZED_EQUITY_SCALE constructed via Decimal::from_parts
        // actually equals 10^18 (1,000,000,000,000,000,000).
        let expected = Decimal::from_str("1000000000000000000").unwrap();
        assert_eq!(
            TOKENIZED_EQUITY_SCALE, expected,
            "TOKENIZED_EQUITY_SCALE must equal 10^18"
        );
    }

    #[test]
    fn add_succeeds() {
        let a = FractionalShares(Decimal::ONE);
        let b = FractionalShares(Decimal::TWO);

        let result = (a + b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let a = FractionalShares(Decimal::from(5));
        let b = FractionalShares(Decimal::TWO);

        let result = (a - b).unwrap();

        assert_eq!(result.0, Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = FractionalShares(Decimal::MAX);
        let one = FractionalShares(Decimal::ONE);

        let result = max + one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "+");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = FractionalShares(Decimal::MIN);
        let one = FractionalShares(Decimal::ONE);

        let result = min - one;

        let err = result.unwrap_err();
        assert_eq!(err.operation, "-");
        assert_eq!(err.lhs, min);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn abs_returns_absolute_value() {
        let negative = FractionalShares(Decimal::NEGATIVE_ONE);
        assert_eq!(negative.abs().0, Decimal::ONE);
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let shares = FractionalShares(Decimal::from(42));
        let decimal: Decimal = shares.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let shares = FractionalShares(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5

        let result = (shares * ratio).unwrap();

        assert_eq!(result.0, Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = FractionalShares(Decimal::MAX);
        let two = Decimal::TWO;

        let err = (max * two).unwrap_err();

        assert_eq!(err.operation, "*");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, FractionalShares(two));
    }

    #[test]
    fn to_u256_18_decimals_zero_returns_zero() {
        let shares = FractionalShares(Decimal::ZERO);
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn to_u256_18_decimals_one_returns_10_pow_18() {
        let shares = FractionalShares(Decimal::ONE);
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1000000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_fractional_value() {
        let shares = FractionalShares(Decimal::from_str("1.5").unwrap());
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1500000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_small_fractional_value() {
        let shares = FractionalShares(Decimal::from_str("0.000000000000000001").unwrap());
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from(1));
    }

    #[test]
    fn to_u256_18_decimals_negative_returns_error() {
        let shares = FractionalShares(Decimal::NEGATIVE_ONE);
        let err = shares.to_u256_18_decimals().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::NegativeValue(_)),
            "Expected NegativeValue error, got: {err:?}"
        );
    }

    #[test]
    fn to_u256_18_decimals_underflow_returns_error() {
        // Value smaller than 1e-18 cannot be represented
        let shares = FractionalShares(Decimal::from_str("0.0000000000000000001").unwrap());
        let err = shares.to_u256_18_decimals().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::Underflow(_)),
            "Expected Underflow error, got: {err:?}"
        );
    }

    #[test]
    fn try_into_u64_positive_whole_number() {
        let shares = FractionalShares(Decimal::from(5));
        let result = shares.try_into_u64().unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn try_into_u64_zero_returns_ok() {
        let shares = FractionalShares(Decimal::ZERO);
        let result = shares.try_into_u64().unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn try_into_u64_negative_returns_error() {
        let shares = FractionalShares(Decimal::NEGATIVE_ONE);
        let err = shares.try_into_u64().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::NegativeValue(_)),
            "Expected NegativeValue error, got: {err:?}"
        );
    }

    #[test]
    fn try_into_u64_overflow_returns_error() {
        // Value larger than u64::MAX (18446744073709551615)
        let shares = FractionalShares(Decimal::from_str("18446744073709551616").unwrap());
        let err = shares.try_into_u64().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::ParseInt(_)),
            "Expected ParseInt error, got: {err:?}"
        );
    }

    #[test]
    fn try_into_u64_fractional_returns_error() {
        let shares = FractionalShares(Decimal::from_str("5.5").unwrap());
        let err = shares.try_into_u64().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::FractionalValue(_)),
            "Expected FractionalValue error, got: {err:?}"
        );
    }
}
