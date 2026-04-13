//! Shared financial primitives used across the st0x workspace.
//!
//! This is a leaf crate with zero workspace dependencies, providing
//! domain types that multiple crates need: `Symbol`, `FractionalShares`,
//! `Usdc`, `Usd`, `Positive`, `HasZero`, and `Id<Tag>`.

use rain_math_float::{Float, FloatError};
use serde::Deserialize;
use std::fmt::Debug;

mod id;
mod shares;
mod symbol;
mod usd;
mod usdc;

pub use id::{BlankIdError, Id};
pub use shares::FractionalShares;
pub use symbol::{EmptySymbolError, Symbol};
pub use usd::Usd;
pub use usdc::{Usdc, UsdcConversionError};

/// Trait for types that have a zero value and can be compared to it.
///
/// Comparisons are fallible because the underlying Float EVM-based
/// operations can technically fail on malformed data.
pub trait HasZero: Sized + Copy {
    const ZERO: Self;

    fn is_zero(&self) -> Result<bool, FloatError>;
    fn is_negative(&self) -> Result<bool, FloatError>;
}

impl HasZero for Float {
    const ZERO: Self = Self::from_raw(alloy_primitives::B256::ZERO);

    fn is_zero(&self) -> Result<bool, FloatError> {
        Self::is_zero(*self)
    }

    fn is_negative(&self) -> Result<bool, FloatError> {
        self.lt(Self::ZERO)
    }
}

/// Value must be positive (greater than zero).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("value must be positive, got {value:?}")]
pub struct NotPositive<T> {
    pub value: T,
}

/// Wrapper that guarantees the inner value is positive (greater than zero).
///
/// Use this when an API requires strictly positive values, such as order quantities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct Positive<T>(T);

impl<T> Positive<T>
where
    T: HasZero + Into<Float>,
{
    /// # Errors
    ///
    /// Returns [`NotPositive`] if `value` is zero or negative.
    pub fn new(value: T) -> Result<Self, NotPositive<T>> {
        let zero = value.is_zero().unwrap_or(false);
        let negative = value.is_negative().unwrap_or(false);

        if zero || negative {
            return Err(NotPositive { value });
        }
        Ok(Self(value))
    }

    pub const fn inner(self) -> T {
        self.0
    }
}

impl<'de, T> Deserialize<'de> for Positive<T>
where
    T: Deserialize<'de> + HasZero + Into<Float> + Debug,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = T::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Positive<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Positive<FractionalShares> {
    /// Converts to whole shares count, returning error if value has a
    /// fractional part or exceeds u64 range. Use this when the target
    /// API does not support fractional shares.
    ///
    /// # Errors
    ///
    /// Returns [`ToWholeSharesError::Fractional`] if the value has a
    /// fractional part, or [`ToWholeSharesError::Overflow`] if it
    /// exceeds `u64` range.
    pub fn to_whole_shares(self) -> Result<u64, ToWholeSharesError> {
        let inner = self.inner();
        if !inner.is_whole()? {
            return Err(ToWholeSharesError::Fractional(inner));
        }

        let formatted = inner.inner().format().map_err(ToWholeSharesError::Float)?;

        let integer_str = formatted.split('.').next().unwrap_or(&formatted);
        integer_str
            .parse::<u64>()
            .map_err(|_| ToWholeSharesError::Overflow(inner))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToWholeSharesError {
    #[error("Cannot convert fractional shares {0} to whole shares")]
    Fractional(FractionalShares),
    #[error("Shares value {0} exceeds u64 range")]
    Overflow(FractionalShares),
    #[error("Float operation failed: {0}")]
    Float(FloatError),
}

impl From<FloatError> for ToWholeSharesError {
    fn from(error: FloatError) -> Self {
        Self::Float(error)
    }
}

#[cfg(test)]
mod tests {
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn positive_rejects_zero() {
        let error = Positive::new(FractionalShares::ZERO).unwrap_err();
        assert_eq!(error.value, FractionalShares::ZERO);
    }

    #[test]
    fn positive_rejects_negative() {
        let negative = FractionalShares::new(float!(-1));
        let error = Positive::new(negative).unwrap_err();
        assert_eq!(error.value, negative);
    }

    #[test]
    fn positive_accepts_positive_value() {
        let value = FractionalShares::new(float!(1));
        let positive = Positive::new(value).unwrap();
        assert_eq!(positive.inner(), value);
    }

    #[test]
    fn positive_deserialize_rejects_zero() {
        let result: Result<Positive<FractionalShares>, _> = serde_json::from_str("\"0\"");
        let error = result.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("positive"),
            "expected 'positive' in error message, got: {error}"
        );
    }

    #[test]
    fn positive_deserialize_rejects_negative() {
        let result: Result<Positive<FractionalShares>, _> = serde_json::from_str("\"-1\"");
        let error = result.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("positive"),
            "expected 'positive' in error message, got: {error}"
        );
    }

    #[test]
    fn positive_deserialize_accepts_positive() {
        let positive: Positive<FractionalShares> = serde_json::from_str("\"5.5\"").unwrap();
        assert!(positive.inner().inner().eq(float!(5.5)).unwrap());
    }

    #[test]
    fn to_whole_shares_succeeds_for_whole_number() {
        let positive = Positive::new(FractionalShares::new(float!(42))).unwrap();
        assert_eq!(positive.to_whole_shares().unwrap(), 42);
    }

    #[test]
    fn to_whole_shares_rejects_fractional() {
        let positive = Positive::new(FractionalShares::new(float!(1.5))).unwrap();
        let error = positive.to_whole_shares().unwrap_err();
        assert!(matches!(error, ToWholeSharesError::Fractional(_)));
    }

    #[test]
    fn float_from_raw_all_bytes_are_valid() {
        use alloy_primitives::B256;

        // Float is a dense encoding: 224-bit signed coefficient (high bytes)
        // + 32-bit signed exponent (low bytes). Every possible B256 value
        // maps to a valid float — there are no invalid bit patterns.
        // This means from_raw can never produce a value that fails basic
        // operations like formatting or comparison.
        let patterns: Vec<B256> = vec![
            B256::from([0xff; 32]),
            B256::from([0x00; 32]),
            B256::from([0x80; 32]),
            B256::from([0xde; 32]),
        ];

        for bytes in patterns {
            let raw = Float::from_raw(bytes);
            assert_eq!(raw.get_inner(), bytes);

            // All basic operations succeed on any raw bytes.
            raw.format().unwrap();
            raw.is_zero().unwrap();
            raw.abs().unwrap();
            (raw + float!(0)).unwrap();
        }
    }
}
