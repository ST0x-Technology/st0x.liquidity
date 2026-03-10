//! Shared financial primitives used across the st0x workspace.
//!
//! This is a leaf crate with zero workspace dependencies, providing
//! domain types that multiple crates need: `Symbol`, `FractionalShares`,
//! `Usdc`, `Usd`, `Positive`, `HasZero`, `ArithmeticError`, and `Id<Tag>`.

mod id;
mod shares;
mod symbol;
mod usd;
mod usdc;

pub use id::{BlankIdError, Id};
pub use shares::FractionalShares;
pub use symbol::{EmptySymbolError, Symbol};
pub use usd::Usd;
pub use usdc::Usdc;
#[cfg(feature = "alloy")]
pub use usdc::UsdcConversionError;

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for types that have a zero value and can be compared to it.
pub trait HasZero: PartialOrd + Sized {
    const ZERO: Self;

    fn is_zero(&self) -> bool
    where
        Self: PartialEq,
    {
        self == &Self::ZERO
    }

    fn is_negative(&self) -> bool {
        self < &Self::ZERO
    }
}

/// Which arithmetic operation overflowed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ArithmeticOperation {
    Add,
    Sub,
    Mul,
}

impl std::fmt::Display for ArithmeticOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Mul => write!(f, "*"),
        }
    }
}

/// Checked arithmetic overflow error preserving both operands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub struct ArithmeticError<T> {
    pub operation: ArithmeticOperation,
    pub lhs: T,
    pub rhs: T,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct Positive<T>(T);

impl<T> Positive<T>
where
    T: PartialOrd + HasZero + Copy + Debug,
{
    /// # Errors
    ///
    /// Returns [`NotPositive`] if `value` is zero or negative.
    pub fn new(value: T) -> Result<Self, NotPositive<T>> {
        if value <= T::ZERO {
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
    T: Deserialize<'de> + PartialOrd + HasZero + Copy + Debug,
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
    pub const ONE: Self = Self(FractionalShares::ONE);

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
        if !inner.is_whole() {
            return Err(ToWholeSharesError::Fractional(inner));
        }

        inner
            .inner()
            .to_u64()
            .ok_or(ToWholeSharesError::Overflow(inner))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ToWholeSharesError {
    #[error("Cannot convert fractional shares {0} to whole shares")]
    Fractional(FractionalShares),
    #[error("Shares value {0} exceeds u64 range")]
    Overflow(FractionalShares),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn positive_rejects_zero() {
        let error = Positive::new(FractionalShares::ZERO).unwrap_err();
        assert_eq!(error.value, FractionalShares::ZERO);
    }

    #[test]
    fn positive_rejects_negative() {
        let negative = FractionalShares::new(Decimal::NEGATIVE_ONE);
        let error = Positive::new(negative).unwrap_err();
        assert_eq!(error.value, negative);
    }

    #[test]
    fn positive_accepts_positive_value() {
        let value = FractionalShares::new(Decimal::ONE);
        let positive = Positive::new(value).unwrap();
        assert_eq!(positive.inner(), value);
    }

    #[test]
    fn positive_deserialize_rejects_zero() {
        let result: Result<Positive<FractionalShares>, _> = serde_json::from_str("0");
        let error = result.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("positive"),
            "expected 'positive' in error message, got: {error}"
        );
    }

    #[test]
    fn positive_deserialize_rejects_negative() {
        let result: Result<Positive<FractionalShares>, _> = serde_json::from_str("-1");
        let error = result.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("positive"),
            "expected 'positive' in error message, got: {error}"
        );
    }

    #[test]
    fn positive_deserialize_accepts_positive() {
        let positive: Positive<FractionalShares> = serde_json::from_str("5.5").unwrap();
        assert_eq!(positive.inner(), FractionalShares::new(dec!(5.5)));
    }

    #[test]
    fn to_whole_shares_succeeds_for_whole_number() {
        let positive = Positive::new(FractionalShares::new(dec!(42))).unwrap();
        assert_eq!(positive.to_whole_shares().unwrap(), 42);
    }

    #[test]
    fn to_whole_shares_rejects_fractional() {
        let positive = Positive::new(FractionalShares::new(dec!(1.5))).unwrap();
        let error = positive.to_whole_shares().unwrap_err();
        assert!(matches!(error, ToWholeSharesError::Fractional(_)));
    }

    #[test]
    fn to_whole_shares_rejects_overflow() {
        let huge = FractionalShares::new(Decimal::MAX);
        let positive = Positive::new(huge).unwrap();
        let error = positive.to_whole_shares().unwrap_err();
        assert!(matches!(error, ToWholeSharesError::Overflow(_)));
    }
}
