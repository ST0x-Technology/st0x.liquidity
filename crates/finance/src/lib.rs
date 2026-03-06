//! Shared financial primitives used across the st0x workspace.
//!
//! This is a leaf crate with zero workspace dependencies, providing
//! domain types that multiple crates need: `Symbol`, `FractionalShares`,
//! `Usdc`, `Positive`, `HasZero`, `ArithmeticError`, and `Id<Tag>`.

mod id;
mod shares;
mod symbol;
mod usdc;

pub use id::Id;
pub use shares::FractionalShares;
pub use symbol::{EmptySymbolError, Symbol};
pub use usdc::Usdc;

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

/// Checked arithmetic overflow error preserving both operands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub struct ArithmeticError<T> {
    pub operation: String,
    pub lhs: T,
    pub rhs: T,
}

/// Value must be positive (greater than zero).
#[derive(Debug, Clone, thiserror::Error)]
#[error("value must be positive, got {value:?}")]
pub struct NotPositive<T: Debug> {
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
    pub fn new(value: T) -> Result<Self, NotPositive<T>> {
        if value <= T::ZERO {
            return Err(NotPositive { value });
        }
        Ok(Self(value))
    }

    pub fn inner(self) -> T {
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
