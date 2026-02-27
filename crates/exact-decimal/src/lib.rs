//! Exact decimal floating-point type backed by Rain's `Float`.
//!
//! `ExactDecimal` wraps `rain_math_float::Float` (224-bit coefficient + 32-bit
//! exponent) and provides the standard Rust trait implementations that `Float`
//! itself cannot offer because its operations are fallible EVM calls.
//!
//! Key properties:
//! - **No precision loss** on values that originate from onchain `Float` data.
//! - **Backward-compatible serde**: serializes as a decimal string (`"1.5"`),
//!   deserializes from both decimal strings and hex B256 strings.
//! - **`PartialEq`/`Eq`/`Ord`** via `Float`'s EVM-based comparison (panics
//!   only on malformed B256 data that cannot occur through valid construction).

use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;

use alloy::primitives::{B256, U256};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};

/// Exact decimal floating-point value using Rain's Float
/// (224-bit coefficient + 32-bit exponent).
///
/// Provides `PartialEq`/`Eq`/`Ord` via Float's EVM-based comparison and
/// custom serde as decimal strings for backward compatibility with existing
/// event payloads that stored `rust_decimal::Decimal` as `"1.5"`.
#[derive(Clone, Copy)]
pub struct ExactDecimal(Float);

/// Unwraps a Float comparison result. Float comparisons fail only on malformed
/// B256 values, which cannot occur through valid construction paths.
fn unwrap_comparison(result: Result<bool, FloatError>) -> bool {
    match result {
        Ok(value) => value,
        Err(error) => panic!("ExactDecimal comparison failed (corrupted Float data): {error}"),
    }
}

impl ExactDecimal {
    pub fn new(float: Float) -> Self {
        Self(float)
    }

    /// Parse a decimal string into an `ExactDecimal`.
    pub fn parse(value: &str) -> Result<Self, FloatError> {
        Float::parse(value.to_string()).map(Self)
    }

    /// Convert from a fixed-point decimal representation.
    ///
    /// E.g., `from_fixed_decimal(U256::from(1_500_000), 6)` produces `1.5`.
    pub fn from_fixed_decimal(value: U256, decimals: u8) -> Result<Self, FloatError> {
        Float::from_fixed_decimal(value, decimals).map(Self)
    }

    /// Convert to a fixed-point decimal representation (lossless).
    ///
    /// E.g., for an `ExactDecimal` of `1.5`, `to_fixed_decimal(6)` returns
    /// `U256::from(1_500_000)`.
    ///
    /// Returns an error if the conversion would lose precision. Use
    /// [`to_fixed_decimal_lossy`](Self::to_fixed_decimal_lossy) when
    /// truncation is acceptable.
    pub fn to_fixed_decimal(self, decimals: u8) -> Result<U256, FloatError> {
        self.0.to_fixed_decimal(decimals)
    }

    /// Convert to a fixed-point decimal representation, truncating any
    /// digits beyond the requested precision.
    ///
    /// Returns `(value, lossless)` where `lossless` is `false` when
    /// truncation occurred.
    pub fn to_fixed_decimal_lossy(self, decimals: u8) -> Result<(U256, bool), FloatError> {
        self.0.to_fixed_decimal_lossy(decimals)
    }

    /// Construct from a raw B256 (onchain Float representation).
    pub fn from_raw(value: B256) -> Self {
        Self(Float::from_raw(value))
    }

    /// Returns the zero value.
    pub const fn zero() -> Self {
        Self(Float::from_raw(B256::ZERO))
    }

    pub fn is_zero(self) -> Result<bool, FloatError> {
        self.0.is_zero()
    }

    pub fn is_negative(self) -> Result<bool, FloatError> {
        let zero = Float::zero()?;
        self.0.lt(zero)
    }

    pub fn abs(self) -> Result<Self, FloatError> {
        self.0.abs().map(Self)
    }

    /// Returns the integer part (truncation toward zero).
    pub fn integer(self) -> Result<Self, FloatError> {
        self.0.integer().map(Self)
    }

    /// Returns the fractional part.
    pub fn frac(self) -> Result<Self, FloatError> {
        self.0.frac().map(Self)
    }

    /// Truncate to `dp` decimal places (toward zero).
    ///
    /// Uses a fixed-decimal roundtrip: convert to fixed-point with `dp`
    /// decimal digits (which truncates), then convert back.
    pub fn round_dp(self, dp: u8) -> Result<Self, FloatError> {
        let (fixed, _lossless) = self.0.to_fixed_decimal_lossy(dp)?;
        Float::from_fixed_decimal(fixed, dp).map(Self)
    }

    /// Escape hatch for direct Float operations.
    pub fn inner(self) -> Float {
        self.0
    }

    /// Returns the raw B256 representation.
    pub fn to_raw(self) -> B256 {
        self.0.get_inner()
    }

    /// Format as a decimal string (never scientific notation).
    pub fn format_decimal(self) -> Result<String, FloatError> {
        self.0.format_with_scientific(false)
    }
}

impl Debug for ExactDecimal {
    fn fmt(&self, dest: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.format_with_scientific(false) {
            Ok(text) => write!(dest, "ExactDecimal({text})"),
            Err(error) => write!(dest, "ExactDecimal(<format error: {error}>)"),
        }
    }
}

impl Display for ExactDecimal {
    fn fmt(&self, dest: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.format_with_scientific(false) {
            Ok(text) => write!(dest, "{text}"),
            Err(error) => write!(dest, "<format error: {error}>"),
        }
    }
}

impl FromStr for ExactDecimal {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Default for ExactDecimal {
    fn default() -> Self {
        Self::zero()
    }
}

impl Hash for ExactDecimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for ExactDecimal {
    fn eq(&self, other: &Self) -> bool {
        unwrap_comparison(self.0.eq(other.0))
    }
}

impl Eq for ExactDecimal {}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        if unwrap_comparison(self.0.lt(other.0)) {
            Ordering::Less
        } else if unwrap_comparison(self.0.eq(other.0)) {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }
}

impl Serialize for ExactDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let formatted = self
            .0
            .format_with_scientific(false)
            .map_err(serde::ser::Error::custom)?;

        serializer.serialize_str(&formatted)
    }
}

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;

        // Try decimal string first (backward compat with existing "1.5" format)
        if let Ok(float) = Float::parse(value.clone()) {
            return Ok(Self(float));
        }

        // Fall back to hex B256 string ("0xffff...")
        Float::from_hex(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl std::ops::Add for ExactDecimal {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl std::ops::Sub for ExactDecimal {
    type Output = Result<Self, FloatError>;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).map(Self)
    }
}

impl std::ops::Mul for ExactDecimal {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Self) -> Self::Output {
        (self.0 * rhs.0).map(Self)
    }
}

impl std::ops::Div for ExactDecimal {
    type Output = Result<Self, FloatError>;

    fn div(self, rhs: Self) -> Self::Output {
        (self.0 / rhs.0).map(Self)
    }
}

impl std::ops::Neg for ExactDecimal {
    type Output = Result<Self, FloatError>;

    fn neg(self) -> Self::Output {
        (-self.0).map(Self)
    }
}

impl From<Float> for ExactDecimal {
    fn from(float: Float) -> Self {
        Self(float)
    }
}

impl From<ExactDecimal> for Float {
    fn from(exact: ExactDecimal) -> Self {
        exact.0
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn parse_and_display_roundtrip() {
        let original = "1.5";
        let exact = ExactDecimal::parse(original).unwrap();
        assert_eq!(exact.to_string(), "1.5");
    }

    #[test]
    fn parse_zero() {
        let exact = ExactDecimal::parse("0").unwrap();
        assert_eq!(exact.to_string(), "0");
        assert!(exact.is_zero().unwrap());
    }

    #[test]
    fn parse_negative() {
        let exact = ExactDecimal::parse("-3.14").unwrap();
        assert_eq!(exact.to_string(), "-3.14");
        assert!(exact.is_negative().unwrap());
    }

    #[test]
    fn parse_integer() {
        let exact = ExactDecimal::parse("42").unwrap();
        assert_eq!(exact.to_string(), "42");
    }

    #[test]
    fn serde_roundtrip_decimal_string() {
        let exact = ExactDecimal::parse("1.5").unwrap();
        let json = serde_json::to_string(&exact).unwrap();
        assert_eq!(json, "\"1.5\"");

        let deserialized: ExactDecimal = serde_json::from_str(&json).unwrap();
        assert_eq!(exact, deserialized);
    }

    #[test]
    fn serde_roundtrip_zero() {
        let exact = ExactDecimal::parse("0").unwrap();
        let json = serde_json::to_string(&exact).unwrap();
        assert_eq!(json, "\"0\"");

        let deserialized: ExactDecimal = serde_json::from_str(&json).unwrap();
        assert_eq!(exact, deserialized);
    }

    #[test]
    fn serde_roundtrip_negative() {
        let exact = ExactDecimal::parse("-3.14").unwrap();
        let json = serde_json::to_string(&exact).unwrap();
        assert_eq!(json, "\"-3.14\"");

        let deserialized: ExactDecimal = serde_json::from_str(&json).unwrap();
        assert_eq!(exact, deserialized);
    }

    #[test]
    fn deserialize_backward_compat_decimal_strings() {
        let test_cases = ["\"1.5\"", "\"0\"", "\"-3.14\"", "\"42\"", "\"0.000001\""];

        for json in test_cases {
            let result: Result<ExactDecimal, _> = serde_json::from_str(json);
            assert!(
                result.is_ok(),
                "Failed to deserialize backward-compat string: {json}"
            );
        }
    }

    #[test]
    fn deserialize_hex_b256_string() {
        let original = ExactDecimal::parse("1.5").unwrap();
        let hex = format!("\"{}\"", original.to_raw());

        let deserialized: ExactDecimal = serde_json::from_str(&hex).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn equality_logically_equal_values() {
        let value_a = ExactDecimal::parse("1.5").unwrap();
        let value_b = ExactDecimal::parse("1.5").unwrap();
        assert_eq!(value_a, value_b);
    }

    #[test]
    fn equality_different_values() {
        let value_a = ExactDecimal::parse("1.5").unwrap();
        let value_b = ExactDecimal::parse("2.5").unwrap();
        assert_ne!(value_a, value_b);
    }

    #[test]
    fn ordering() {
        let one = ExactDecimal::parse("1").unwrap();
        let two = ExactDecimal::parse("2").unwrap();
        let negative = ExactDecimal::parse("-1").unwrap();

        assert!(negative < one);
        assert!(one < two);
        assert!(two > one);
        assert!(one > negative);
    }

    #[test]
    fn add_basic() {
        let value_a = ExactDecimal::parse("1.5").unwrap();
        let value_b = ExactDecimal::parse("2.5").unwrap();
        let result = (value_a + value_b).unwrap();
        assert_eq!(result.to_string(), "4");
    }

    #[test]
    fn sub_basic() {
        let value_a = ExactDecimal::parse("5").unwrap();
        let value_b = ExactDecimal::parse("2").unwrap();
        let result = (value_a - value_b).unwrap();
        assert_eq!(result.to_string(), "3");
    }

    #[test]
    fn mul_basic() {
        let value_a = ExactDecimal::parse("2").unwrap();
        let value_b = ExactDecimal::parse("3").unwrap();
        let result = (value_a * value_b).unwrap();
        assert_eq!(result.to_string(), "6");
    }

    #[test]
    fn div_basic() {
        let value_a = ExactDecimal::parse("6").unwrap();
        let value_b = ExactDecimal::parse("2").unwrap();
        let result = (value_a / value_b).unwrap();
        assert_eq!(result.to_string(), "3");
    }

    #[test]
    fn neg_basic() {
        let value = ExactDecimal::parse("3.14").unwrap();
        let negated = (-value).unwrap();
        assert_eq!(negated.to_string(), "-3.14");
    }

    #[test]
    fn abs_negative() {
        let value = ExactDecimal::parse("-3.14").unwrap();
        let absolute = value.abs().unwrap();
        assert_eq!(absolute.to_string(), "3.14");
    }

    #[test]
    fn abs_positive() {
        let value = ExactDecimal::parse("3.14").unwrap();
        let absolute = value.abs().unwrap();
        assert_eq!(absolute.to_string(), "3.14");
    }

    #[test]
    fn from_fixed_decimal_roundtrip() {
        let original = U256::from(1_500_000_000_000_000_000u64);
        let exact = ExactDecimal::from_fixed_decimal(original, 18).unwrap();
        assert_eq!(exact.to_string(), "1.5");

        let back = exact.to_fixed_decimal(18).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn from_fixed_decimal_usdc() {
        let original = U256::from(1_500_000u64);
        let exact = ExactDecimal::from_fixed_decimal(original, 6).unwrap();
        assert_eq!(exact.to_string(), "1.5");

        let back = exact.to_fixed_decimal(6).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn from_raw_preserves_value() {
        let parsed = ExactDecimal::parse("7.5").unwrap();
        let raw = parsed.to_raw();
        let from_raw = ExactDecimal::from_raw(raw);
        assert_eq!(parsed, from_raw);
    }

    #[test]
    fn zero_is_zero() {
        let zero = ExactDecimal::zero();
        assert!(zero.is_zero().unwrap());
        assert!(!zero.is_negative().unwrap());
    }

    #[test]
    fn default_is_zero() {
        let default = ExactDecimal::default();
        let zero = ExactDecimal::zero();
        assert_eq!(default.to_string(), zero.to_string());
    }

    #[test]
    fn integer_and_frac_parts() {
        let value = ExactDecimal::parse("3.75").unwrap();
        let int_part = value.integer().unwrap();
        let frac_part = value.frac().unwrap();

        assert_eq!(int_part.to_string(), "3");
        assert_eq!(frac_part.to_string(), "0.75");
    }

    #[test]
    fn regression_7_5_shares_precision() {
        let shares_u256 = U256::from(7_500_000_000_000_000_000u64);
        let exact = ExactDecimal::from_fixed_decimal(shares_u256, 18).unwrap();

        assert_eq!(
            exact.to_string(),
            "7.5",
            "7.5 shares must not have precision artifacts"
        );

        let back = exact.to_fixed_decimal(18).unwrap();
        assert_eq!(back, shares_u256, "Roundtrip must be lossless");
    }

    proptest! {
        #[test]
        fn parse_format_roundtrip(
            integer in 0u64..1_000_000,
            fractional in 0u64..1_000_000,
        ) {
            let value_str = format!("{integer}.{fractional:06}");
            if let Ok(exact) = ExactDecimal::parse(&value_str) {
                let formatted = exact.to_string();
                if let Ok(reparsed) = ExactDecimal::parse(&formatted) {
                    prop_assert_eq!(exact, reparsed);
                }
            }
        }

        #[test]
        fn from_fixed_decimal_roundtrip_18(
            raw_value in 0u64..1_000_000_000_000_000_000u64,
        ) {
            let value = U256::from(raw_value);
            if let Ok(exact) = ExactDecimal::from_fixed_decimal(value, 18)
                && let Ok(back) = exact.to_fixed_decimal(18)
            {
                prop_assert_eq!(value, back);
            }
        }

        #[test]
        fn from_fixed_decimal_roundtrip_6(
            raw_value in 0u64..1_000_000_000_000u64,
        ) {
            let value = U256::from(raw_value);
            if let Ok(exact) = ExactDecimal::from_fixed_decimal(value, 6)
                && let Ok(back) = exact.to_fixed_decimal(6)
            {
                prop_assert_eq!(value, back);
            }
        }
    }
}
