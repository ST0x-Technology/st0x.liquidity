//! Proptest strategies for generating arbitrary [`Float`] values.
//!
//! Available behind the `test-support` feature flag for use in
//! property tests of any crate that depends on `st0x-finance`.

use proptest::prelude::*;
use rain_math_float::Float;

/// Strategy that generates arbitrary [`Float`] values from random
/// `(mantissa, scale)` pairs, formatted as decimal strings and parsed.
///
/// Filters out values that fail [`Float::parse`].
pub fn arb_float() -> impl Strategy<Value = Float> {
    (any::<i64>(), 0u32..=10).prop_filter_map("Float::parse must succeed", |(mantissa, scale)| {
        let divisor = 10u64.checked_pow(scale).unwrap_or(1);
        let abs_mantissa = mantissa.unsigned_abs();
        let integer_part = abs_mantissa / divisor;
        let frac_part = abs_mantissa % divisor;
        let sign = if mantissa.is_negative() { "-" } else { "" };

        let value_str = if scale == 0 {
            format!("{sign}{integer_part}")
        } else {
            format!(
                "{sign}{integer_part}.{frac_part:0>width$}",
                width = scale as usize
            )
        };
        Float::parse(value_str).ok()
    })
}
