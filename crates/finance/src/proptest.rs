//! Proptest strategies for generating arbitrary [`Float`] values.
//!
//! Available behind the `test-support` feature flag for use in
//! property tests of any crate that depends on `st0x-finance`.

use proptest::prelude::Strategy;
use proptest::prelude::*;
use rain_math_float::Float;

/// Strategy that generates arbitrary [`Float`] values from random
/// `(mantissa, scale)` pairs, formatted as decimal strings and parsed.
///
/// Filters out values that fail [`Float::parse`].
pub fn arb_float() -> impl Strategy<Value = Float> {
    (any::<i64>(), 0u32..=10).prop_filter_map("Float::parse must succeed", |(mantissa, scale)| {
        let divisor = 10i64.checked_pow(scale).unwrap_or(1);
        let integer_part = mantissa / divisor;
        let frac_part = (mantissa % divisor).unsigned_abs();

        let value_str = format!(
            "{integer_part}.{frac_part:0>width$}",
            width = scale as usize
        );
        Float::parse(value_str).ok()
    })
}
