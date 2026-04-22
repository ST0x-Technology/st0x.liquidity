//! Serde helpers for `rain_math_float::Float`.
//!
//! Provides serialization as decimal strings and deserialization from
//! JSON strings, numbers, or hex-encoded Float values.

use rain_math_float::Float;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;

/// Format a Float as a decimal string, falling back to scientific
/// notation when the exponent exceeds the non-scientific formatter's
/// range.
///
/// Rain Float's `format_with_scientific(false)` rejects exponents
/// below -76. Rather than losing data, we fall back to scientific
/// notation (e.g., `"1.23e-5"`), which the parser handles natively.
pub fn format_float(value: &Float) -> Result<String, rain_math_float::FloatError> {
    value
        .format_with_scientific(false)
        .or_else(|_| value.format_with_scientific(true))
}

/// Format a Float value as a decimal string for display/logging purposes.
/// Falls back to debug representation on error.
pub fn format_float_with_fallback(value: &Float) -> String {
    format_float(value).unwrap_or_else(|_| format!("{value:?}"))
}

/// Wrapper for formatting a `Float` as decimal in `Debug` output.
///
/// Use in manual `Debug` impls to avoid hex representation:
/// ```ignore
/// .field("price", &DebugFloat(&self.price))
/// ```
pub struct DebugFloat<'a>(pub &'a Float);

impl std::fmt::Debug for DebugFloat<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_float_with_fallback(self.0))
    }
}

/// Wrapper for formatting an `Option<Float>` as decimal in `Debug` output.
pub struct DebugOptionFloat<'a>(pub &'a Option<Float>);

impl std::fmt::Debug for DebugOptionFloat<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(value) => write!(f, "Some({})", format_float_with_fallback(value)),
            None => write!(f, "None"),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum FloatSerdeInput {
    String(String),
    Number(serde_json::Number),
}

pub fn parse_float_string_or_hex(value: &str) -> Result<Float, rain_math_float::FloatError> {
    if let Ok(float) = Float::parse(value.to_string()) {
        return Ok(float);
    }

    Float::from_hex(value)
}

pub fn serialize_float_as_string<S>(value: &Float, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let formatted = format_float(value).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&formatted)
}

pub fn deserialize_float_from_number_or_string<'de, D>(deserializer: D) -> Result<Float, D::Error>
where
    D: Deserializer<'de>,
{
    match FloatSerdeInput::deserialize(deserializer)? {
        FloatSerdeInput::String(value) => {
            parse_float_string_or_hex(&value).map_err(serde::de::Error::custom)
        }
        FloatSerdeInput::Number(value) => {
            Float::parse(value.to_string()).map_err(serde::de::Error::custom)
        }
    }
}

pub fn deserialize_option_float_from_number_or_string<'de, D>(
    deserializer: D,
) -> Result<Option<Float>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<FloatSerdeInput>::deserialize(deserializer)?
        .map(|value| match value {
            FloatSerdeInput::String(value) => {
                parse_float_string_or_hex(&value).map_err(serde::de::Error::custom)
            }
            FloatSerdeInput::Number(value) => {
                Float::parse(value.to_string()).map_err(serde::de::Error::custom)
            }
        })
        .transpose()
}

pub fn serialize_option_float<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Borrow<Option<Float>>,
{
    option_float_string_serde::serialize(value, serializer)
}

pub struct FloatDisplay<'a>(pub &'a Float);

impl Serialize for FloatDisplay<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_float_as_string(self.0, serializer)
    }
}

pub mod float_string_serde {
    use super::{Deserializer, Float, Serializer};
    use super::{deserialize_float_from_number_or_string, serialize_float_as_string};

    pub fn serialize<S>(value: &Float, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_float_as_string(value, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Float, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_float_from_number_or_string(deserializer)
    }
}

pub mod option_float_string_serde {
    use super::{Deserializer, Float, Serializer};
    use super::{deserialize_option_float_from_number_or_string, serialize_float_as_string};

    pub fn serialize<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: super::Borrow<Option<Float>>,
    {
        match value.borrow().as_ref() {
            Some(float) => serialize_float_as_string(float, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Float>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_option_float_from_number_or_string(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[derive(Serialize)]
    struct SerializeFloat {
        #[serde(serialize_with = "serialize_float_as_string")]
        value: Float,
    }

    #[derive(Deserialize)]
    struct DeserializeFloat {
        #[serde(deserialize_with = "deserialize_float_from_number_or_string")]
        value: Float,
    }

    #[derive(Deserialize)]
    struct DeserializeOptionalFloat {
        #[serde(
            default,
            deserialize_with = "deserialize_option_float_from_number_or_string"
        )]
        value: Option<Float>,
    }

    #[derive(Serialize, Deserialize)]
    struct WithSerdeModule {
        #[serde(with = "float_string_serde")]
        value: Float,
    }

    #[derive(Serialize, Deserialize)]
    struct WithOptionalSerdeModule {
        #[serde(default, with = "option_float_string_serde")]
        value: Option<Float>,
    }

    #[test]
    fn serialize_float_as_string_outputs_decimal_text() {
        let payload = SerializeFloat {
            value: Float::parse("12.5".to_string()).unwrap(),
        };
        let json = serde_json::to_value(payload).unwrap();
        assert_eq!(json["value"], json!("12.5"));
    }

    #[test]
    fn deserialize_float_accepts_string_number_and_hex() {
        let from_string: DeserializeFloat =
            serde_json::from_value(json!({"value": "12.5"})).unwrap();
        assert!(
            from_string
                .value
                .eq(Float::parse("12.5".to_string()).unwrap())
                .unwrap()
        );

        let from_number: DeserializeFloat = serde_json::from_value(json!({"value": 12.5})).unwrap();
        assert!(
            from_number
                .value
                .eq(Float::parse("12.5".to_string()).unwrap())
                .unwrap()
        );

        let from_hex: DeserializeFloat = serde_json::from_value(
            json!({"value": Float::parse("12.5".to_string()).unwrap().as_hex()}),
        )
        .unwrap();
        assert!(
            from_hex
                .value
                .eq(Float::parse("12.5".to_string()).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn deserialize_optional_float_supports_none() {
        let payload: DeserializeOptionalFloat = serde_json::from_value(json!({})).unwrap();
        assert!(payload.value.is_none());

        let payload: DeserializeOptionalFloat =
            serde_json::from_value(json!({"value": null})).unwrap();
        assert!(payload.value.is_none());
    }

    #[test]
    fn float_string_serde_round_trips() {
        let payload = WithSerdeModule {
            value: Float::parse("12.5".to_string()).unwrap(),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["value"], json!("12.5"));

        let parsed: WithSerdeModule = serde_json::from_value(json!({"value": "12.5"})).unwrap();
        assert!(
            parsed
                .value
                .eq(Float::parse("12.5".to_string()).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn option_float_string_serde_handles_null_and_values() {
        let payload = WithOptionalSerdeModule {
            value: Some(Float::parse("12.5".to_string()).unwrap()),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["value"], json!("12.5"));

        let parsed: WithOptionalSerdeModule =
            serde_json::from_value(json!({"value": "12.5"})).unwrap();
        assert!(
            parsed
                .value
                .unwrap()
                .eq(Float::parse("12.5".to_string()).unwrap())
                .unwrap()
        );

        let parsed: WithOptionalSerdeModule = serde_json::from_value(json!({})).unwrap();
        assert!(parsed.value.is_none());

        let parsed: WithOptionalSerdeModule =
            serde_json::from_value(json!({"value": null})).unwrap();
        assert!(parsed.value.is_none());
    }

    /// Construct a Float from raw coefficient and exponent by packing
    /// them into the B256 layout: upper 32 bits = exponent (int32),
    /// lower 224 bits = coefficient (int224).
    fn pack_raw(coefficient: i64, exponent: i32) -> Float {
        use alloy::primitives::B256;

        let mut bytes = [0u8; 32];

        // Exponent in the top 4 bytes (big-endian).
        bytes[..4].copy_from_slice(&exponent.to_be_bytes());

        // Coefficient in the lower 28 bytes (big-endian, sign-extended).
        let coeff_bytes = coefficient.to_be_bytes();
        let fill = if coefficient < 0 { 0xFF } else { 0x00 };
        bytes[4..24].fill(fill);
        bytes[24..32].copy_from_slice(&coeff_bytes);

        Float::from_raw(B256::from(bytes))
    }

    #[test]
    fn serialize_float_with_extreme_exponent_uses_scientific_fallback() {
        // Construct a Float with exponent -77, which exceeds the
        // non-scientific formatter's -76 limit. This reproduces the
        // crash from RAI-218: accumulated Float arithmetic can produce
        // such exponents through catastrophic cancellation.
        let float = pack_raw(9_999_999_910_959_448, -77);

        // Non-scientific formatting should fail for this exponent.
        assert!(
            float.format_with_scientific(false).is_err(),
            "Expected non-scientific format to fail for exponent -77"
        );

        // But our format_float should succeed via scientific fallback.
        let formatted = format_float(&float).unwrap();
        assert!(
            !formatted.is_empty(),
            "format_float should produce output for exponent -77"
        );

        // The formatted string should roundtrip through parse.
        let roundtripped = Float::parse(formatted.clone()).unwrap();
        assert!(
            roundtripped.eq(float).unwrap(),
            "Roundtrip failed: formatted as '{formatted}', parsed back to different value"
        );
    }

    #[test]
    fn serialize_float_with_extreme_exponent_roundtrips_through_serde() {
        let float = pack_raw(9_999_999_910_959_448, -77);

        let payload = SerializeFloat { value: float };
        let json = serde_json::to_value(payload).unwrap();

        // Should serialize without error (previously panicked).
        let serialized = json["value"].as_str().unwrap();
        assert!(!serialized.is_empty());

        // Should deserialize back to the same value.
        let parsed: DeserializeFloat = serde_json::from_value(json).unwrap();
        assert!(
            parsed.value.eq(float).unwrap(),
            "Serde roundtrip failed for Float with exponent -77"
        );
    }

    /// Proves the upstream Rain Float bug: adding two values whose
    /// magnitudes nearly cancel produces a Float that the non-scientific
    /// formatter cannot handle (UnformatableExponent).
    ///
    /// These are the exact values from the production crash (staging
    /// logs, event 605 applied to position view version 604):
    ///   net position = -0.09999999910959448  (17 decimal places)
    ///   hedge fill   =  0.099999999          (9 decimal places)
    ///   sum          = -0.00000000010959448  (~-1.1e-10)
    ///
    /// Both inputs format fine individually. Their sum is a valid tiny
    /// number, but its internal Float representation has exponent -77
    /// because `add` inflates both inputs via `maximizeFull` (adding
    /// trailing zeros), and after cancellation `packLossy` doesn't
    /// strip them.
    #[test]
    fn addition_of_near_cancelling_values_produces_unformattable_float() {
        let net_position = Float::parse("-0.09999999910959448".to_string()).unwrap();
        let hedge_fill = Float::parse("0.099999999".to_string()).unwrap();

        // Both inputs format fine individually.
        net_position.format_with_scientific(false).unwrap();
        hedge_fill.format_with_scientific(false).unwrap();

        // Their sum is a valid, tiny number (~-1.1e-10).
        let result = (net_position + hedge_fill).unwrap();

        // Scientific notation works — the value is real and finite.
        let scientific = result.format_with_scientific(true).unwrap();
        assert!(
            !scientific.is_empty(),
            "Scientific format should work for the sum"
        );

        // But non-scientific formatting crashes — this is the upstream bug.
        assert!(
            result.format_with_scientific(false).is_err(),
            "Expected non-scientific format to fail for near-cancellation \
             result. Rain Float's add produces exponent -77 from \
             maximizeFull trailing zeros that packLossy doesn't strip. \
             Scientific result: {scientific}"
        );

        // Our format_float works around this via scientific fallback.
        let formatted = format_float(&result).unwrap();
        let roundtripped = Float::parse(formatted.clone()).unwrap();
        assert!(
            roundtripped.eq(result).unwrap(),
            "Roundtrip failed: '{formatted}'"
        );
    }

    /// Verify that smaller numbers produce even worse exponents after
    /// near-cancellation. If 0.0999... → exponent -77, then 0.00999...
    /// should → -78 and 0.000999... → -79, since maximizeFull inflates
    /// by more powers of 10 for smaller starting coefficients.
    #[test]
    fn smaller_magnitudes_produce_worse_exponents() {
        // Each pair: a negative value and a slightly smaller positive value,
        // so their sum is a tiny residual that triggers the formatter bug.

        // ~0.01 magnitude: expect exponent -78
        let hundredths = (Float::parse("-0.0099999991".to_string()).unwrap()
            + Float::parse("0.009999999".to_string()).unwrap())
        .unwrap();
        assert!(
            hundredths.format_with_scientific(false).is_err(),
            "Expected exponent -78 to fail non-scientific format. \
             Scientific: {}",
            hundredths.format_with_scientific(true).unwrap()
        );

        // ~0.001 magnitude: expect exponent -79
        let thousandths = (Float::parse("-0.00099999991".to_string()).unwrap()
            + Float::parse("0.0009999999".to_string()).unwrap())
        .unwrap();
        assert!(
            thousandths.format_with_scientific(false).is_err(),
            "Expected exponent -79 to fail non-scientific format. \
             Scientific: {}",
            thousandths.format_with_scientific(true).unwrap()
        );

        // All should work with our format_float fallback.
        format_float(&hundredths).unwrap();
        format_float(&thousandths).unwrap();
    }

    #[test]
    fn serialize_normal_float_still_uses_decimal_format() {
        // Normal values should still produce clean decimal strings,
        // not scientific notation.
        let formatted = format_float(&Float::parse("72.5".to_string()).unwrap()).unwrap();
        assert_eq!(formatted, "72.5");

        let formatted = format_float(&Float::parse("0.1".to_string()).unwrap()).unwrap();
        assert_eq!(formatted, "0.1");
    }

    #[test]
    fn debug_float_formats_as_decimal() {
        let value = Float::parse("12.5".to_string()).unwrap();
        let output = format!("{:?}", DebugFloat(&value));
        assert_eq!(output, "12.5");
    }

    #[test]
    fn debug_option_float_formats_some_as_decimal() {
        let value = Some(Float::parse("42".to_string()).unwrap());
        let output = format!("{:?}", DebugOptionFloat(&value));
        assert_eq!(output, "Some(42)");
    }

    #[test]
    fn debug_option_float_formats_none() {
        let value: Option<Float> = None;
        let output = format!("{:?}", DebugOptionFloat(&value));
        assert_eq!(output, "None");
    }
}
