//! Serde helpers for `rain_math_float::Float`.
//!
//! Provides serialization as decimal strings and deserialization from
//! JSON strings, numbers, or hex-encoded Float values.

use rain_math_float::Float;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;

/// Format a Float value as a decimal string for display/logging purposes.
/// Falls back to debug representation on error.
pub fn format_float_with_fallback(value: &Float) -> String {
    value
        .format_with_scientific(false)
        .unwrap_or_else(|_| format!("{value:?}"))
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
    let formatted = value
        .format_with_scientific(false)
        .map_err(serde::ser::Error::custom)?;
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
}
