//! Shared serde helpers for Alpaca wallet API payloads.

use rain_math_float::Float;
use serde::{Deserialize, Deserializer};

pub(super) fn deserialize_float_from_string<'de, D>(deserializer: D) -> Result<Float, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Float::parse(raw).map_err(serde::de::Error::custom)
}

pub(super) fn serialize_float_as_string<S>(
    value: &Float,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let formatted = value
        .format_with_scientific(false)
        .map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&formatted)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use st0x_float_macro::float;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct FloatFromString {
        #[serde(rename = "value", deserialize_with = "deserialize_float_from_string")]
        _value: Float,
    }

    #[derive(Debug, Serialize)]
    struct FloatAsString {
        #[serde(serialize_with = "serialize_float_as_string")]
        value: Float,
    }

    #[test]
    fn deserialize_float_from_string_rejects_invalid() {
        let error =
            serde_json::from_str::<FloatFromString>(r#"{"value":"not-a-number"}"#).unwrap_err();

        assert!(
            error.to_string().contains("error"),
            "Expected error message, got: {error}"
        );
    }

    #[test]
    fn serialize_float_as_string_emits_json_string() {
        let value = FloatAsString {
            value: float!(1250.75),
        };

        let json = serde_json::to_string(&value).unwrap();

        assert_eq!(json, r#"{"value":"1250.75"}"#);
    }
}
