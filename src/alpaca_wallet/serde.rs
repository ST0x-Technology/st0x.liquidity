//! Shared serde helpers for Alpaca wallet API payloads.

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

pub(super) fn deserialize_decimal_from_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    raw.parse::<Decimal>().map_err(serde::de::Error::custom)
}

pub(super) fn serialize_decimal_as_string<S>(
    value: &Decimal,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Deserialize)]
    struct DecimalFromString {
        #[serde(rename = "value", deserialize_with = "deserialize_decimal_from_string")]
        _value: Decimal,
    }

    #[derive(Debug, Serialize)]
    struct DecimalAsString {
        #[serde(serialize_with = "serialize_decimal_as_string")]
        value: Decimal,
    }

    #[test]
    fn deserialize_decimal_from_string_rejects_invalid_decimal() {
        let error =
            serde_json::from_str::<DecimalFromString>(r#"{"value":"not-a-decimal"}"#).unwrap_err();

        assert!(error.to_string().contains("Invalid decimal"));
    }

    #[test]
    fn serialize_decimal_as_string_emits_json_string() {
        let value = DecimalAsString {
            value: dec!(1250.75),
        };

        let json = serde_json::to_string(&value).unwrap();

        assert_eq!(json, r#"{"value":"1250.75"}"#);
    }
}
