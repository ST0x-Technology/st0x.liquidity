//! Live dashboard equity-price DTOs.

use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use st0x_finance::Symbol;
use st0x_float_serde::float_string_serde;

/// Latest pricing-service state for one configured equity.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct EquityPrice {
    #[ts(type = "string")]
    pub symbol: Symbol,
    pub status: EquityPriceStatus,
}

/// A price is either currently usable or explicitly unavailable.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EquityPriceStatus {
    Available {
        #[serde(with = "float_string_serde")]
        #[ts(type = "string")]
        price_usd: Float,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    },
    Unavailable,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn available_price_serializes_as_a_decimal_string() {
        let observed_at = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        let expires_at = Utc.timestamp_millis_opt(1_700_000_030_000).unwrap();
        let price = EquityPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            status: EquityPriceStatus::Available {
                price_usd: float!(187.25),
                observed_at,
                expires_at,
            },
        };

        let value = serde_json::to_value(price).unwrap();

        assert_eq!(value["symbol"], json!("AAPL"));
        assert_eq!(value["status"]["status"], json!("available"));
        assert_eq!(value["status"]["priceUsd"], json!("187.25"));
        assert_eq!(value["status"]["observedAt"], json!("2023-11-14T22:13:20Z"));
        assert_eq!(value["status"]["expiresAt"], json!("2023-11-14T22:13:50Z"));
    }

    #[test]
    fn unavailable_price_has_no_fabricated_value() {
        let price = EquityPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            status: EquityPriceStatus::Unavailable,
        };

        let value = serde_json::to_value(price).unwrap();

        assert_eq!(value["status"], json!({ "status": "unavailable" }));
    }
}
