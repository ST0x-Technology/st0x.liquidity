//! Trade fill DTOs for completed onchain and offchain trades.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use st0x_finance::{FractionalShares, Symbol};

/// Where a trade was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    Raindex,
    Alpaca,
    DryRun,
}

/// Whether the trade was a buy or sell. Canonical Direction type used by
/// both broker execution and dashboard DTOs -- there is no separate
/// "TradeDirection" that needs converting back and forth.
///
/// Serializes as snake_case (`"buy"`/`"sell"`) to match the dashboard wire
/// format. Deserialization accepts both snake_case and the legacy PascalCase
/// (`"Buy"`/`"Sell"`) variant names so old OffchainOrder event payloads
/// continue to load after this type was promoted from `st0x_execution`.
/// The `Deserialize` impl is hand-rolled because per-variant `#[serde(alias)]`
/// confuses `ts-rs` (it warns on unrecognized attributes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Buy,
    Sell,
}

impl<'de> Deserialize<'de> for Direction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

impl Direction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for Direction {
    type Err = InvalidDirectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept both the broker-API wire form ("BUY"/"SELL") that `Display`
        // emits and the serde snake_case form ("buy"/"sell") used in
        // dashboard/HTTP payloads, so `s.parse::<Direction>()` round-trips
        // through either representation.
        match s.to_ascii_uppercase().as_str() {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            _ => Err(InvalidDirectionError {
                direction_provided: s.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid direction: {direction_provided}")]
pub struct InvalidDirectionError {
    direction_provided: String,
}

/// A completed trade fill.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    /// Unique identifier for deduplication on reconnect.
    /// Onchain: `"tx_hash:log_index"`. Offchain: offchain order aggregate ID.
    pub id: String,
    pub filled_at: DateTime<Utc>,
    pub venue: TradingVenue,
    pub direction: Direction,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub shares: FractionalShares,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::str::FromStr;

    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn direction_from_str_accepts_both_wire_forms() {
        assert_eq!(Direction::from_str("BUY").unwrap(), Direction::Buy);
        assert_eq!(Direction::from_str("SELL").unwrap(), Direction::Sell);
        assert_eq!(Direction::from_str("buy").unwrap(), Direction::Buy);
        assert_eq!(Direction::from_str("sell").unwrap(), Direction::Sell);
    }

    #[test]
    fn direction_from_str_rejects_unknown_input() {
        let error = Direction::from_str("hold").unwrap_err();
        assert_eq!(error.direction_provided, "hold");
    }

    #[test]
    fn trade_serializes_all_fields() {
        let trade = Trade {
            id: "test-order-id".to_string(),
            filled_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: FractionalShares::new(float!(5.5)),
        };
        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(json["id"], json!("test-order-id"));
        assert_eq!(json["venue"], json!("alpaca"));
        assert_eq!(json["direction"], json!("sell"));
        assert_eq!(json["symbol"], json!("TSLA"));
        assert_eq!(json["shares"], json!("5.5"));
        assert_eq!(json["filledAt"], json!("2023-11-14T22:13:20Z"));
    }
}
