//! Trade fill DTOs for completed onchain and offchain trades.

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{FractionalShares, Symbol};

/// Where a trade was executed.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    Raindex,
    Alpaca,
    DryRun,
}

/// Whether the trade was a buy or sell.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradeDirection {
    Buy,
    Sell,
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
    pub direction: TradeDirection,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub shares: FractionalShares,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn trade_serializes_all_fields() {
        let trade = Trade {
            id: "test-order-id".to_string(),
            filled_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: TradeDirection::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: FractionalShares::new(float!(5.5)),
        };
        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(json["id"], json!("test-order-id"));
        assert_eq!(json["venue"], json!("alpaca"));
        assert_eq!(json["direction"], json!("sell"));
        assert_eq!(json["symbol"], json!("TSLA"));
        assert_eq!(json["shares"], json!("5.5"));
        assert!(json["filledAt"].is_string());
    }
}
