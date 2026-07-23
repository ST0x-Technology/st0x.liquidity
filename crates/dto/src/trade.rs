//! Trade fill DTOs for completed onchain and offchain trades.

use chrono::{DateTime, Utc};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use ts_rs::TS;

use st0x_finance::{FractionalShares, NonNegative, Symbol};

/// Where a trade was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    Raindex,
    Alpaca,
    DryRun,
}

impl TradingVenue {
    fn as_str(self) -> &'static str {
        match self {
            Self::Raindex => "raindex",
            Self::Alpaca => "alpaca",
            Self::DryRun => "dry_run",
        }
    }
}

impl std::fmt::Display for TradingVenue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for TradingVenue {
    type Err = InvalidTradingVenue;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "raindex" => Ok(Self::Raindex),
            "alpaca" => Ok(Self::Alpaca),
            "dry_run" => Ok(Self::DryRun),
            other => Err(InvalidTradingVenue {
                venue_provided: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid trading venue: {venue_provided}")]
pub struct InvalidTradingVenue {
    venue_provided: String,
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

/// Terminal outcome of a dashboard trade entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TradeOutcome {
    Filled,
    Failed {
        error: String,
        #[ts(type = "string")]
        filled_shares: NonNegative<FractionalShares>,
        #[ts(type = "string")]
        remaining_shares: NonNegative<FractionalShares>,
        /// Shares filled beyond the broker-accepted order quantity. This is
        /// separate from remaining shares so anomalous broker state is never
        /// clamped away.
        #[ts(type = "string")]
        excess_shares: NonNegative<FractionalShares>,
    },
}

/// A completed onchain fill or terminal offchain counter-trade.
#[derive(Debug, Clone, TS)]
pub struct Trade {
    /// Unique identifier for deduplication on reconnect.
    /// Onchain: `"tx_hash:log_index"`. Offchain: offchain order aggregate ID.
    pub id: String,
    #[ts(rename = "occurredAt")]
    pub occurred_at: DateTime<Utc>,
    pub venue: TradingVenue,
    pub direction: Direction,
    #[ts(type = "string")]
    pub symbol: Symbol,
    /// Quantity submitted for this counter-trade. Before broker acceptance this
    /// is the requested quantity; afterward it is the quantity the broker
    /// accepted. Failed outcomes split this order quantity into filled and
    /// remaining portions in [`TradeOutcome::Failed`].
    #[ts(type = "string")]
    pub shares: FractionalShares,
    pub outcome: TradeOutcome,
}

impl Serialize for Trade {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let is_filled = matches!(self.outcome, TradeOutcome::Filled);
        let mut trade = serializer.serialize_struct("Trade", 7 + usize::from(is_filled))?;
        trade.serialize_field("id", &self.id)?;
        trade.serialize_field("occurredAt", &self.occurred_at)?;
        if is_filled {
            trade.serialize_field("filledAt", &self.occurred_at)?;
        }
        trade.serialize_field("venue", &self.venue)?;
        trade.serialize_field("direction", &self.direction)?;
        trade.serialize_field("symbol", &self.symbol)?;
        trade.serialize_field("shares", &self.shares)?;
        trade.serialize_field("outcome", &self.outcome)?;
        trade.end()
    }
}

/// Filled-trade wire shape consumed by dashboard versions before terminal
/// outcomes were added to [`Trade`].
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LegacyTrade {
    pub id: String,
    pub filled_at: DateTime<Utc>,
    pub venue: TradingVenue,
    pub direction: Direction,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub shares: FractionalShares,
}

impl Trade {
    /// Returns the pre-terminal-outcome representation for filled trades.
    #[must_use]
    pub fn legacy_fill(&self) -> Option<LegacyTrade> {
        if !matches!(self.outcome, TradeOutcome::Filled) {
            return None;
        }

        Some(LegacyTrade {
            id: self.id.clone(),
            filled_at: self.occurred_at,
            venue: self.venue,
            direction: self.direction,
            symbol: self.symbol.clone(),
            shares: self.shares,
        })
    }
}

/// Sorts dashboard trades newest-first with a stable cross-loader tie-breaker.
pub fn sort_trades_newest_first(trades: &mut [Trade]) {
    trades.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| compare_trade_ids(left, right))
    });
}

fn compare_trade_ids(left: &Trade, right: &Trade) -> std::cmp::Ordering {
    if left.venue == TradingVenue::Raindex
        && right.venue == TradingVenue::Raindex
        && let (Some((left_hash, left_index)), Some((right_hash, right_index))) = (
            parse_onchain_trade_id(&left.id),
            parse_onchain_trade_id(&right.id),
        )
        && left_hash == right_hash
    {
        return left_index.cmp(&right_index);
    }

    left.id.cmp(&right.id)
}

fn parse_onchain_trade_id(id: &str) -> Option<(&str, u64)> {
    let (tx_hash, log_index) = id.rsplit_once(':')?;
    Some((tx_hash, log_index.parse().ok()?))
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
    fn filled_trade_serializes_all_fields() {
        let trade = Trade {
            id: "test-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: FractionalShares::new(float!(5.5)),
            outcome: TradeOutcome::Filled,
        };
        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(json["id"], json!("test-order-id"));
        assert_eq!(json["venue"], json!("alpaca"));
        assert_eq!(json["direction"], json!("sell"));
        assert_eq!(json["symbol"], json!("TSLA"));
        assert_eq!(json["shares"], json!("5.5"));
        assert_eq!(json["occurredAt"], json!("2023-11-14T22:13:20Z"));
        assert_eq!(json["filledAt"], json!("2023-11-14T22:13:20Z"));
        assert_eq!(json["outcome"], json!({ "status": "filled" }));
    }

    #[test]
    fn failed_trade_serializes_error() {
        let trade = Trade {
            id: "failed-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: FractionalShares::new(float!(1)),
            outcome: TradeOutcome::Failed {
                error: "asset is not tradable".to_string(),
                filled_shares: NonNegative::new(FractionalShares::new(float!(0.25))).unwrap(),
                remaining_shares: NonNegative::new(FractionalShares::new(float!(0.75))).unwrap(),
                excess_shares: NonNegative::new(FractionalShares::ZERO).unwrap(),
            },
        };

        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(
            json["outcome"],
            json!({
                "status": "failed",
                "error": "asset is not tradable",
                "filledShares": "0.25",
                "remainingShares": "0.75",
                "excessShares": "0"
            })
        );
        assert!(
            json.get("filledAt").is_none(),
            "failed outcomes must not masquerade as legacy fills"
        );
    }

    #[test]
    fn newest_first_sort_uses_numeric_log_index_for_tied_onchain_trades() {
        let timestamp = DateTime::from_timestamp(1_700_000_001, 0).unwrap();
        let older = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let trade = |id: &str, occurred_at| Trade {
            id: id.to_string(),
            occurred_at,
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares::new(float!(1)),
            outcome: TradeOutcome::Filled,
        };
        let tx_hash = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let mut trades = vec![
            trade(&format!("{tx_hash}:2"), timestamp),
            trade("older", older),
            trade(&format!("{tx_hash}:10"), timestamp),
        ];

        sort_trades_newest_first(&mut trades);

        assert_eq!(
            trades.into_iter().map(|trade| trade.id).collect::<Vec<_>>(),
            [
                format!("{tx_hash}:2"),
                format!("{tx_hash}:10"),
                "older".to_string()
            ]
        );
    }

    #[test]
    fn newest_first_sort_preserves_sub_millisecond_precision_and_fallback_ties() {
        let earlier = DateTime::from_timestamp(1_700_000_000, 123_456_788).unwrap();
        let later = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let trade = |id: &str, occurred_at| Trade {
            id: id.to_string(),
            occurred_at,
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares::new(float!(1)),
            outcome: TradeOutcome::Filled,
        };
        let mut trades = vec![
            trade("z-tied", earlier),
            trade("later", later),
            trade("a-tied", earlier),
        ];

        sort_trades_newest_first(&mut trades);

        assert_eq!(
            trades.into_iter().map(|trade| trade.id).collect::<Vec<_>>(),
            ["later", "a-tied", "z-tied"]
        );
    }
}
