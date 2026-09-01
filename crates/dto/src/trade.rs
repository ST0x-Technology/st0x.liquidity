//! Trade fill DTOs for completed onchain and offchain trades.

use chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::ser::{Error as _, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ts_rs::TS;

use st0x_finance::{FractionalShares, NonNegative, Positive, Symbol};

/// Where a trade was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    Raindex,
    Bebop,
    UniswapV4,
    UnknownOnchain,
    Alpaca,
    DryRun,
}

impl TradingVenue {
    /// Every venue, for callers that must enumerate the domain rather than
    /// branch on one value -- notably expanding a legacy venue filter to the
    /// concrete venues that collapse onto it.
    pub const ALL: [Self; 6] = [
        Self::Raindex,
        Self::Bebop,
        Self::UniswapV4,
        Self::UnknownOnchain,
        Self::Alpaca,
        Self::DryRun,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Raindex => "raindex",
            Self::Bebop => "bebop",
            Self::UniswapV4 => "uniswap_v4",
            Self::UnknownOnchain => "unknown_onchain",
            Self::Alpaca => "alpaca",
            Self::DryRun => "dry_run",
        }
    }

    /// Whether trades from this venue settle onchain.
    #[must_use]
    pub const fn is_onchain(self) -> bool {
        match self {
            Self::Raindex | Self::Bebop | Self::UniswapV4 | Self::UnknownOnchain => true,
            Self::Alpaca | Self::DryRun => false,
        }
    }

    /// Venue value understood by dashboard protocols that predate adapter
    /// attribution.
    #[must_use]
    pub const fn legacy_compatible(self) -> Self {
        match self {
            Self::Raindex | Self::Alpaca | Self::DryRun => self,
            Self::Bebop | Self::UniswapV4 | Self::UnknownOnchain => Self::Raindex,
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
            "bebop" => Ok(Self::Bebop),
            "uniswap_v4" => Ok(Self::UniswapV4),
            "unknown_onchain" => Ok(Self::UnknownOnchain),
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TradeOutcome {
    Filled,
    Failed {
        error: String,
        #[ts(type = "string | null")]
        accepted_shares: Option<Positive<FractionalShares>>,
        #[ts(type = "string | null")]
        filled_shares: Option<NonNegative<FractionalShares>>,
        #[ts(type = "string | null")]
        remaining_shares: Option<NonNegative<FractionalShares>>,
        /// Shares filled beyond the broker-accepted order quantity. This is
        /// separate from remaining shares so anomalous broker state is never
        /// clamped away.
        #[ts(type = "string | null")]
        excess_shares: Option<NonNegative<FractionalShares>>,
    },
    Cancelled {
        #[ts(type = "string | null")]
        accepted_shares: Option<Positive<FractionalShares>>,
        #[ts(type = "string | null")]
        filled_shares: Option<NonNegative<FractionalShares>>,
        #[ts(type = "string | null")]
        remaining_shares: Option<NonNegative<FractionalShares>>,
        #[ts(type = "string | null")]
        excess_shares: Option<NonNegative<FractionalShares>>,
    },
}

#[derive(Default)]
enum FieldPresence<T> {
    #[default]
    Missing,
    Present(T),
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<FieldPresence<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(FieldPresence::Present)
}

#[derive(Deserialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum TradeOutcomeWire {
    Filled,
    Failed {
        error: String,
        #[serde(default, deserialize_with = "deserialize_present")]
        accepted_shares: FieldPresence<Option<Positive<FractionalShares>>>,
        filled_shares: Option<NonNegative<FractionalShares>>,
        remaining_shares: Option<NonNegative<FractionalShares>>,
        excess_shares: Option<NonNegative<FractionalShares>>,
    },
    Cancelled {
        #[serde(default, deserialize_with = "deserialize_present")]
        accepted_shares: FieldPresence<Option<Positive<FractionalShares>>>,
        filled_shares: Option<NonNegative<FractionalShares>>,
        remaining_shares: Option<NonNegative<FractionalShares>>,
        excess_shares: Option<NonNegative<FractionalShares>>,
    },
}

/// Rejects a terminal outcome whose derived quantities contradict its
/// accepted and filled quantities.
///
/// The dashboard parser enforces this contract on the same payload, so
/// without it the two ends of the wire disagree on what a valid outcome is:
/// `Trade` also deserializes from durable job payloads, and a corrupt row
/// would pass here only to fail later in the browser.
fn validate_derived_quantities<E: serde::de::Error>(
    accepted_shares: Option<Positive<FractionalShares>>,
    filled_shares: Option<NonNegative<FractionalShares>>,
    remaining_shares: Option<NonNegative<FractionalShares>>,
    excess_shares: Option<NonNegative<FractionalShares>>,
) -> Result<(), E> {
    let (Some(accepted), Some(filled)) = (accepted_shares, filled_shares) else {
        if remaining_shares.is_some() {
            return Err(E::custom(
                "remainingShares must be null when fill provenance is incomplete",
            ));
        }
        if excess_shares.is_some() {
            return Err(E::custom(
                "excessShares must be null when fill provenance is incomplete",
            ));
        }

        return Ok(());
    };

    let remaining = remaining_shares.ok_or_else(|| E::missing_field("remainingShares"))?;
    let excess = excess_shares.ok_or_else(|| E::missing_field("excessShares"))?;
    let accepted = accepted.inner();
    let filled = filled.inner();
    let overfilled = filled.inner().gt(accepted.inner()).map_err(E::custom)?;
    let (expected_remaining, expected_excess) = if overfilled {
        (
            FractionalShares::ZERO,
            (filled - accepted).map_err(E::custom)?,
        )
    } else {
        (
            (accepted - filled).map_err(E::custom)?,
            FractionalShares::ZERO,
        )
    };

    if !remaining
        .inner()
        .inner()
        .eq(expected_remaining.inner())
        .map_err(E::custom)?
    {
        return Err(E::custom(
            "remainingShares must be the accepted quantity minus the fill",
        ));
    }

    if !excess
        .inner()
        .inner()
        .eq(expected_excess.inner())
        .map_err(E::custom)?
    {
        return Err(E::custom(
            "excessShares must be the fill beyond the accepted quantity",
        ));
    }

    Ok(())
}

impl<'de> Deserialize<'de> for TradeOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match TradeOutcomeWire::deserialize(deserializer)? {
            TradeOutcomeWire::Filled => Ok(Self::Filled),
            TradeOutcomeWire::Failed {
                error,
                accepted_shares: FieldPresence::Present(accepted_shares),
                filled_shares,
                remaining_shares,
                excess_shares,
            } => {
                validate_derived_quantities::<D::Error>(
                    accepted_shares,
                    filled_shares,
                    remaining_shares,
                    excess_shares,
                )?;

                Ok(Self::Failed {
                    error,
                    accepted_shares,
                    filled_shares,
                    remaining_shares,
                    excess_shares,
                })
            }
            TradeOutcomeWire::Failed {
                error,
                accepted_shares: FieldPresence::Missing,
                filled_shares,
                remaining_shares,
                excess_shares,
            } => {
                // terminal_outcomes_v1 predates accepted-quantity provenance.
                // It split an overfill between filledShares and excessShares,
                // so reconstruct the complete broker fill before discarding the
                // request-derived remaining/excess values.
                let filled =
                    filled_shares.ok_or_else(|| D::Error::missing_field("filledShares"))?;
                // Presence-only: the request-derived remainder is discarded in
                // favour of the reconstructed complete fill below.
                let _ =
                    remaining_shares.ok_or_else(|| D::Error::missing_field("remainingShares"))?;
                let excess =
                    excess_shares.ok_or_else(|| D::Error::missing_field("excessShares"))?;
                let complete_fill = (filled.inner() + excess.inner()).map_err(D::Error::custom)?;
                let filled_shares = if complete_fill.inner().is_zero().map_err(D::Error::custom)? {
                    None
                } else {
                    Some(NonNegative::new(complete_fill).map_err(D::Error::custom)?)
                };

                Ok(Self::Failed {
                    error,
                    accepted_shares: None,
                    filled_shares,
                    remaining_shares: None,
                    excess_shares: None,
                })
            }
            TradeOutcomeWire::Cancelled {
                accepted_shares: FieldPresence::Present(accepted_shares),
                filled_shares,
                remaining_shares,
                excess_shares,
            } => {
                validate_derived_quantities::<D::Error>(
                    accepted_shares,
                    filled_shares,
                    remaining_shares,
                    excess_shares,
                )?;

                Ok(Self::Cancelled {
                    accepted_shares,
                    filled_shares,
                    remaining_shares,
                    excess_shares,
                })
            }
            TradeOutcomeWire::Cancelled {
                accepted_shares: FieldPresence::Missing,
                ..
            } => Err(D::Error::missing_field("acceptedShares")),
        }
    }
}

/// The trading session an offchain counter-trade was recorded in.
///
/// Mirrors the execution crate's session model on the wire; the
/// serialized variant names match, so view payloads and DTO rows spell
/// the session identically. Onchain fills have no session -- `Trade`
/// carries `None` for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum MarketSession {
    Regular,
    Extended,
    Overnight,
    Closed,
}

/// A completed onchain fill or terminal offchain counter-trade.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
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
    /// Executed quantity for fills, or requested quantity for a failed or
    /// cancelled counter-trade. Terminal non-fill outcomes carry
    /// broker-accepted and fill provenance separately when those facts are
    /// known.
    #[ts(type = "string")]
    pub shares: Positive<FractionalShares>,
    /// The session the counter-trade was recorded in. `None` for onchain
    /// fills, and for offchain rows serialized before the session was
    /// recorded on terminal states.
    #[serde(default)]
    #[ts(rename = "marketSession")]
    pub market_session: Option<MarketSession>,
    pub outcome: TradeOutcome,
}

impl Serialize for Trade {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let is_filled = matches!(self.outcome, TradeOutcome::Filled);
        let mut trade = serializer.serialize_struct("Trade", 8 + usize::from(is_filled))?;
        trade.serialize_field("id", &self.id)?;
        trade.serialize_field("occurredAt", &self.occurred_at)?;
        if is_filled {
            trade.serialize_field("filledAt", &self.occurred_at)?;
        }
        trade.serialize_field("venue", &self.venue)?;
        trade.serialize_field("direction", &self.direction)?;
        trade.serialize_field("symbol", &self.symbol)?;
        trade.serialize_field("shares", &self.shares)?;
        trade.serialize_field("marketSession", &self.market_session)?;
        trade.serialize_field("outcome", &self.outcome)?;
        trade.end()
    }
}

/// Stable `terminal_outcomes_v1` representation.
///
/// Retained for older dashboard bundles. That contract cannot express unknown
/// quantity provenance, so its
/// failed outcome uses the legacy non-null split while v2 exposes the canonical
/// nullable fields from [`TradeOutcome`].
pub struct TerminalOutcomesV1Trade<'a> {
    trade: &'a Trade,
}

#[derive(Serialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum TerminalOutcomesV1Outcome<'a> {
    Failed {
        error: &'a str,
        filled_shares: NonNegative<FractionalShares>,
        remaining_shares: NonNegative<FractionalShares>,
        excess_shares: NonNegative<FractionalShares>,
    },
}

impl Serialize for TerminalOutcomesV1Trade<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let TradeOutcome::Failed {
            error,
            accepted_shares,
            filled_shares,
            ..
        } = &self.trade.outcome
        else {
            return self.trade.serialize(serializer);
        };

        // When acceptance provenance is unknown, v1 has no way to express
        // that, so it reports the requested quantity as the accepted quantity
        // and derives the split from it. An older dashboard therefore cannot
        // tell an assumed accepted quantity from a proven one.
        let shares = accepted_shares.unwrap_or(self.trade.shares);
        let accepted = shares.inner();
        // v1 represented a failure observed before fill provenance existed as
        // an unfilled order. Keep that historical wire contract only in this
        // compatibility adapter; the canonical v2 outcome remains unknown.
        let filled = filled_shares
            .map(NonNegative::inner)
            .unwrap_or(FractionalShares::ZERO);
        let (filled_portion, remaining_shares, excess_shares) = if filled
            .inner()
            .gt(accepted.inner())
            .map_err(S::Error::custom)?
        {
            (
                accepted,
                FractionalShares::ZERO,
                (filled - accepted).map_err(S::Error::custom)?,
            )
        } else {
            (
                filled,
                (accepted - filled).map_err(S::Error::custom)?,
                FractionalShares::ZERO,
            )
        };
        let outcome = TerminalOutcomesV1Outcome::Failed {
            error,
            filled_shares: NonNegative::new(filled_portion).map_err(S::Error::custom)?,
            remaining_shares: NonNegative::new(remaining_shares).map_err(S::Error::custom)?,
            excess_shares: NonNegative::new(excess_shares).map_err(S::Error::custom)?,
        };

        let mut trade = serializer.serialize_struct("Trade", 7)?;
        trade.serialize_field("id", &self.trade.id)?;
        trade.serialize_field("occurredAt", &self.trade.occurred_at)?;
        trade.serialize_field("venue", &self.trade.venue)?;
        trade.serialize_field("direction", &self.trade.direction)?;
        trade.serialize_field("symbol", &self.trade.symbol)?;
        trade.serialize_field("shares", &shares)?;
        trade.serialize_field("outcome", &outcome)?;
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
    pub shares: Positive<FractionalShares>,
}

impl Trade {
    /// Returns the stable terminal-outcomes v1 wire representation.
    #[must_use]
    pub const fn terminal_outcomes_v1(&self) -> TerminalOutcomesV1Trade<'_> {
        TerminalOutcomesV1Trade { trade: self }
    }

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
    if left.venue.is_onchain()
        && right.venue.is_onchain()
        && let (Some((left_hash, left_index)), Some((right_hash, right_index))) = (
            parse_onchain_trade_id(&left.id),
            parse_onchain_trade_id(&right.id),
        )
        && left_hash == right_hash
    {
        return right_index.cmp(&left_index);
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

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        Positive::new(FractionalShares::from_str(value).unwrap()).unwrap()
    }

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
    fn all_venues_lists_every_variant_exactly_once() {
        // The match makes a new variant a compile error here, and the count
        // makes it a test failure until `ALL` is extended.
        for venue in TradingVenue::ALL {
            match venue {
                TradingVenue::Raindex
                | TradingVenue::Bebop
                | TradingVenue::UniswapV4
                | TradingVenue::UnknownOnchain
                | TradingVenue::Alpaca
                | TradingVenue::DryRun => {}
            }
        }

        let mut unique = TradingVenue::ALL.to_vec();
        unique.sort_by_key(|venue| venue.as_str());
        unique.dedup();
        assert_eq!(unique.len(), TradingVenue::ALL.len());
    }

    #[test]
    fn venue_classifies_onchain_execution() {
        assert!(TradingVenue::Raindex.is_onchain());
        assert!(TradingVenue::Bebop.is_onchain());
        assert!(TradingVenue::UniswapV4.is_onchain());
        assert!(TradingVenue::UnknownOnchain.is_onchain());
        assert!(!TradingVenue::Alpaca.is_onchain());
        assert!(!TradingVenue::DryRun.is_onchain());
    }

    #[test]
    fn legacy_venue_protocols_collapse_adapter_attribution_to_raindex() {
        assert_eq!(
            TradingVenue::Bebop.legacy_compatible(),
            TradingVenue::Raindex
        );
        assert_eq!(
            TradingVenue::UniswapV4.legacy_compatible(),
            TradingVenue::Raindex
        );
        assert_eq!(
            TradingVenue::UnknownOnchain.legacy_compatible(),
            TradingVenue::Raindex
        );
        assert_eq!(
            TradingVenue::Alpaca.legacy_compatible(),
            TradingVenue::Alpaca
        );
    }

    #[test]
    fn filled_trade_serializes_all_fields() {
        let trade = Trade {
            market_session: None,
            id: "test-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: positive_shares("5.5"),
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
        assert_eq!(json["marketSession"], json!(null));
        assert_eq!(json["outcome"], json!({ "status": "filled" }));
    }

    #[test]
    fn trade_serializes_the_market_session_when_present() {
        let trade = Trade {
            market_session: Some(MarketSession::Overnight),
            id: "overnight-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: positive_shares("1"),
            outcome: TradeOutcome::Filled,
        };

        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(json["marketSession"], json!("Overnight"));

        let restored: Trade = serde_json::from_value(json).unwrap();
        assert_eq!(restored.market_session, Some(MarketSession::Overnight));
    }

    #[test]
    fn trade_roundtrips_through_persistent_job_payload() {
        let trade = Trade {
            market_session: None,
            id: "durable-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: positive_shares("5.5"),
            outcome: TradeOutcome::Filled,
        };

        let payload = serde_json::to_vec(&trade).unwrap();
        let restored: Trade = serde_json::from_slice(&payload).unwrap();

        assert_eq!(restored.id, trade.id);
        assert_eq!(restored.occurred_at, trade.occurred_at);
        assert_eq!(restored.venue, trade.venue);
        assert_eq!(restored.direction, trade.direction);
        assert_eq!(restored.symbol, trade.symbol);
        assert_eq!(restored.shares, trade.shares);
        assert_eq!(restored.outcome, trade.outcome);
    }

    #[test]
    fn trade_deserialization_rejects_non_positive_total_quantity() {
        let valid = json!({
            "id": "order-1",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "AAPL",
            "shares": "1",
            "outcome": { "status": "filled" }
        });

        for invalid_shares in ["0", "-1"] {
            let mut invalid = valid.clone();
            invalid["shares"] = json!(invalid_shares);

            let error = serde_json::from_value::<Trade>(invalid).unwrap_err();
            assert!(
                error.to_string().contains("value must be positive"),
                "unexpected error for {invalid_shares}: {error}"
            );
        }
    }

    #[test]
    fn failed_trade_serializes_error() {
        let trade = Trade {
            market_session: None,
            id: "failed-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: positive_shares("1"),
            outcome: TradeOutcome::Failed {
                error: "asset is not tradable".to_string(),
                accepted_shares: Some(positive_shares("1")),
                filled_shares: Some(NonNegative::new(FractionalShares::new(float!(0.25))).unwrap()),
                remaining_shares: Some(
                    NonNegative::new(FractionalShares::new(float!(0.75))).unwrap(),
                ),
                excess_shares: Some(NonNegative::new(FractionalShares::ZERO).unwrap()),
            },
        };

        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(
            json["outcome"],
            json!({
                "status": "failed",
                "error": "asset is not tradable",
                "acceptedShares": "1",
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
    fn failed_trade_deserializes_legacy_outcome_without_accepted_shares() {
        let legacy = json!({
            "id": "failed-order-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": {
                "status": "failed",
                "error": "asset is not tradable",
                "filledShares": "0.25",
                "remainingShares": "0.75",
                "excessShares": "0"
            }
        });

        let trade: Trade = serde_json::from_value(legacy).expect("legacy trade should deserialize");
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("legacy failed trade must retain its outcome");
        };

        assert_eq!(accepted_shares, None);
        assert_eq!(
            filled_shares,
            Some(NonNegative::new(FractionalShares::new(float!(0.25))).unwrap())
        );
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    #[test]
    fn cancelled_trade_roundtrips_explicit_zero_fill() {
        let trade = Trade {
            market_session: None,
            id: "cancelled-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: positive_shares("1.5"),
            outcome: TradeOutcome::Cancelled {
                accepted_shares: Some(positive_shares("1")),
                filled_shares: Some(NonNegative::new(FractionalShares::ZERO).unwrap()),
                remaining_shares: Some(NonNegative::new(FractionalShares::new(float!(1))).unwrap()),
                excess_shares: Some(NonNegative::new(FractionalShares::ZERO).unwrap()),
            },
        };

        let wire = serde_json::to_value(&trade).unwrap();
        assert_eq!(
            wire["outcome"],
            json!({
                "status": "cancelled",
                "acceptedShares": "1",
                "filledShares": "0",
                "remainingShares": "1",
                "excessShares": "0"
            })
        );
        assert!(
            wire.get("filledAt").is_none(),
            "cancelled outcomes must not masquerade as legacy fills"
        );
        let restored: Trade = serde_json::from_value(wire).unwrap();
        assert_eq!(restored.outcome, trade.outcome);
    }

    #[test]
    fn cancelled_trade_roundtrips_all_null_provenance_in_job_payload() {
        let trade = Trade {
            market_session: None,
            id: "legacy-cancelled-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: positive_shares("1"),
            outcome: TradeOutcome::Cancelled {
                accepted_shares: None,
                filled_shares: None,
                remaining_shares: None,
                excess_shares: None,
            },
        };

        let payload = serde_json::to_vec(&trade).unwrap();
        let restored: Trade = serde_json::from_slice(&payload).unwrap();
        assert_eq!(restored.outcome, trade.outcome);
    }

    #[test]
    fn cancelled_trade_rejects_missing_v2_acceptance_field() {
        let wire = json!({
            "id": "cancelled-order-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": {
                "status": "cancelled",
                "filledShares": "0",
                "remainingShares": "1",
                "excessShares": "0"
            }
        });

        let error = serde_json::from_value::<Trade>(wire).unwrap_err();
        assert!(error.to_string().contains("acceptedShares"));
    }

    fn v2_cancelled_wire(outcome: &serde_json::Value) -> serde_json::Value {
        json!({
            "id": "cancelled-order-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": outcome
        })
    }

    #[test]
    fn v2_outcome_rejects_remaining_shares_that_are_not_the_unfilled_quantity() {
        let error = serde_json::from_value::<Trade>(v2_cancelled_wire(&json!({
            "status": "cancelled",
            "acceptedShares": "1",
            "filledShares": "0.25",
            "remainingShares": "0.5",
            "excessShares": "0"
        })))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "remainingShares must be the accepted quantity minus the fill"
        );
    }

    #[test]
    fn v2_outcome_rejects_excess_shares_that_are_not_the_overfill() {
        let error = serde_json::from_value::<Trade>(v2_cancelled_wire(&json!({
            "status": "cancelled",
            "acceptedShares": "1",
            "filledShares": "1.5",
            "remainingShares": "0",
            "excessShares": "0.25"
        })))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "excessShares must be the fill beyond the accepted quantity"
        );
    }

    #[test]
    fn v2_outcome_rejects_derived_quantities_without_complete_provenance() {
        let error = serde_json::from_value::<Trade>(v2_cancelled_wire(&json!({
            "status": "cancelled",
            "acceptedShares": null,
            "filledShares": null,
            "remainingShares": "1",
            "excessShares": null
        })))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "remainingShares must be null when fill provenance is incomplete"
        );
    }

    #[test]
    fn v2_outcome_rejects_missing_derived_quantities_with_complete_provenance() {
        let error = serde_json::from_value::<Trade>(v2_cancelled_wire(&json!({
            "status": "cancelled",
            "acceptedShares": "1",
            "filledShares": "0.25",
            "remainingShares": null,
            "excessShares": "0"
        })))
        .unwrap_err();

        assert!(
            error.to_string().contains("remainingShares"),
            "expected a missing-field error for remainingShares, got: {error}"
        );
    }

    #[test]
    fn v2_failed_outcome_rejects_inconsistent_derived_quantities() {
        let error = serde_json::from_value::<Trade>(json!({
            "id": "failed-order-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": {
                "status": "failed",
                "error": "asset is not tradable",
                "acceptedShares": "1",
                "filledShares": "0.25",
                "remainingShares": "0.75",
                "excessShares": "0.5"
            }
        }))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "excessShares must be the fill beyond the accepted quantity"
        );
    }

    #[test]
    fn failed_trade_deserialization_reconstructs_legacy_overfill() {
        let legacy = json!({
            "id": "overfilled-order-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": {
                "status": "failed",
                "error": "broker failed after overfill",
                "filledShares": "1",
                "remainingShares": "0",
                "excessShares": "0.25"
            }
        });

        let trade: Trade = serde_json::from_value(legacy).expect("legacy trade should deserialize");
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("legacy failed trade must retain its outcome");
        };

        assert_eq!(accepted_shares, None);
        assert_eq!(
            filled_shares,
            Some(NonNegative::new(FractionalShares::new(float!(1.25))).unwrap())
        );
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    #[test]
    fn failed_trade_deserialization_keeps_legacy_zero_fill_unknown() {
        let legacy = json!({
            "id": "placement-failure-id",
            "occurredAt": "2026-07-20T12:00:00Z",
            "venue": "alpaca",
            "direction": "buy",
            "symbol": "SPCX",
            "shares": "1",
            "outcome": {
                "status": "failed",
                "error": "placement rejected",
                "filledShares": "0",
                "remainingShares": "1",
                "excessShares": "0"
            }
        });

        let trade: Trade = serde_json::from_value(legacy).expect("legacy trade should deserialize");
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("legacy failed trade must retain its outcome");
        };

        assert_eq!(accepted_shares, None);
        assert_eq!(filled_shares, None);
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    #[test]
    fn terminal_outcomes_v1_preserves_non_null_legacy_failure_shape() {
        let trade = Trade {
            market_session: None,
            id: "failed-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: positive_shares("2"),
            outcome: TradeOutcome::Failed {
                error: "broker failed after overfill".to_string(),
                accepted_shares: Some(positive_shares("1")),
                filled_shares: Some(NonNegative::new(FractionalShares::new(float!(1.25))).unwrap()),
                remaining_shares: Some(NonNegative::new(FractionalShares::ZERO).unwrap()),
                excess_shares: Some(NonNegative::new(FractionalShares::new(float!(0.25))).unwrap()),
            },
        };

        let wire = serde_json::to_value(trade.terminal_outcomes_v1())
            .expect("v1 compatibility serialization should succeed");

        assert_eq!(wire["shares"], "1");
        assert_eq!(wire["outcome"]["filledShares"], "1");
        assert_eq!(wire["outcome"]["remainingShares"], "0");
        assert_eq!(wire["outcome"]["excessShares"], "0.25");
        assert!(wire["outcome"].get("acceptedShares").is_none());
    }

    #[test]
    fn terminal_outcomes_v1_assumes_the_requested_quantity_when_acceptance_is_unknown() {
        let trade = Trade {
            market_session: None,
            id: "failed-order-id".to_string(),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("SPCX").unwrap(),
            shares: positive_shares("2"),
            outcome: TradeOutcome::Failed {
                error: "asset is not tradable".to_string(),
                accepted_shares: None,
                filled_shares: None,
                remaining_shares: None,
                excess_shares: None,
            },
        };

        let wire = serde_json::to_value(trade.terminal_outcomes_v1())
            .expect("v1 compatibility serialization should succeed");

        // Unknown acceptance falls back to the requested quantity, and the
        // absent fill becomes v1's synthesized zero, so the whole request
        // reads as unfilled.
        assert_eq!(wire["shares"], "2");
        assert_eq!(wire["outcome"]["filledShares"], "0");
        assert_eq!(wire["outcome"]["remainingShares"], "2");
        assert_eq!(wire["outcome"]["excessShares"], "0");
        assert!(wire["outcome"].get("acceptedShares").is_none());
    }

    #[test]
    fn newest_first_sort_uses_numeric_log_index_for_tied_onchain_trades() {
        let timestamp = DateTime::from_timestamp(1_700_000_001, 0).unwrap();
        let older = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let trade = |id: &str, occurred_at, venue| Trade {
            market_session: None,
            id: id.to_string(),
            occurred_at,
            venue,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("1"),
            outcome: TradeOutcome::Filled,
        };
        let tx_hash = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let mut trades = vec![
            trade(&format!("{tx_hash}:11"), timestamp, TradingVenue::Raindex),
            trade("older", older, TradingVenue::Raindex),
            trade(&format!("{tx_hash}:20"), timestamp, TradingVenue::Bebop),
        ];

        sort_trades_newest_first(&mut trades);

        assert_eq!(
            trades.into_iter().map(|trade| trade.id).collect::<Vec<_>>(),
            [
                format!("{tx_hash}:20"),
                format!("{tx_hash}:11"),
                "older".to_string()
            ]
        );
    }

    #[test]
    fn newest_first_sort_preserves_sub_millisecond_precision_and_fallback_ties() {
        let earlier = DateTime::from_timestamp(1_700_000_000, 123_456_788).unwrap();
        let later = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let trade = |id: &str, occurred_at| Trade {
            market_session: None,
            id: id.to_string(),
            occurred_at,
            venue: TradingVenue::Alpaca,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("1"),
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
