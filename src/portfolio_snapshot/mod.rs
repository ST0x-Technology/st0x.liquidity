//! Daily portfolio snapshot: an event-sourced record of every balance under
//! management, in USD terms, feeding `/pnl`'s capital and return-on-capital
//! figures (RAI-1457, see SPEC.md "Portfolio Capital and Return Tracking").
//!
//! [`PortfolioSnapshot`] is one aggregate instance per Eastern Time calendar
//! day (`et_day`): the day itself is the id, so "has today been captured" is
//! a question the framework already answers -- `Uninitialized` routes a
//! `Capture` command through [`EventSourced::initialize`] (emits `Captured`);
//! `Live` routes it through [`EventSourced::transition`], which unconditionally
//! rejects with [`PortfolioSnapshotError::AlreadyCaptured`] and emits no
//! event. This makes per-day idempotency a property of the framework's
//! lifecycle routing, not a `SELECT EXISTS` precheck. A live aggregate also
//! accepts audited `SetEquityMark` corrections for equities captured that day.
//!
//! Snapshot events are retained forever (no `COMPACTION_POLICY` override):
//! captures and operator corrections are financial-audit records, not observational
//! stream eligible for compaction, and the storage volume is negligible.
//! `type Materialized = Nil` -- the aggregate's own state only distinguishes
//! `initialize` from `transition` routing; the queryable balances live in the
//! plain `portfolio_snapshot` table maintained by the [`projection`] reactor,
//! not on this aggregate.
//!
//! The write side splits into [`write`] (the self-rescheduling job that
//! resolves USD marks and issues the `Capture` command) and [`projection`]
//! (the reactor that maintains the flat read-model table from the retained
//! `Captured` events). The [`read`] module computes the capital and return
//! figures exposed by `/pnl` from that read model.

use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_finance::{Positive, Symbol};

use crate::inventory::PortfolioBalanceRow;
use crate::position::option_float_eq;

pub(crate) mod projection;
pub(crate) mod read;
pub(crate) mod write;

pub(crate) use projection::PortfolioSnapshotProjection;
pub(crate) use read::{EtDayRange, ReadError, capital_summary, load_portfolio_days};
pub(crate) use write::{
    PortfolioSnapshotCtx, PortfolioSnapshotJob, PortfolioSnapshotJobQueue,
    bootstrap_portfolio_snapshot,
};
// Only consumed by api.rs's `#[cfg(test)]` DST-boundary /pnl test, which
// derives the same day a real capture would rather than assuming it;
// `cfg(test)` only because that test module is not part of the e2e binary.
#[cfg(test)]
pub(crate) use write::et_day;

/// Typed identifier for [`PortfolioSnapshot`] aggregates: one instance per
/// Eastern Time calendar day. Using the day itself as the id gives
/// "has today been captured" idempotency at the framework level -- no extra
/// state is needed to route a `Capture` command to `initialize` (first
/// capture) or `transition` (already captured, rejected).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct PortfolioSnapshotId(pub(crate) NaiveDate);

impl fmt::Display for PortfolioSnapshotId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.format("%Y-%m-%d"))
    }
}

/// Error parsing a [`PortfolioSnapshotId`] from its `YYYY-MM-DD` string form.
#[derive(Debug, Error)]
#[error("invalid portfolio snapshot id '{value}': expected YYYY-MM-DD: {source}")]
pub(crate) struct ParsePortfolioSnapshotIdError {
    value: String,
    #[source]
    source: chrono::ParseError,
}

impl FromStr for PortfolioSnapshotId {
    type Err = ParsePortfolioSnapshotIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Self)
            .map_err(|source| ParsePortfolioSnapshotIdError {
                value: value.to_string(),
                source,
            })
    }
}

/// A [`PortfolioBalanceRow`] with the USD mark resolved at capture time.
/// Composes rather than duplicates `PortfolioBalanceRow`'s fields, so a field
/// added there is inherited here automatically instead of needing to be
/// mirrored by hand. The captured value remains immutable in its event; an
/// audited correction is a later event whose projection value supersedes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PortfolioBalanceRowWithMark {
    #[serde(flatten)]
    pub(crate) row: PortfolioBalanceRow,
    /// `None` when the balance is nonzero but no price has been observed
    /// yet -- never a fabricated zero.
    pub(crate) usd_mark: Option<Float>,
    pub(crate) mark_captured_at: Option<DateTime<Utc>>,
}

impl PartialEq for PortfolioBalanceRowWithMark {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
            && option_float_eq(self.usd_mark, other.usd_mark)
            && self.mark_captured_at == other.mark_captured_at
    }
}

/// Domain events for [`PortfolioSnapshot`], retained forever.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PortfolioSnapshotEvent {
    Captured {
        captured_at: DateTime<Utc>,
        rows: Vec<PortfolioBalanceRowWithMark>,
    },
    EquityMarkSet {
        symbol: Symbol,
        usd_mark: Positive<Float>,
        observed_at: DateTime<Utc>,
        source: String,
        reason: String,
        corrected_at: DateTime<Utc>,
    },
    UnusableMarkAlerted {
        symbol: Symbol,
        alerted_at: DateTime<Utc>,
    },
    UnusableMarkDetected {
        symbol: Symbol,
        detected_at: DateTime<Utc>,
    },
}

impl PartialEq for PortfolioSnapshotEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Captured {
                    captured_at: left_at,
                    rows: left_rows,
                },
                Self::Captured {
                    captured_at: right_at,
                    rows: right_rows,
                },
            ) => left_at == right_at && left_rows == right_rows,
            (
                Self::EquityMarkSet {
                    symbol: left_symbol,
                    usd_mark: left_mark,
                    observed_at: left_observed,
                    source: left_source,
                    reason: left_reason,
                    corrected_at: left_corrected,
                },
                Self::EquityMarkSet {
                    symbol: right_symbol,
                    usd_mark: right_mark,
                    observed_at: right_observed,
                    source: right_source,
                    reason: right_reason,
                    corrected_at: right_corrected,
                },
            ) => {
                left_symbol == right_symbol
                    && left_mark.inner().eq(right_mark.inner()).unwrap_or(false)
                    && left_observed == right_observed
                    && left_source == right_source
                    && left_reason == right_reason
                    && left_corrected == right_corrected
            }
            (
                Self::UnusableMarkAlerted {
                    symbol: left_symbol,
                    alerted_at: left_at,
                },
                Self::UnusableMarkAlerted {
                    symbol: right_symbol,
                    alerted_at: right_at,
                },
            )
            | (
                Self::UnusableMarkDetected {
                    symbol: left_symbol,
                    detected_at: left_at,
                },
                Self::UnusableMarkDetected {
                    symbol: right_symbol,
                    detected_at: right_at,
                },
            ) => left_symbol == right_symbol && left_at == right_at,
            _ => false,
        }
    }
}

impl DomainEvent for PortfolioSnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Captured { .. } => "PortfolioSnapshotEvent::Captured".to_string(),
            Self::EquityMarkSet { .. } => "PortfolioSnapshotEvent::EquityMarkSet".to_string(),
            Self::UnusableMarkAlerted { .. } => {
                "PortfolioSnapshotEvent::UnusableMarkAlerted".to_string()
            }
            Self::UnusableMarkDetected { .. } => {
                "PortfolioSnapshotEvent::UnusableMarkDetected".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Commands for [`PortfolioSnapshot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PortfolioSnapshotCommand {
    Capture {
        captured_at: DateTime<Utc>,
        rows: Vec<PortfolioBalanceRowWithMark>,
    },
    SetEquityMark {
        symbol: Symbol,
        usd_mark: Positive<Float>,
        observed_at: DateTime<Utc>,
        source: String,
        reason: String,
        corrected_at: DateTime<Utc>,
    },
    RecordUnusableMarkAlerted {
        symbol: Symbol,
        alerted_at: DateTime<Utc>,
    },
    RecordUnusableMarkDetected {
        symbol: Symbol,
        detected_at: DateTime<Utc>,
    },
}

/// Errors from [`PortfolioSnapshot`] command handling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub(crate) enum PortfolioSnapshotError {
    #[error("portfolio already captured for this day")]
    AlreadyCaptured,
    #[error("portfolio has not been captured for this day")]
    NotCaptured,
    #[error("portfolio snapshot does not contain equity {symbol}")]
    EquityNotCaptured { symbol: Symbol },
    #[error("historical price source must not be blank")]
    BlankSource,
    #[error("operator reason must not be blank")]
    BlankReason,
    #[error("historical mark for {symbol} must be observed before ET day {et_day}")]
    MarkNotBeforeCaptureDay { symbol: Symbol, et_day: NaiveDate },
    #[error("historical mark for {symbol} is too stale to make ET day {et_day} usable")]
    MarkTooStale { symbol: Symbol, et_day: NaiveDate },
}

/// One instance per ET day. Alongside capture idempotency it retains original
/// rows and per-symbol correction/alert state so durable job retries neither
/// lose nor repeatedly send unusable-mark alerts. Queryable balances still
/// live in the `portfolio_snapshot` projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PortfolioSnapshot {
    pub(crate) captured_at: DateTime<Utc>,
    pub(crate) row_count: usize,
    equity_symbols: HashSet<Symbol>,
    captured_rows: Vec<PortfolioBalanceRowWithMark>,
    alerted_symbols: HashSet<Symbol>,
    detected_symbols: HashSet<Symbol>,
    corrected_symbols: HashSet<Symbol>,
}

impl PortfolioSnapshot {
    pub(crate) fn captured_equity_row_count(&self, symbol: &Symbol) -> usize {
        self.captured_rows
            .iter()
            .filter(|row| {
                matches!(
                    &row.row.asset,
                    crate::inventory::PortfolioAsset::Equity(captured) if captured == symbol
                )
            })
            .count()
    }
}

#[async_trait]
impl EventSourced for PortfolioSnapshot {
    type Id = PortfolioSnapshotId;
    type Event = PortfolioSnapshotEvent;
    type Command = PortfolioSnapshotCommand;
    type Error = PortfolioSnapshotError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "PortfolioSnapshot";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 2;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            PortfolioSnapshotEvent::Captured { captured_at, rows } => Some(Self {
                captured_at: *captured_at,
                row_count: rows.len(),
                equity_symbols: rows
                    .iter()
                    .filter_map(|row| match &row.row.asset {
                        crate::inventory::PortfolioAsset::Equity(symbol) => Some(symbol.clone()),
                        crate::inventory::PortfolioAsset::Usdc => None,
                    })
                    .collect(),
                captured_rows: rows.clone(),
                alerted_symbols: HashSet::new(),
                detected_symbols: HashSet::new(),
                corrected_symbols: HashSet::new(),
            }),
            PortfolioSnapshotEvent::EquityMarkSet { .. }
            | PortfolioSnapshotEvent::UnusableMarkAlerted { .. }
            | PortfolioSnapshotEvent::UnusableMarkDetected { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let mut next = entity.clone();
        match event {
            PortfolioSnapshotEvent::Captured { .. } => {}
            PortfolioSnapshotEvent::EquityMarkSet { symbol, .. } => {
                next.corrected_symbols.insert(symbol.clone());
            }
            PortfolioSnapshotEvent::UnusableMarkAlerted { symbol, .. } => {
                next.alerted_symbols.insert(symbol.clone());
            }
            PortfolioSnapshotEvent::UnusableMarkDetected { symbol, .. } => {
                next.detected_symbols.insert(symbol.clone());
            }
        }
        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let PortfolioSnapshotCommand::Capture { captured_at, rows } = command else {
            return Err(PortfolioSnapshotError::NotCaptured);
        };
        // Logged here, not by the job that sent the command (AGENTS.md "Log
        // in command handlers, not callers"): this handler has the full
        // command in scope. `target_et_day` is derived from `captured_at`
        // rather than threaded in separately -- the job always issues
        // `Capture` under the id matching the ET day its balances were
        // observed on, so the two agree by construction.
        let target_et_day = write::et_day(captured_at);
        info!(%target_et_day, row_count = rows.len(), "Portfolio snapshot captured");
        Ok(vec![PortfolioSnapshotEvent::Captured { captured_at, rows }])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        // Expected, not a failure (decision 11): a scheduling bug that fires
        // more than once a day is still visible via this log naming the day
        // that was already captured, just not treated as retryable by the
        // caller (`write.rs`'s `PortfolioSnapshotJob::perform`).
        match command {
            PortfolioSnapshotCommand::Capture { .. } => {
                let existing_et_day = write::et_day(self.captured_at);
                info!(%existing_et_day, "Portfolio already captured for this ET day");
                Err(PortfolioSnapshotError::AlreadyCaptured)
            }
            PortfolioSnapshotCommand::SetEquityMark {
                symbol,
                usd_mark,
                observed_at,
                source,
                reason,
                corrected_at,
            } => {
                if !self.equity_symbols.contains(&symbol) {
                    return Err(PortfolioSnapshotError::EquityNotCaptured { symbol });
                }
                if source.trim().is_empty() {
                    return Err(PortfolioSnapshotError::BlankSource);
                }
                if reason.trim().is_empty() {
                    return Err(PortfolioSnapshotError::BlankReason);
                }

                let et_day = write::et_day(self.captured_at);
                if write::et_day(observed_at) >= et_day {
                    return Err(PortfolioSnapshotError::MarkNotBeforeCaptureDay { symbol, et_day });
                }
                let asset = crate::inventory::PortfolioAsset::Equity(symbol.clone());
                if read::is_stale_mark(&asset, observed_at, et_day) {
                    return Err(PortfolioSnapshotError::MarkTooStale { symbol, et_day });
                }
                info!(%et_day, %symbol, ?usd_mark, %observed_at, %source, %reason, %corrected_at, "Portfolio snapshot equity mark set");
                Ok(vec![PortfolioSnapshotEvent::EquityMarkSet {
                    symbol,
                    usd_mark,
                    observed_at,
                    source,
                    reason,
                    corrected_at,
                }])
            }
            PortfolioSnapshotCommand::RecordUnusableMarkAlerted { symbol, alerted_at } => {
                if !self.equity_symbols.contains(&symbol) {
                    return Err(PortfolioSnapshotError::EquityNotCaptured { symbol });
                }
                info!(%symbol, %alerted_at, "Portfolio snapshot unusable-mark alert delivered");
                Ok(vec![PortfolioSnapshotEvent::UnusableMarkAlerted {
                    symbol,
                    alerted_at,
                }])
            }
            PortfolioSnapshotCommand::RecordUnusableMarkDetected {
                symbol,
                detected_at,
            } => {
                if !self.equity_symbols.contains(&symbol) {
                    return Err(PortfolioSnapshotError::EquityNotCaptured { symbol });
                }
                info!(%symbol, %detected_at, "Portfolio snapshot unusable mark detected");
                Ok(vec![PortfolioSnapshotEvent::UnusableMarkDetected {
                    symbol,
                    detected_at,
                }])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};
    use st0x_float_macro::float;

    use crate::inventory::{PortfolioAsset, PortfolioLocation};

    use super::*;

    fn test_row(symbol_available: i64) -> PortfolioBalanceRowWithMark {
        PortfolioBalanceRowWithMark {
            row: PortfolioBalanceRow {
                location: PortfolioLocation::MarketMaking,
                asset: PortfolioAsset::Usdc,
                available: float!(&symbol_available.to_string()),
                inflight: float!(0),
            },
            usd_mark: Some(float!(1)),
            mark_captured_at: Some(Utc::now()),
        }
    }

    fn equity_row(symbol: &str) -> PortfolioBalanceRowWithMark {
        PortfolioBalanceRowWithMark {
            row: PortfolioBalanceRow {
                location: PortfolioLocation::MarketMaking,
                asset: PortfolioAsset::Equity(Symbol::new(symbol).unwrap()),
                available: float!(10),
                inflight: float!(0),
            },
            usd_mark: None,
            mark_captured_at: None,
        }
    }

    #[tokio::test]
    async fn first_capture_initializes_and_emits_captured() {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let rows = vec![test_row(100)];

        TestHarness::<PortfolioSnapshot>::with(())
            .given_no_previous_events()
            .when(PortfolioSnapshotCommand::Capture {
                captured_at,
                rows: rows.clone(),
            })
            .await
            .then_expect_events(&[PortfolioSnapshotEvent::Captured { captured_at, rows }]);
    }

    #[tokio::test]
    async fn second_capture_same_day_is_rejected_and_emits_no_event() {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let rows = vec![test_row(100)];

        let error = TestHarness::<PortfolioSnapshot>::with(())
            .given(vec![PortfolioSnapshotEvent::Captured {
                captured_at,
                rows: rows.clone(),
            }])
            .when(PortfolioSnapshotCommand::Capture {
                captured_at: captured_at + chrono::Duration::hours(1),
                rows,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(PortfolioSnapshotError::AlreadyCaptured)
        ));
    }

    #[tokio::test]
    async fn captured_equity_mark_can_be_set_repeatedly_as_append_only_events() {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 17, 20, 0, 0).unwrap();
        let corrected_at = Utc.with_ymd_and_hms(2026, 7, 22, 15, 0, 0).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let command = |mark| PortfolioSnapshotCommand::SetEquityMark {
            symbol: symbol.clone(),
            usd_mark: Positive::new(mark).unwrap(),
            observed_at,
            source: "Nasdaq historical close".to_owned(),
            reason: "repair missing capture mark".to_owned(),
            corrected_at,
        };

        let first_event = PortfolioSnapshotEvent::EquityMarkSet {
            symbol: symbol.clone(),
            usd_mark: Positive::new(float!(150)).unwrap(),
            observed_at,
            source: "Nasdaq historical close".to_owned(),
            reason: "repair missing capture mark".to_owned(),
            corrected_at,
        };
        TestHarness::<PortfolioSnapshot>::with(())
            .given(vec![PortfolioSnapshotEvent::Captured {
                captured_at,
                rows: vec![equity_row("AAPL")],
            }])
            .when(command(float!(150)))
            .await
            .then_expect_events(std::slice::from_ref(&first_event));

        TestHarness::<PortfolioSnapshot>::with(())
            .given(vec![
                PortfolioSnapshotEvent::Captured {
                    captured_at,
                    rows: vec![equity_row("AAPL")],
                },
                first_event,
            ])
            .when(command(float!(151)))
            .await
            .then_expect_events(&[PortfolioSnapshotEvent::EquityMarkSet {
                symbol,
                usd_mark: Positive::new(float!(151)).unwrap(),
                observed_at,
                source: "Nasdaq historical close".to_owned(),
                reason: "repair missing capture mark".to_owned(),
                corrected_at,
            }]);
    }

    #[tokio::test]
    async fn correction_rejects_same_day_or_stale_observation() {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let command = |observed_at| PortfolioSnapshotCommand::SetEquityMark {
            symbol: symbol.clone(),
            usd_mark: Positive::new(float!(150)).unwrap(),
            observed_at,
            source: "Nasdaq historical close".to_owned(),
            reason: "repair missing capture mark".to_owned(),
            corrected_at: captured_at + chrono::Duration::days(2),
        };
        let given = || {
            vec![PortfolioSnapshotEvent::Captured {
                captured_at,
                rows: vec![equity_row("AAPL")],
            }]
        };

        let same_day = TestHarness::<PortfolioSnapshot>::with(())
            .given(given())
            .when(command(Utc.with_ymd_and_hms(2026, 7, 20, 4, 1, 0).unwrap()))
            .await
            .then_expect_error();
        assert!(matches!(
            same_day,
            LifecycleError::Apply(PortfolioSnapshotError::MarkNotBeforeCaptureDay { .. })
        ));

        let stale = TestHarness::<PortfolioSnapshot>::with(())
            .given(given())
            .when(command(
                Utc.with_ymd_and_hms(2026, 7, 10, 20, 0, 0).unwrap(),
            ))
            .await
            .then_expect_error();
        assert!(matches!(
            stale,
            LifecycleError::Apply(PortfolioSnapshotError::MarkTooStale { .. })
        ));
    }

    #[test]
    fn replay_reconstructs_captured_at_and_row_count() {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap();
        let rows = vec![test_row(100), test_row(200)];

        let snapshot = replay::<PortfolioSnapshot>(vec![PortfolioSnapshotEvent::Captured {
            captured_at,
            rows,
        }])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.captured_at, captured_at);
        assert_eq!(snapshot.row_count, 2);
    }

    #[test]
    fn portfolio_snapshot_id_roundtrips_through_display_and_parse() {
        let id = PortfolioSnapshotId(NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());

        let parsed: PortfolioSnapshotId = id.to_string().parse().unwrap();

        assert_eq!(parsed, id);
        assert_eq!(id.to_string(), "2026-07-20");
    }

    #[test]
    fn portfolio_snapshot_id_rejects_malformed_input() {
        let error = "not-a-date".parse::<PortfolioSnapshotId>().unwrap_err();

        assert_eq!(error.value, "not-a-date");
    }
}
