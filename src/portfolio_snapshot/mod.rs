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
//! lifecycle routing, not a `SELECT EXISTS` precheck.
//!
//! `Captured` events are retained forever (no `COMPACTION_POLICY` override):
//! one event per ET day is a financial-audit record, not an observational
//! stream eligible for compaction, and the storage volume is negligible.
//! `type Materialized = Nil` -- the aggregate's own state only distinguishes
//! `initialize` from `transition` routing; the queryable balances live in the
//! plain `portfolio_snapshot` table maintained by the [`projection`] reactor,
//! not on this aggregate.
//!
//! The write side splits into [`write`] (the self-rescheduling job that
//! resolves USD marks and issues the `Capture` command) and [`projection`]
//! (the reactor that maintains the flat read-model table from the retained
//! `Captured` events).

use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};

use crate::inventory::PortfolioBalanceRow;
use crate::position::option_float_eq;

pub(crate) mod projection;
pub(crate) mod write;

pub(crate) use projection::PortfolioSnapshotProjection;
pub(crate) use write::{
    PortfolioSnapshotCtx, PortfolioSnapshotJob, PortfolioSnapshotJobQueue,
    bootstrap_portfolio_snapshot,
};

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
/// mirrored by hand. Marks are fixed permanently once captured: because the
/// aggregate's event is retained and immutable, a later price correction
/// never retroactively changes a previously reported day's capital.
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

/// Domain events for [`PortfolioSnapshot`]. `Captured` is the only variant:
/// one per ET day, retained forever.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum PortfolioSnapshotEvent {
    Captured {
        captured_at: DateTime<Utc>,
        rows: Vec<PortfolioBalanceRowWithMark>,
    },
}

impl DomainEvent for PortfolioSnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Captured { .. } => "PortfolioSnapshotEvent::Captured".to_string(),
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
}

/// Errors from [`PortfolioSnapshot`] command handling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub(crate) enum PortfolioSnapshotError {
    #[error("portfolio already captured for this day")]
    AlreadyCaptured,
}

/// One instance per ET day. Tracks only enough state to route a `Capture`
/// command to `initialize` (not yet captured) or `transition` (already
/// captured, rejected) -- the queryable balances live in the
/// `portfolio_snapshot` table maintained by [`PortfolioSnapshotProjection`],
/// not on this aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PortfolioSnapshot {
    pub(crate) captured_at: DateTime<Utc>,
    pub(crate) row_count: usize,
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
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            PortfolioSnapshotEvent::Captured { captured_at, rows } => Some(Self {
                captured_at: *captured_at,
                row_count: rows.len(),
            }),
        }
    }

    fn evolve(entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        // Defensive, not expected: transition() below unconditionally
        // rejects a second Capture for an already-Live aggregate, so a Live
        // instance never actually observes a second Captured event in
        // normal operation. Total but a no-op if it ever does.
        Ok(Some(entity.clone()))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let PortfolioSnapshotCommand::Capture { captured_at, rows } = command;
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
        _command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        // Expected, not a failure (decision 11): a scheduling bug that fires
        // more than once a day is still visible via this log naming the day
        // that was already captured, just not treated as retryable by the
        // caller (`write.rs`'s `PortfolioSnapshotJob::perform`).
        let existing_et_day = write::et_day(self.captured_at);
        info!(%existing_et_day, "Portfolio already captured for this ET day");
        Err(PortfolioSnapshotError::AlreadyCaptured)
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
