//! Hedge-latency read model maintained forward-only by a reactor.
//!
//! [`HedgeLatencyProjection`] subscribes to the `Position` and `OffchainOrder`
//! event streams and maintains three append-only tables (`hedge_fill`,
//! `hedge_cycle`, `hedge_attribution_reset`) from live events. The report read
//! path ([`load_hedge_performance`]) queries ONLY those tables and never folds
//! the `events` table.
//!
//! Metric definitions live in SPEC.md under "Performance Observability". The
//! attribution rules: onchain fills accumulate in a per-symbol uncovered pool;
//! an `OffChainOrderPlaced` consumes the pool as the placed hedge's covered
//! batch; a failed hedge returns its covered fills to the uncovered pool so the
//! retry inherits attribution back to the original fill; a manual adjustment
//! resets the pool and drops in-flight attribution.
//!
//! Crucially there is NO persisted mutable attribution state -- the uncovered
//! pool is a pure function of the durable, append-only tables, recomputed by
//! [`uncovered_fills`] on demand (see its doc for the exact algorithm). Both the
//! read path and the placement writer derive attribution by replaying those
//! tables, so the read model survives a restart with no in-memory state.
//!
//! Forward-only: the reactor processes only events emitted after construction.
//! There is NO startup backfill of pre-existing history. A crash between an
//! event being persisted and this reactor's tables being updated can drop that
//! single event from the read model -- accepted as best-effort, matching the
//! `Broadcaster` reactor's guarantees.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;
use st0x_event_sorcery::{EntityList, Reactor, deps};
use thiserror::Error;
use tracing::{debug, warn};

use st0x_dto::{
    HedgeCycleReport, HedgeCycleStatus, HedgeLatencies, LatencyBucket, LatencyStats,
    LatencySummary, OpenExposureReport, StageLatencies,
};
use st0x_execution::Symbol;

use crate::offchain::order::{OffchainOrder, OffchainOrderEvent, OffchainOrderId};
use crate::position::{Position, PositionEvent, TradeId};

pub(crate) mod infra;
pub(crate) mod rebalance;
pub(crate) mod reliability;

/// Waterfall rows returned per report; the full cycle count is still
/// reported via `total_cycles`.
const MAX_CYCLE_REPORTS: usize = 100;

/// Per-symbol hedge performance assembled from the read-model tables.
#[derive(Debug, Clone)]
pub(crate) struct SymbolPerformance {
    pub(crate) symbol: Symbol,
    pub(crate) fills: Vec<FillObservation>,
    pub(crate) cycles: Vec<HedgeCycle>,
    pub(crate) open_exposure: Option<OpenExposure>,
}

/// A single onchain fill as witnessed by the bot.
#[derive(Debug, Clone)]
pub(crate) struct FillObservation {
    pub(crate) block_timestamp: DateTime<Utc>,
    pub(crate) seen_at: DateTime<Utc>,
}

impl FillObservation {
    /// Time from the fill's block to the bot observing it, clamped to zero:
    /// a freshly mined block can carry a timestamp slightly ahead of the
    /// bot's wall clock, and a negative latency would corrupt percentile
    /// aggregations.
    pub(crate) fn detection_latency(&self) -> Duration {
        (self.seen_at - self.block_timestamp).max(Duration::zero())
    }
}

/// One hedge order and the timestamps along its pipeline.
///
/// Because hedges trigger on execution thresholds, a single hedge order may
/// cover several accumulated onchain fills ([`CoveredFills`]).
#[derive(Debug, Clone)]
pub(crate) struct HedgeCycle {
    pub(crate) offchain_order_id: OffchainOrderId,
    pub(crate) placed_at: DateTime<Utc>,
    pub(crate) covered: Option<CoveredFills>,
    pub(crate) submitted_at: Option<DateTime<Utc>>,
    pub(crate) outcome: HedgeOutcome,
}

impl HedgeCycle {
    /// Observation of the threshold-crossing fill to hedge placement, clamped
    /// to zero: both timestamps are bot wall-clock values; NTP corrections can
    /// produce placed_at < latest_seen_at, yielding a negative Duration that
    /// would corrupt percentile aggregations.
    pub(crate) fn decision_latency(&self) -> Option<Duration> {
        let covered = self.covered.as_ref()?;
        Some((self.placed_at - covered.latest_seen_at).max(Duration::zero()))
    }

    /// Hedge placement to broker acceptance, clamped to zero for the same
    /// reason as decision_latency.
    pub(crate) fn submission_latency(&self) -> Option<Duration> {
        Some((self.submitted_at? - self.placed_at).max(Duration::zero()))
    }

    /// Broker acceptance to broker fill, clamped to zero for the same reason
    /// as decision_latency.
    pub(crate) fn execution_latency(&self) -> Option<Duration> {
        let HedgeOutcome::Filled { filled_at } = self.outcome else {
            return None;
        };
        Some((filled_at - self.submitted_at?).max(Duration::zero()))
    }

    /// Earliest covered fill's block to broker fill: the full period during
    /// which the system carried unhedged delta for this batch, clamped to
    /// zero: the broker clock and the block clock are independent time
    /// sources, and cross-source jitter can produce a negative duration that
    /// would corrupt percentile aggregations.
    pub(crate) fn exposure_window(&self) -> Option<Duration> {
        let HedgeOutcome::Filled { filled_at } = self.outcome else {
            return None;
        };
        let covered = self.covered.as_ref()?;
        Some((filled_at - covered.earliest_block_timestamp).max(Duration::zero()))
    }
}

/// The batch of onchain fills a hedge order covers: every fill observed since
/// the previous hedge placement (or attribution reset) in the Position stream.
#[derive(Debug, Clone)]
pub(crate) struct CoveredFills {
    pub(crate) count: usize,
    pub(crate) earliest_block_timestamp: DateTime<Utc>,
    pub(crate) latest_seen_at: DateTime<Utc>,
}

/// Terminal state of a hedge order, read from the read-model tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HedgeOutcome {
    Pending,
    Filled { filled_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

/// Onchain fills observed after the most recent hedge placement: exposure the
/// system currently carries unhedged.
#[derive(Debug, Clone)]
pub(crate) struct OpenExposure {
    pub(crate) fill_count: usize,
    pub(crate) oldest_block_timestamp: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub(crate) enum PerformanceError {
    #[error("failed to query the hedge-latency read model")]
    Database(#[from] sqlx::Error),
    #[error("read-model row carried an invalid symbol")]
    Symbol(#[from] st0x_execution::EmptySymbolError),
    #[error("read-model row carried an unparseable timestamp")]
    Timestamp(#[from] chrono::ParseError),
    #[error("read-model row carried a covered count that does not fit in usize")]
    CoveredCount(#[from] std::num::TryFromIntError),
    #[error("read-model row had an earliest_block_timestamp but no covered fills")]
    InconsistentCoveredBatch,
    #[error("job queue or failure aggregation produced a count outside usize range: {value}")]
    AggregateCount { value: i64 },
    #[error("read-model row carried an unparseable offchain order id")]
    OrderId(#[from] uuid::Error),
}

/// Load hedge performance for every symbol present in the read model.
///
/// Reads ONLY the reactor-maintained tables: fills from `hedge_fill`, hedge
/// cycles from `hedge_cycle`, and open exposure recomputed by [`uncovered_fills`]
/// from `hedge_fill` + `hedge_cycle` + `hedge_attribution_reset`. Symbols are
/// emitted in deterministic (ascending) order.
///
/// `range` filters `hedge_fill` rows by `seen_at` and `hedge_cycle` rows by
/// `placed_at` in SQL, using the existing per-column indexes. Open exposure is
/// always recomputed unconditionally because it reflects the present unhedged
/// state regardless of the requested time window.
pub(crate) async fn load_hedge_performance(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<Vec<SymbolPerformance>, PerformanceError> {
    let mut by_symbol: BTreeMap<Symbol, SymbolAccumulator> = BTreeMap::new();

    let from = range.from.to_rfc3339();
    let to = range.to.to_rfc3339();

    let fill_rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT symbol, block_timestamp, seen_at FROM hedge_fill \
         WHERE seen_at >= ? AND seen_at <= ? ORDER BY id",
    )
    .bind(&from)
    .bind(&to)
    .fetch_all(pool)
    .await?;

    for (symbol, block_timestamp, seen_at) in fill_rows {
        let symbol: Symbol = match symbol.parse() {
            Ok(sym) => sym,
            Err(error) => {
                warn!(%error, raw_symbol = %symbol, "Fill row has invalid symbol, skipping");
                continue;
            }
        };

        match parse_fill_row(&block_timestamp, &seen_at) {
            Ok(observation) => by_symbol.entry(symbol).or_default().fills.push(observation),
            Err(error) => {
                warn!(%error, %symbol, "Fill row has unparseable timestamp, skipping");
            }
        }
    }

    // submitted_at lives in hedge_submission, written independently of the
    // cycle row so a Submitted event that lands before its placement is never
    // lost. The LEFT JOIN folds it back in.
    let cycle_rows: Vec<CycleRow> = sqlx::query_as(
        "SELECT hc.offchain_order_id, hc.symbol, hc.placed_at, hc.covered_count, \
         hc.covered_earliest_block_timestamp, hc.covered_latest_seen_at, \
         hs.submitted_at, hc.filled_at, hc.failed_at \
         FROM hedge_cycle hc \
         LEFT JOIN hedge_submission hs ON hs.offchain_order_id = hc.offchain_order_id \
         WHERE hc.placed_at >= ? AND hc.placed_at <= ? \
         ORDER BY hc.placed_at, hc.offchain_order_id",
    )
    .bind(&from)
    .bind(&to)
    .fetch_all(pool)
    .await?;

    for row in cycle_rows {
        let symbol: Symbol = match row.symbol.parse() {
            Ok(sym) => sym,
            Err(error) => {
                warn!(%error, raw_symbol = %row.symbol, "Cycle row has invalid symbol, skipping");
                continue;
            }
        };

        match row.into_cycle() {
            Ok(cycle) => by_symbol.entry(symbol).or_default().cycles.push(cycle),
            Err(error) => {
                warn!(%error, %symbol, "Cycle row is undeserializable, skipping");
            }
        }
    }

    // Open exposure reflects the present unhedged state and must surface even
    // for a symbol whose fills all predate the requested window. Open exposure
    // can only originate from a `hedge_fill` row, so seed every distinct fill
    // symbol (date-unfiltered) before recomputing -- otherwise a quiet
    // instrument carrying live exposure but no in-window activity would be
    // dropped, contradicting this function's documented contract.
    let exposure_symbols: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT symbol FROM hedge_fill")
            .fetch_all(pool)
            .await?;

    for raw_symbol in exposure_symbols {
        match raw_symbol.parse::<Symbol>() {
            Ok(symbol) => {
                by_symbol.entry(symbol).or_default();
            }
            Err(error) => {
                warn!(%error, raw_symbol = %raw_symbol, "hedge_fill row has invalid symbol, skipping");
            }
        }
    }

    // Recompute open exposure per symbol from the durable, append-only tables.
    // The uncovered pool is a pure replay of hedge_fill + hedge_cycle +
    // hedge_attribution_reset, so the read model needs no in-memory state and
    // survives a restart.
    for (symbol, accumulator) in &mut by_symbol {
        let uncovered = uncovered_fills(pool, symbol).await?;
        accumulator.open_exposure = open_exposure(&uncovered);
    }

    // Keep only symbols with in-window activity or live open exposure. Symbols
    // seeded solely to evaluate exposure but found fully hedged would otherwise
    // surface as empty rows.
    Ok(by_symbol
        .into_iter()
        .filter(|(_, accumulator)| {
            !accumulator.fills.is_empty()
                || !accumulator.cycles.is_empty()
                || accumulator.open_exposure.is_some()
        })
        .map(|(symbol, accumulator)| SymbolPerformance {
            symbol,
            fills: accumulator.fills,
            cycles: accumulator.cycles,
            open_exposure: accumulator.open_exposure,
        })
        .collect())
}

#[derive(Default)]
struct SymbolAccumulator {
    fills: Vec<FillObservation>,
    cycles: Vec<HedgeCycle>,
    open_exposure: Option<OpenExposure>,
}

#[derive(sqlx::FromRow)]
struct CycleRow {
    offchain_order_id: String,
    symbol: String,
    placed_at: String,
    covered_count: i64,
    covered_earliest_block_timestamp: Option<String>,
    covered_latest_seen_at: Option<String>,
    submitted_at: Option<String>,
    filled_at: Option<String>,
    failed_at: Option<String>,
}

impl CycleRow {
    fn into_cycle(self) -> Result<HedgeCycle, PerformanceError> {
        let covered = match (
            self.covered_earliest_block_timestamp,
            self.covered_latest_seen_at,
        ) {
            (Some(earliest), Some(latest)) => Some(CoveredFills {
                count: usize::try_from(self.covered_count)?,
                earliest_block_timestamp: parse_timestamp(&earliest)?,
                latest_seen_at: parse_timestamp(&latest)?,
            }),
            // A non-zero covered_count with no timestamps means the writer
            // claimed attribution it cannot describe; refuse it. The DB CHECK
            // also rejects this, so this is belt-and-suspenders for rows that
            // predate the constraint or arrive via a different writer.
            (None, None) if self.covered_count > 0 => {
                return Err(PerformanceError::InconsistentCoveredBatch);
            }
            (None, None) => None,
            // A half-populated covered batch means the writer violated its own
            // invariant; refuse to silently report partial attribution.
            (Some(_), None) | (None, Some(_)) => {
                return Err(PerformanceError::InconsistentCoveredBatch);
            }
        };

        // Filled wins over failed: the Position stream's broker fill is the
        // authoritative terminal outcome even if a failure was recorded first.
        let outcome = match (self.filled_at, self.failed_at) {
            (Some(filled_at), _) => HedgeOutcome::Filled {
                filled_at: parse_timestamp(&filled_at)?,
            },
            (None, Some(failed_at)) => HedgeOutcome::Failed {
                failed_at: parse_timestamp(&failed_at)?,
            },
            (None, None) => HedgeOutcome::Pending,
        };

        Ok(HedgeCycle {
            offchain_order_id: self.offchain_order_id.parse()?,
            placed_at: parse_timestamp(&self.placed_at)?,
            covered,
            submitted_at: self
                .submitted_at
                .as_deref()
                .map(parse_timestamp)
                .transpose()?,
            outcome,
        })
    }
}

fn open_exposure(uncovered: &[UncoveredFill]) -> Option<OpenExposure> {
    uncovered
        .iter()
        .map(|fill| fill.block_timestamp)
        .min()
        .map(|oldest_block_timestamp| OpenExposure {
            fill_count: uncovered.len(),
            oldest_block_timestamp,
        })
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

/// Parses a `hedge_fill` row's two timestamp columns into a [`FillObservation`].
/// A dedicated function gives the two parses a shared `?`-scope, mirroring
/// [`CycleRow::into_cycle`].
fn parse_fill_row(
    block_timestamp: &str,
    seen_at: &str,
) -> Result<FillObservation, PerformanceError> {
    Ok(FillObservation {
        block_timestamp: parse_timestamp(block_timestamp)?,
        seen_at: parse_timestamp(seen_at)?,
    })
}

/// One fill in the recompute queue, carrying the timestamps the attribution
/// algorithm needs (block time and observation time). Reconstructed from
/// `hedge_fill` rows when [`uncovered_fills`] replays the durable tables.
#[derive(Debug, Clone)]
struct UncoveredFill {
    block_timestamp: DateTime<Utc>,
    seen_at: DateTime<Utc>,
}

/// Parses a `hedge_fill` row's two timestamp columns into an [`UncoveredFill`].
/// Shares a `?`-scope across both parses, mirroring [`parse_fill_row`].
fn parse_uncovered_fill(
    block_timestamp: &str,
    seen_at: &str,
) -> Result<UncoveredFill, PerformanceError> {
    Ok(UncoveredFill {
        block_timestamp: parse_timestamp(block_timestamp)?,
        seen_at: parse_timestamp(seen_at)?,
    })
}

/// Computes the current uncovered fill set for a symbol PURELY from the durable,
/// append-only tables `hedge_fill`, `hedge_cycle`, and `hedge_attribution_reset`.
///
/// Algorithm:
/// 1. Load `hedge_fill` rows for the symbol in arrival order (by `id`).
/// 2. `latest_reset_at` = `MAX(adjusted_at)` from `hedge_attribution_reset` for
///    the symbol (absent if no reset). Drop fills with `seen_at <= reset` and
///    cycles with `placed_at <= reset`: a manual adjustment means those fills no
///    longer drive hedging and the cycles that covered them are no longer
///    attributable.
/// 3. Load `hedge_cycle` rows for the symbol ordered by `(placed_at,
///    offchain_order_id)` and walk them: a Filled or Pending cycle consumes its
///    `covered_count` fills from the FRONT of the queue (hedged / in-flight); a
///    Failed cycle consumes nothing (its covered fills return to the pool so a
///    retry inherits attribution). Filled wins over Failed.
/// 4. The remaining queue is the uncovered pool.
///
/// Used by BOTH the read path and the placement writer: the uncovered set at
/// placement time IS the covered batch for that placement.
async fn uncovered_fills(
    pool: &SqlitePool,
    symbol: &Symbol,
) -> Result<Vec<UncoveredFill>, PerformanceError> {
    let symbol_text = symbol.to_string();

    // MAX over zero matching rows yields a single NULL row, so the aggregate is
    // typed Option<String>: None when the symbol has never been reset.
    let (latest_reset,): (Option<String>,) =
        sqlx::query_as("SELECT MAX(adjusted_at) FROM hedge_attribution_reset WHERE symbol = ?")
            .bind(&symbol_text)
            .fetch_one(pool)
            .await?;

    let latest_reset_at = latest_reset
        .map(|value| parse_timestamp(&value))
        .transpose()?;

    let fill_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT block_timestamp, seen_at FROM hedge_fill WHERE symbol = ? ORDER BY id",
    )
    .bind(&symbol_text)
    .fetch_all(pool)
    .await?;

    // Parse, reset-filter, and collect in a single pass. Malformed rows are
    // skipped with a warning rather than propagating, matching
    // load_hedge_performance's outer loop: one unparseable row for a symbol that
    // also has valid data must not blank the entire report.
    let mut queue: std::collections::VecDeque<UncoveredFill> = fill_rows
        .into_iter()
        .filter_map(|(block_timestamp, seen_at)| {
            let fill = match parse_uncovered_fill(&block_timestamp, &seen_at) {
                Ok(fill) => fill,
                Err(error) => {
                    warn!(%error, %symbol, "Fill row has unparseable timestamp, skipping");
                    return None;
                }
            };

            latest_reset_at
                .is_none_or(|reset| fill.seen_at > reset)
                .then_some(fill)
        })
        .collect();

    let cycle_rows: Vec<(String, i64, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT placed_at, covered_count, filled_at, failed_at \
         FROM hedge_cycle WHERE symbol = ? ORDER BY placed_at, offchain_order_id",
    )
    .bind(&symbol_text)
    .fetch_all(pool)
    .await?;

    for (placed_at, covered_count, filled_at, failed_at) in cycle_rows {
        let placed_at = match parse_timestamp(&placed_at) {
            Ok(parsed) => parsed,
            Err(error) => {
                warn!(%error, %symbol, "Cycle row has unparseable placed_at, skipping");
                continue;
            }
        };

        if latest_reset_at.is_some_and(|reset| placed_at <= reset) {
            continue;
        }

        // Filled wins over Failed: a recorded broker fill is the authoritative
        // terminal outcome. A Failed cycle returns its fills to the pool, so it
        // consumes nothing; Filled and Pending cycles consume their batch.
        let consumes = filled_at.is_some() || failed_at.is_none();
        if consumes {
            let count = usize::try_from(covered_count)?;
            queue.drain(..count.min(queue.len()));
        }
    }

    Ok(queue.into())
}

/// Reactor maintaining the hedge-latency read model from live events.
pub(crate) struct HedgeLatencyProjection {
    pool: SqlitePool,
}

deps!(HedgeLatencyProjection, [Position, OffchainOrder]);

impl HedgeLatencyProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Dispatches Position events to per-variant handlers.
    async fn on_position(
        &self,
        symbol: Symbol,
        event: PositionEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            PositionEvent::Initialized { .. } | PositionEvent::ThresholdUpdated { .. } => Ok(()),
            PositionEvent::OnChainOrderFilled {
                trade_id,
                block_timestamp,
                seen_at,
                ..
            } => {
                self.on_chain_fill(&symbol, &trade_id, block_timestamp, seen_at)
                    .await
            }
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                placed_at,
                ..
            } => {
                self.offchain_order_placed(&symbol, offchain_order_id, placed_at)
                    .await
            }
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                broker_timestamp,
                ..
            } => {
                self.offchain_order_filled(offchain_order_id, broker_timestamp)
                    .await
            }
            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                failed_at,
                ..
            } => {
                self.offchain_order_failed(offchain_order_id, failed_at)
                    .await
            }
            // A manual adjustment means accumulated fills no longer drive
            // hedging decisions; attributing them to a later hedge would
            // overstate its exposure window. Record the reset boundary so the
            // read path drops every fill and cycle at or before adjusted_at when
            // recomputing the uncovered pool.
            PositionEvent::ManualPositionAdjusted { adjusted_at, .. } => {
                self.manual_position_adjusted(&symbol, adjusted_at).await
            }
        }
    }

    /// Records a manual-adjustment reset boundary in `hedge_attribution_reset`.
    /// The read path drops every fill (`seen_at`) and cycle (`placed_at`) at or
    /// before `adjusted_at`, so accumulated fills can no longer be attributed to
    /// a later hedge and in-flight batches cannot resurface.
    async fn manual_position_adjusted(
        &self,
        symbol: &Symbol,
        adjusted_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        sqlx::query("INSERT INTO hedge_attribution_reset (symbol, adjusted_at) VALUES (?, ?)")
            .bind(symbol.to_string())
            .bind(adjusted_at.to_rfc3339())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Records a new onchain fill in `hedge_fill`, keyed by the originating
    /// fill's `(tx_hash, log_index)`. `ON CONFLICT(tx_hash, log_index) DO
    /// NOTHING` makes redelivery idempotent: a replayed Position event cannot
    /// insert a duplicate row, so fill counts and the uncovered pool (recomputed
    /// from this table on read) stay accurate.
    async fn on_chain_fill(
        &self,
        symbol: &Symbol,
        trade_id: &TradeId,
        block_timestamp: DateTime<Utc>,
        seen_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let log_index = i64::try_from(trade_id.log_index)?;
        let updated = sqlx::query(
            "INSERT INTO hedge_fill (symbol, tx_hash, log_index, block_timestamp, seen_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(tx_hash, log_index) DO NOTHING",
        )
        .bind(symbol.to_string())
        .bind(trade_id.tx_hash.to_string())
        .bind(log_index)
        .bind(block_timestamp.to_rfc3339())
        .bind(seen_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            debug!(%symbol, %trade_id, "Duplicate onchain fill event, keeping first observation");
        }

        Ok(())
    }

    /// Records a new hedge cycle in `hedge_cycle`, snapshotting the symbol's
    /// current uncovered fill set ([`uncovered_fills`]) as the covered batch.
    /// `ON CONFLICT(offchain_order_id) DO NOTHING` makes redelivery idempotent:
    /// the covered batch is derived from durable tables, not mutated, so a
    /// duplicate placement leaves attribution untouched.
    async fn offchain_order_placed(
        &self,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        placed_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        // Snapshot the current uncovered set as this placement's covered batch.
        // The per-aggregate serial-delivery invariant means no concurrent writer
        // mutates hedge_fill or hedge_attribution_reset between this read and the
        // INSERT, and ON CONFLICT DO NOTHING keeps redelivery idempotent.
        let batch = uncovered_fills(&self.pool, symbol).await?;
        let covered = covered_fills(&batch);

        let (earliest_block_ts, latest_seen_at) = covered.as_ref().map_or((None, None), |cov| {
            (
                Some(cov.earliest_block_timestamp.to_rfc3339()),
                Some(cov.latest_seen_at.to_rfc3339()),
            )
        });

        let count = i64::try_from(batch.len())?;
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count, \
              covered_earliest_block_timestamp, covered_latest_seen_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(offchain_order_id) DO NOTHING",
        )
        .bind(offchain_order_id.to_string())
        .bind(symbol.to_string())
        .bind(placed_at.to_rfc3339())
        .bind(count)
        .bind(earliest_block_ts)
        .bind(latest_seen_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Stamps `filled_at` on the cycle. The broker's own fill time is
    /// authoritative over the OffchainOrder aggregate's reconciliation time,
    /// which would overstate execution latency by polling and queue delay.
    async fn offchain_order_filled(
        &self,
        offchain_order_id: OffchainOrderId,
        broker_timestamp: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let id_str = offchain_order_id.to_string();
        let updated = sqlx::query(
            "UPDATE hedge_cycle SET filled_at = ? \
             WHERE offchain_order_id = ? AND filled_at IS NULL",
        )
        .bind(broker_timestamp.to_rfc3339())
        .bind(&id_str)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            // Distinguish a missing row from a duplicate fill event.
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT offchain_order_id FROM hedge_cycle WHERE offchain_order_id = ?",
            )
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await?;

            if exists.is_none() {
                warn!(
                    %offchain_order_id,
                    "Fill reported for a hedge with no placement in the read model"
                );
            } else {
                debug!(
                    %offchain_order_id,
                    "Duplicate fill event, keeping first filled_at timestamp"
                );
            }
        }

        Ok(())
    }

    /// Stamps `failed_at` on the cycle. A Failed cycle consumes no fills when
    /// the uncovered pool is recomputed ([`uncovered_fills`]), so its covered
    /// batch returns to the pool and a retry inherits attribution back to the
    /// original fill -- no mutable state to rewrite.
    async fn offchain_order_failed(
        &self,
        offchain_order_id: OffchainOrderId,
        failed_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let id_str = offchain_order_id.to_string();

        // Only set failed_at when the cycle has no terminal outcome yet:
        // a recorded fill must win.
        let updated = sqlx::query(
            "UPDATE hedge_cycle SET failed_at = ? \
             WHERE offchain_order_id = ? AND filled_at IS NULL AND failed_at IS NULL",
        )
        .bind(failed_at.to_rfc3339())
        .bind(&id_str)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT offchain_order_id FROM hedge_cycle WHERE offchain_order_id = ?",
            )
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await?;

            if exists.is_none() {
                warn!(
                    %offchain_order_id,
                    "Failure reported for a hedge with no placement in the read model"
                );
            } else {
                warn!(
                    %offchain_order_id,
                    "Failure reported for a hedge that already has a terminal outcome \
                     -- failed_at dropped"
                );
            }
        }

        Ok(())
    }

    async fn on_offchain_order(
        &self,
        id: OffchainOrderId,
        event: OffchainOrderEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            OffchainOrderEvent::Submitted { submitted_at, .. } => {
                // Record submission in its own table, keyed by order id, so it
                // survives regardless of whether the placement row exists yet:
                // the Position and OffchainOrder streams are independent, and a
                // Submitted event can arrive before OffChainOrderPlaced. The
                // read path LEFT JOINs this back in. ON CONFLICT DO NOTHING keeps
                // the first timestamp (idempotent redelivery).
                let updated = sqlx::query(
                    "INSERT INTO hedge_submission (offchain_order_id, submitted_at) \
                     VALUES (?, ?) \
                     ON CONFLICT(offchain_order_id) DO NOTHING",
                )
                .bind(id.to_string())
                .bind(submitted_at.to_rfc3339())
                .execute(&self.pool)
                .await?;

                if updated.rows_affected() == 0 {
                    debug!(
                        %id,
                        "Duplicate Submitted event, keeping first submitted_at timestamp"
                    );
                }

                Ok(())
            }
            OffchainOrderEvent::Placed { .. }
            | OffchainOrderEvent::PartiallyFilled { .. }
            | OffchainOrderEvent::Filled { .. }
            | OffchainOrderEvent::Failed { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("hedge-latency read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("covered fill count does not fit in i64")]
    CoveredCount(#[from] std::num::TryFromIntError),
    #[error("recomputing the uncovered pool failed")]
    Uncovered(#[from] PerformanceError),
}

#[async_trait]
impl Reactor for HedgeLatencyProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move { self.on_position(symbol, event).await })
            .on(|id, event| async move { self.on_offchain_order(id, event).await })
            .exhaustive()
            .await
    }
}

fn covered_fills(uncovered: &[UncoveredFill]) -> Option<CoveredFills> {
    let first = uncovered.first()?;
    let (earliest_block_timestamp, latest_seen_at) = uncovered.iter().skip(1).fold(
        (first.block_timestamp, first.seen_at),
        |(earliest, latest), fill| (earliest.min(fill.block_timestamp), latest.max(fill.seen_at)),
    );

    Some(CoveredFills {
        count: uncovered.len(),
        earliest_block_timestamp,
        latest_seen_at,
    })
}

/// Inclusive time range a latency report covers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportRange {
    pub(crate) from: DateTime<Utc>,
    pub(crate) to: DateTime<Utc>,
}

impl ReportRange {
    /// An all-encompassing range that matches every timestamp in the database.
    ///
    /// Used in tests that want to load the full read model without windowing.
    /// Uses year 1000 and year 9000 as sentinels: far enough from any test
    /// timestamp that the SQL WHERE clause is effectively a no-op, while
    /// remaining within the RFC 3339 / SQLite text-comparison range.
    #[cfg(test)]
    pub(crate) fn all_time() -> Self {
        use chrono::TimeZone;
        Self {
            from: Utc.with_ymd_and_hms(1000, 1, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(9000, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.from <= timestamp && timestamp <= self.to
    }

    /// Bucket width mirroring the P&L tab's cadence: daily up to a month,
    /// weekly up to half a year, otherwise ~monthly -- with hourly buckets for
    /// short windows so a 24h report still shows a trend.
    ///
    /// The daily/weekly/monthly thresholds use the P&L tab's INCLUSIVE day
    /// count -- `floor(span_in_days) + 1`, stepping up past 31 and 183 days --
    /// rather than the exclusive span. A range whose endpoints are exactly 31
    /// (or 183) days apart spans 32 (184) calendar days, so it must already
    /// step to the coarser cadence, matching the P&L tab at the exact boundary.
    fn bucket_width(&self) -> Duration {
        let span = self.to - self.from;
        let inclusive_days = span.num_days() + 1;
        if span <= Duration::days(2) {
            Duration::hours(1)
        } else if inclusive_days <= 31 {
            Duration::days(1)
        } else if inclusive_days <= 183 {
            Duration::days(7)
        } else {
            Duration::days(30)
        }
    }
}

/// Latency samples (in milliseconds) for each pipeline stage.
#[derive(Debug, Default)]
struct StageSamples {
    detection: Vec<i64>,
    decision: Vec<i64>,
    submission: Vec<i64>,
    execution: Vec<i64>,
    exposure_window: Vec<i64>,
}

impl StageSamples {
    fn push_cycle(&mut self, cycle: &HedgeCycle) {
        let stages = [
            (&mut self.decision, cycle.decision_latency()),
            (&mut self.submission, cycle.submission_latency()),
            (&mut self.execution, cycle.execution_latency()),
            (&mut self.exposure_window, cycle.exposure_window()),
        ];
        for (samples, latency) in stages {
            if let Some(latency) = latency {
                // The HedgeCycle latency methods already clamp to
                // Duration::zero(), so num_milliseconds() is non-negative
                // here. No secondary clamp needed.
                samples.push(latency.num_milliseconds());
            }
        }
    }

    fn into_stage_latencies(mut self) -> StageLatencies {
        StageLatencies {
            detection: latency_stats(&mut self.detection),
            decision: latency_stats(&mut self.decision),
            submission: latency_stats(&mut self.submission),
            execution: latency_stats(&mut self.execution),
            exposure_window: latency_stats(&mut self.exposure_window),
        }
    }
}

/// Assemble the dashboard latency report from per-symbol performance.
///
/// Fills are windowed by `seen_at`, cycles by `placed_at`. Open exposures
/// reflect the present state regardless of the requested range.
pub(crate) fn hedge_latency_report(
    performances: &[SymbolPerformance],
    range: &ReportRange,
) -> HedgeLatencies {
    let width = range.bucket_width();
    let bucket_index = |timestamp: DateTime<Utc>| -> i64 {
        (timestamp - range.from).num_seconds() / width.num_seconds()
    };

    let mut summary_samples = StageSamples::default();
    let mut bucket_samples: BTreeMap<i64, StageSamples> = BTreeMap::new();
    let mut cycles = Vec::new();
    let mut open_exposures = Vec::new();
    let mut fill_count = 0;

    for performance in performances {
        for fill in &performance.fills {
            if !range.contains(fill.seen_at) {
                continue;
            }
            fill_count += 1;
            let latency_ms = fill.detection_latency().num_milliseconds();
            summary_samples.detection.push(latency_ms);
            bucket_samples
                .entry(bucket_index(fill.seen_at))
                .or_default()
                .detection
                .push(latency_ms);
        }

        for cycle in &performance.cycles {
            if !range.contains(cycle.placed_at) {
                continue;
            }
            summary_samples.push_cycle(cycle);
            bucket_samples
                .entry(bucket_index(cycle.placed_at))
                .or_default()
                .push_cycle(cycle);
            cycles.push(cycle_report(&performance.symbol, cycle));
        }

        if let Some(open) = &performance.open_exposure {
            open_exposures.push(OpenExposureReport {
                symbol: performance.symbol.clone(),
                fill_count: open.fill_count,
                oldest_fill_block_timestamp: open.oldest_block_timestamp,
            });
        }
    }

    cycles.sort_unstable_by_key(|cycle| std::cmp::Reverse(cycle.placed_at));
    let total_cycles = cycles.len();
    cycles.truncate(MAX_CYCLE_REPORTS);

    let buckets = bucket_samples
        .into_iter()
        .map(|(index, samples)| LatencyBucket {
            start: range.from + Duration::seconds(width.num_seconds() * index),
            stages: samples.into_stage_latencies(),
        })
        .collect();

    HedgeLatencies {
        summary: LatencySummary {
            fill_count,
            stages: summary_samples.into_stage_latencies(),
        },
        buckets,
        cycles,
        total_cycles,
        open_exposures,
    }
}

fn cycle_report(symbol: &Symbol, cycle: &HedgeCycle) -> HedgeCycleReport {
    let (status, completed_at) = match cycle.outcome {
        HedgeOutcome::Pending => (HedgeCycleStatus::Pending, None),
        HedgeOutcome::Filled { filled_at } => (HedgeCycleStatus::Filled, Some(filled_at)),
        HedgeOutcome::Failed { failed_at } => (HedgeCycleStatus::Failed, Some(failed_at)),
    };

    HedgeCycleReport {
        symbol: symbol.clone(),
        offchain_order_id: cycle.offchain_order_id.as_uuid(),
        placed_at: cycle.placed_at,
        covered_fill_count: cycle.covered.as_ref().map_or(0, |covered| covered.count),
        earliest_fill_block_timestamp: cycle
            .covered
            .as_ref()
            .map(|covered| covered.earliest_block_timestamp),
        submitted_at: cycle.submitted_at,
        status,
        completed_at,
        decision_ms: cycle
            .decision_latency()
            .map(|latency| latency.num_milliseconds()),
        submission_ms: cycle
            .submission_latency()
            .map(|latency| latency.num_milliseconds()),
        execution_ms: cycle
            .execution_latency()
            .map(|latency| latency.num_milliseconds()),
        exposure_window_ms: cycle
            .exposure_window()
            .map(|latency| latency.num_milliseconds()),
    }
}

/// Nearest-rank percentiles over the given samples; `None` when empty.
fn latency_stats(samples_ms: &mut [i64]) -> Option<LatencyStats> {
    samples_ms.sort_unstable();
    let len = samples_ms.len();
    let nearest_rank = |numerator: usize, denominator: usize| -> Option<i64> {
        let rank = (numerator * len).div_ceil(denominator).max(1);
        samples_ms.get(rank - 1).copied()
    };

    Some(LatencyStats {
        p50_ms: nearest_rank(1, 2)?,
        p90_ms: nearest_rank(9, 10)?,
        p95_ms: nearest_rank(19, 20)?,
        p99_ms: nearest_rank(99, 100)?,
        max_ms: samples_ms.last().copied()?,
        sample_count: len,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use chrono::TimeZone;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor};
    use st0x_float_macro::float;

    use crate::position::TriggerReason;
    use crate::test_utils::setup_test_db;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    fn fill_event(log_index: u64, block_offset: i64, seen_offset: i64) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index,
            },
            amount: FractionalShares::new(float!(1)),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_timestamp: timestamp(block_offset),
            seen_at: timestamp(seen_offset),
        }
    }

    fn placed_event(order_id: OffchainOrderId, placed_offset: i64) -> PositionEvent {
        PositionEvent::OffChainOrderPlaced {
            offchain_order_id: order_id,
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: float!(1),
                threshold_shares: float!(1),
            },
            placed_at: timestamp(placed_offset),
        }
    }

    fn symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn position_failed_event(order_id: OffchainOrderId, failed_offset: i64) -> PositionEvent {
        PositionEvent::OffChainOrderFailed {
            offchain_order_id: order_id,
            error: "broker rejected".to_string(),
            failed_at: timestamp(failed_offset),
        }
    }

    fn position_filled_event(order_id: OffchainOrderId, broker_offset: i64) -> PositionEvent {
        PositionEvent::OffChainOrderFilled {
            offchain_order_id: order_id,
            shares_filled: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
            price: "150.25".parse().unwrap(),
            broker_timestamp: timestamp(broker_offset),
        }
    }

    /// Drives a sequence of `Position` events for one symbol through the
    /// reactor, then loads that symbol's performance from the read model.
    async fn run_position_stream(
        symbol: Symbol,
        events: Vec<PositionEvent>,
    ) -> (SqlitePool, SymbolPerformance) {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        for event in events {
            harness
                .receive::<Position>(symbol.clone(), event)
                .await
                .unwrap();
        }

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let performance = report
            .into_iter()
            .find(|performance| performance.symbol == symbol)
            .unwrap_or_else(|| SymbolPerformance {
                symbol: symbol.clone(),
                fills: Vec::new(),
                cycles: Vec::new(),
                open_exposure: None,
            });

        (pool, performance)
    }

    #[tokio::test]
    async fn failed_hedge_returns_fills_to_open_exposure() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_failed_event(order_id, 5),
            ],
        )
        .await;

        let open = performance.open_exposure.unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
        assert_eq!(
            performance.cycles[0].outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(5)
            }
        );
    }

    #[tokio::test]
    async fn retry_after_failed_hedge_inherits_attribution() {
        let failed_order = OffchainOrderId::new();
        let retry_order = OffchainOrderId::new();

        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        for event in [
            fill_event(1, 0, 1),
            placed_event(failed_order, 2),
            position_failed_event(failed_order, 5),
            placed_event(retry_order, 10),
        ] {
            harness.receive::<Position>(symbol(), event).await.unwrap();
        }
        // The retry's broker pipeline: submitted then filled at the broker.
        harness
            .receive::<OffchainOrder>(
                retry_order,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(11),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(retry_order, 12))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let performance = &report[0];

        assert!(performance.open_exposure.is_none());
        let retry = &performance.cycles[1];
        let covered = retry.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(retry.exposure_window(), Some(Duration::seconds(12)));
        assert_eq!(retry.submitted_at, Some(timestamp(11)));
        assert_eq!(
            retry.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(12)
            }
        );
    }

    #[tokio::test]
    async fn manual_adjustment_clears_in_flight_attribution() {
        // The same order is placed before the reset and fails after it: the
        // reset must prevent its parked fills from resurfacing.
        let adjusted_order = OffchainOrderId::new();
        let later_order = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(adjusted_order, 2),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: FractionalShares::new(float!(1)),
                    target_net: FractionalShares::new(float!(0)),
                    reason: "manual reset".to_string(),
                    price_usdc: None,
                    adjusted_at: timestamp(5),
                },
                position_failed_event(adjusted_order, 6),
                placed_event(later_order, 10),
            ],
        )
        .await;

        assert!(performance.open_exposure.is_none());
        assert!(performance.cycles[1].covered.is_none());
        // The failure still marks the first cycle's outcome; only the fill
        // attribution is reset.
        assert_eq!(
            performance.cycles[0].outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(6)
            }
        );
    }

    #[tokio::test]
    async fn broker_fill_timestamp_is_the_terminal_outcome() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_filled_event(order_id, 8),
            ],
        )
        .await;

        let cycle = &performance.cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(8)
            }
        );
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(8)));
    }

    #[tokio::test]
    async fn detection_latency_clamps_to_zero_when_block_timestamp_is_ahead() {
        let (_pool, performance) = run_position_stream(symbol(), vec![fill_event(1, 5, 3)]).await;

        assert_eq!(performance.fills[0].detection_latency(), Duration::zero());
    }

    /// When the broker fills before the earliest covered block timestamp (broker
    /// clock behind block clock), the exposure window must clamp to zero rather
    /// than returning a negative duration that would corrupt percentile
    /// aggregations.
    #[tokio::test]
    async fn exposure_window_clamps_to_zero_when_block_timestamp_is_ahead() {
        let order_id = OffchainOrderId::new();

        // Fill has block_timestamp=+10 (ahead of broker fill at +5); the exposure
        // window would be negative without the clamp.
        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 10, 11),
                placed_event(order_id, 12),
                position_filled_event(order_id, 5),
            ],
        )
        .await;

        let cycle = &performance.cycles[0];
        assert_eq!(cycle.exposure_window(), Some(Duration::zero()));
    }

    #[tokio::test]
    async fn filled_outcome_without_submitted_at_has_no_execution_latency() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_filled_event(order_id, 9),
            ],
        )
        .await;

        let cycle = &performance.cycles[0];
        assert_eq!(cycle.submitted_at, None);
        assert_eq!(cycle.execution_latency(), None);
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(9)));
    }

    #[tokio::test]
    async fn filled_hedge_reports_all_latencies() {
        let order_id = OffchainOrderId::new();

        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 5))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 10))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(12),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 15))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(report[0].cycles.len(), 1);
        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.decision_latency(), Some(Duration::seconds(5)));
        assert_eq!(cycle.submission_latency(), Some(Duration::seconds(2)));
        assert_eq!(cycle.execution_latency(), Some(Duration::seconds(3)));
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(15)));
        assert!(report[0].open_exposure.is_none());
    }

    #[tokio::test]
    async fn hedge_covers_every_fill_since_previous_placement() {
        let first_order = OffchainOrderId::new();
        let second_order = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 2),
                placed_event(first_order, 3),
                fill_event(2, 10, 12),
                fill_event(3, 20, 22),
                fill_event(4, 30, 32),
                placed_event(second_order, 40),
            ],
        )
        .await;

        assert_eq!(performance.cycles.len(), 2);

        let first_covered = performance.cycles[0].covered.as_ref().unwrap();
        assert_eq!(first_covered.count, 1);

        let second_covered = performance.cycles[1].covered.as_ref().unwrap();
        assert_eq!(second_covered.count, 3);
        assert_eq!(second_covered.earliest_block_timestamp, timestamp(10));
        assert_eq!(second_covered.latest_seen_at, timestamp(32));
        assert_eq!(
            performance.cycles[1].decision_latency(),
            Some(Duration::seconds(8))
        );
    }

    #[tokio::test]
    async fn fills_after_last_placement_are_open_exposure() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                fill_event(2, 50, 52),
                fill_event(3, 40, 43),
            ],
        )
        .await;

        let open = performance.open_exposure.unwrap();
        assert_eq!(open.fill_count, 2);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    #[tokio::test]
    async fn failed_hedge_has_no_execution_latency_or_exposure_window() {
        let order_id = OffchainOrderId::new();

        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 3))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(4),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_failed_event(order_id, 6))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(6)
            }
        );
        assert_eq!(cycle.submission_latency(), Some(Duration::seconds(1)));
        assert_eq!(cycle.execution_latency(), None);
        assert_eq!(cycle.exposure_window(), None);
    }

    #[tokio::test]
    async fn placement_without_covered_fills_has_no_attribution() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) =
            run_position_stream(symbol(), vec![placed_event(order_id, 0)]).await;

        let cycle = &performance.cycles[0];
        assert!(cycle.covered.is_none());
        assert_eq!(cycle.decision_latency(), None);
        assert_eq!(cycle.exposure_window(), None);
    }

    #[tokio::test]
    async fn manual_adjustment_resets_attribution() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: FractionalShares::new(float!(1)),
                    target_net: FractionalShares::new(float!(0)),
                    reason: "test reset".to_string(),
                    price_usdc: None,
                    adjusted_at: timestamp(5),
                },
                placed_event(order_id, 10),
            ],
        )
        .await;

        assert!(performance.cycles[0].covered.is_none());
        assert!(performance.open_exposure.is_none());
        assert_eq!(performance.fills.len(), 1);
    }

    #[tokio::test]
    async fn unknown_order_id_reports_pending_outcome() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![fill_event(1, 0, 1), placed_event(order_id, 2)],
        )
        .await;

        let cycle = &performance.cycles[0];
        assert_eq!(cycle.outcome, HedgeOutcome::Pending);
        assert_eq!(cycle.submitted_at, None);
        assert_eq!(cycle.submission_latency(), None);
    }

    #[tokio::test]
    async fn load_hedge_performance_joins_position_and_order_streams() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 5))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 10))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(12),
                },
            )
            .await
            .unwrap();
        // The Position stream carries the broker's own fill time (13s), the
        // authoritative terminal outcome for the cycle.
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 13))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();

        assert_eq!(report.len(), 1);
        assert_eq!(report[0].symbol, symbol());
        assert_eq!(report[0].fills.len(), 1);

        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(12)));
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(13)
            }
        );
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(13)));
    }

    #[tokio::test]
    async fn load_hedge_performance_isolates_symbols_in_deterministic_order() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(Symbol::new("TSLA").unwrap(), fill_event(1, 0, 3))
            .await
            .unwrap();
        harness
            .receive::<Position>(Symbol::new("AAPL").unwrap(), fill_event(2, 0, 5))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();

        assert_eq!(report.len(), 2);
        assert_eq!(report[0].symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(report[1].symbol, Symbol::new("TSLA").unwrap());
        assert_eq!(report[0].fills.len(), 1);
        assert_eq!(report[1].fills.len(), 1);
        assert_eq!(report[0].fills[0].detection_latency(), Duration::seconds(5));
        assert_eq!(report[1].fills[0].detection_latency(), Duration::seconds(3));
    }

    #[tokio::test]
    async fn duplicate_submitted_keeps_the_first_timestamp() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), placed_event(order_id, 1))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(2),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-2"),
                    submitted_at: timestamp(4),
                },
            )
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(2)));
    }

    /// A restart re-instantiates the reactor over the SAME database. Because
    /// the uncovered pool is recomputed from the durable, append-only tables
    /// (no persisted mutable state), a fresh instance attributes a later
    /// placement back to fills observed before the restart.
    #[tokio::test]
    async fn restart_resumes_from_durable_tables() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        // First reactor instance observes a fill, then is dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            harness
                .receive::<Position>(symbol(), fill_event(1, 0, 1))
                .await
                .unwrap();
        }

        // Fresh reactor instance (simulated restart) places a hedge: it must
        // cover the fill recorded by the first instance, recomputed from the
        // durable hedge_fill table.
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 5))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        let covered = cycle.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(cycle.decision_latency(), Some(Duration::seconds(4)));
        assert!(report[0].open_exposure.is_none());
    }

    /// Open exposure is correct after a fresh `load_hedge_performance` with no
    /// in-memory reactor state surviving: a fill is recorded, then the only
    /// reader is a brand-new `load` call against the durable tables. This proves
    /// the read model survives a "restart" because it is derived purely from
    /// hedge_fill + hedge_cycle + hedge_attribution_reset.
    #[tokio::test]
    async fn open_exposure_recomputed_from_durable_tables_after_restart() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        // Reactor records a fill and a placement that covers it, then a later
        // unhedged fill, then is dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            for event in [
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                fill_event(2, 40, 43),
            ] {
                harness.receive::<Position>(symbol(), event).await.unwrap();
            }
        }

        // No reactor instance is alive. The read path alone must report the
        // single post-placement fill as open exposure.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    #[test]
    fn latency_stats_uses_nearest_rank() {
        let stats = latency_stats(&mut (1..=100).collect::<Vec<_>>()).unwrap();

        assert_eq!(stats.p50_ms, 50);
        assert_eq!(stats.p90_ms, 90);
        assert_eq!(stats.p95_ms, 95);
        assert_eq!(stats.p99_ms, 99);
        assert_eq!(stats.max_ms, 100);
        assert_eq!(stats.sample_count, 100);
    }

    #[test]
    fn latency_stats_single_sample_is_every_percentile() {
        let stats = latency_stats(&mut [42]).unwrap();

        assert_eq!(stats.p50_ms, 42);
        assert_eq!(stats.p90_ms, 42);
        assert_eq!(stats.p95_ms, 42);
        assert_eq!(stats.p99_ms, 42);
        assert_eq!(stats.max_ms, 42);
        assert_eq!(stats.sample_count, 1);
    }

    #[test]
    fn latency_stats_empty_is_none() {
        assert_eq!(latency_stats(&mut []), None);
    }

    fn report_range(from_offset: i64, to_offset: i64) -> ReportRange {
        ReportRange {
            from: timestamp(from_offset),
            to: timestamp(to_offset),
        }
    }

    #[test]
    fn bucket_width_is_hourly_for_24h_range() {
        let range = report_range(0, 86_400);
        assert_eq!(range.bucket_width(), Duration::hours(1));
    }

    #[test]
    fn bucket_width_is_hourly_at_exactly_2_days() {
        let range = report_range(0, 2 * 86_400);
        assert_eq!(range.bucket_width(), Duration::hours(1));
    }

    #[test]
    fn bucket_width_is_daily_just_past_2_days() {
        // 2 days + 1 second exceeds the hourly threshold.
        let range = report_range(0, 2 * 86_400 + 1);
        assert_eq!(range.bucket_width(), Duration::days(1));
    }

    /// Drive `Position` events through the reactor and return the full
    /// per-symbol report, the input the report-assembly layer consumes.
    async fn report_performances(events: Vec<PositionEvent>) -> Vec<SymbolPerformance> {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        for event in events {
            harness.receive::<Position>(symbol(), event).await.unwrap();
        }

        load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn report_windows_fills_and_cycles_by_range() {
        let order_id = OffchainOrderId::new();
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 5))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 10))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(11),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 12))
            .await
            .unwrap();
        // Outside the window below.
        harness
            .receive::<Position>(symbol(), fill_event(2, 100_000, 100_005))
            .await
            .unwrap();

        let performances = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let report = hedge_latency_report(&performances, &report_range(0, 1_000));

        assert_eq!(report.summary.fill_count, 1);
        assert_eq!(report.total_cycles, 1);
        assert_eq!(report.cycles.len(), 1);
        assert_eq!(
            report.summary.stages.detection.as_ref().unwrap().p50_ms,
            5_000
        );
        assert_eq!(
            report
                .summary
                .stages
                .exposure_window
                .as_ref()
                .unwrap()
                .p50_ms,
            12_000
        );
        // The out-of-window fill still counts as present open exposure.
        let open = &report.open_exposures[0];
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_fill_block_timestamp, timestamp(100_000));
    }

    /// Verify that `load_hedge_performance` excludes fills and cycles outside
    /// the requested range at the SQL level, not just in-memory. A fill with
    /// `seen_at` and a cycle with `placed_at` both outside the narrow range
    /// must be absent from the returned `SymbolPerformance`.
    #[tokio::test]
    async fn sql_range_filter_excludes_out_of_range_fills_and_cycles() {
        let in_range_order = OffchainOrderId::new();
        let out_of_range_order = OffchainOrderId::new();

        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // In-range fill (seen_at = timestamp(5), inside [0, 1_000]).
        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 5))
            .await
            .unwrap();
        // In-range cycle (placed_at = timestamp(10), inside [0, 1_000]).
        harness
            .receive::<Position>(symbol(), placed_event(in_range_order, 10))
            .await
            .unwrap();

        // Out-of-range fill (seen_at = timestamp(100_005), outside [0, 1_000]).
        harness
            .receive::<Position>(symbol(), fill_event(2, 100_000, 100_005))
            .await
            .unwrap();
        // Out-of-range cycle (placed_at = timestamp(200_000), outside [0, 1_000]).
        harness
            .receive::<Position>(symbol(), placed_event(out_of_range_order, 200_000))
            .await
            .unwrap();

        let narrow_range = report_range(0, 1_000);
        let performances = load_hedge_performance(&pool, &narrow_range).await.unwrap();
        let perf = performances
            .iter()
            .find(|perf| perf.symbol == symbol())
            .unwrap();

        // SQL filter must have excluded the out-of-range fill.
        assert_eq!(perf.fills.len(), 1, "only the in-range fill must be loaded");
        assert_eq!(perf.fills[0].seen_at, timestamp(5));

        // SQL filter must have excluded the out-of-range cycle.
        assert_eq!(
            perf.cycles.len(),
            1,
            "only the in-range cycle must be loaded"
        );
        assert_eq!(perf.cycles[0].placed_at, timestamp(10));
    }

    /// Open exposure must surface for a symbol whose only fill predates the
    /// requested window and was never covered by a cycle. Loading with the
    /// SQL range filter must not drop the symbol just because it has no
    /// in-window fills or cycles -- the unhedged exposure is a live risk
    /// signal regardless of the window.
    #[tokio::test]
    async fn open_exposure_surfaces_for_symbol_with_only_out_of_window_fills() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // The symbol's only fill is far outside [0, 1_000] and is never covered
        // by a cycle, so the symbol carries open exposure but zero in-window
        // activity.
        harness
            .receive::<Position>(symbol(), fill_event(1, 100_000, 100_005))
            .await
            .unwrap();

        let narrow_range = report_range(0, 1_000);
        let performances = load_hedge_performance(&pool, &narrow_range).await.unwrap();

        let perf = performances
            .iter()
            .find(|perf| perf.symbol == symbol())
            .expect("symbol with live open exposure must surface despite no in-window activity");

        assert_eq!(
            perf.fills.len(),
            0,
            "the out-of-window fill is range-excluded"
        );
        assert_eq!(perf.cycles.len(), 0, "no cycles exist for the symbol");
        let open = perf
            .open_exposure
            .as_ref()
            .expect("the uncovered out-of-window fill must produce open exposure");
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(100_000));
    }

    /// A symbol whose out-of-window fill was fully covered by an out-of-window
    /// cycle carries no open exposure and has no in-window data, so it must NOT
    /// appear as an empty row in a narrow-window report.
    #[tokio::test]
    async fn fully_hedged_symbol_with_no_in_window_activity_is_absent() {
        let order_id = OffchainOrderId::new();
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // Fill and its covering placement both land outside [0, 1_000]. The
        // placement covers the fill, so no open exposure remains.
        harness
            .receive::<Position>(symbol(), fill_event(1, 100_000, 100_005))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 100_010))
            .await
            .unwrap();

        let narrow_range = report_range(0, 1_000);
        let performances = load_hedge_performance(&pool, &narrow_range).await.unwrap();

        assert!(
            performances.iter().all(|perf| perf.symbol != symbol()),
            "a fully hedged symbol with no in-window activity must not surface as an empty row"
        );
    }

    #[tokio::test]
    async fn report_buckets_fills_by_day() {
        let day = 86_400;
        let performances = report_performances(vec![
            fill_event(1, 0, 2),
            fill_event(2, 3 * day, 3 * day + 4),
        ])
        .await;

        let report = hedge_latency_report(&performances, &report_range(0, 10 * day));

        assert_eq!(report.buckets.len(), 2);
        assert_eq!(report.buckets[0].start, timestamp(0));
        assert_eq!(report.buckets[1].start, timestamp(3 * day));
        assert_eq!(
            report.buckets[0].stages.detection.as_ref().unwrap().p50_ms,
            2_000
        );
        assert_eq!(
            report.buckets[1].stages.detection.as_ref().unwrap().p50_ms,
            4_000
        );
    }

    #[tokio::test]
    async fn report_buckets_weekly_for_ranges_over_a_month() {
        let day = 86_400;
        let performances = report_performances(vec![
            fill_event(1, 0, 2),
            fill_event(2, 10 * day, 10 * day + 4),
        ])
        .await;

        let report = hedge_latency_report(&performances, &report_range(0, 60 * day));

        assert_eq!(report.buckets.len(), 2);
        assert_eq!(report.buckets[0].start, timestamp(0));
        assert_eq!(report.buckets[1].start, timestamp(7 * day));
    }

    #[tokio::test]
    async fn report_buckets_monthly_for_ranges_over_half_a_year() {
        let day = 86_400;
        let performances = report_performances(vec![
            fill_event(1, 0, 2),
            fill_event(2, 45 * day, 45 * day + 4),
        ])
        .await;

        let report = hedge_latency_report(&performances, &report_range(0, 300 * day));

        assert_eq!(report.buckets.len(), 2);
        assert_eq!(report.buckets[0].start, timestamp(0));
        assert_eq!(report.buckets[1].start, timestamp(30 * day));
    }

    /// A range whose endpoints are EXACTLY 31 days apart spans 32 inclusive
    /// calendar days, so it must step from daily to weekly -- matching the P&L
    /// tab's `> 31` inclusive-day threshold. The old exclusive-span logic
    /// (`span <= 31 days`) stayed daily here, diverging from P&L.
    #[test]
    fn bucket_width_steps_to_weekly_at_exactly_31_days() {
        let day = 86_400;
        assert_eq!(report_range(0, 31 * day).bucket_width(), Duration::days(7));
        // One day short: inclusive count 31, still daily.
        assert_eq!(report_range(0, 30 * day).bucket_width(), Duration::days(1));
    }

    /// A range whose endpoints are EXACTLY 183 days apart spans 184 inclusive
    /// calendar days, so it must step from weekly to monthly -- matching the
    /// P&L tab's `> 183` threshold. The old logic stayed weekly here.
    #[test]
    fn bucket_width_steps_to_monthly_at_exactly_183_days() {
        let day = 86_400;
        assert_eq!(
            report_range(0, 183 * day).bucket_width(),
            Duration::days(30)
        );
        // One day short: inclusive count 183, still weekly.
        assert_eq!(report_range(0, 182 * day).bucket_width(), Duration::days(7));
    }

    #[tokio::test]
    async fn report_caps_cycle_rows_but_counts_all() {
        let events = (0..=i64::try_from(MAX_CYCLE_REPORTS).unwrap())
            .map(|index| placed_event(OffchainOrderId::new(), index))
            .collect();

        let performances = report_performances(events).await;
        let report = hedge_latency_report(&performances, &report_range(0, 1_000));

        assert_eq!(report.total_cycles, MAX_CYCLE_REPORTS + 1);
        assert_eq!(report.cycles.len(), MAX_CYCLE_REPORTS);
    }

    #[tokio::test]
    async fn report_clamps_negative_stage_latencies_to_zero() {
        let order_id = OffchainOrderId::new();
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 3))
            .await
            .unwrap();
        // Broker clock skew: submitted before placed.
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(1),
                },
            )
            .await
            .unwrap();

        let performances = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let report = hedge_latency_report(&performances, &report_range(0, 1_000));

        let submission = report.summary.stages.submission.as_ref().unwrap();
        assert_eq!(submission.p50_ms, 0);
        assert_eq!(submission.max_ms, 0);
    }

    #[tokio::test]
    async fn report_orders_cycles_newest_first() {
        let first = OffchainOrderId::new();
        let second = OffchainOrderId::new();
        let performances = report_performances(vec![
            fill_event(1, 0, 1),
            placed_event(first, 5),
            fill_event(2, 10, 11),
            placed_event(second, 20),
        ])
        .await;

        let report = hedge_latency_report(&performances, &report_range(0, 1_000));

        assert_eq!(report.cycles.len(), 2);
        assert_eq!(report.cycles[0].placed_at, timestamp(20));
        assert_eq!(report.cycles[1].placed_at, timestamp(5));
        assert_eq!(report.cycles[0].status, HedgeCycleStatus::Pending);
    }

    #[test]
    fn report_empty_input_has_no_stats() {
        let report = hedge_latency_report(&[], &report_range(0, 1_000));

        assert_eq!(report.summary.fill_count, 0);
        assert_eq!(report.total_cycles, 0);
        assert_eq!(report.summary.stages.detection, None);
        assert_eq!(report.buckets.len(), 0);
        assert_eq!(report.cycles.len(), 0);
        assert_eq!(report.open_exposures.len(), 0);
    }

    /// Multiple fills accumulate into one batch and a single placement
    /// attributes all of them, with the batch's earliest block and latest
    /// observation driving the exposure and decision windows.
    #[tokio::test]
    async fn multi_fill_batch_attribution() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 2),
                fill_event(2, 10, 13),
                fill_event(3, 5, 8),
                placed_event(order_id, 20),
            ],
        )
        .await;

        let cycle = &performance.cycles[0];
        let covered = cycle.covered.as_ref().unwrap();
        assert_eq!(covered.count, 3);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(covered.latest_seen_at, timestamp(13));
        assert_eq!(cycle.decision_latency(), Some(Duration::seconds(7)));
        assert!(performance.open_exposure.is_none());
    }

    /// A broker fill event arrives for an order_id that was never placed.
    /// The report has no cycles (the warn path fires; rows_affected == 0).
    #[tokio::test]
    async fn filled_event_with_no_prior_placement_produces_no_cycles() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) =
            run_position_stream(symbol(), vec![position_filled_event(order_id, 5)]).await;

        assert!(performance.cycles.is_empty());
    }

    /// A broker failure event arrives for an order_id that was never placed.
    /// The report has no cycles (the warn path fires; existence check finds no row).
    #[tokio::test]
    async fn failed_event_with_no_prior_placement_produces_no_cycles() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) =
            run_position_stream(symbol(), vec![position_failed_event(order_id, 5)]).await;

        assert!(performance.cycles.is_empty());
    }

    /// A Submitted event arrives for an order_id that was never placed. The
    /// submission IS recorded durably in `hedge_submission` (keyed by order id,
    /// so it survives until the placement arrives), but with no `hedge_cycle`
    /// row the report's LEFT JOIN has nothing to attach it to and no cycle
    /// appears. Asserting the row was written guards the durable-storage INSERT:
    /// dropping it would silently lose early-arriving submissions.
    #[tokio::test]
    async fn submitted_event_with_no_prior_placement_stores_submission_but_no_cycle() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-x"),
                    submitted_at: timestamp(3),
                },
            )
            .await
            .unwrap();

        // The submission was stored durably even though no placement exists yet.
        let stored: Option<(String,)> =
            sqlx::query_as("SELECT submitted_at FROM hedge_submission WHERE offchain_order_id = ?")
                .bind(order_id.to_string())
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(stored, Some((timestamp(3).to_rfc3339(),)));

        // But with no cycle row, the report has nothing to attach it to.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert!(report.is_empty());
    }

    /// A `hedge_cycle` row with a non-UUID offchain_order_id is skipped with a
    /// warning; valid rows for other symbols are still returned.
    #[tokio::test]
    async fn malformed_offchain_order_id_in_read_model_is_skipped() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // Valid data for TSLA should survive the load.
        harness
            .receive::<Position>(Symbol::new("TSLA").unwrap(), fill_event(1, 0, 3))
            .await
            .unwrap();

        // Inject a malformed row for AAPL directly. placed_at carries the
        // UTC `+00:00` suffix the schema CHECK requires; the order id is the
        // malformed part under test.
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count) \
             VALUES ('not-a-uuid', 'AAPL', '2025-01-01T00:00:00+00:00', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].symbol, Symbol::new("TSLA").unwrap());
        assert!(result[0].cycles.is_empty());
    }

    /// A `hedge_fill` row with an unparseable `block_timestamp` is skipped;
    /// a valid fill for another symbol survives.
    #[tokio::test]
    async fn malformed_fill_timestamp_in_read_model_is_skipped() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(Symbol::new("TSLA").unwrap(), fill_event(1, 0, 3))
            .await
            .unwrap();

        // Inject a malformed fill row for AAPL directly. The value carries the
        // `+00:00` suffix the schema CHECK requires but is still not a valid
        // RFC3339 datetime, so the loader's parse must reject it.
        sqlx::query(
            "INSERT INTO hedge_fill (symbol, tx_hash, log_index, block_timestamp, seen_at) \
             VALUES ('AAPL', '0xbeef', 1, 'garbage+00:00', '2025-01-01T00:00:00+00:00')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let tsla = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("TSLA").unwrap());
        assert!(tsla.is_some());
        assert_eq!(tsla.unwrap().fills.len(), 1);

        // AAPL's only fill was malformed, so the symbol contributes no valid
        // rows and is absent from the report.
        let aapl = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("AAPL").unwrap());
        assert!(aapl.is_none());
    }

    /// A `hedge_cycle` row with an unparseable `placed_at` is skipped; a valid
    /// cycle for another symbol survives.
    #[tokio::test]
    async fn malformed_cycle_timestamp_in_read_model_is_skipped() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(Symbol::new("TSLA").unwrap(), fill_event(1, 0, 3))
            .await
            .unwrap();

        // Inject a cycle row with a malformed placed_at timestamp. The value
        // carries the `+00:00` suffix the schema CHECK requires but is still
        // not a valid RFC3339 datetime, so the loader's parse must reject it.
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count) \
             VALUES ('00000000-0000-0000-0000-000000000001', 'AAPL', 'garbage+00:00', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let tsla = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("TSLA").unwrap());
        assert!(tsla.is_some());

        // AAPL's only cycle had a malformed placed_at and was skipped; with no
        // fills either, the symbol is absent from the report entirely. An
        // unconditional assertion proves the row was dropped rather than the
        // symbol happening to be missing for an unrelated reason.
        let aapl = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("AAPL").unwrap());
        assert!(
            aapl.is_none(),
            "AAPL should be absent -- its only cycle had a malformed timestamp"
        );
    }

    /// A `hedge_cycle` row with a covered count but only one timestamp (the
    /// writer's invariant violated) is skipped by the loader's defensive guard.
    ///
    /// The schema CHECK now rejects this half-populated batch on write, so the
    /// loader guard is belt-and-suspenders. To still exercise it we bypass the
    /// CHECK with `PRAGMA ignore_check_constraints` on a pinned connection,
    /// proving the loader STILL refuses the inconsistent row even if one ever
    /// reaches the table through a path that skipped the constraint.
    #[tokio::test]
    async fn inconsistent_covered_batch_in_read_model_is_skipped() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(Symbol::new("TSLA").unwrap(), fill_event(1, 0, 3))
            .await
            .unwrap();

        // Inject a row with covered_earliest_block_timestamp but no
        // covered_latest_seen_at — violates the writer's invariant. The pragma
        // is per-connection, so the INSERT must run on the same held connection.
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count, \
              covered_earliest_block_timestamp) \
             VALUES ('00000000-0000-0000-0000-000000000002', 'AAPL', \
                     '2025-01-01T00:00:00+00:00', 1, '2025-01-01T00:00:00+00:00')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        drop(conn);

        let result = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let tsla = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("TSLA").unwrap());
        assert!(tsla.is_some());

        // AAPL cycle with inconsistent covered batch was skipped.
        let aapl = result
            .iter()
            .find(|perf| perf.symbol == Symbol::new("AAPL").unwrap());
        if let Some(aapl_perf) = aapl {
            assert!(aapl_perf.cycles.is_empty());
        }
    }

    /// A symbol with BOTH a valid fill (so it enters the report) AND a malformed
    /// fill row must not blank the whole report. `uncovered_fills` is invoked for
    /// such a symbol; it must skip the malformed row with a warning rather than
    /// propagating the parse error -- mirroring `load_hedge_performance`'s outer
    /// loop. Before the resilience fix, this propagated an `Err` that blanked the
    /// entire `/performance/latencies` report for ALL symbols.
    #[tokio::test]
    async fn mixed_valid_and_malformed_fill_for_same_symbol_does_not_blank_report() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A valid fill for AAPL: this enters `by_symbol`, so `uncovered_fills`
        // WILL be called for AAPL (the path the bug lives on).
        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();

        // A malformed fill row for the SAME symbol. The value carries the
        // `+00:00` suffix the schema CHECK requires but is not valid RFC3339,
        // so the parse must reject it.
        sqlx::query(
            "INSERT INTO hedge_fill (symbol, tx_hash, log_index, block_timestamp, seen_at) \
             VALUES ('AAPL', '0xdead', 2, 'garbage+00:00', '2025-01-01T00:00:00+00:00')",
        )
        .execute(&pool)
        .await
        .unwrap();

        // The report still loads: the malformed row is skipped, the valid fill
        // survives, and open exposure counts only the valid fill.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let aapl = report
            .iter()
            .find(|perf| perf.symbol == symbol())
            .expect("AAPL has a valid fill and must appear in the report");
        assert_eq!(aapl.fills.len(), 1);
        let open = aapl
            .open_exposure
            .as_ref()
            .expect("the single valid fill is open exposure");
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
    }

    /// The cycle-row counterpart of the mixed-validity test: a symbol with a
    /// valid fill (so it enters the report) AND a `hedge_cycle` row whose
    /// `placed_at` is malformed. `uncovered_fills` re-reads `hedge_cycle` and
    /// must skip the bad `placed_at` with a warning rather than propagating,
    /// so the report still returns for the symbol.
    #[tokio::test]
    async fn mixed_valid_fill_and_malformed_cycle_for_same_symbol_does_not_blank_report() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A valid fill for AAPL: enters `by_symbol`, so `uncovered_fills` runs.
        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();

        // A cycle row for the SAME symbol with a malformed placed_at. The outer
        // cycle loader skips it (into_cycle fails), and `uncovered_fills` must
        // also skip it when walking cycle rows.
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count) \
             VALUES ('00000000-0000-0000-0000-000000000003', 'AAPL', 'garbage+00:00', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        // The report still loads: the malformed cycle is skipped everywhere and
        // the valid fill remains open exposure.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let aapl = report
            .iter()
            .find(|perf| perf.symbol == symbol())
            .expect("AAPL has a valid fill and must appear in the report");
        assert_eq!(aapl.fills.len(), 1);
        assert!(aapl.cycles.is_empty());
        let open = aapl
            .open_exposure
            .as_ref()
            .expect("the single valid fill is open exposure");
        assert_eq!(open.fill_count, 1);
    }

    /// A duplicate `OffChainOrderFilled` event keeps the first `filled_at`
    /// timestamp, mirroring the `duplicate_submitted_keeps_the_first_timestamp`
    /// behaviour for `submitted_at`.
    #[tokio::test]
    async fn duplicate_filled_keeps_the_first_timestamp() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), placed_event(order_id, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 5))
            .await
            .unwrap();
        // Second fill at a different timestamp — should be ignored.
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 10))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(5)
            }
        );
    }

    /// A redelivered `OnChainOrderFilled` for the SAME `(tx_hash, log_index)` is
    /// a no-op: the `UNIQUE(tx_hash, log_index)` constraint plus `ON CONFLICT DO
    /// NOTHING` keeps exactly one `hedge_fill` row, so the fill is not
    /// double-counted and open exposure is unchanged.
    #[tokio::test]
    async fn duplicate_onchain_fill_is_idempotent() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A single fill event with a fixed identity, delivered twice.
        let fill = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::repeat_byte(0xAB),
                log_index: 7,
            },
            amount: FractionalShares::new(float!(1)),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_timestamp: timestamp(0),
            seen_at: timestamp(1),
        };

        harness
            .receive::<Position>(symbol(), fill.clone())
            .await
            .unwrap();
        harness.receive::<Position>(symbol(), fill).await.unwrap();

        // Exactly one row survives the redelivery.
        let (fill_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hedge_fill")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(fill_count, 1);

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        // One fill observation, and the open exposure counts a single fill.
        assert_eq!(report[0].fills.len(), 1);
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
    }

    /// A redelivered `OffChainOrderPlaced` for an already-placed cycle is a
    /// no-op: the `ON CONFLICT(offchain_order_id) DO NOTHING` guard keeps the
    /// first covered batch and does not duplicate the cycle, so open exposure
    /// is unchanged.
    #[tokio::test]
    async fn duplicate_placement_is_idempotent() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A fill, then a placement that covers it.
        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 2))
            .await
            .unwrap();

        let before = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(before[0].cycles.len(), 1);
        assert!(before[0].open_exposure.is_none());

        // Redeliver the SAME placement event.
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 2))
            .await
            .unwrap();

        let after = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        // Still exactly one cycle, still covering the original fill, still no
        // open exposure: the redelivery changed nothing.
        assert_eq!(after[0].cycles.len(), 1);
        let covered = after[0].cycles[0].covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert!(after[0].open_exposure.is_none());
    }

    /// A manual adjustment writes a durable reset boundary; fills observed after
    /// it are open exposure while fills before it are dropped. The read path
    /// derives this from `hedge_attribution_reset` alone (no in-memory state),
    /// proving the reset survives a restart.
    #[tokio::test]
    async fn manual_reset_is_read_from_durable_table() {
        let pool = setup_test_db().await;

        // Pre-reset fill, manual adjustment, post-reset fill -- all recorded by
        // one reactor instance which is then dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            harness
                .receive::<Position>(symbol(), fill_event(1, 0, 1))
                .await
                .unwrap();
            harness
                .receive::<Position>(
                    symbol(),
                    PositionEvent::ManualPositionAdjusted {
                        previous_net: FractionalShares::new(float!(1)),
                        target_net: FractionalShares::new(float!(0)),
                        reason: "durable reset".to_string(),
                        price_usdc: None,
                        adjusted_at: timestamp(5),
                    },
                )
                .await
                .unwrap();
            harness
                .receive::<Position>(symbol(), fill_event(2, 40, 43))
                .await
                .unwrap();
        }

        // The read path alone must drop the pre-reset fill and report only the
        // post-reset fill as open exposure.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    /// A `Submitted` event that arrives BEFORE its `OffChainOrderPlaced` (the
    /// Position and OffchainOrder streams are independent) must not lose its
    /// timestamp. The submission is recorded in `hedge_submission` keyed by
    /// order id, so the later placement's cycle still reports it via the LEFT
    /// JOIN -- regardless of arrival order.
    #[tokio::test]
    async fn submitted_before_placed_preserves_submitted_at() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // Submission lands first, before any placement row exists.
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(2),
                },
            )
            .await
            .unwrap();

        // The placement arrives afterwards.
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 5))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        // The fix: submitted_at survives even though Submitted preceded the
        // placement. Before the durable hedge_submission table, the UPDATE-only
        // handler found no cycle row and dropped this timestamp.
        assert_eq!(cycle.submitted_at, Some(timestamp(2)));
        // Submission preceded placement, so the submission latency
        // (placed_at - submitted_at = -3s) clamps to zero.
        assert_eq!(cycle.submission_latency(), Some(Duration::zero()));
    }

    /// The FILL path after a manual reset: a placement covers a fill, a manual
    /// adjustment resets attribution, then the broker fills the order. The cycle
    /// keeps the covered batch it snapshotted at placement time and records the
    /// broker fill as its terminal outcome. The reset drops the pre-reset fill
    /// from the uncovered-pool recompute, so no exposure is carried open.
    #[tokio::test]
    async fn fill_after_manual_adjustment_records_outcome_and_clears_exposure() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: FractionalShares::new(float!(1)),
                    target_net: FractionalShares::new(float!(0)),
                    reason: "manual reset".to_string(),
                    price_usdc: None,
                    adjusted_at: timestamp(5),
                },
                position_filled_event(order_id, 8),
            ],
        )
        .await;

        // The broker fill is the cycle's terminal outcome despite the reset.
        let cycle = &performance.cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(8)
            }
        );
        // The placement (offset 2, before the reset) snapshotted the fill it
        // covered; that batch stays recorded on the cycle.
        let covered = cycle.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        // The reset (offset 5) drops the pre-reset fill (seen at 1) from the
        // recompute, so no exposure is carried open.
        assert!(performance.open_exposure.is_none());
    }
}
