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
//!
//! The implementation splits into a write side ([`projection`]) that reacts to
//! events and a read side ([`report`]) that loads and assembles the report. The
//! attribution recompute they share lives here as [`uncovered_fills`].

#[cfg(any(test, feature = "test-support"))]
use chrono::Duration;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_execution::Symbol;

pub(crate) mod infra;
pub(crate) mod projection;
pub(crate) mod rebalance;
pub(crate) mod reliability;
pub(crate) mod report;

// Re-exported for external callers (`crate::api`, `crate::conductor`) so the
// split is transparent at the `crate::performance::NAME` path.
pub(crate) use projection::HedgeLatencyProjection;
pub(crate) use report::{
    PerformanceError, ReportRange, hedge_latency_report, load_hedge_performance,
};

/// Seeds deterministic hedge-latency read-model rows for local dashboard simulation.
#[cfg(any(test, feature = "test-support"))]
pub async fn seed_simulated_hedge_latency_history(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    days: u32,
) -> Result<(), sqlx::Error> {
    const SAMPLES_PER_DAY: u32 = 12;

    let range_start = now - Duration::days(i64::from(days));
    let mut transaction = pool.begin().await?;

    for day in 0..days {
        for sample in 0..SAMPLES_PER_DAY {
            let symbol = if sample % 2 == 0 { "AAPL" } else { "TSLA" };
            let sample_start = range_start
                + Duration::days(i64::from(day))
                + Duration::hours(12)
                + Duration::minutes(i64::from(sample) * 4);
            let latency_ms = i64::from(day) * 75 + i64::from(sample) * 125;
            let block_timestamp = sample_start;
            let seen_at = block_timestamp + Duration::milliseconds(1_000 + latency_ms);
            let placed_at = seen_at + Duration::milliseconds(2_000 + latency_ms);
            let submitted_at = placed_at + Duration::milliseconds(750 + latency_ms);
            let filled_at = submitted_at + Duration::milliseconds(3_000 + latency_ms);
            let fill_uuid = simulated_latency_uuid("fill", day, sample);
            let order_uuid = simulated_latency_uuid("order", day, sample);
            let tx_hash = format!("0x{:064x}", fill_uuid.as_u128());

            sqlx::query(
                "INSERT OR IGNORE INTO hedge_fill \
                 (symbol, tx_hash, log_index, block_timestamp, seen_at) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(symbol)
            .bind(tx_hash)
            .bind(i64::from(sample))
            .bind(block_timestamp.to_rfc3339())
            .bind(seen_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;

            sqlx::query(
                "INSERT OR IGNORE INTO hedge_cycle \
                 (offchain_order_id, symbol, placed_at, covered_count, \
                  covered_earliest_block_timestamp, covered_latest_seen_at, filled_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(order_uuid.to_string())
            .bind(symbol)
            .bind(placed_at.to_rfc3339())
            .bind(1_i64)
            .bind(block_timestamp.to_rfc3339())
            .bind(seen_at.to_rfc3339())
            .bind(filled_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;

            sqlx::query(
                "INSERT OR IGNORE INTO hedge_submission (offchain_order_id, submitted_at) \
                 VALUES (?, ?)",
            )
            .bind(order_uuid.to_string())
            .bind(submitted_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
    }

    transaction.commit().await
}

#[cfg(any(test, feature = "test-support"))]
fn simulated_latency_uuid(kind: &str, day: u32, sample: u32) -> uuid::Uuid {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("st0x-simulated-hedge-latency:{kind}:{day}:{sample}").as_bytes(),
    )
}

// Shared with the sibling `rebalance`/`reliability` submodules via `super::`,
// matching the visibility these items had before the split. `latency_stats`
// and `CoveredFills` are crate-internal helpers, not part of the public path.
use report::{CoveredFills, latency_stats};

/// One fill in the recompute queue, carrying the timestamps the attribution
/// algorithm needs (block time and observation time). Reconstructed from
/// `hedge_fill` rows when [`uncovered_fills`] replays the durable tables.
#[derive(Debug, Clone)]
struct UncoveredFill {
    block_timestamp: DateTime<Utc>,
    seen_at: DateTime<Utc>,
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

/// Parses a `hedge_fill` row's two timestamp columns into an [`UncoveredFill`].
/// Shares a `?`-scope across both parses, mirroring [`report::parse_fill_row`].
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

/// Test helpers shared by the [`projection`] and [`report`] test modules. They
/// drive `Position`/`OffchainOrder` events through the reactor and read the
/// result back, exercising both sides, so they live in the parent.
#[cfg(test)]
pub(super) mod test_helpers {
    use alloy::primitives::TxHash;
    use chrono::{DateTime, TimeZone, Utc};
    use sqlx::SqlitePool;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};
    use st0x_float_macro::float;

    use crate::position::{Position, PositionEvent, TradeId, TriggerReason};
    use crate::test_utils::setup_test_db;

    use super::projection::HedgeLatencyProjection;
    use super::report::{ReportRange, SymbolPerformance, load_hedge_performance};
    use crate::offchain::order::OffchainOrderId;

    pub(crate) fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    pub(crate) fn fill_event(log_index: u64, block_offset: i64, seen_offset: i64) -> PositionEvent {
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

    pub(crate) fn placed_event(order_id: OffchainOrderId, placed_offset: i64) -> PositionEvent {
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

    pub(crate) fn symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    pub(crate) fn position_failed_event(
        order_id: OffchainOrderId,
        failed_offset: i64,
    ) -> PositionEvent {
        PositionEvent::OffChainOrderFailed {
            offchain_order_id: order_id,
            error: "broker rejected".to_string(),
            failed_at: timestamp(failed_offset),
        }
    }

    pub(crate) fn position_filled_event(
        order_id: OffchainOrderId,
        broker_offset: i64,
    ) -> PositionEvent {
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
    pub(crate) async fn run_position_stream(
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

    /// Drive `Position` events through the reactor and return the full
    /// per-symbol report, the input the report-assembly layer consumes.
    pub(crate) async fn report_performances(events: Vec<PositionEvent>) -> Vec<SymbolPerformance> {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        for event in events {
            harness.receive::<Position>(symbol(), event).await.unwrap();
        }

        load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod simulated_history_tests {
    use chrono::TimeZone;

    use super::report::{ReportRange, hedge_latency_report, load_hedge_performance};
    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn simulated_hedge_latency_history_populates_daily_percentile_buckets() {
        let pool = setup_test_db().await;
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 18, 0, 0).unwrap();
        let days = 14;

        seed_simulated_hedge_latency_history(&pool, now, days)
            .await
            .unwrap();

        let range = ReportRange {
            from: now - Duration::days(i64::from(days)),
            to: now,
        };
        let performances = load_hedge_performance(&pool, &range).await.unwrap();
        let report = hedge_latency_report(&performances, &range);

        assert_eq!(report.summary.fill_count, 168);
        assert_eq!(report.total_cycles, 168);
        assert_eq!(report.cycles.len(), 100);
        assert_eq!(report.buckets.len(), 14);
        assert!(report.open_exposures.is_empty());
        assert!(
            report
                .buckets
                .iter()
                .all(|bucket| bucket.stages.exposure_window.is_some()),
            "every seeded day must produce exposure-window percentile samples",
        );

        let first = report.buckets[0].stages.exposure_window.as_ref().unwrap();
        let last = report.buckets[13].stages.exposure_window.as_ref().unwrap();

        assert!(first.p50_ms < first.p90_ms);
        assert!(first.p90_ms < first.p99_ms);
        assert!(last.p50_ms > first.p50_ms);
    }
}
