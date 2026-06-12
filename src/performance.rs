//! Performance read models derived from the CQRS event store.
//!
//! Folds `Position` and `OffchainOrder` event streams into per-fill detection
//! latencies and per-hedge cycles measuring how fast the system turns onchain
//! fills into completed hedges. Metric definitions live in SPEC.md under
//! "Performance Observability". Strictly read-only: queries the `events`
//! table and never writes.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;
use thiserror::Error;
use tracing::warn;

use st0x_dto::{
    HedgeCycleReport, HedgeCycleStatus, HedgeLatencies, LatencyBucket, LatencyStats,
    LatencySummary, OpenExposureReport, StageLatencies,
};
use st0x_execution::Symbol;

use crate::offchain::order::{OffchainOrderEvent, OffchainOrderId};
use crate::position::PositionEvent;

pub(crate) mod rebalance;
pub(crate) mod reliability;

/// Waterfall rows returned per report; the full cycle count is still
/// reported via `total_cycles`.
const MAX_CYCLE_REPORTS: usize = 100;

/// Per-symbol hedge performance assembled from the event store.
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
    /// Observation of the threshold-crossing fill to hedge placement.
    pub(crate) fn decision_latency(&self) -> Option<Duration> {
        let covered = self.covered.as_ref()?;
        Some(self.placed_at - covered.latest_seen_at)
    }

    /// Hedge placement to broker acceptance.
    pub(crate) fn submission_latency(&self) -> Option<Duration> {
        Some(self.submitted_at? - self.placed_at)
    }

    /// Broker acceptance to broker fill.
    pub(crate) fn execution_latency(&self) -> Option<Duration> {
        let HedgeOutcome::Filled { filled_at } = self.outcome else {
            return None;
        };
        Some(filled_at - self.submitted_at?)
    }

    /// Earliest covered fill's block to broker fill: the full period during
    /// which the system carried unhedged delta for this batch.
    pub(crate) fn exposure_window(&self) -> Option<Duration> {
        let HedgeOutcome::Filled { filled_at } = self.outcome else {
            return None;
        };
        let covered = self.covered.as_ref()?;
        Some(filled_at - covered.earliest_block_timestamp)
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

/// Terminal state of a hedge order, read from the `OffchainOrder` aggregate.
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
    #[error("failed to query the event store")]
    Database(#[from] sqlx::Error),
}

/// Load hedge performance for every symbol with a `Position` aggregate.
///
/// Both event streams are read inside one transaction so the report folds a
/// consistent snapshot: without it, the bot could commit a hedge's events
/// between the two reads and the report would misclassify it as pending.
///
/// Undeserializable events and aggregates with malformed ids are skipped with
/// a warning rather than failing the whole report: one bad historical row
/// must not take down the dashboard's view of every other symbol.
pub(crate) async fn load_hedge_performance(
    pool: &SqlitePool,
) -> Result<Vec<SymbolPerformance>, PerformanceError> {
    let mut transaction = pool.begin().await?;

    let order_timelines = load_order_timelines(&mut transaction).await?;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT aggregate_id, payload FROM events \
         WHERE aggregate_type = 'Position' \
         ORDER BY aggregate_id, sequence",
    )
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;

    // Keyed by Symbol (BTreeMap for deterministic output ordering): every
    // Position aggregate id is a symbol, so parse at insertion and fail fast
    // on malformed ids instead of accumulating raw strings.
    let mut streams: BTreeMap<Symbol, Vec<PositionEvent>> = BTreeMap::new();
    for (aggregate_id, payload) in rows {
        let Ok(symbol) = Symbol::new(aggregate_id.as_str())
            .inspect_err(|error| warn!(%aggregate_id, %error, "Skipping Position with invalid id"))
        else {
            continue;
        };
        match serde_json::from_str(&payload) {
            Ok(event) => streams.entry(symbol).or_default().push(event),
            Err(error) => {
                warn!(%aggregate_id, %error, "Skipping undeserializable Position event");
            }
        }
    }

    Ok(streams
        .into_iter()
        .map(|(symbol, events)| fold_position_stream(symbol, events, &order_timelines))
        .collect())
}

/// Broker-side timestamps of one hedge order, folded from its
/// `OffchainOrder` event stream.
#[derive(Debug, Clone)]
struct OrderTimeline {
    submitted_at: Option<DateTime<Utc>>,
    outcome: HedgeOutcome,
}

async fn load_order_timelines(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<HashMap<OffchainOrderId, OrderTimeline>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT aggregate_id, payload FROM events \
         WHERE aggregate_type = 'OffchainOrder' \
         ORDER BY aggregate_id, sequence",
    )
    .fetch_all(&mut **transaction)
    .await?;

    let mut timelines = HashMap::new();
    for (aggregate_id, payload) in rows {
        let Ok(order_id) = aggregate_id.parse::<OffchainOrderId>() else {
            warn!(%aggregate_id, "Skipping OffchainOrder with invalid id");
            continue;
        };
        let event: OffchainOrderEvent = match serde_json::from_str(&payload) {
            Ok(event) => event,
            Err(error) => {
                warn!(%aggregate_id, %error, "Skipping undeserializable OffchainOrder event");
                continue;
            }
        };

        let timeline = timelines.entry(order_id).or_insert(OrderTimeline {
            submitted_at: None,
            outcome: HedgeOutcome::Pending,
        });
        match event {
            OffchainOrderEvent::Placed { .. } | OffchainOrderEvent::PartiallyFilled { .. } => {}
            OffchainOrderEvent::Submitted { submitted_at, .. } => {
                if timeline.submitted_at.is_some() {
                    warn!(
                        %aggregate_id,
                        "Duplicate Submitted event for OffchainOrder; keeping the first"
                    );
                } else {
                    timeline.submitted_at = Some(submitted_at);
                }
            }
            OffchainOrderEvent::Filled { filled_at, .. } => {
                if timeline.outcome == HedgeOutcome::Pending {
                    timeline.outcome = HedgeOutcome::Filled { filled_at };
                } else {
                    warn!(
                        %aggregate_id,
                        "Duplicate terminal event for OffchainOrder; keeping the first outcome"
                    );
                }
            }
            OffchainOrderEvent::Failed { failed_at, .. } => {
                if timeline.outcome == HedgeOutcome::Pending {
                    timeline.outcome = HedgeOutcome::Failed { failed_at };
                } else {
                    warn!(
                        %aggregate_id,
                        "Duplicate terminal event for OffchainOrder; keeping the first outcome"
                    );
                }
            }
        }
    }

    Ok(timelines)
}

fn fold_position_stream(
    symbol: Symbol,
    events: Vec<PositionEvent>,
    order_timelines: &HashMap<OffchainOrderId, OrderTimeline>,
) -> SymbolPerformance {
    let mut fills = Vec::new();
    let mut cycles: Vec<HedgeCycle> = Vec::new();
    let mut uncovered: Vec<FillObservation> = Vec::new();
    // Fills consumed by a placement stay parked here until the hedge reaches
    // a terminal state: a failure returns them to `uncovered` so the retry
    // that eventually closes the risk inherits the attribution.
    let mut in_flight: HashMap<OffchainOrderId, Vec<FillObservation>> = HashMap::new();

    for event in events {
        match event {
            PositionEvent::Initialized { .. } | PositionEvent::ThresholdUpdated { .. } => {}
            PositionEvent::OnChainOrderFilled {
                block_timestamp,
                seen_at,
                ..
            } => {
                let observation = FillObservation {
                    block_timestamp,
                    seen_at,
                };
                uncovered.push(observation.clone());
                fills.push(observation);
            }
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                placed_at,
                ..
            } => {
                let covered = covered_fills(&uncovered);
                in_flight.insert(offchain_order_id, std::mem::take(&mut uncovered));

                let timeline = order_timelines.get(&offchain_order_id);
                if timeline.is_none() {
                    warn!(
                        %offchain_order_id,
                        "No OffchainOrder events for placed hedge; reporting it as pending"
                    );
                }

                cycles.push(HedgeCycle {
                    offchain_order_id,
                    placed_at,
                    covered,
                    submitted_at: timeline.and_then(|timeline| timeline.submitted_at),
                    outcome: timeline.map_or(HedgeOutcome::Pending, |timeline| timeline.outcome),
                });
            }
            // The broker's own fill time; the OffchainOrder aggregate's
            // Filled event records reconciliation time instead, which would
            // overstate execution latency by polling and queue delay.
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                broker_timestamp,
                ..
            } => {
                in_flight.remove(&offchain_order_id);
                let Some(cycle) = cycles
                    .iter_mut()
                    .rev()
                    .find(|cycle| cycle.offchain_order_id == offchain_order_id)
                else {
                    warn!(
                        %offchain_order_id,
                        "Fill reported for a hedge with no placement in the stream"
                    );
                    continue;
                };
                if matches!(cycle.outcome, HedgeOutcome::Failed { .. }) {
                    warn!(
                        %offchain_order_id,
                        "Position stream reports a fill for a hedge the \
                         OffchainOrder aggregate recorded as failed"
                    );
                }
                cycle.outcome = HedgeOutcome::Filled {
                    filled_at: broker_timestamp,
                };
            }
            // The fills the failed hedge was covering are still unhedged
            // exposure; return them so the retry inherits the attribution.
            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                failed_at,
                ..
            } => {
                if let Some(batch) = in_flight.remove(&offchain_order_id) {
                    uncovered.extend(batch);
                }
                let Some(cycle) = cycles
                    .iter_mut()
                    .rev()
                    .find(|cycle| cycle.offchain_order_id == offchain_order_id)
                else {
                    warn!(
                        %offchain_order_id,
                        "Failure reported for a hedge with no placement in the stream"
                    );
                    continue;
                };
                if cycle.outcome == HedgeOutcome::Pending {
                    cycle.outcome = HedgeOutcome::Failed { failed_at };
                }
            }
            // A manual adjustment means accumulated fills no longer drive
            // hedging decisions; attributing them to a later hedge would
            // overstate its exposure window.
            PositionEvent::ManualPositionAdjusted { .. } => {
                uncovered.clear();
                in_flight.clear();
            }
        }
    }

    let open_exposure =
        uncovered
            .iter()
            .map(|fill| fill.block_timestamp)
            .min()
            .map(|oldest_block_timestamp| OpenExposure {
                fill_count: uncovered.len(),
                oldest_block_timestamp,
            });

    SymbolPerformance {
        symbol,
        fills,
        cycles,
        open_exposure,
    }
}

fn covered_fills(uncovered: &[FillObservation]) -> Option<CoveredFills> {
    let earliest_block_timestamp = uncovered.iter().map(|fill| fill.block_timestamp).min()?;
    let latest_seen_at = uncovered.iter().map(|fill| fill.seen_at).max()?;

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
    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.from <= timestamp && timestamp <= self.to
    }

    /// Bucket width mirroring the P&L tab's cadence (daily up to a month,
    /// weekly up to half a year, otherwise ~monthly), with hourly buckets
    /// for short windows so a 24h report still shows a trend.
    fn bucket_width(&self) -> Duration {
        let span = self.to - self.from;
        if span <= Duration::days(2) {
            Duration::hours(1)
        } else if span <= Duration::days(31) {
            Duration::days(1)
        } else if span <= Duration::days(183) {
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
                // Clamped like detection latency: clock skew between event
                // sources must not push negative samples into percentiles.
                samples.push(latency.num_milliseconds().max(0));
            }
        }
    }

    fn into_stage_latencies(self) -> StageLatencies {
        StageLatencies {
            detection: latency_stats(self.detection),
            decision: latency_stats(self.decision),
            submission: latency_stats(self.submission),
            execution: latency_stats(self.execution),
            exposure_window: latency_stats(self.exposure_window),
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
fn latency_stats(mut samples_ms: Vec<i64>) -> Option<LatencyStats> {
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

    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor};
    use st0x_float_macro::float;

    use crate::position::{TradeId, TriggerReason};
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

    #[test]
    fn failed_hedge_returns_fills_to_open_exposure() {
        let order_id = OffchainOrderId::new();

        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_failed_event(order_id, 5),
            ],
            &HashMap::new(),
        );

        let open = performance.open_exposure.unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
        // Even without OffchainOrder events, the Position stream alone marks
        // the cycle failed.
        assert_eq!(
            performance.cycles[0].outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(5)
            }
        );
    }

    #[test]
    fn retry_after_failed_hedge_inherits_attribution() {
        let failed_order = OffchainOrderId::new();
        let retry_order = OffchainOrderId::new();
        let timelines = HashMap::from([(
            retry_order,
            OrderTimeline {
                submitted_at: Some(timestamp(11)),
                outcome: HedgeOutcome::Filled {
                    filled_at: timestamp(12),
                },
            },
        )]);

        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(failed_order, 2),
                position_failed_event(failed_order, 5),
                placed_event(retry_order, 10),
                position_filled_event(retry_order, 12),
            ],
            &timelines,
        );

        assert!(performance.open_exposure.is_none());
        let retry = &performance.cycles[1];
        let covered = retry.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(retry.exposure_window(), Some(Duration::seconds(12)));
        assert_eq!(
            retry.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(12)
            }
        );
    }

    #[test]
    fn manual_adjustment_clears_in_flight_attribution() {
        // The same order is placed before the reset and fails after it: the
        // reset must prevent its parked fills from resurfacing.
        let adjusted_order = OffchainOrderId::new();
        let later_order = OffchainOrderId::new();

        let performance = fold_position_stream(
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
            &HashMap::new(),
        );

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

    #[test]
    fn broker_fill_timestamp_overrides_reconciliation_time() {
        let order_id = OffchainOrderId::new();
        // The OffchainOrder aggregate records reconciliation time (30s),
        // but the broker actually filled at 8s.
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: Some(timestamp(3)),
                outcome: HedgeOutcome::Filled {
                    filled_at: timestamp(30),
                },
            },
        )]);

        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_filled_event(order_id, 8),
            ],
            &timelines,
        );

        let cycle = &performance.cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(8)
            }
        );
        assert_eq!(cycle.execution_latency(), Some(Duration::seconds(5)));
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(8)));
    }

    #[test]
    fn detection_latency_clamps_to_zero_when_block_timestamp_is_ahead() {
        let performance =
            fold_position_stream(symbol(), vec![fill_event(1, 5, 3)], &HashMap::new());

        assert_eq!(performance.fills[0].detection_latency(), Duration::zero());
    }

    #[test]
    fn filled_outcome_without_submitted_at_has_no_execution_latency() {
        let order_id = OffchainOrderId::new();
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: None,
                outcome: HedgeOutcome::Filled {
                    filled_at: timestamp(9),
                },
            },
        )]);

        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 1), placed_event(order_id, 2)],
            &timelines,
        );

        let cycle = &performance.cycles[0];
        assert_eq!(cycle.execution_latency(), None);
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(9)));
    }

    #[test]
    fn detection_latency_is_seen_minus_block_timestamp() {
        let performance =
            fold_position_stream(symbol(), vec![fill_event(1, 0, 7)], &HashMap::new());

        assert_eq!(performance.fills.len(), 1);
        assert_eq!(
            performance.fills[0].detection_latency(),
            Duration::seconds(7)
        );
    }

    #[test]
    fn filled_hedge_reports_all_latencies() {
        let order_id = OffchainOrderId::new();
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: Some(timestamp(12)),
                outcome: HedgeOutcome::Filled {
                    filled_at: timestamp(15),
                },
            },
        )]);

        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 5), placed_event(order_id, 10)],
            &timelines,
        );

        assert_eq!(performance.cycles.len(), 1);
        let cycle = &performance.cycles[0];
        assert_eq!(cycle.decision_latency(), Some(Duration::seconds(5)));
        assert_eq!(cycle.submission_latency(), Some(Duration::seconds(2)));
        assert_eq!(cycle.execution_latency(), Some(Duration::seconds(3)));
        assert_eq!(cycle.exposure_window(), Some(Duration::seconds(15)));
        assert!(performance.open_exposure.is_none());
    }

    #[test]
    fn hedge_covers_every_fill_since_previous_placement() {
        let first_order = OffchainOrderId::new();
        let second_order = OffchainOrderId::new();

        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 2),
                placed_event(first_order, 3),
                fill_event(2, 10, 12),
                fill_event(3, 20, 22),
                fill_event(4, 30, 32),
                placed_event(second_order, 40),
            ],
            &HashMap::new(),
        );

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

    #[test]
    fn fills_after_last_placement_are_open_exposure() {
        let order_id = OffchainOrderId::new();

        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                fill_event(2, 50, 52),
                fill_event(3, 40, 43),
            ],
            &HashMap::new(),
        );

        let open = performance.open_exposure.unwrap();
        assert_eq!(open.fill_count, 2);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    #[test]
    fn failed_hedge_has_no_execution_latency_or_exposure_window() {
        let order_id = OffchainOrderId::new();
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: Some(timestamp(4)),
                outcome: HedgeOutcome::Failed {
                    failed_at: timestamp(6),
                },
            },
        )]);

        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 1), placed_event(order_id, 3)],
            &timelines,
        );

        let cycle = &performance.cycles[0];
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

    #[test]
    fn placement_without_covered_fills_has_no_attribution() {
        let order_id = OffchainOrderId::new();

        let performance =
            fold_position_stream(symbol(), vec![placed_event(order_id, 0)], &HashMap::new());

        let cycle = &performance.cycles[0];
        assert!(cycle.covered.is_none());
        assert_eq!(cycle.decision_latency(), None);
        assert_eq!(cycle.exposure_window(), None);
    }

    #[test]
    fn manual_adjustment_resets_attribution() {
        let order_id = OffchainOrderId::new();

        let performance = fold_position_stream(
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
            &HashMap::new(),
        );

        assert!(performance.cycles[0].covered.is_none());
        assert!(performance.open_exposure.is_none());
        assert_eq!(performance.fills.len(), 1);
    }

    #[test]
    fn unknown_order_id_reports_pending_outcome() {
        let order_id = OffchainOrderId::new();

        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 1), placed_event(order_id, 2)],
            &HashMap::new(),
        );

        let cycle = &performance.cycles[0];
        assert_eq!(cycle.outcome, HedgeOutcome::Pending);
        assert_eq!(cycle.submitted_at, None);
        assert_eq!(cycle.submission_latency(), None);
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, \
              payload, metadata) \
             VALUES ($1, $2, $3, $4, '1.0', $5, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_hedge_performance_joins_position_and_order_streams() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        let placed = serde_json::to_string(&placed_event(order_id, 10)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            2,
            "PositionEvent::OffChainOrderPlaced",
            &placed,
        )
        .await;

        let submitted = serde_json::to_string(&OffchainOrderEvent::Submitted {
            executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
            submitted_at: timestamp(12),
        })
        .unwrap();
        let filled = serde_json::to_string(&OffchainOrderEvent::Filled {
            price: "150.25".parse().unwrap(),
            filled_at: timestamp(15),
        })
        .unwrap();
        let order_aggregate_id = order_id.to_string();
        insert_event(
            &pool,
            "OffchainOrder",
            &order_aggregate_id,
            1,
            "OffchainOrderEvent::Submitted",
            &submitted,
        )
        .await;
        insert_event(
            &pool,
            "OffchainOrder",
            &order_aggregate_id,
            2,
            "OffchainOrderEvent::Filled",
            &filled,
        )
        .await;
        // The Position stream carries the broker's own fill time (13s),
        // which must win over the reconciliation time above (15s).
        let position_filled = serde_json::to_string(&position_filled_event(order_id, 13)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            3,
            "PositionEvent::OffChainOrderFilled",
            &position_filled,
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

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
    async fn load_hedge_performance_reports_pending_on_malformed_order_events() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        let placed = serde_json::to_string(&placed_event(order_id, 10)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            2,
            "PositionEvent::OffChainOrderPlaced",
            &placed,
        )
        .await;
        insert_event(
            &pool,
            "OffchainOrder",
            &order_id.to_string(),
            1,
            "OffchainOrderEvent::Submitted",
            "{\"not\": \"an order event\"}",
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.outcome, HedgeOutcome::Pending);
        assert_eq!(cycle.submitted_at, None);
    }

    #[tokio::test]
    async fn load_hedge_performance_skips_orders_with_invalid_ids() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        let placed = serde_json::to_string(&placed_event(order_id, 10)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            2,
            "PositionEvent::OffChainOrderPlaced",
            &placed,
        )
        .await;

        let submitted = serde_json::to_string(&OffchainOrderEvent::Submitted {
            executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
            submitted_at: timestamp(12),
        })
        .unwrap();
        // The malformed row must be skipped without disturbing the valid one.
        insert_event(
            &pool,
            "OffchainOrder",
            "not-a-uuid",
            1,
            "OffchainOrderEvent::Submitted",
            &submitted,
        )
        .await;
        insert_event(
            &pool,
            "OffchainOrder",
            &order_id.to_string(),
            1,
            "OffchainOrderEvent::Submitted",
            &submitted,
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

        assert_eq!(report.len(), 1);
        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(12)));
        assert_eq!(cycle.outcome, HedgeOutcome::Pending);
    }

    #[tokio::test]
    async fn load_hedge_performance_isolates_symbols_in_deterministic_order() {
        let pool = setup_test_db().await;

        let tsla_fill = serde_json::to_string(&fill_event(1, 0, 3)).unwrap();
        insert_event(
            &pool,
            "Position",
            "TSLA",
            1,
            "PositionEvent::OnChainOrderFilled",
            &tsla_fill,
        )
        .await;
        let aapl_fill = serde_json::to_string(&fill_event(2, 0, 5)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &aapl_fill,
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

        assert_eq!(report.len(), 2);
        assert_eq!(report[0].symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(report[1].symbol, Symbol::new("TSLA").unwrap());
        assert_eq!(report[0].fills.len(), 1);
        assert_eq!(report[1].fills.len(), 1);
        assert_eq!(report[0].fills[0].detection_latency(), Duration::seconds(5));
        assert_eq!(report[1].fills[0].detection_latency(), Duration::seconds(3));
    }

    #[tokio::test]
    async fn load_hedge_performance_skips_positions_with_invalid_ids() {
        let pool = setup_test_db().await;

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        insert_event(
            &pool,
            "Position",
            "   ",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

        assert_eq!(report.len(), 1);
        assert_eq!(report[0].symbol, symbol());
        assert_eq!(report[0].fills.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_order_events_keep_the_first_timestamps() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        let placed = serde_json::to_string(&placed_event(order_id, 1)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OffChainOrderPlaced",
            &placed,
        )
        .await;

        let order_aggregate_id = order_id.to_string();
        let order_events = [
            serde_json::to_string(&OffchainOrderEvent::Submitted {
                executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                submitted_at: timestamp(2),
            })
            .unwrap(),
            serde_json::to_string(&OffchainOrderEvent::Submitted {
                executor_order_id: st0x_execution::ExecutorOrderId::new("broker-2"),
                submitted_at: timestamp(4),
            })
            .unwrap(),
            serde_json::to_string(&OffchainOrderEvent::Filled {
                price: "150.25".parse().unwrap(),
                filled_at: timestamp(6),
            })
            .unwrap(),
            serde_json::to_string(&OffchainOrderEvent::Filled {
                price: "150.25".parse().unwrap(),
                filled_at: timestamp(9),
            })
            .unwrap(),
        ];
        for (index, payload) in order_events.iter().enumerate() {
            insert_event(
                &pool,
                "OffchainOrder",
                &order_aggregate_id,
                i64::try_from(index).unwrap() + 1,
                "OffchainOrderEvent",
                payload,
            )
            .await;
        }

        let report = load_hedge_performance(&pool).await.unwrap();

        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(2)));
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(6)
            }
        );
    }

    #[tokio::test]
    async fn partially_filled_order_keeps_timeline_intact() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        let placed = serde_json::to_string(&placed_event(order_id, 10)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            2,
            "PositionEvent::OffChainOrderPlaced",
            &placed,
        )
        .await;

        let order_events = [
            serde_json::to_string(&OffchainOrderEvent::Submitted {
                executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                submitted_at: timestamp(12),
            })
            .unwrap(),
            serde_json::to_string(&OffchainOrderEvent::PartiallyFilled {
                shares_filled: FractionalShares::new(float!(0.5)),
                avg_price: "150.25".parse().unwrap(),
                partially_filled_at: timestamp(13),
            })
            .unwrap(),
            serde_json::to_string(&OffchainOrderEvent::Filled {
                price: "150.25".parse().unwrap(),
                filled_at: timestamp(15),
            })
            .unwrap(),
        ];
        for (index, payload) in order_events.iter().enumerate() {
            insert_event(
                &pool,
                "OffchainOrder",
                &order_id.to_string(),
                i64::try_from(index).unwrap() + 1,
                "OffchainOrderEvent",
                payload,
            )
            .await;
        }

        let report = load_hedge_performance(&pool).await.unwrap();

        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(12)));
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(15)
            }
        );
    }

    #[test]
    fn latency_stats_uses_nearest_rank() {
        let stats = latency_stats((1..=100).collect()).unwrap();

        assert_eq!(stats.p50_ms, 50);
        assert_eq!(stats.p90_ms, 90);
        assert_eq!(stats.p95_ms, 95);
        assert_eq!(stats.p99_ms, 99);
        assert_eq!(stats.max_ms, 100);
        assert_eq!(stats.sample_count, 100);
    }

    #[test]
    fn latency_stats_single_sample_is_every_percentile() {
        let stats = latency_stats(vec![42]).unwrap();

        assert_eq!(stats.p50_ms, 42);
        assert_eq!(stats.p90_ms, 42);
        assert_eq!(stats.p95_ms, 42);
        assert_eq!(stats.p99_ms, 42);
        assert_eq!(stats.max_ms, 42);
        assert_eq!(stats.sample_count, 1);
    }

    #[test]
    fn latency_stats_empty_is_none() {
        assert_eq!(latency_stats(Vec::new()), None);
    }

    fn report_range(from_offset: i64, to_offset: i64) -> ReportRange {
        ReportRange {
            from: timestamp(from_offset),
            to: timestamp(to_offset),
        }
    }

    #[test]
    fn report_windows_fills_and_cycles_by_range() {
        let order_id = OffchainOrderId::new();
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: Some(timestamp(11)),
                outcome: HedgeOutcome::Filled {
                    filled_at: timestamp(12),
                },
            },
        )]);
        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 5),
                placed_event(order_id, 10),
                // Outside the window below.
                fill_event(2, 100_000, 100_005),
            ],
            &timelines,
        );

        let report = hedge_latency_report(&[performance], &report_range(0, 1_000));

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

    #[test]
    fn report_buckets_fills_by_day() {
        let day = 86_400;
        let first = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 2), fill_event(2, 3 * day, 3 * day + 4)],
            &HashMap::new(),
        );

        let report = hedge_latency_report(&[first], &report_range(0, 10 * day));

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

    #[test]
    fn report_buckets_weekly_for_ranges_over_a_month() {
        let day = 86_400;
        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 2), fill_event(2, 10 * day, 10 * day + 4)],
            &HashMap::new(),
        );

        let report = hedge_latency_report(&[performance], &report_range(0, 60 * day));

        assert_eq!(report.buckets.len(), 2);
        assert_eq!(report.buckets[0].start, timestamp(0));
        assert_eq!(report.buckets[1].start, timestamp(7 * day));
    }

    #[test]
    fn report_buckets_monthly_for_ranges_over_half_a_year() {
        let day = 86_400;
        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 2), fill_event(2, 45 * day, 45 * day + 4)],
            &HashMap::new(),
        );

        let report = hedge_latency_report(&[performance], &report_range(0, 300 * day));

        assert_eq!(report.buckets.len(), 2);
        assert_eq!(report.buckets[0].start, timestamp(0));
        assert_eq!(report.buckets[1].start, timestamp(30 * day));
    }

    #[test]
    fn report_caps_cycle_rows_but_counts_all() {
        let events = (0..=i64::try_from(MAX_CYCLE_REPORTS).unwrap())
            .map(|index| placed_event(OffchainOrderId::new(), index))
            .collect();

        let report = hedge_latency_report(
            &[fold_position_stream(symbol(), events, &HashMap::new())],
            &report_range(0, 1_000),
        );

        assert_eq!(report.total_cycles, MAX_CYCLE_REPORTS + 1);
        assert_eq!(report.cycles.len(), MAX_CYCLE_REPORTS);
    }

    #[test]
    fn report_clamps_negative_stage_latencies_to_zero() {
        let order_id = OffchainOrderId::new();
        // Broker clock skew: submitted before placed.
        let timelines = HashMap::from([(
            order_id,
            OrderTimeline {
                submitted_at: Some(timestamp(1)),
                outcome: HedgeOutcome::Pending,
            },
        )]);
        let performance = fold_position_stream(
            symbol(),
            vec![fill_event(1, 0, 1), placed_event(order_id, 3)],
            &timelines,
        );

        let report = hedge_latency_report(&[performance], &report_range(0, 1_000));

        let submission = report.summary.stages.submission.as_ref().unwrap();
        assert_eq!(submission.p50_ms, 0);
        assert_eq!(submission.max_ms, 0);
    }

    #[test]
    fn report_orders_cycles_newest_first() {
        let first = OffchainOrderId::new();
        let second = OffchainOrderId::new();
        let performance = fold_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(first, 5),
                fill_event(2, 10, 11),
                placed_event(second, 20),
            ],
            &HashMap::new(),
        );

        let report = hedge_latency_report(&[performance], &report_range(0, 1_000));

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

    #[tokio::test]
    async fn load_hedge_performance_skips_malformed_events() {
        let pool = setup_test_db().await;

        let fill = serde_json::to_string(&fill_event(1, 0, 5)).unwrap();
        insert_event(
            &pool,
            "Position",
            "AAPL",
            1,
            "PositionEvent::OnChainOrderFilled",
            &fill,
        )
        .await;
        insert_event(
            &pool,
            "Position",
            "AAPL",
            2,
            "PositionEvent::OnChainOrderFilled",
            "{\"not\": \"a position event\"}",
        )
        .await;

        let report = load_hedge_performance(&pool).await.unwrap();

        assert_eq!(report.len(), 1);
        assert_eq!(report[0].fills.len(), 1);
    }
}
