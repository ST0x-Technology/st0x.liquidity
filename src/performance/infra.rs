//! Ingestion-infrastructure read model over the telemetry store.
//!
//! Surfaces the order-fill monitor's block-lag and poll-cycle samples
//! (recorded by `crate::telemetry`) as the dashboard's ingestion-health
//! report: current block lag, worst lag per time bucket, and poll-cycle
//! duration/error/skipped-tick aggregates. Strictly read-only.

use std::collections::BTreeMap;

use alloy::primitives::Address;
use chrono::{DateTime, Duration, SubsecRound, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::{
    BlockLagPoint, DependencyBucket, DependencyName, DependencyStats, MonitorTelemetry, PollHealth,
};

use super::{PerformanceError, ReportRange, latency_stats};
use crate::telemetry::{Monitor, PollOutcome, sqlite_timestamp};

/// Load the monitor's ingestion-health telemetry for `range`, scoped to
/// the configured `orderbook` (samples carry the orderbook they were taken
/// against, so a database reused across configs never mixes lag series).
///
/// The current block lag reflects the latest sample regardless of the
/// range: it answers "how far behind is detection right now", while the
/// bucketed series answers "how did lag trend over the window".
pub(crate) async fn load_monitor_telemetry(
    pool: &SqlitePool,
    range: &ReportRange,
    orderbook: Address,
) -> Result<MonitorTelemetry, PerformanceError> {
    let (current_lag_blocks, current_lag_sampled_at) = current_lag(pool, orderbook).await?;
    let block_lag = block_lag_buckets(pool, range, orderbook).await?;
    let poll_summary = poll_health(pool, range, orderbook).await?;

    Ok(MonitorTelemetry {
        current_lag_blocks,
        current_lag_sampled_at,
        block_lag,
        poll: poll_summary,
    })
}

async fn current_lag(
    pool: &SqlitePool,
    orderbook: Address,
) -> Result<(Option<i64>, Option<DateTime<Utc>>), PerformanceError> {
    let latest: Option<(String, i64)> = sqlx::query_as(
        "SELECT sampled_at, lag_blocks FROM block_lag_samples \
         WHERE lag_blocks IS NOT NULL AND orderbook = $1 \
         ORDER BY sampled_at DESC, id DESC LIMIT 1",
    )
    .bind(orderbook.to_string())
    .fetch_optional(pool)
    .await?;

    Ok(match latest {
        Some((raw_sampled_at, lag_blocks)) => {
            // A corrupt timestamp makes the lag value unanchorable in time.
            // Return both fields as null so the TS client treats it as "no
            // sample" rather than showing lag without a timestamp.
            let ts = parse_timestamp(&raw_sampled_at);
            (ts.map(|_| lag_blocks), ts)
        }
        None => (None, None),
    })
}

/// Worst lag per time bucket, aggregated in SQL so a multi-day range never
/// materializes its raw sample rows (one per poll tick) into the heap.
/// Samples without a checkpoint carry a NULL lag; they prove the monitor
/// polled but contribute nothing to the lag trend.
async fn block_lag_buckets(
    pool: &SqlitePool,
    range: &ReportRange,
    orderbook: Address,
) -> Result<Vec<BlockLagPoint>, PerformanceError> {
    let width = range.bucket_width();
    // strftime('%s', ...) truncates sample timestamps to whole seconds, so
    // the bucket origin is truncated the same way; otherwise a fractional
    // origin could push the last bucket's computed start past the newest
    // sample it contains.
    let origin = range.from.trunc_subsecs(0);
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT (CAST(strftime('%s', sampled_at) AS INTEGER) - $4) / $5 AS bucket_index, \
                MAX(lag_blocks) AS max_lag_blocks \
         FROM block_lag_samples \
         WHERE sampled_at BETWEEN $1 AND $2 AND orderbook = $3 \
           AND lag_blocks IS NOT NULL \
         GROUP BY bucket_index \
         ORDER BY bucket_index",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(orderbook.to_string())
    .bind(origin.timestamp())
    .bind(width.num_seconds())
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(index, max_lag_blocks)| BlockLagPoint {
            start: origin + Duration::seconds(width.num_seconds() * index),
            max_lag_blocks,
        })
        .collect())
}

async fn poll_health(
    pool: &SqlitePool,
    range: &ReportRange,
    orderbook: Address,
) -> Result<PollHealth, PerformanceError> {
    // Aggregate cycles, errors, and skipped_ticks in SQL to avoid
    // materializing potentially large row sets into the heap. The error count
    // uses the canonical PollOutcome discriminator so writer and reader cannot
    // drift. Duration percentiles still require the individual values, so only
    // that column is fetched as a separate query.
    let aggregate: AggregateRow = sqlx::query_as(
        "SELECT COUNT(*) AS cycles, \
                SUM(CASE WHEN outcome = $5 THEN 1 ELSE 0 END) AS errors, \
                SUM(skipped_ticks) AS skipped_ticks_sum \
         FROM poll_cycle_samples \
         WHERE sampled_at BETWEEN $1 AND $2 AND monitor = $3 AND orderbook = $4",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(Monitor::OrderFill.as_str())
    .bind(orderbook.to_string())
    .bind(PollOutcome::Error.as_str())
    .fetch_one(pool)
    .await?;

    let mut durations: Vec<i64> = sqlx::query_scalar(
        "SELECT duration_ms FROM poll_cycle_samples \
         WHERE sampled_at BETWEEN $1 AND $2 AND monitor = $3 AND orderbook = $4",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(Monitor::OrderFill.as_str())
    .bind(orderbook.to_string())
    .fetch_all(pool)
    .await?;

    Ok(PollHealth {
        cycles: count(aggregate.cycles),
        errors: count(aggregate.errors.unwrap_or(0)),
        skipped_ticks: count(aggregate.skipped_ticks_sum.unwrap_or(0)),
        duration: latency_stats(&mut durations),
    })
}

/// SQL-aggregated counts for one poll-health query. `cycles` is never null
/// (`COUNT(*)` always returns a value); the `SUM` columns are `NULL` when
/// there are no matching rows, mapped to zero by the caller.
#[derive(sqlx::FromRow)]
struct AggregateRow {
    cycles: i64,
    errors: Option<i64>,
    skipped_ticks_sum: Option<i64>,
}

/// One dependency call sample as returned by the SQL query in
/// `load_dependency_stats`. Named fields prevent the two `i64`s from being
/// transposed at the construction site.
struct CallSample {
    bucket_index: i64,
    duration_ms: i64,
    is_error: bool,
}

/// Per-(dependency, operation) accumulator of [`CallSample`]s, grouped before
/// aggregation into `DependencyStats`.
type DependencyGroups = BTreeMap<(DependencyName, String), Vec<CallSample>>;

/// Load per-(dependency, operation) call aggregates for `range`.
///
/// The time-bucket index and the error flag are computed in SQL (mirroring
/// `block_lag_buckets`) so each row arrives as compact scalars rather than
/// four heap strings plus a parsed timestamp. Raw durations still come back
/// per row -- SQLite has no percentile function, so the p50/p90/p99 stats are
/// computed in Rust -- but the per-row footprint is bounded to a single small
/// `operation` string.
pub(crate) async fn load_dependency_stats(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<Vec<DependencyStats>, PerformanceError> {
    let width = range.bucket_width();
    // strftime('%s', ...) truncates to whole seconds, so anchor the bucket
    // origin the same way (see `block_lag_buckets`).
    let origin = range.from.trunc_subsecs(0);
    let rows: Vec<(i64, String, String, i64, bool)> = sqlx::query_as(
        "SELECT (CAST(strftime('%s', recorded_at) AS INTEGER) - $3) / $4 AS bucket_index, \
                dependency, operation, duration_ms, outcome = 'error' AS is_error \
         FROM dependency_call_samples \
         WHERE recorded_at BETWEEN $1 AND $2 \
         ORDER BY dependency, operation, bucket_index",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(origin.timestamp())
    .bind(width.num_seconds())
    .fetch_all(pool)
    .await?;

    let mut groups: DependencyGroups = BTreeMap::new();
    for (bucket_index, raw_dependency, operation, duration_ms, is_error) in rows {
        let dependency = match raw_dependency.as_str() {
            "rpc" => DependencyName::Rpc,
            "broker" => DependencyName::Broker,
            other => {
                warn!(
                    dependency = other,
                    "Skipping sample with unknown dependency"
                );
                continue;
            }
        };

        groups
            .entry((dependency, operation))
            .or_default()
            .push(CallSample {
                bucket_index,
                duration_ms,
                is_error,
            });
    }

    Ok(groups
        .into_iter()
        .map(|((dependency, operation), samples)| {
            dependency_stats(dependency, operation, &samples, origin, width)
        })
        .collect())
}

fn dependency_stats(
    dependency: DependencyName,
    operation: String,
    samples: &[CallSample],
    origin: DateTime<Utc>,
    width: Duration,
) -> DependencyStats {
    let mut buckets: BTreeMap<i64, (Vec<i64>, usize)> = BTreeMap::new();
    for sample in samples {
        let (durations, errors) = buckets.entry(sample.bucket_index).or_default();
        durations.push(sample.duration_ms);
        if sample.is_error {
            *errors += 1;
        }
    }

    let errors = samples.iter().filter(|sample| sample.is_error).count();
    let mut durations: Vec<i64> = samples.iter().map(|sample| sample.duration_ms).collect();

    DependencyStats {
        dependency,
        operation,
        calls: samples.len(),
        errors,
        latency: latency_stats(&mut durations),
        buckets: buckets
            .into_iter()
            .map(|(index, (mut durations, errors))| DependencyBucket {
                start: origin + Duration::seconds(width.num_seconds() * index),
                calls: durations.len(),
                errors,
                p50_ms: latency_stats(&mut durations).map(|stats| stats.p50_ms),
            })
            .collect(),
    }
}

/// Stored counts are non-negative by schema CHECK; a negative value means a
/// corrupted row, logged loudly before falling back to zero.
fn count(value: i64) -> usize {
    usize::try_from(value).unwrap_or_else(|_| {
        warn!(value, "Negative count in telemetry row; reporting zero");
        0
    })
}

fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|parsed| parsed.with_timezone(&Utc))
        .inspect_err(|error| warn!(%raw, %error, "Skipping telemetry row with malformed timestamp"))
        .ok()
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::time::Duration as StdDuration;

    use alloy::primitives::address;
    use chrono::TimeZone;

    use crate::telemetry::{BlockLagSample, record_block_lag, record_poll_cycle};
    use crate::test_utils::setup_test_db;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    /// Orderbook all test samples are recorded against.
    const ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");

    fn range() -> ReportRange {
        ReportRange {
            from: timestamp(0),
            to: timestamp(86_400),
        }
    }

    async fn insert_lag_for(
        pool: &SqlitePool,
        orderbook: Address,
        seconds: i64,
        chain_tip: u64,
        checkpoint: Option<u64>,
    ) {
        record_block_lag(
            pool,
            &BlockLagSample {
                sampled_at: timestamp(seconds),
                orderbook,
                chain_tip,
                finalized_block: chain_tip.saturating_sub(3),
                last_processed_block: checkpoint,
            },
        )
        .await
        .unwrap();
    }

    async fn insert_lag(pool: &SqlitePool, seconds: i64, chain_tip: u64, checkpoint: Option<u64>) {
        insert_lag_for(pool, ORDERBOOK, seconds, chain_tip, checkpoint).await;
    }

    #[tokio::test]
    async fn reports_latest_lag_and_bucketed_maxima() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, Some(100)).await; // finalized 107, lag 7
        insert_lag(&pool, 20, 125, Some(100)).await; // finalized 122, lag 22
        insert_lag(&pool, 4_000, 210, Some(205)).await; // finalized 207, lag 2

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(telemetry.current_lag_blocks, Some(2));
        assert_eq!(telemetry.current_lag_sampled_at, Some(timestamp(4_000)));
        assert_eq!(
            telemetry.block_lag,
            vec![
                BlockLagPoint {
                    start: timestamp(0),
                    max_lag_blocks: 22,
                },
                BlockLagPoint {
                    start: timestamp(3_600),
                    max_lag_blocks: 2,
                },
            ]
        );
    }

    #[tokio::test]
    async fn current_lag_ignores_range_but_buckets_respect_it() {
        let pool = setup_test_db().await;
        // Outside (after) the report range, with a DISTINCT lag value so
        // the assertion can tell which sample won: finalized 497, lag 2.
        insert_lag(&pool, 100_000, 500, Some(495)).await;
        // In range: finalized 107, lag 7.
        insert_lag(&pool, 10, 110, Some(100)).await;

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(
            telemetry.current_lag_blocks,
            Some(2),
            "current lag must come from the freshest sample, even out of range"
        );
        assert_eq!(telemetry.block_lag.len(), 1);
        assert_eq!(telemetry.block_lag[0].max_lag_blocks, 7);
    }

    #[tokio::test]
    async fn other_orderbooks_samples_are_excluded() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, Some(100)).await; // lag 7
        insert_lag_for(
            &pool,
            address!("0x2222222222222222222222222222222222222222"),
            20,
            500,
            Some(400),
        )
        .await;

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(telemetry.current_lag_blocks, Some(7));
        assert_eq!(telemetry.block_lag.len(), 1);
        assert_eq!(telemetry.block_lag[0].max_lag_blocks, 7);
    }

    #[tokio::test]
    async fn checkpointless_samples_do_not_produce_lag_points() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, None).await;

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(telemetry.current_lag_blocks, None);
        assert_eq!(telemetry.current_lag_sampled_at, None);
        assert!(telemetry.block_lag.is_empty());
    }

    #[tokio::test]
    async fn aggregates_poll_cycle_health() {
        let pool = setup_test_db().await;
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            ORDERBOOK,
            timestamp(10),
            StdDuration::from_millis(100),
            0,
            Ok::<(), &Infallible>(()),
        )
        .await
        .unwrap();
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            ORDERBOOK,
            timestamp(20),
            StdDuration::from_millis(300),
            2,
            Err(&"rpc unreachable"),
        )
        .await
        .unwrap();
        // Outside the range: must not be counted.
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            ORDERBOOK,
            timestamp(-100),
            StdDuration::from_millis(900),
            5,
            Ok::<(), &Infallible>(()),
        )
        .await
        .unwrap();
        // A different monitor's samples must not pollute the aggregates.
        sqlx::query(
            "INSERT INTO poll_cycle_samples \
             (sampled_at, monitor, orderbook, duration_ms, skipped_ticks, outcome, error) \
             VALUES ($1, 'other_monitor', $2, 9000, 9, 'ok', NULL)",
        )
        .bind(sqlite_timestamp(timestamp(30)))
        .bind(ORDERBOOK.to_string())
        .execute(&pool)
        .await
        .unwrap();
        // Another orderbook's poll cycles must not pollute the aggregates
        // either: a database reused across configs keeps series separate.
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            address!("0x2222222222222222222222222222222222222222"),
            timestamp(40),
            StdDuration::from_millis(7_000),
            8,
            Err(&"other orderbook outage"),
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(telemetry.poll.cycles, 2);
        assert_eq!(telemetry.poll.errors, 1);
        assert_eq!(telemetry.poll.skipped_ticks, 2);
        let duration = telemetry.poll.duration.unwrap();
        assert_eq!(duration.sample_count, 2);
        assert_eq!(duration.max_ms, 300);
    }

    async fn insert_call(
        pool: &SqlitePool,
        seconds: i64,
        dependency: &str,
        operation: &str,
        duration_ms: i64,
        error: Option<&str>,
    ) {
        let outcome = if error.is_some() { "error" } else { "ok" };
        sqlx::query(
            "INSERT INTO dependency_call_samples \
             (recorded_at, dependency, operation, duration_ms, outcome, error) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(sqlite_timestamp(timestamp(seconds)))
        .bind(dependency)
        .bind(operation)
        .bind(duration_ms)
        .bind(outcome)
        .bind(error)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn groups_dependency_calls_by_operation_with_buckets() {
        let pool = setup_test_db().await;
        insert_call(&pool, 10, "rpc", "eth_blockNumber", 50, None).await;
        insert_call(&pool, 20, "rpc", "eth_blockNumber", 150, Some("timeout")).await;
        insert_call(&pool, 4_000, "rpc", "eth_blockNumber", 70, None).await;
        insert_call(&pool, 30, "broker", "place_market_order", 400, None).await;
        // Outside the range: must not be counted.
        insert_call(&pool, -100, "rpc", "eth_blockNumber", 999, None).await;

        let stats = load_dependency_stats(&pool, &range()).await.unwrap();

        assert_eq!(stats.len(), 2);
        // Look the groups up by their (dependency, operation) key rather than
        // by index, so the assertions test the grouping invariant instead of
        // the current sort order.
        let broker = stats
            .iter()
            .find(|row| {
                row.dependency == DependencyName::Broker && row.operation == "place_market_order"
            })
            .expect("missing broker/place_market_order group");
        assert_eq!(broker.calls, 1);
        assert_eq!(broker.errors, 0);

        let rpc = stats
            .iter()
            .find(|row| row.dependency == DependencyName::Rpc && row.operation == "eth_blockNumber")
            .expect("missing rpc/eth_blockNumber group");
        assert_eq!(rpc.calls, 3);
        assert_eq!(rpc.errors, 1);
        assert_eq!(rpc.latency.as_ref().unwrap().max_ms, 150);
        assert_eq!(rpc.latency.as_ref().unwrap().sample_count, 3);

        assert_eq!(rpc.buckets.len(), 2);
        assert_eq!(rpc.buckets[0].start, timestamp(0));
        assert_eq!(rpc.buckets[0].calls, 2);
        assert_eq!(rpc.buckets[0].errors, 1);
        assert_eq!(rpc.buckets[0].p50_ms, Some(50));
        assert_eq!(rpc.buckets[1].start, timestamp(3_600));
        assert_eq!(rpc.buckets[1].calls, 1);
        assert_eq!(
            rpc.buckets[1].errors, 0,
            "errors must not leak across buckets"
        );
        assert_eq!(rpc.buckets[1].p50_ms, Some(70));
    }

    #[tokio::test]
    async fn empty_store_yields_empty_report() {
        let pool = setup_test_db().await;

        let telemetry = load_monitor_telemetry(&pool, &range(), ORDERBOOK)
            .await
            .unwrap();

        assert_eq!(telemetry.current_lag_blocks, None);
        assert!(telemetry.block_lag.is_empty());
        assert_eq!(
            telemetry.poll,
            PollHealth {
                cycles: 0,
                errors: 0,
                skipped_ticks: 0,
                duration: None,
            }
        );
    }
}
