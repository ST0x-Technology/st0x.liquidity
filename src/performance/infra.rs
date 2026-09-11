//! Ingestion-infrastructure read model over the telemetry store.
//!
//! Surfaces the order-fill monitors' block-lag and poll-cycle samples
//! (recorded by `crate::telemetry`) as the dashboard's ingestion-health
//! report: per hedged chain, the current block lag and the worst lag per
//! time bucket; plus poll-cycle duration/error/skipped-tick aggregates.
//! Strictly read-only.

use std::collections::{BTreeMap, BTreeSet};

use alloy::primitives::Address;
use chrono::{DateTime, Duration, SubsecRound, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_config::{ChainRegistry, HedgedChain};
use st0x_dto::{
    BlockLagPoint, ChainBlockLag, ChainName, DependencyBucket, DependencyName, DependencyStats,
    MonitorTelemetry, PollHealth,
};
use st0x_evm::Chain;

use super::{PerformanceError, ReportRange, latency_stats};
use crate::telemetry::{Monitor, PollOutcome, sqlite_timestamp};

/// Load the monitors' ingestion-health telemetry for `range`: one block-lag
/// series per hedged chain (primary first), each scoped to that chain and
/// its orderbook so a database reused across configs, or two chains sharing
/// an orderbook address, never mix lag series. Poll health covers every
/// hedged chain's orderbook: each runs its own fill watcher, so a report
/// scoped to the primary would read as healthy through a secondary's
/// outage.
///
/// The current block lag reflects the latest sample regardless of the
/// range: it answers "how far behind is detection right now", while the
/// bucketed series answers "how did lag trend over the window".
pub(crate) async fn load_monitor_telemetry(
    pool: &SqlitePool,
    range: &ReportRange,
    chains: &ChainRegistry,
) -> Result<MonitorTelemetry, PerformanceError> {
    let mut block_lag = Vec::new();
    for hedged_chain in chains.hedged() {
        block_lag.push(chain_block_lag(pool, range, hedged_chain).await?);
    }
    let poll_summary = poll_health(pool, range, chains).await?;

    Ok(MonitorTelemetry {
        block_lag,
        poll: poll_summary,
    })
}

async fn chain_block_lag(
    pool: &SqlitePool,
    range: &ReportRange,
    hedged_chain: &HedgedChain,
) -> Result<ChainBlockLag, PerformanceError> {
    let (current_lag_blocks, current_lag_sampled_at) = current_lag(pool, hedged_chain).await?;
    let points = block_lag_buckets(pool, range, hedged_chain).await?;

    Ok(ChainBlockLag {
        chain: chain_name(hedged_chain.chain),
        current_lag_blocks,
        current_lag_sampled_at,
        points,
    })
}

/// The dashboard's chain discriminator for a chain the bot operates on.
/// Exhaustive so a chain added to `Chain` cannot reach the report unnamed.
fn chain_name(chain: Chain) -> ChainName {
    match chain {
        Chain::Base => ChainName::Base,
        Chain::Ethereum => ChainName::Ethereum,
        Chain::HyperEvm => ChainName::HyperEvm,
    }
}

async fn current_lag(
    pool: &SqlitePool,
    hedged_chain: &HedgedChain,
) -> Result<(Option<i64>, Option<DateTime<Utc>>), PerformanceError> {
    let latest: Option<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT sampled_at, cutoff_block, lag_blocks FROM block_lag_samples \
         WHERE chain = $1 AND orderbook = $2 \
         ORDER BY sampled_at DESC, id DESC LIMIT 1",
    )
    .bind(hedged_chain.chain.as_str())
    .bind(hedged_chain.orderbook.to_string())
    .fetch_optional(pool)
    .await?;

    Ok(match latest {
        Some((raw_sampled_at, cutoff_block, lag_blocks)) => {
            // A corrupt timestamp makes the lag value unanchorable in time.
            // Return both fields as null so the TS client treats it as "no
            // sample" rather than showing lag without a timestamp.
            let ts = parse_timestamp(&raw_sampled_at);
            match (cutoff_block, lag_blocks, ts) {
                (None, _, sampled_at) => (None, sampled_at),
                (Some(_), Some(lag_blocks), Some(sampled_at)) => {
                    (Some(lag_blocks), Some(sampled_at))
                }
                (Some(_), _, _) => (None, None),
            }
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
    hedged_chain: &HedgedChain,
) -> Result<Vec<BlockLagPoint>, PerformanceError> {
    let width = range.bucket_width();
    // strftime('%s', ...) truncates sample timestamps to whole seconds, so
    // the bucket origin is truncated the same way; otherwise a fractional
    // origin could push the last bucket's computed start past the newest
    // sample it contains.
    let origin = range.from.trunc_subsecs(0);
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT (CAST(strftime('%s', sampled_at) AS INTEGER) - $5) / $6 AS bucket_index, \
                MAX(lag_blocks) AS max_lag_blocks \
         FROM block_lag_samples \
         WHERE sampled_at BETWEEN $1 AND $2 AND chain = $3 AND orderbook = $4 \
           AND lag_blocks IS NOT NULL \
         GROUP BY bucket_index \
         ORDER BY bucket_index",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(hedged_chain.chain.as_str())
    .bind(hedged_chain.orderbook.to_string())
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
    chains: &ChainRegistry,
) -> Result<PollHealth, PerformanceError> {
    // Deduplicated because deterministic deployments put the same orderbook
    // address on several chains, and its samples must be counted once.
    let orderbooks: BTreeSet<Address> = chains.hedged().map(|hedged| hedged.orderbook).collect();

    let mut cycles = 0_i64;
    let mut errors = 0_i64;
    let mut skipped_ticks = 0_i64;
    let mut durations = Vec::new();

    for orderbook in orderbooks {
        let aggregate = orderbook_poll_aggregate(pool, range, orderbook).await?;
        cycles += aggregate.cycles;
        errors += aggregate.errors.unwrap_or(0);
        skipped_ticks += aggregate.skipped_ticks_sum.unwrap_or(0);
        durations.extend(orderbook_poll_durations(pool, range, orderbook).await?);
    }

    Ok(PollHealth {
        cycles: count(cycles),
        errors: count(errors),
        skipped_ticks: count(skipped_ticks),
        duration: latency_stats(&mut durations),
    })
}

/// One orderbook's cycle, error and skipped-tick counts. Aggregated in SQL to
/// avoid materializing potentially large row sets into the heap. The error
/// count uses the canonical [`PollOutcome`] discriminator so writer and reader
/// cannot drift.
async fn orderbook_poll_aggregate(
    pool: &SqlitePool,
    range: &ReportRange,
    orderbook: Address,
) -> Result<AggregateRow, PerformanceError> {
    Ok(sqlx::query_as(
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
    .await?)
}

/// One orderbook's individual cycle durations: percentiles need the raw
/// values, so this column alone comes back row by row.
async fn orderbook_poll_durations(
    pool: &SqlitePool,
    range: &ReportRange,
    orderbook: Address,
) -> Result<Vec<i64>, PerformanceError> {
    Ok(sqlx::query_scalar(
        "SELECT duration_ms FROM poll_cycle_samples \
         WHERE sampled_at BETWEEN $1 AND $2 AND monitor = $3 AND orderbook = $4",
    )
    .bind(sqlite_timestamp(range.from))
    .bind(sqlite_timestamp(range.to))
    .bind(Monitor::OrderFill.as_str())
    .bind(orderbook.to_string())
    .fetch_all(pool)
    .await?)
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

    use st0x_config::{ChainRegistry, HedgedChain};
    use st0x_dto::{ChainBlockLag, ChainName};
    use st0x_evm::Chain;

    use crate::telemetry::{BlockLagSample, record_block_lag, record_poll_cycle};
    use crate::test_utils::setup_test_db;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    /// The dashboard's chain discriminator serializes to the wire name
    /// `st0x_evm::Chain` pins, for every chain the bot can watch. It lives
    /// here because the dto crate cannot see `Chain`: the two spellings are
    /// separately pinned literals, so nothing else catches them drifting.
    #[test]
    fn chain_name_wire_names_match_the_evm_chain_names() {
        for chain in Chain::ALL {
            assert_eq!(
                serde_json::to_value(chain_name(chain)).unwrap(),
                serde_json::json!(chain.as_str()),
                "{chain:?} must reach the dashboard under its pinned wire name"
            );
        }
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
        chain: Chain,
        orderbook: Address,
        seconds: i64,
        chain_tip: u64,
        checkpoint: Option<u64>,
    ) {
        record_block_lag(
            pool,
            &BlockLagSample {
                sampled_at: timestamp(seconds),
                chain,
                orderbook,
                chain_tip,
                cutoff_block: Some(chain_tip.saturating_sub(3)),
                last_processed_block: checkpoint,
            },
        )
        .await
        .unwrap();
    }

    async fn insert_lag(pool: &SqlitePool, seconds: i64, chain_tip: u64, checkpoint: Option<u64>) {
        insert_lag_for(pool, Chain::Base, ORDERBOOK, seconds, chain_tip, checkpoint).await;
    }

    /// Base hedged alone, against [`ORDERBOOK`].
    fn base_only() -> ChainRegistry {
        ChainRegistry::single_hedged_chain(HedgedChain::test().orderbook(ORDERBOOK).call())
    }

    /// The one series a Base-only report carries.
    fn base_series(telemetry: &MonitorTelemetry) -> &ChainBlockLag {
        let [series] = telemetry.block_lag.as_slice() else {
            panic!("expected exactly one series, got {:?}", telemetry.block_lag);
        };
        assert_eq!(series.chain, ChainName::Base);
        series
    }

    /// Two hedged chains keep separate lag series even when the Raindex
    /// orderbook lands at the same deterministic address on both.
    #[tokio::test]
    async fn each_hedged_chain_gets_its_own_lag_series() {
        let pool = setup_test_db().await;
        let mut chains = base_only();
        chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::Ethereum)
                .orderbook(ORDERBOOK)
                .call(),
        );
        insert_lag(&pool, 10, 110, Some(100)).await; // base: cutoff 107, lag 7
        // ethereum, same orderbook address: cutoff 497, lag 97.
        insert_lag_for(&pool, Chain::Ethereum, ORDERBOOK, 20, 500, Some(400)).await;

        let telemetry = load_monitor_telemetry(&pool, &range(), &chains)
            .await
            .unwrap();

        assert_eq!(
            telemetry.block_lag,
            vec![
                ChainBlockLag {
                    chain: ChainName::Base,
                    current_lag_blocks: Some(7),
                    current_lag_sampled_at: Some(timestamp(10)),
                    points: vec![BlockLagPoint {
                        start: timestamp(0),
                        max_lag_blocks: 7,
                    }],
                },
                ChainBlockLag {
                    chain: ChainName::Ethereum,
                    current_lag_blocks: Some(97),
                    current_lag_sampled_at: Some(timestamp(20)),
                    points: vec![BlockLagPoint {
                        start: timestamp(0),
                        max_lag_blocks: 97,
                    }],
                },
            ],
            "the primary's series comes first; the secondary's is never merged into it"
        );
    }

    #[tokio::test]
    async fn reports_latest_lag_and_bucketed_maxima() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, Some(100)).await; // cutoff 107, lag 7
        insert_lag(&pool, 20, 125, Some(100)).await; // cutoff 122, lag 22
        insert_lag(&pool, 4_000, 210, Some(205)).await; // cutoff 207, lag 2

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(base_series(&telemetry).current_lag_blocks, Some(2));
        assert_eq!(
            base_series(&telemetry).current_lag_sampled_at,
            Some(timestamp(4_000))
        );
        assert_eq!(
            base_series(&telemetry).points,
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
    async fn latest_unknown_cutoff_supersedes_previous_healthy_lag() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, Some(100)).await;
        record_block_lag(
            &pool,
            &BlockLagSample {
                sampled_at: timestamp(20),
                chain: Chain::Base,
                orderbook: ORDERBOOK,
                chain_tip: 120,
                cutoff_block: None,
                last_processed_block: Some(100),
            },
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(base_series(&telemetry).current_lag_blocks, None);
        assert_eq!(
            base_series(&telemetry).current_lag_sampled_at,
            Some(timestamp(20))
        );
        assert_eq!(
            base_series(&telemetry).points,
            vec![BlockLagPoint {
                start: timestamp(0),
                max_lag_blocks: 7,
            }],
            "unknown samples must not fabricate a zero-lag chart point"
        );
    }

    #[tokio::test]
    async fn current_lag_ignores_range_but_buckets_respect_it() {
        let pool = setup_test_db().await;
        // Outside (after) the report range, with a DISTINCT lag value so
        // the assertion can tell which sample won: cutoff 497, lag 2.
        insert_lag(&pool, 100_000, 500, Some(495)).await;
        // In range: cutoff 107, lag 7.
        insert_lag(&pool, 10, 110, Some(100)).await;

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(
            base_series(&telemetry).current_lag_blocks,
            Some(2),
            "current lag must come from the freshest sample, even out of range"
        );
        assert_eq!(base_series(&telemetry).points.len(), 1);
        assert_eq!(base_series(&telemetry).points[0].max_lag_blocks, 7);
    }

    #[tokio::test]
    async fn other_orderbooks_samples_are_excluded() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, Some(100)).await; // lag 7
        insert_lag_for(
            &pool,
            Chain::Base,
            address!("0x2222222222222222222222222222222222222222"),
            20,
            500,
            Some(400),
        )
        .await;

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(base_series(&telemetry).current_lag_blocks, Some(7));
        assert_eq!(base_series(&telemetry).points.len(), 1);
        assert_eq!(base_series(&telemetry).points[0].max_lag_blocks, 7);
    }

    #[tokio::test]
    async fn checkpointless_samples_do_not_produce_lag_points() {
        let pool = setup_test_db().await;
        insert_lag(&pool, 10, 110, None).await;

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(base_series(&telemetry).current_lag_blocks, None);
        assert_eq!(base_series(&telemetry).current_lag_sampled_at, None);
        assert_eq!(base_series(&telemetry).points, vec![]);
    }

    #[tokio::test]
    async fn aggregates_poll_cycle_health() {
        let pool = setup_test_db().await;
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            Chain::Base,
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
            Chain::Base,
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
            Chain::Base,
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
             (sampled_at, monitor, chain, orderbook, duration_ms, skipped_ticks, outcome, error) \
             VALUES ($1, 'other_monitor', $2, $3, 9000, 9, 'ok', NULL)",
        )
        .bind(sqlite_timestamp(timestamp(30)))
        .bind(Chain::Base.as_str())
        .bind(ORDERBOOK.to_string())
        .execute(&pool)
        .await
        .unwrap();
        // Another orderbook's poll cycles must not pollute the aggregates
        // either: a database reused across configs keeps series separate.
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            Chain::Base,
            address!("0x2222222222222222222222222222222222222222"),
            timestamp(40),
            StdDuration::from_millis(7_000),
            8,
            Err(&"other orderbook outage"),
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(telemetry.poll.cycles, 2);
        assert_eq!(telemetry.poll.errors, 1);
        assert_eq!(telemetry.poll.skipped_ticks, 2);
        let duration = telemetry.poll.duration.unwrap();
        assert_eq!(duration.sample_count, 2);
        assert_eq!(duration.max_ms, 300);
    }

    /// Poll cycles belong to the chain whose watcher ran them: a sample from
    /// a chain that is not hedged must not be counted for one that is, even
    /// when both name the same deterministic orderbook address.
    #[tokio::test]
    async fn a_chains_poll_cycles_are_not_counted_for_another_chain() {
        let pool = setup_test_db().await;
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            Chain::Ethereum,
            ORDERBOOK,
            timestamp(10),
            StdDuration::from_millis(100),
            4,
            Err(&"ethereum rpc unreachable"),
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(telemetry.poll.cycles, 0);
        assert_eq!(telemetry.poll.errors, 0);
        assert_eq!(telemetry.poll.skipped_ticks, 0);
        assert_eq!(telemetry.poll.duration, None);
    }

    /// A secondary chain runs its own fill watcher against its own
    /// orderbook, so its poll cycles belong in the report's poll health --
    /// keyed to the primary alone, an outage there would read as healthy.
    #[tokio::test]
    async fn poll_health_aggregates_every_hedged_chain() {
        let ethereum_orderbook = address!("0x3333333333333333333333333333333333333333");
        let pool = setup_test_db().await;
        let mut chains = base_only();
        chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::Ethereum)
                .orderbook(ethereum_orderbook)
                .call(),
        );
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            Chain::Base,
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
            Chain::Ethereum,
            ethereum_orderbook,
            timestamp(20),
            StdDuration::from_millis(400),
            3,
            Err(&"secondary rpc unreachable"),
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), &chains)
            .await
            .unwrap();

        assert_eq!(telemetry.poll.cycles, 2);
        assert_eq!(telemetry.poll.errors, 1);
        assert_eq!(telemetry.poll.skipped_ticks, 3);
        assert_eq!(telemetry.poll.duration.unwrap().max_ms, 400);
    }

    /// Deterministic deployments put the Raindex orderbook at the same address
    /// on several chains, and poll samples are keyed by orderbook alone. Poll
    /// health must therefore count each cycle once however many hedged chains
    /// name that address -- iterating chains instead of distinct orderbooks
    /// would double every figure in the report.
    #[tokio::test]
    async fn poll_health_counts_a_shared_orderbooks_cycles_once() {
        let pool = setup_test_db().await;
        let mut chains = base_only();
        chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::Ethereum)
                .orderbook(ORDERBOOK)
                .call(),
        );
        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            Chain::Base,
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
            Chain::Ethereum,
            ORDERBOOK,
            timestamp(20),
            StdDuration::from_millis(400),
            3,
            Err(&"rpc unreachable"),
        )
        .await
        .unwrap();

        let telemetry = load_monitor_telemetry(&pool, &range(), &chains)
            .await
            .unwrap();

        assert_eq!(telemetry.poll.cycles, 2);
        assert_eq!(telemetry.poll.errors, 1);
        assert_eq!(telemetry.poll.skipped_ticks, 3);
        let duration = telemetry.poll.duration.unwrap();
        assert_eq!(duration.sample_count, 2);
        assert_eq!(duration.max_ms, 400);
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

        let telemetry = load_monitor_telemetry(&pool, &range(), &base_only())
            .await
            .unwrap();

        assert_eq!(base_series(&telemetry).current_lag_blocks, None);
        assert_eq!(base_series(&telemetry).points, vec![]);
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
