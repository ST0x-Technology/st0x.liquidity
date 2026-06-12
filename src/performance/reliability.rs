//! Reliability read models: lifecycle failure events, log volume
//! aggregation, and job queue health.
//!
//! Distinguishes money-at-risk failures (failed hedges, failed rebalance
//! stages) recorded as CQRS events from cosmetic log noise, and surfaces
//! apalis queue backlogs and retry pressure. Strictly read-only.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::{FailureEventCount, JobQueueHealth, LogTargetCount, LogVolumeBucket};

use super::rebalance::event_timestamp;
use super::{PerformanceError, ReportRange};
use crate::equity_redemption::EquityRedemptionEvent;
use crate::offchain::order::OffchainOrderEvent;
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
use crate::usdc_rebalance::UsdcRebalanceEvent;

/// Lifecycle failure event types treated as money-at-risk signals.
const FAILURE_EVENT_TYPES: [&str; 13] = [
    "OffchainOrderEvent::Failed",
    "UsdcRebalanceEvent::ConversionFailed",
    "UsdcRebalanceEvent::WithdrawalFailed",
    "UsdcRebalanceEvent::BridgingFailed",
    "UsdcRebalanceEvent::DepositFailed",
    "UsdcRebalanceEvent::AttestationTimedOut",
    "EquityRedemptionEvent::TransferFailed",
    "EquityRedemptionEvent::DetectionFailed",
    "EquityRedemptionEvent::RedemptionRejected",
    "TokenizedEquityMintEvent::MintRejected",
    "TokenizedEquityMintEvent::MintAcceptanceFailed",
    "TokenizedEquityMintEvent::WrappingFailed",
    "TokenizedEquityMintEvent::RaindexDepositFailed",
];

/// Most recent failure rows scanned per report. Failure events are rare in a
/// healthy system; the cap bounds the endpoint's cost when they are not.
const MAX_FAILURE_EVENT_ROWS: i64 = 10_000;

/// Count lifecycle failure events within `range`, newest category first.
pub(crate) async fn load_failure_events(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<Vec<FailureEventCount>, PerformanceError> {
    let placeholders = (1..=FAILURE_EVENT_TYPES.len())
        .map(|position| format!("${position}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT event_type, payload FROM events \
         WHERE event_type IN ({placeholders}) \
         ORDER BY rowid DESC LIMIT {MAX_FAILURE_EVENT_ROWS}"
    );

    let mut query = sqlx::query_as::<_, (String, String)>(&sql);
    for event_type in FAILURE_EVENT_TYPES {
        query = query.bind(event_type);
    }
    let rows = query.fetch_all(pool).await?;

    let mut counts: BTreeMap<String, FailureEventCount> = BTreeMap::new();
    for (event_type, payload) in rows {
        let Some(occurred_at) = failure_timestamp(&event_type, &payload) else {
            warn!(%event_type, "Skipping failure event with undeserializable payload");
            continue;
        };
        if !range.contains(occurred_at) {
            continue;
        }

        counts
            .entry(event_type.clone())
            .and_modify(|entry| {
                entry.count += 1;
                entry.last_at = entry.last_at.max(occurred_at);
            })
            .or_insert(FailureEventCount {
                event_type,
                count: 1,
                last_at: occurred_at,
            });
    }

    let mut failure_events: Vec<FailureEventCount> = counts.into_values().collect();
    failure_events.sort_by(|left, right| right.last_at.cmp(&left.last_at));
    Ok(failure_events)
}

fn failure_timestamp(event_type: &str, payload: &str) -> Option<DateTime<Utc>> {
    if event_type == "OffchainOrderEvent::Failed" {
        let OffchainOrderEvent::Failed { failed_at, .. } = serde_json::from_str(payload).ok()?
        else {
            return None;
        };
        return Some(failed_at);
    }

    if event_type.starts_with("EquityRedemptionEvent::") {
        return match serde_json::from_str(payload).ok()? {
            EquityRedemptionEvent::TransferFailed { failed_at, .. }
            | EquityRedemptionEvent::DetectionFailed { failed_at, .. } => Some(failed_at),
            EquityRedemptionEvent::RedemptionRejected { rejected_at, .. } => Some(rejected_at),
            _ => None,
        };
    }

    if event_type.starts_with("TokenizedEquityMintEvent::") {
        return match serde_json::from_str(payload).ok()? {
            TokenizedEquityMintEvent::MintRejected { rejected_at, .. } => Some(rejected_at),
            TokenizedEquityMintEvent::MintAcceptanceFailed { failed_at, .. }
            | TokenizedEquityMintEvent::WrappingFailed { failed_at, .. }
            | TokenizedEquityMintEvent::RaindexDepositFailed { failed_at, .. } => Some(failed_at),
            _ => None,
        };
    }

    let event: UsdcRebalanceEvent = serde_json::from_str(payload).ok()?;
    Some(event_timestamp(&event))
}

/// Aggregate already-filtered ERROR/WARN log entries into time buckets and
/// per-target counts.
///
/// Entries with missing or malformed timestamps are skipped; counts come
/// from the dashboard's own log files, so a skipped line only understates
/// noise, never money-at-risk failures.
pub(crate) fn aggregate_log_entries(
    entries: &[serde_json::Value],
    range: &ReportRange,
) -> (Vec<LogVolumeBucket>, Vec<LogTargetCount>) {
    let width = range.bucket_width();
    let mut buckets: BTreeMap<i64, (usize, usize)> = BTreeMap::new();
    let mut targets: BTreeMap<(String, String), usize> = BTreeMap::new();

    for entry in entries {
        let Some(timestamp) = entry["timestamp"]
            .as_str()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|parsed| parsed.with_timezone(&Utc))
        else {
            continue;
        };
        if !range.contains(timestamp) {
            continue;
        }

        let level = entry["level"].as_str().unwrap_or("UNKNOWN").to_uppercase();
        let target = entry["target"].as_str().unwrap_or("unknown").to_string();

        let index = (timestamp - range.from).num_seconds() / width.num_seconds();
        match level.as_str() {
            "ERROR" => buckets.entry(index).or_default().0 += 1,
            "WARN" => buckets.entry(index).or_default().1 += 1,
            _ => continue,
        }

        *targets.entry((target, level)).or_default() += 1;
    }

    let log_buckets = buckets
        .into_iter()
        .map(|(index, (errors, warnings))| LogVolumeBucket {
            start: range.from + chrono::Duration::seconds(width.num_seconds() * index),
            errors,
            warnings,
        })
        .collect();

    let mut log_targets: Vec<LogTargetCount> = targets
        .into_iter()
        .map(|((target, level), count)| LogTargetCount {
            target,
            level,
            count,
        })
        .collect();
    log_targets.sort_by(|left, right| right.count.cmp(&left.count));

    (log_buckets, log_targets)
}

/// One aggregated apalis job-queue row: job type, status counts (pending,
/// running, done, failed, awaiting retry, killed, retried), and the oldest
/// live-backlog `run_at`.
type JobQueueRow = (String, i64, i64, i64, i64, i64, i64, i64, Option<i64>);

/// Current health of every apalis job queue.
pub(crate) async fn load_job_queue_health(
    pool: &SqlitePool,
) -> Result<Vec<JobQueueHealth>, PerformanceError> {
    // apalis leaves retry-eligible jobs in status 'Failed' with
    // attempts < max_attempts and re-fetches them; they are live backlog,
    // not terminal failures (those become 'Killed').
    let rows: Vec<JobQueueRow> = sqlx::query_as(
        "SELECT job_type, \
            SUM(CASE WHEN status IN ('Pending', 'Queued') THEN 1 ELSE 0 END), \
            SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN status = 'Failed' AND attempts < max_attempts \
                THEN 1 ELSE 0 END), \
            SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END), \
            MIN(CASE WHEN status IN ('Pending', 'Queued') \
                OR (status = 'Failed' AND attempts < max_attempts) \
                THEN run_at ELSE NULL END) \
         FROM Jobs \
         GROUP BY job_type \
         ORDER BY job_type",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                job_type,
                pending,
                running,
                done,
                failed,
                awaiting_retry,
                killed,
                retried,
                oldest_run_at,
            )| {
                JobQueueHealth {
                    job_type,
                    pending: count(pending),
                    running: count(running),
                    done: count(done),
                    failed: count(failed),
                    awaiting_retry: count(awaiting_retry),
                    killed: count(killed),
                    retried: count(retried),
                    oldest_pending_run_at: oldest_run_at
                        .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
                }
            },
        )
        .collect())
}

/// SQLite SUM/COUNT results are non-negative; a negative value would mean a
/// corrupted query, so it is logged loudly before falling back to zero.
fn count(value: i64) -> usize {
    usize::try_from(value).unwrap_or_else(|_| {
        warn!(
            value,
            "Negative count from job queue aggregation; reporting zero"
        );
        0
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use crate::conductor::setup_apalis_tables;
    use crate::test_utils::setup_test_db;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    fn range() -> ReportRange {
        ReportRange {
            from: timestamp(0),
            to: timestamp(86_400),
        }
    }

    fn log_entry(offset: i64, level: &str, target: &str) -> serde_json::Value {
        json!({
            "timestamp": timestamp(offset).to_rfc3339(),
            "level": level,
            "target": target,
            "message": "boom",
        })
    }

    #[test]
    fn aggregate_log_entries_buckets_and_counts_targets() {
        let entries = vec![
            log_entry(10, "ERROR", "hedge"),
            log_entry(20, "WARN", "hedge"),
            log_entry(30, "ERROR", "rebalance"),
            log_entry(40, "ERROR", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &range());

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].start, timestamp(0));
        assert_eq!(buckets[0].errors, 3);
        assert_eq!(buckets[0].warnings, 1);

        assert_eq!(
            targets[0],
            LogTargetCount {
                target: "hedge".to_string(),
                level: "ERROR".to_string(),
                count: 2,
            }
        );
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn aggregate_log_entries_skips_malformed_and_out_of_range() {
        let entries = vec![
            json!({"level": "ERROR", "target": "hedge", "message": "no timestamp"}),
            log_entry(-100, "ERROR", "hedge"),
            log_entry(10, "ERROR", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &range());

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].errors, 1);
        assert_eq!(targets.len(), 1);
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, \
              payload, metadata) \
             VALUES ($1, $2, 1, $3, '1.0', $4, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(event_type)
        .bind(payload.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_failure_events_counts_by_type_within_range() {
        let pool = setup_test_db().await;

        insert_event(
            &pool,
            "OffchainOrder",
            "order-1",
            "OffchainOrderEvent::Failed",
            json!({"Failed": {"error": "rejected", "failed_at": timestamp(100).to_rfc3339()}}),
        )
        .await;
        insert_event(
            &pool,
            "UsdcRebalance",
            "op-1",
            "UsdcRebalanceEvent::WithdrawalFailed",
            json!({"WithdrawalFailed": {
                "reason": "revert",
                "failed_at": timestamp(200).to_rfc3339(),
            }}),
        )
        .await;
        // Outside the range: must not be counted.
        insert_event(
            &pool,
            "OffchainOrder",
            "order-2",
            "OffchainOrderEvent::Failed",
            json!({"Failed": {
                "error": "rejected",
                "failed_at": timestamp(-999_999).to_rfc3339(),
            }}),
        )
        .await;

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 2);
        assert_eq!(
            failures[0].event_type,
            "UsdcRebalanceEvent::WithdrawalFailed"
        );
        assert_eq!(failures[0].count, 1);
        assert_eq!(failures[0].last_at, timestamp(200));
        assert_eq!(failures[1].event_type, "OffchainOrderEvent::Failed");
        assert_eq!(failures[1].count, 1);
    }

    #[tokio::test]
    async fn load_job_queue_health_aggregates_per_job_type() {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        // (id, job_type, status, attempts, max_attempts, run_at)
        let jobs = [
            ("a-1", "queue::A", "Pending", 0, 25, 1_750_000_100_i64),
            ("a-2", "queue::A", "Queued", 0, 25, 1_750_000_080),
            ("a-3", "queue::A", "Running", 1, 25, 1_750_000_050),
            // Retry-eligible failure: live backlog, not terminal.
            ("a-4", "queue::A", "Failed", 3, 25, 1_750_000_000),
            // Exhausted failure: terminal, excluded from backlog metrics.
            ("a-5", "queue::A", "Failed", 25, 25, 1_749_999_000),
            ("b-1", "queue::B", "Done", 2, 25, 1_750_000_000),
        ];
        for (id, job_type, status, attempts, max_attempts, run_at) in jobs {
            sqlx::query(
                "INSERT INTO Jobs \
                 (job, id, job_type, status, attempts, max_attempts, run_at) \
                 VALUES ('{}', $1, $2, $3, $4, $5, $6)",
            )
            .bind(id)
            .bind(job_type)
            .bind(status)
            .bind(attempts)
            .bind(max_attempts)
            .bind(run_at)
            .execute(&pool)
            .await
            .unwrap();
        }

        let queues = load_job_queue_health(&pool).await.unwrap();

        assert_eq!(queues.len(), 2);
        let queue_a = &queues[0];
        assert_eq!(queue_a.job_type, "queue::A");
        assert_eq!(queue_a.pending, 2);
        assert_eq!(queue_a.running, 1);
        assert_eq!(queue_a.failed, 2);
        assert_eq!(queue_a.awaiting_retry, 1);
        assert_eq!(queue_a.retried, 2);
        // The retry-eligible failure is the oldest live backlog entry; the
        // exhausted one must not count.
        assert_eq!(
            queue_a.oldest_pending_run_at,
            DateTime::from_timestamp(1_750_000_000, 0)
        );

        let queue_b = &queues[1];
        assert_eq!(queue_b.done, 1);
        assert_eq!(queue_b.retried, 1);
        assert_eq!(queue_b.awaiting_retry, 0);
        assert_eq!(queue_b.oldest_pending_run_at, None);
    }

    #[test]
    fn aggregate_log_entries_distributes_multi_day_buckets() {
        let day = 86_400;
        let wide_range = ReportRange {
            from: timestamp(0),
            to: timestamp(7 * day),
        };
        let entries = vec![
            log_entry(10, "ERROR", "hedge"),
            log_entry(3 * day + 5, "WARN", "rebalance"),
        ];

        let (buckets, _) = aggregate_log_entries(&entries, &wide_range);

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].start, timestamp(0));
        assert_eq!(buckets[0].errors, 1);
        assert_eq!(buckets[1].start, timestamp(3 * day));
        assert_eq!(buckets[1].warnings, 1);
    }

    #[test]
    fn aggregate_log_entries_ignores_unexpected_levels() {
        let entries = vec![
            log_entry(10, "INFO", "hedge"),
            log_entry(20, "ERROR", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &range());

        // The INFO entry must not create a phantom empty bucket or target.
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].errors, 1);
        assert_eq!(buckets[0].warnings, 0);
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn failure_event_types_match_domain_event_names() {
        use st0x_event_sorcery::DomainEvent;

        let samples: Vec<(&str, String)> = vec![
            (
                "OffchainOrderEvent::Failed",
                OffchainOrderEvent::Failed {
                    error: "rejected".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                "UsdcRebalanceEvent::WithdrawalFailed",
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "revert".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                "EquityRedemptionEvent::RedemptionRejected",
                EquityRedemptionEvent::RedemptionRejected {
                    reason: "rejected".to_string(),
                    rejected_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                "TokenizedEquityMintEvent::MintRejected",
                TokenizedEquityMintEvent::MintRejected {
                    reason: "rejected".to_string(),
                    rejected_at: timestamp(0),
                }
                .event_type(),
            ),
        ];

        for (expected, actual) in samples {
            assert_eq!(actual, expected);
            assert!(
                FAILURE_EVENT_TYPES.contains(&expected),
                "{expected} missing from FAILURE_EVENT_TYPES"
            );
        }
    }
}
