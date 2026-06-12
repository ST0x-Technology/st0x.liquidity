//! Reliability read models: lifecycle failure events, log volume
//! aggregation, and job queue health.
//!
//! Distinguishes money-at-risk failures (failed hedges, failed rebalance
//! stages) recorded as CQRS events from cosmetic log noise, and surfaces
//! apalis queue backlogs and retry pressure. Strictly read-only.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::{
    CountedLogLevel, FailureEventCount, FailureEventType, JobQueueHealth, LogTargetCount,
    LogVolumeBucket,
};

use super::{PerformanceError, ReportRange};
use crate::equity_redemption::EquityRedemptionEvent;
use crate::offchain::order::OffchainOrderEvent;
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
use crate::usdc_rebalance::UsdcRebalanceEvent;

/// Most recent failure rows scanned per report. Failure events are rare in a
/// healthy system; the cap bounds the endpoint's cost when they are not.
const MAX_FAILURE_EVENT_ROWS: i64 = 10_000;

/// Count lifecycle failure events within `range`, newest category first.
pub(crate) async fn load_failure_events(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<Vec<FailureEventCount>, PerformanceError> {
    let placeholders = (1..=FailureEventType::ALL.len())
        .map(|position| format!("${position}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT event_type, payload FROM events \
         WHERE event_type IN ({placeholders}) \
         ORDER BY rowid DESC LIMIT {MAX_FAILURE_EVENT_ROWS}"
    );

    let mut query = sqlx::query_as::<_, (String, String)>(&sql);
    for event_type in FailureEventType::ALL {
        query = query.bind(event_type.as_str());
    }
    let rows = query.fetch_all(pool).await?;

    let mut counts: BTreeMap<FailureEventType, FailureEventCount> = BTreeMap::new();
    for (raw_event_type, payload) in rows {
        let Ok(event_type) = FailureEventType::from_str(&raw_event_type) else {
            warn!(%raw_event_type, "Skipping unrecognized failure event type");
            continue;
        };
        let Some(occurred_at) = failure_timestamp(event_type, &payload) else {
            warn!(
                event_type = event_type.as_str(),
                "Skipping failure event with undeserializable payload"
            );
            continue;
        };
        if !range.contains(occurred_at) {
            continue;
        }

        counts
            .entry(event_type)
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
    // Tie-break equal timestamps by discriminator string so the order
    // matches the pre-enum (string-keyed) implementation byte for byte.
    failure_events.sort_by(|left, right| {
        right
            .last_at
            .cmp(&left.last_at)
            .then_with(|| left.event_type.as_str().cmp(right.event_type.as_str()))
    });
    Ok(failure_events)
}

fn failure_timestamp(event_type: FailureEventType, payload: &str) -> Option<DateTime<Utc>> {
    use FailureEventType::{
        AttestationTimedOut, BridgingFailed, ConversionFailed, DepositFailed, MintAcceptanceFailed,
        MintRejected, OffchainOrderFailed, RaindexDepositFailed, RedemptionDetectionFailed,
        RedemptionRejected, RedemptionTransferFailed, WithdrawalFailed, WrappingFailed,
    };

    match event_type {
        OffchainOrderFailed => {
            match serde_json::from_str(payload).ok()? {
                OffchainOrderEvent::Failed { failed_at, .. } => Some(failed_at),
                // Non-failure variants cannot appear under this failure
                // discriminator; listed explicitly (not `_`) so the
                // compiler flags new domain variants for triage here.
                OffchainOrderEvent::Placed { .. }
                | OffchainOrderEvent::Submitted { .. }
                | OffchainOrderEvent::PartiallyFilled { .. }
                | OffchainOrderEvent::Filled { .. } => None,
            }
        }
        ConversionFailed | WithdrawalFailed | BridgingFailed | DepositFailed
        | AttestationTimedOut => {
            match serde_json::from_str(payload).ok()? {
                UsdcRebalanceEvent::ConversionFailed { failed_at, .. }
                | UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. }
                | UsdcRebalanceEvent::BridgingFailed { failed_at, .. }
                | UsdcRebalanceEvent::DepositFailed { failed_at, .. } => Some(failed_at),
                UsdcRebalanceEvent::AttestationTimedOut { timed_out_at, .. } => Some(timed_out_at),
                // Same compile-time triage guard as below.
                UsdcRebalanceEvent::ConversionInitiated { .. }
                | UsdcRebalanceEvent::ConversionConfirmed { .. }
                | UsdcRebalanceEvent::Initiated { .. }
                | UsdcRebalanceEvent::WithdrawalSubmitting { .. }
                | UsdcRebalanceEvent::WithdrawalConfirmed { .. }
                | UsdcRebalanceEvent::BridgingSubmitting { .. }
                | UsdcRebalanceEvent::BridgingInitiated { .. }
                | UsdcRebalanceEvent::BridgeAttestationReceived { .. }
                | UsdcRebalanceEvent::Bridged { .. }
                | UsdcRebalanceEvent::BridgingCompletionRecovered { .. }
                | UsdcRebalanceEvent::DepositInitiated { .. }
                | UsdcRebalanceEvent::DepositConfirmed { .. }
                | UsdcRebalanceEvent::OperatorReconciled { .. } => None,
            }
        }
        RedemptionTransferFailed | RedemptionDetectionFailed | RedemptionRejected => {
            match serde_json::from_str(payload).ok()? {
                EquityRedemptionEvent::TransferFailed { failed_at, .. }
                | EquityRedemptionEvent::DetectionFailed { failed_at, .. } => Some(failed_at),
                EquityRedemptionEvent::RedemptionRejected { rejected_at, .. } => Some(rejected_at),
                // Non-failure variants cannot appear under these failure
                // discriminators; listed explicitly (not `_`) so the
                // compiler flags new domain variants for triage here.
                EquityRedemptionEvent::VaultWithdrawPending { .. }
                | EquityRedemptionEvent::VaultWithdrawSubmitted { .. }
                | EquityRedemptionEvent::WithdrawnFromRaindex { .. }
                | EquityRedemptionEvent::TokensUnwrapped { .. }
                | EquityRedemptionEvent::UnwrapPending { .. }
                | EquityRedemptionEvent::UnwrapSubmitted { .. }
                | EquityRedemptionEvent::SendPending { .. }
                | EquityRedemptionEvent::TokensSent { .. }
                | EquityRedemptionEvent::Detected { .. }
                | EquityRedemptionEvent::Completed { .. }
                | EquityRedemptionEvent::ProviderCompletionRecovered { .. } => None,
            }
        }
        MintRejected | MintAcceptanceFailed | WrappingFailed | RaindexDepositFailed => {
            match serde_json::from_str(payload).ok()? {
                TokenizedEquityMintEvent::MintRejected { rejected_at, .. } => Some(rejected_at),
                TokenizedEquityMintEvent::MintAcceptanceFailed { failed_at, .. }
                | TokenizedEquityMintEvent::WrappingFailed { failed_at, .. }
                | TokenizedEquityMintEvent::RaindexDepositFailed { failed_at, .. } => {
                    Some(failed_at)
                }
                // Same compile-time triage guard as above.
                TokenizedEquityMintEvent::MintRequested { .. }
                | TokenizedEquityMintEvent::MintAccepted { .. }
                | TokenizedEquityMintEvent::TokensReceived { .. }
                | TokenizedEquityMintEvent::WrapSubmitted { .. }
                | TokenizedEquityMintEvent::TokensWrapped { .. }
                | TokenizedEquityMintEvent::VaultDepositSubmitted { .. }
                | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
                | TokenizedEquityMintEvent::ProviderCompletionRecovered { .. } => None,
            }
        }
    }
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
    let mut targets: BTreeMap<(String, CountedLogLevel), BTreeMap<i64, usize>> = BTreeMap::new();

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

        let level = match entry["level"].as_str() {
            Some(raw_level) if raw_level.eq_ignore_ascii_case("ERROR") => CountedLogLevel::Error,
            Some(raw_level) if raw_level.eq_ignore_ascii_case("WARN") => CountedLogLevel::Warn,
            _ => continue,
        };
        let target = entry["target"].as_str().unwrap_or("unknown").to_string();

        let index = (timestamp - range.from).num_seconds() / width.num_seconds();
        match level {
            CountedLogLevel::Error => buckets.entry(index).or_default().0 += 1,
            CountedLogLevel::Warn => buckets.entry(index).or_default().1 += 1,
        }

        *targets
            .entry((target, level))
            .or_default()
            .entry(index)
            .or_default() += 1;
    }

    let bucket_indices: Vec<i64> = buckets.keys().copied().collect();

    let log_buckets = buckets
        .iter()
        .map(|(&index, &(errors, warnings))| LogVolumeBucket {
            start: range.from + chrono::Duration::seconds(width.num_seconds() * index),
            errors,
            warnings,
        })
        .collect();

    let mut log_targets: Vec<LogTargetCount> = targets
        .into_iter()
        .map(|((target, level), per_bucket)| {
            let sparkline: Vec<usize> = bucket_indices
                .iter()
                .map(|index| per_bucket.get(index).copied().unwrap_or_default())
                .collect();

            LogTargetCount {
                target,
                level,
                count: per_bucket.values().sum(),
                sparkline,
            }
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
    use std::collections::BTreeSet;

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
                level: CountedLogLevel::Error,
                count: 2,
                sparkline: vec![2],
            }
        );
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn aggregate_log_entries_aligns_sparklines_with_buckets() {
        let day = 86_400;
        let wide_range = ReportRange {
            from: timestamp(0),
            to: timestamp(10 * day),
        };
        let entries = vec![
            log_entry(10, "ERROR", "hedge"),
            log_entry(3 * day, "WARN", "rebalance"),
            log_entry(3 * day + 5, "ERROR", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &wide_range);

        assert_eq!(buckets.len(), 2);
        let hedge_errors = targets
            .iter()
            .find(|target| target.target == "hedge")
            .unwrap();
        assert_eq!(hedge_errors.sparkline, vec![1, 1]);

        let rebalance_warnings = targets
            .iter()
            .find(|target| target.target == "rebalance")
            .unwrap();
        assert_eq!(rebalance_warnings.sparkline, vec![0, 1]);
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
        assert_eq!(failures[0].event_type, FailureEventType::WithdrawalFailed);
        assert_eq!(failures[0].count, 1);
        assert_eq!(failures[0].last_at, timestamp(200));
        assert_eq!(
            failures[1].event_type,
            FailureEventType::OffchainOrderFailed
        );
        assert_eq!(failures[1].count, 1);
    }

    #[tokio::test]
    async fn load_failure_events_accumulates_counts_and_latest_timestamp() {
        let pool = setup_test_db().await;

        for (sequence, offset) in [(1_i64, 300_i64), (2, 100)] {
            insert_event(
                &pool,
                "OffchainOrder",
                &format!("order-{sequence}"),
                "OffchainOrderEvent::Failed",
                json!({"Failed": {
                    "error": "rejected",
                    "failed_at": timestamp(offset).to_rfc3339(),
                }}),
            )
            .await;
        }

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].count, 2);
        // last_at must be the most recent occurrence, regardless of the
        // order the rows were scanned in.
        assert_eq!(failures[0].last_at, timestamp(300));
    }

    #[tokio::test]
    async fn load_failure_events_extracts_rebalance_timestamps_per_variant() {
        let pool = setup_test_db().await;

        insert_event(
            &pool,
            "UsdcRebalance",
            "op-1",
            "UsdcRebalanceEvent::ConversionFailed",
            json!({"ConversionFailed": {
                "reason": "rejected",
                "failed_at": timestamp(100).to_rfc3339(),
            }}),
        )
        .await;
        // AttestationTimedOut carries timed_out_at, not failed_at: the
        // riskiest extraction path in the grouped rebalance arm.
        insert_event(
            &pool,
            "UsdcRebalance",
            "op-2",
            "UsdcRebalanceEvent::AttestationTimedOut",
            json!({"AttestationTimedOut": {
                "burn_tx_hash":
                    "0x1111111111111111111111111111111111111111111111111111111111111111",
                "retry_deadline_at": timestamp(50).to_rfc3339(),
                "timed_out_at": timestamp(200).to_rfc3339(),
            }}),
        )
        .await;

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 2);
        assert_eq!(
            failures[0].event_type,
            FailureEventType::AttestationTimedOut
        );
        assert_eq!(failures[0].last_at, timestamp(200));
        assert_eq!(failures[1].event_type, FailureEventType::ConversionFailed);
        assert_eq!(failures[1].last_at, timestamp(100));
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
    fn aggregate_log_entries_accepts_case_variant_levels() {
        let entries = vec![
            log_entry(10, "error", "hedge"),
            log_entry(20, "Warn", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &range());

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].errors, 1);
        assert_eq!(buckets[0].warnings, 1);
        assert_eq!(targets.len(), 2);
        // Pin the classification itself, not just the totals: a swapped
        // mapping would keep the counts at 1/1.
        let error_target = targets
            .iter()
            .find(|target| target.level == CountedLogLevel::Error)
            .unwrap();
        assert_eq!(error_target.count, 1);
        let warn_target = targets
            .iter()
            .find(|target| target.level == CountedLogLevel::Warn)
            .unwrap();
        assert_eq!(warn_target.count, 1);
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
        use alloy::primitives::TxHash;
        use st0x_event_sorcery::DomainEvent;
        use st0x_execution::Symbol;
        use st0x_float_macro::float;

        use crate::equity_redemption::DetectionFailure;

        // One constructed domain event per FailureEventType variant: this is
        // the authoritative binding between the DTO enum and the event
        // store's discriminator strings.
        let samples: Vec<(FailureEventType, String)> = vec![
            (
                FailureEventType::OffchainOrderFailed,
                OffchainOrderEvent::Failed {
                    error: "rejected".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::ConversionFailed,
                UsdcRebalanceEvent::ConversionFailed {
                    reason: "rejected".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::WithdrawalFailed,
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "revert".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::BridgingFailed,
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: None,
                    cctp_nonce: None,
                    reason: "revert".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::DepositFailed,
                UsdcRebalanceEvent::DepositFailed {
                    deposit_ref: None,
                    reason: "revert".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::AttestationTimedOut,
                UsdcRebalanceEvent::AttestationTimedOut {
                    burn_tx_hash: TxHash::random(),
                    retry_deadline_at: timestamp(0),
                    timed_out_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::RedemptionTransferFailed,
                EquityRedemptionEvent::TransferFailed {
                    tx_hash: None,
                    reason: None,
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::RedemptionDetectionFailed,
                EquityRedemptionEvent::DetectionFailed {
                    failure: DetectionFailure::Timeout,
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::RedemptionRejected,
                EquityRedemptionEvent::RedemptionRejected {
                    reason: "rejected".to_string(),
                    rejected_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::MintRejected,
                TokenizedEquityMintEvent::MintRejected {
                    reason: "rejected".to_string(),
                    rejected_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::MintAcceptanceFailed,
                TokenizedEquityMintEvent::MintAcceptanceFailed {
                    reason: "rejected".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::WrappingFailed,
                TokenizedEquityMintEvent::WrappingFailed {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(1),
                    reason: None,
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
            (
                FailureEventType::RaindexDepositFailed,
                TokenizedEquityMintEvent::RaindexDepositFailed {
                    reason: "revert".to_string(),
                    failed_at: timestamp(0),
                }
                .event_type(),
            ),
        ];

        let sampled_types: BTreeSet<FailureEventType> = samples
            .iter()
            .map(|(failure_type, _)| *failure_type)
            .collect();
        let all: BTreeSet<FailureEventType> = FailureEventType::ALL.iter().copied().collect();
        assert_eq!(
            sampled_types, all,
            "every failure event type needs exactly one domain sample here"
        );

        for (failure_type, domain_name) in samples {
            assert_eq!(
                failure_type.as_str(),
                domain_name,
                "{failure_type:?} drifted from the domain event's event_type()"
            );
            assert_eq!(
                FailureEventType::from_str(&domain_name).unwrap(),
                failure_type
            );
        }
    }
}
