//! Reliability read models: lifecycle failure events, log volume
//! aggregation, and job queue health.
//!
//! Distinguishes money-at-risk failures (failed hedges, failed rebalance
//! stages) recorded as CQRS events from cosmetic log noise, and surfaces
//! apalis queue backlogs and retry pressure.
//!
//! Lifecycle failure events are maintained forward-only by
//! [`LifecycleFailureProjection`], which subscribes to the `OffchainOrder`,
//! `UsdcRebalance`, `EquityRedemption`, and `TokenizedEquityMint` event
//! streams and records one row per failure event into `lifecycle_failure_event`.
//! [`load_failure_events`] reads ONLY that table and never folds the `events`
//! table. The log-aggregation and job-queue-health read paths read their own
//! sources (structured logs, the apalis `Jobs` table) and are left untouched.
//!
//! Forward-only: the reactor processes only events emitted after construction.
//! There is NO startup backfill of pre-existing history. A single event is
//! dropped from the read model if it cannot be persisted -- whether a crash
//! between the source event committing and this reactor's INSERT, or the INSERT
//! itself erroring (contention, disk full). The drop is logged at error level
//! with the event's type and timestamp so an operator can reconcile from the
//! event log. It is deliberately NOT retried: `lifecycle_failure_event` has no
//! idempotency key, so a retry after a partially-applied write would
//! double-count a money-at-risk failure. This best-effort guarantee matches the
//! `Broadcaster` reactor.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use thiserror::Error;
use tracing::error;

use st0x_dto::{FailureEventCount, JobQueueHealth, LogTargetCount, LogVolumeBucket};
use st0x_event_sorcery::{EntityList, Reactor, deps};

use super::{PerformanceError, ReportRange};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent};
use crate::offchain::order::{OffchainOrder, OffchainOrderEvent};
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceEvent};

/// Count lifecycle failure events within `range`, newest category first.
///
/// Reads ONLY the reactor-maintained `lifecycle_failure_event` table,
/// aggregating per `event_type` in SQL (index-backed on `occurred_at`). Because
/// the grouping happens in the database there is no row cap, so the per-type
/// counts stay exact even during a failure storm with millions of rows in
/// range -- the view this serves is money-at-risk, so it must not undercount.
///
/// `occurred_at` is always written by [`LifecycleFailureProjection::record`] as
/// valid RFC 3339, so an unparseable `MAX(occurred_at)` means data corruption
/// and fails the report loudly rather than silently dropping a category.
pub(crate) async fn load_failure_events(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<Vec<FailureEventCount>, PerformanceError> {
    let rows: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT event_type, COUNT(*) AS count, MAX(occurred_at) AS last_at \
         FROM lifecycle_failure_event \
         WHERE occurred_at >= ? AND occurred_at <= ? \
         GROUP BY event_type",
    )
    .bind(range.from.to_rfc3339())
    .bind(range.to.to_rfc3339())
    .fetch_all(pool)
    .await?;

    let mut failure_events = rows
        .into_iter()
        .map(|(event_type, occurrences, last_at)| {
            Ok(FailureEventCount {
                event_type,
                count: count(occurrences)?,
                last_at: DateTime::parse_from_rfc3339(&last_at)?.with_timezone(&Utc),
            })
        })
        .collect::<Result<Vec<_>, PerformanceError>>()?;

    failure_events.sort_by(|left, right| right.last_at.cmp(&left.last_at));
    Ok(failure_events)
}

/// Reactor maintaining the lifecycle-failure read model from live events.
pub(crate) struct LifecycleFailureProjection {
    pool: SqlitePool,
}

deps!(
    LifecycleFailureProjection,
    [
        OffchainOrder,
        UsdcRebalance,
        EquityRedemption,
        TokenizedEquityMint,
    ]
);

impl LifecycleFailureProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Record a money-at-risk failure event. Non-failure events map to `None`
    /// and are ignored.
    async fn record(
        &self,
        failure: Option<(&'static str, DateTime<Utc>)>,
    ) -> Result<(), FailureProjectionError> {
        let Some((event_type, occurred_at)) = failure else {
            return Ok(());
        };
        sqlx::query("INSERT INTO lifecycle_failure_event (event_type, occurred_at) VALUES (?, ?)")
            .bind(event_type)
            .bind(occurred_at.to_rfc3339())
            .execute(&self.pool)
            .await
            .inspect_err(|error| {
                error!(
                    ?error,
                    event_type,
                    %occurred_at,
                    "failed to persist lifecycle failure event; this money-at-risk \
                     failure is dropped from the reliability read model (the source \
                     event remains in the event log for reconciliation)"
                );
            })?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum FailureProjectionError {
    #[error("lifecycle-failure read-model write failed")]
    Database(#[from] sqlx::Error),
}

#[async_trait]
impl Reactor for LifecycleFailureProjection {
    type Error = FailureProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|_id, event| async move { self.record(offchain_order_failure(&event)).await })
            .on(|_id, event| async move { self.record(usdc_rebalance_failure(&event)).await })
            .on(|_id, event| async move { self.record(equity_redemption_failure(&event)).await })
            .on(|_id, event| async move { self.record(tokenized_equity_mint_failure(&event)).await })
            .exhaustive()
            .await
    }
}

/// Failure signal carried by an `OffchainOrder` event, if any.
fn offchain_order_failure(event: &OffchainOrderEvent) -> Option<(&'static str, DateTime<Utc>)> {
    match event {
        OffchainOrderEvent::Failed { failed_at, .. } => {
            Some(("OffchainOrderEvent::Failed", *failed_at))
        }
        OffchainOrderEvent::Placed { .. }
        | OffchainOrderEvent::Submitted { .. }
        | OffchainOrderEvent::PartiallyFilled { .. }
        | OffchainOrderEvent::Filled { .. } => None,
    }
}

/// Failure signal carried by a `UsdcRebalance` event, if any.
fn usdc_rebalance_failure(event: &UsdcRebalanceEvent) -> Option<(&'static str, DateTime<Utc>)> {
    match event {
        UsdcRebalanceEvent::ConversionFailed { failed_at, .. } => {
            Some(("UsdcRebalanceEvent::ConversionFailed", *failed_at))
        }
        UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. } => {
            Some(("UsdcRebalanceEvent::WithdrawalFailed", *failed_at))
        }
        UsdcRebalanceEvent::BridgingFailed { failed_at, .. } => {
            Some(("UsdcRebalanceEvent::BridgingFailed", *failed_at))
        }
        UsdcRebalanceEvent::DepositFailed { failed_at, .. } => {
            Some(("UsdcRebalanceEvent::DepositFailed", *failed_at))
        }
        UsdcRebalanceEvent::AttestationTimedOut { timed_out_at, .. } => {
            Some(("UsdcRebalanceEvent::AttestationTimedOut", *timed_out_at))
        }
        UsdcRebalanceEvent::ConversionInitiated { .. }
        | UsdcRebalanceEvent::ConversionConfirmed { .. }
        | UsdcRebalanceEvent::WithdrawalSubmitting { .. }
        | UsdcRebalanceEvent::Initiated { .. }
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

/// Failure signal carried by an `EquityRedemption` event, if any.
fn equity_redemption_failure(
    event: &EquityRedemptionEvent,
) -> Option<(&'static str, DateTime<Utc>)> {
    match event {
        EquityRedemptionEvent::TransferFailed { failed_at, .. } => {
            Some(("EquityRedemptionEvent::TransferFailed", *failed_at))
        }
        EquityRedemptionEvent::DetectionFailed { failed_at, .. } => {
            Some(("EquityRedemptionEvent::DetectionFailed", *failed_at))
        }
        EquityRedemptionEvent::RedemptionRejected { rejected_at, .. } => {
            Some(("EquityRedemptionEvent::RedemptionRejected", *rejected_at))
        }
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

/// Failure signal carried by a `TokenizedEquityMint` event, if any.
fn tokenized_equity_mint_failure(
    event: &TokenizedEquityMintEvent,
) -> Option<(&'static str, DateTime<Utc>)> {
    match event {
        TokenizedEquityMintEvent::MintRejected { rejected_at, .. } => {
            Some(("TokenizedEquityMintEvent::MintRejected", *rejected_at))
        }
        TokenizedEquityMintEvent::MintAcceptanceFailed { failed_at, .. } => {
            Some(("TokenizedEquityMintEvent::MintAcceptanceFailed", *failed_at))
        }
        TokenizedEquityMintEvent::WrappingFailed { failed_at, .. } => {
            Some(("TokenizedEquityMintEvent::WrappingFailed", *failed_at))
        }
        TokenizedEquityMintEvent::RaindexDepositFailed { failed_at, .. } => {
            Some(("TokenizedEquityMintEvent::RaindexDepositFailed", *failed_at))
        }
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
#[derive(sqlx::FromRow)]
struct JobQueueRow {
    job_type: String,
    pending: i64,
    running: i64,
    done: i64,
    failed: i64,
    awaiting_retry: i64,
    killed: i64,
    retried: i64,
    oldest_pending_run_at: Option<i64>,
}

/// Current health of every apalis job queue.
pub(crate) async fn load_job_queue_health(
    pool: &SqlitePool,
) -> Result<Vec<JobQueueHealth>, PerformanceError> {
    // apalis's `calculate_status` writes 'Killed' (not 'Failed') once a job
    // exhausts max_attempts, carrying attempts >= max_attempts; it also writes
    // 'Killed' on an explicit abort (attempts < max_attempts). A retryable
    // error stays 'Failed' with attempts < max_attempts and is re-fetched. So
    // terminal exhaustion is 'Killed' AND attempts >= max_attempts (failed), an
    // abort is 'Killed' AND attempts < max_attempts (killed), and every 'Failed'
    // row is live retry backlog (awaiting_retry).
    let rows: Vec<JobQueueRow> = sqlx::query_as(
        "SELECT job_type, \
            SUM(CASE WHEN status IN ('Pending', 'Queued') THEN 1 ELSE 0 END) AS pending, \
            SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS running, \
            SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) AS done, \
            SUM(CASE WHEN status = 'Killed' AND attempts >= max_attempts \
                THEN 1 ELSE 0 END) AS failed, \
            SUM(CASE WHEN status = 'Failed' AND attempts < max_attempts \
                THEN 1 ELSE 0 END) AS awaiting_retry, \
            SUM(CASE WHEN status = 'Killed' AND attempts < max_attempts \
                THEN 1 ELSE 0 END) AS killed, \
            SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS retried, \
            MIN(CASE WHEN status IN ('Pending', 'Queued') \
                OR (status = 'Failed' AND attempts < max_attempts) \
                THEN run_at ELSE NULL END) AS oldest_pending_run_at \
         FROM Jobs \
         GROUP BY job_type \
         ORDER BY job_type",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(JobQueueHealth {
                job_type: row.job_type,
                pending: count(row.pending)?,
                running: count(row.running)?,
                done: count(row.done)?,
                failed: count(row.failed)?,
                awaiting_retry: count(row.awaiting_retry)?,
                killed: count(row.killed)?,
                retried: count(row.retried)?,
                oldest_pending_run_at: row
                    .oldest_pending_run_at
                    .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
            })
        })
        .collect()
}

/// Convert a SQLite SUM/COUNT aggregate to `usize`. These are structurally
/// non-negative, so a negative value means a corrupted aggregate -- on an
/// operational-metrics path masking it to a healthy-looking zero would be the
/// opposite of fail-fast, so it surfaces as an error (a 500 at the endpoint).
fn count(value: i64) -> Result<usize, PerformanceError> {
    usize::try_from(value).map_err(|_| PerformanceError::AggregateCount { value })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;
    use uuid::Uuid;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::Symbol;

    use crate::offchain::order::OffchainOrderId;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::UsdcRebalanceId;

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

    #[test]
    fn aggregate_log_entries_includes_inclusive_range_boundaries() {
        // Entries exactly at range.from and range.to must both be counted:
        // the window is inclusive on both ends.
        let entries = vec![
            log_entry(0, "ERROR", "hedge"),
            log_entry(86_400, "WARN", "hedge"),
        ];

        let (buckets, targets) = aggregate_log_entries(&entries, &range());

        let errors: usize = buckets.iter().map(|bucket| bucket.errors).sum();
        let warnings: usize = buckets.iter().map(|bucket| bucket.warnings).sum();
        assert_eq!(errors, 1);
        assert_eq!(warnings, 1);
        assert_eq!(targets.iter().map(|target| target.count).sum::<usize>(), 2);
    }

    fn offchain_order_failed(failed_offset: i64) -> OffchainOrderEvent {
        OffchainOrderEvent::Failed {
            error: "rejected".to_string(),
            failed_at: timestamp(failed_offset),
        }
    }

    #[tokio::test]
    async fn load_failure_events_counts_by_type_within_range() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));

        harness
            .receive::<OffchainOrder>(OffchainOrderId::new(), offchain_order_failed(100))
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(
                UsdcRebalanceId(Uuid::new_v4()),
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "revert".to_string(),
                    failed_at: timestamp(200),
                },
            )
            .await
            .unwrap();
        // Outside the range: must not be counted.
        harness
            .receive::<OffchainOrder>(OffchainOrderId::new(), offchain_order_failed(-999_999))
            .await
            .unwrap();

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
    async fn reactor_ignores_non_failure_events() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));

        // A successful confirmation must not land in the failure read model.
        harness
            .receive::<UsdcRebalance>(
                UsdcRebalanceId(Uuid::new_v4()),
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: timestamp(50),
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert!(failures.is_empty());
    }

    #[tokio::test]
    async fn load_failure_events_fails_fast_on_in_range_invalid_timestamp() {
        let pool = setup_test_db().await;

        // An occurred_at that sorts within [from, to] lexically (so the SQL
        // range filter admits it) yet is not valid RFC 3339. `record` only ever
        // writes valid RFC 3339, so this models data corruption: the money-at-
        // risk view must fail loudly rather than silently drop the category.
        let invalid = format!("{}T99:99:99Z", timestamp(100).format("%Y-%m-%d"));
        sqlx::query(
            "INSERT INTO lifecycle_failure_event (event_type, occurred_at) \
             VALUES ('OffchainOrderEvent::Failed', $1)",
        )
        .bind(&invalid)
        .execute(&pool)
        .await
        .unwrap();

        let error = load_failure_events(&pool, &range()).await.unwrap_err();

        assert!(
            matches!(error, PerformanceError::Timestamp(_)),
            "expected fail-fast on unparseable occurred_at, got {error:?}"
        );
    }

    #[tokio::test]
    async fn load_failure_events_includes_inclusive_range_boundaries() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));

        // Failures exactly at range.from and range.to: both must be counted,
        // the window is inclusive on both ends.
        harness
            .receive::<OffchainOrder>(OffchainOrderId::new(), offchain_order_failed(0))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(OffchainOrderId::new(), offchain_order_failed(86_400))
            .await
            .unwrap();

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].event_type, "OffchainOrderEvent::Failed");
        assert_eq!(failures[0].count, 2);
    }

    #[tokio::test]
    async fn record_propagates_insert_failure_instead_of_swallowing() {
        let pool = setup_test_db().await;

        // Drop the read-model table so the INSERT errors, modelling a transient
        // write failure (contention, disk full). record() must surface the
        // error (the reactor bridge logs it) instead of silently dropping a
        // money-at-risk failure.
        sqlx::query("DROP TABLE lifecycle_failure_event")
            .execute(&pool)
            .await
            .unwrap();
        let projection = LifecycleFailureProjection::new(pool);

        let error = projection
            .record(Some(("OffchainOrderEvent::Failed", timestamp(100))))
            .await
            .unwrap_err();

        assert!(
            matches!(error, FailureProjectionError::Database(_)),
            "expected the INSERT error to propagate, got {error:?}"
        );
    }

    #[test]
    fn count_rejects_negative_aggregate() {
        // A negative aggregate is structurally impossible, but if a corrupted
        // query ever produced one it must fail loud, not mask to zero.
        let error = count(-1).unwrap_err();

        assert!(
            matches!(error, PerformanceError::AggregateCount { value: -1 }),
            "expected AggregateCount, got {error:?}"
        );
    }

    #[tokio::test]
    async fn load_job_queue_health_aggregates_per_job_type() {
        // `setup_test_db` already creates the apalis `Jobs` table on the
        // shared-cache in-memory database.
        let pool = setup_test_db().await;

        // (id, job_type, status, attempts, max_attempts, run_at)
        let jobs = [
            ("a-1", "queue::A", "Pending", 0, 25, 1_750_000_100_i64),
            ("a-2", "queue::A", "Queued", 0, 25, 1_750_000_080),
            ("a-3", "queue::A", "Running", 1, 25, 1_750_000_050),
            // Retry-eligible failure: live backlog, not terminal.
            ("a-4", "queue::A", "Failed", 3, 25, 1_750_000_000),
            // Retry exhaustion: apalis writes 'Killed' with attempts >=
            // max_attempts (verified against its calculate_status), so this is
            // the terminal `failed` bucket. A hand-built ('Failed', 25, 25) row
            // is a state apalis can never persist.
            ("a-5", "queue::A", "Killed", 25, 25, 1_749_999_000),
            // Aborted before exhausting retries: 'Killed' with attempts <
            // max_attempts -> the `killed` bucket, disjoint from `failed`.
            ("a-6", "queue::A", "Killed", 1, 25, 1_749_998_000),
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
        assert_eq!(queue_a.failed, 1);
        assert_eq!(queue_a.killed, 1);
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
    fn failure_mappers_match_domain_event_names() {
        use st0x_event_sorcery::DomainEvent;
        use st0x_float_macro::float;

        use crate::equity_redemption::DetectionFailure;

        // The reactor records the canonical DomainEvent type string. Verify
        // every mapper's hardcoded string against the event's own `event_type()`
        // so a rename of any failure variant cannot silently desync the read
        // model. All 13 tracked failure types are asserted here.

        // --- OffchainOrder (1 variant) ---
        let offchain = OffchainOrderEvent::Failed {
            error: "rejected".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            offchain_order_failure(&offchain).unwrap().0,
            offchain.event_type()
        );

        // --- UsdcRebalance (5 variants) ---
        let rebalance_conversion = UsdcRebalanceEvent::ConversionFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            usdc_rebalance_failure(&rebalance_conversion).unwrap().0,
            rebalance_conversion.event_type()
        );

        let rebalance_withdrawal = UsdcRebalanceEvent::WithdrawalFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            usdc_rebalance_failure(&rebalance_withdrawal).unwrap().0,
            rebalance_withdrawal.event_type()
        );

        let rebalance_bridging = UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            usdc_rebalance_failure(&rebalance_bridging).unwrap().0,
            rebalance_bridging.event_type()
        );

        let rebalance_deposit = UsdcRebalanceEvent::DepositFailed {
            deposit_ref: None,
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            usdc_rebalance_failure(&rebalance_deposit).unwrap().0,
            rebalance_deposit.event_type()
        );

        let rebalance_attestation = UsdcRebalanceEvent::AttestationTimedOut {
            burn_tx_hash: alloy::primitives::TxHash::ZERO,
            retry_deadline_at: timestamp(1),
            timed_out_at: timestamp(0),
        };
        assert_eq!(
            usdc_rebalance_failure(&rebalance_attestation).unwrap().0,
            rebalance_attestation.event_type()
        );

        // --- EquityRedemption (3 variants) ---
        let redemption_transfer = EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(0),
        };
        assert_eq!(
            equity_redemption_failure(&redemption_transfer).unwrap().0,
            redemption_transfer.event_type()
        );

        let redemption_detection = EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: timestamp(0),
        };
        assert_eq!(
            equity_redemption_failure(&redemption_detection).unwrap().0,
            redemption_detection.event_type()
        );

        let redemption_rejected = EquityRedemptionEvent::RedemptionRejected {
            reason: "rejected".to_string(),
            rejected_at: timestamp(0),
        };
        assert_eq!(
            equity_redemption_failure(&redemption_rejected).unwrap().0,
            redemption_rejected.event_type()
        );

        // --- TokenizedEquityMint (4 variants) ---
        let mint_rejected = TokenizedEquityMintEvent::MintRejected {
            reason: "rejected".to_string(),
            rejected_at: timestamp(0),
        };
        assert_eq!(
            tokenized_equity_mint_failure(&mint_rejected).unwrap().0,
            mint_rejected.event_type()
        );

        let mint_acceptance_failed = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "timeout".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            tokenized_equity_mint_failure(&mint_acceptance_failed)
                .unwrap()
                .0,
            mint_acceptance_failed.event_type()
        );

        let mint_wrapping_failed = TokenizedEquityMintEvent::WrappingFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(1),
            reason: None,
            failed_at: timestamp(0),
        };
        assert_eq!(
            tokenized_equity_mint_failure(&mint_wrapping_failed)
                .unwrap()
                .0,
            mint_wrapping_failed.event_type()
        );

        let mint_raindex_deposit_failed = TokenizedEquityMintEvent::RaindexDepositFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        };
        assert_eq!(
            tokenized_equity_mint_failure(&mint_raindex_deposit_failed)
                .unwrap()
                .0,
            mint_raindex_deposit_failed.event_type()
        );
    }
}
