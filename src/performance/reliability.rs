//! Reliability read models: lifecycle failure events, log volume
//! aggregation, and job queue health.
//!
//! Distinguishes money-at-risk failures (failed hedges, failed rebalance
//! stages) recorded as CQRS events from cosmetic log noise, and surfaces
//! apalis queue backlogs and retry pressure.
//!
//! Lifecycle failure events are maintained by [`LifecycleFailureProjection`],
//! which subscribes to the `OffchainOrder`, `UsdcRebalance`, `EquityRedemption`,
//! and `TokenizedEquityMint` event streams and records one row per failure event
//! into `lifecycle_failure_event`. [`load_failure_events`] reads ONLY that table
//! and never folds the `events` table. The log-aggregation and job-queue-health
//! read paths read their own sources (structured logs, the apalis `Jobs` table)
//! and are left untouched.
//!
//! Checkpointed catch-up: [`LifecycleFailureProjection::catch_up`] runs at
//! startup, before the live reactor consumes events, replaying every failure
//! across all four streams past each aggregate's persisted checkpoint
//! (`lifecycle_failure_checkpoint`). The live reactor is forward-only WHILE
//! running, but an event it drops (a crash between an event being persisted and
//! this table being updated) is replayed on the next startup -- so the read
//! model converges on the event log instead of staying best-effort until someone
//! rebuilds.
//!
//! Rebuildable: each failure row is a pure function of an event payload (the
//! mappers read the event's own `failed_at`/`timed_out_at`, never a
//! processing-time clock) and is written with `ON CONFLICT DO NOTHING` on its
//! `(aggregate_type, aggregate_id, event_type, occurred_at)` identity, so
//! replaying the log reproduces the exact rows live processing produced -- no
//! duplicates. The live reactor has no access to the event `sequence` (the
//! `Reactor` trait only passes the domain event), so the dedup key is that
//! identity tuple rather than the sequence, and the checkpoint advances only on
//! the replay/catch-up path. [`LifecycleFailureProjection::rebuild_all`]
//! truncates both tables and re-folds every stream. That replay-equals-live
//! property is what makes this a valid CQRS/ES projection: the event log stays
//! the single source of truth.

use std::collections::BTreeMap;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use tracing::warn;

use st0x_dto::{
    CountedLogLevel, FailureEventCount, FailureEventType, JobQueueHealth, LogTargetCount,
    LogVolumeBucket,
};

use st0x_event_sorcery::{EntityList, EventSourced, Reactor, deps};

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
/// A grouped `event_type` that does not name a known [`FailureEventType`] is
/// skipped with a warning rather than failing the report: an unmapped
/// discriminator is a coverage gap in the enum, not corruption of an existing
/// category's count.
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
        .filter_map(|(raw_event_type, occurrences, last_at)| {
            // An event_type that maps to no known variant is a coverage gap in
            // the enum, not corruption of a counted category: skip it with a
            // warning rather than failing the whole report.
            let Ok(event_type) = FailureEventType::from_str(&raw_event_type) else {
                warn!(%raw_event_type, "unrecognized failure event type; skipping");
                return None;
            };
            Some((event_type, occurrences, last_at))
        })
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

/// The dedup insert: one row per distinct failure event, keyed by event
/// identity. Both the live reactor and the replay path write through it, so
/// re-folding an already-recorded failure is a no-op rather than a duplicate.
const INSERT_FAILURE_SQL: &str = "INSERT INTO lifecycle_failure_event (aggregate_type, aggregate_id, event_type, occurred_at) \
     VALUES (?, ?, ?, ?) \
     ON CONFLICT(aggregate_type, aggregate_id, event_type, occurred_at) DO NOTHING";

impl LifecycleFailureProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Record a live failure event. Non-failure events map to `None` and are
    /// ignored. Writes through the same dedup insert the replay path uses, so a
    /// later catch-up that re-folds this event is a no-op rather than a duplicate.
    /// The live reactor has no event `sequence`, so it leaves the
    /// `lifecycle_failure_checkpoint` untouched -- only replay advances it.
    async fn record(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        failure: Option<(FailureEventType, DateTime<Utc>)>,
    ) -> Result<(), FailureProjectionError> {
        // The reactor sees every event on four high-volume streams but only
        // failures produce a row, so skip the write transaction entirely for the
        // common non-failure case rather than opening and committing an empty one.
        if failure.is_none() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        Self::insert_failure_tx(&mut tx, aggregate_type, aggregate_id, failure).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Catches the read model up to the event log, then returns events folded.
    ///
    /// Replays only events past each aggregate's stored checkpoint, so a restart
    /// re-folds the small tail emitted since the last catch-up rather than the
    /// whole history. Run at startup, before the live reactor begins consuming
    /// events: this seeds the table after a fresh deploy and recovers anything the
    /// forward-only live path dropped in the previous run.
    pub(crate) async fn catch_up(&self) -> Result<u64, FailureProjectionError> {
        let mut tx = self.pool.begin().await?;

        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Rebuilds the entire lifecycle-failure read model from the event log.
    ///
    /// Truncates both the failure table and its checkpoints, then re-folds every
    /// stream from scratch via [`Self::replay_pending`] (empty checkpoints mean
    /// every event is past `COALESCE(last_sequence, 0)`). Use to repair a
    /// corrupted read model when an incremental [`Self::catch_up`] is not enough.
    /// Runs in one transaction: a failure mid-rebuild rolls back, never leaving a
    /// half-rebuilt table. Returns the number of events folded.
    pub(crate) async fn rebuild_all(&self) -> Result<u64, FailureProjectionError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM lifecycle_failure_event")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM lifecycle_failure_checkpoint")
            .execute(&mut *tx)
            .await?;
        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Folds every event past its aggregate's checkpoint into the read model,
    /// advancing each aggregate's checkpoint as it goes.
    ///
    /// Reads all four subscribed aggregate types' events past their per-aggregate
    /// `lifecycle_failure_checkpoint`, maps each to its failure signal (if any),
    /// inserts the failure idempotently, and advances the checkpoint. The fold
    /// reads only event payloads (no processing-time clock) and dedups by event
    /// identity, so replaying reproduces the exact rows live processing produced.
    /// The `LEFT JOIN` yields only un-folded events, so a steady-state catch-up
    /// reads just the tail.
    ///
    /// A row whose payload cannot be parsed is skipped with a warning rather than
    /// propagated: a single poison row must not brick startup (or the
    /// `rebuild_all` repair path, which calls this too). The skip is non-silent by
    /// construction -- a poison row caps its aggregate's checkpoint at the last
    /// sequence before the gap, so the poison row, and every event after it on
    /// that aggregate, is re-scanned (and re-warned) on every catch-up until the
    /// corrupt payload is repaired in the `events` table. Parseable failures after
    /// the gap are still folded, so no failure is silently dropped; the dedup
    /// insert makes the repeated re-fold a no-op. `rebuild_all` cannot repair a
    /// corrupt payload -- only fixing or deleting the offending `events` row can.
    /// Database errors still propagate -- they are infrastructure failures, not
    /// poison rows. Returns the count of events folded this pass (non-failures
    /// count too); the same tail re-counts each pass while a poison gap persists.
    async fn replay_pending(
        tx: &mut Transaction<'_, Sqlite>,
    ) -> Result<u64, FailureProjectionError> {
        let pending: Vec<(String, String, i64, String)> = sqlx::query_as(
            "SELECT event.aggregate_type, event.aggregate_id, event.sequence, event.payload \
             FROM events event \
             LEFT JOIN lifecycle_failure_checkpoint checkpoint \
               ON checkpoint.aggregate_type = event.aggregate_type \
              AND checkpoint.aggregate_id = event.aggregate_id \
             WHERE event.aggregate_type IN (?, ?, ?, ?) \
               AND event.sequence > COALESCE(checkpoint.last_sequence, 0) \
             ORDER BY event.aggregate_type, event.aggregate_id, event.sequence ASC",
        )
        .bind(OffchainOrder::AGGREGATE_TYPE)
        .bind(UsdcRebalance::AGGREGATE_TYPE)
        .bind(EquityRedemption::AGGREGATE_TYPE)
        .bind(TokenizedEquityMint::AGGREGATE_TYPE)
        .fetch_all(&mut **tx)
        .await?;

        let mut replayed: u64 = 0;
        // Events arrive grouped by aggregate in ascending sequence. Track the
        // aggregate currently being folded so a poison row can cap that
        // aggregate's checkpoint at the last sequence before the gap.
        let mut current_aggregate: Option<(String, String)> = None;
        let mut aggregate_poisoned = false;

        for (aggregate_type, aggregate_id, sequence, payload) in pending {
            let same_aggregate =
                current_aggregate
                    .as_ref()
                    .is_some_and(|(current_type, current_id)| {
                        current_type == &aggregate_type && current_id == &aggregate_id
                    });
            if !same_aggregate {
                current_aggregate = Some((aggregate_type.clone(), aggregate_id.clone()));
                aggregate_poisoned = false;
            }

            let failure = match map_failure(&aggregate_type, &payload) {
                Ok(failure) => failure,
                Err(error) => {
                    warn!(
                        %aggregate_type,
                        %aggregate_id,
                        sequence,
                        %error,
                        "Skipping unparseable lifecycle-failure event payload"
                    );
                    aggregate_poisoned = true;
                    continue;
                }
            };

            Self::insert_failure_tx(tx, &aggregate_type, &aggregate_id, failure).await?;
            replayed += 1;

            // Stop advancing the checkpoint once a poison row has gapped this
            // aggregate's stream, so the gap is re-scanned every catch-up instead
            // of silently skipped past. The fold above still records parseable
            // failures after the gap; the dedup insert makes the re-fold a no-op.
            if !aggregate_poisoned {
                Self::advance_checkpoint_tx(tx, &aggregate_type, &aggregate_id, sequence).await?;
            }
        }

        Ok(replayed)
    }

    /// Idempotent failure insert shared by the live (`record`) and replay paths.
    async fn insert_failure_tx(
        tx: &mut Transaction<'_, Sqlite>,
        aggregate_type: &str,
        aggregate_id: &str,
        failure: Option<(FailureEventType, DateTime<Utc>)>,
    ) -> Result<(), FailureProjectionError> {
        let Some((event_type, occurred_at)) = failure else {
            return Ok(());
        };
        sqlx::query(INSERT_FAILURE_SQL)
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(event_type.as_str())
            .bind(occurred_at.to_rfc3339())
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Advances an aggregate's replay checkpoint to `sequence`, monotonically.
    ///
    /// The `MAX` guard means a lower or out-of-order `sequence` can never regress
    /// a checkpoint, so correctness does not depend on the caller folding events
    /// in ascending order -- a future reordering or second caller cannot silently
    /// rewind the checkpoint and trigger a redundant re-scan.
    async fn advance_checkpoint_tx(
        tx: &mut Transaction<'_, Sqlite>,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
    ) -> Result<(), FailureProjectionError> {
        sqlx::query(
            "INSERT INTO lifecycle_failure_checkpoint \
                 (aggregate_type, aggregate_id, last_sequence) \
             VALUES (?, ?, ?) \
             ON CONFLICT(aggregate_type, aggregate_id) \
                 DO UPDATE SET last_sequence = MAX(last_sequence, excluded.last_sequence)",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}

/// Deserializes a raw event payload for `aggregate_type` and maps it to its
/// failure signal (if any). Returns `Ok(None)` for non-failure events and for an
/// unknown aggregate type (unreachable given the replay query's `IN` filter).
fn map_failure(
    aggregate_type: &str,
    payload: &str,
) -> Result<Option<(FailureEventType, DateTime<Utc>)>, serde_json::Error> {
    if aggregate_type == OffchainOrder::AGGREGATE_TYPE {
        Ok(offchain_order_failure(&serde_json::from_str(payload)?))
    } else if aggregate_type == UsdcRebalance::AGGREGATE_TYPE {
        Ok(usdc_rebalance_failure(&serde_json::from_str(payload)?))
    } else if aggregate_type == EquityRedemption::AGGREGATE_TYPE {
        Ok(equity_redemption_failure(&serde_json::from_str(payload)?))
    } else if aggregate_type == TokenizedEquityMint::AGGREGATE_TYPE {
        Ok(tokenized_equity_mint_failure(&serde_json::from_str(
            payload,
        )?))
    } else {
        warn!(
            %aggregate_type,
            "lifecycle-failure replay saw an unsubscribed aggregate type; ignoring"
        );
        Ok(None)
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
            .on(|id, event| async move {
                self.record(
                    OffchainOrder::AGGREGATE_TYPE,
                    &id.to_string(),
                    offchain_order_failure(&event),
                )
                .await
            })
            .on(|id, event| async move {
                self.record(
                    UsdcRebalance::AGGREGATE_TYPE,
                    &id.to_string(),
                    usdc_rebalance_failure(&event),
                )
                .await
            })
            .on(|id, event| async move {
                self.record(
                    EquityRedemption::AGGREGATE_TYPE,
                    &id.to_string(),
                    equity_redemption_failure(&event),
                )
                .await
            })
            .on(|id, event| async move {
                self.record(
                    TokenizedEquityMint::AGGREGATE_TYPE,
                    &id.to_string(),
                    tokenized_equity_mint_failure(&event),
                )
                .await
            })
            .exhaustive()
            .await
    }
}

/// Failure signal carried by an `OffchainOrder` event, if any.
fn offchain_order_failure(event: &OffchainOrderEvent) -> Option<(FailureEventType, DateTime<Utc>)> {
    match event {
        OffchainOrderEvent::Failed { failed_at, .. } => {
            Some((FailureEventType::OffchainOrderFailed, *failed_at))
        }
        OffchainOrderEvent::Placed { .. }
        | OffchainOrderEvent::Submitted { .. }
        | OffchainOrderEvent::Accepted { .. }
        | OffchainOrderEvent::PartiallyFilled { .. }
        | OffchainOrderEvent::Filled { .. }
        | OffchainOrderEvent::CancelRequested { .. }
        | OffchainOrderEvent::Cancelled { .. } => None,
    }
}

/// Failure signal carried by a `UsdcRebalance` event, if any.
fn usdc_rebalance_failure(event: &UsdcRebalanceEvent) -> Option<(FailureEventType, DateTime<Utc>)> {
    match event {
        UsdcRebalanceEvent::ConversionFailed { failed_at, .. } => {
            Some((FailureEventType::ConversionFailed, *failed_at))
        }
        UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. } => {
            Some((FailureEventType::WithdrawalFailed, *failed_at))
        }
        UsdcRebalanceEvent::BridgingFailed { failed_at, .. } => {
            Some((FailureEventType::BridgingFailed, *failed_at))
        }
        UsdcRebalanceEvent::DepositFailed { failed_at, .. } => {
            Some((FailureEventType::DepositFailed, *failed_at))
        }
        UsdcRebalanceEvent::AttestationTimedOut { timed_out_at, .. } => {
            Some((FailureEventType::AttestationTimedOut, *timed_out_at))
        }
        UsdcRebalanceEvent::ConversionInitiated { .. }
        | UsdcRebalanceEvent::ConversionConfirmed { .. }
        | UsdcRebalanceEvent::WithdrawalSubmitting { .. }
        | UsdcRebalanceEvent::Initiated { .. }
        | UsdcRebalanceEvent::WithdrawalConfirmed { .. }
        | UsdcRebalanceEvent::BridgingSubmitting { .. }
        | UsdcRebalanceEvent::PendingBurnRecorded { .. }
        | UsdcRebalanceEvent::PendingBurnCleared { .. }
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
) -> Option<(FailureEventType, DateTime<Utc>)> {
    match event {
        EquityRedemptionEvent::TransferFailed { failed_at, .. } => {
            Some((FailureEventType::RedemptionTransferFailed, *failed_at))
        }
        EquityRedemptionEvent::DetectionFailed { failed_at, .. } => {
            Some((FailureEventType::RedemptionDetectionFailed, *failed_at))
        }
        EquityRedemptionEvent::RedemptionRejected { rejected_at, .. } => {
            Some((FailureEventType::RedemptionRejected, *rejected_at))
        }
        EquityRedemptionEvent::VaultWithdrawPending { .. }
        | EquityRedemptionEvent::VaultWithdrawSubmitted { .. }
        | EquityRedemptionEvent::WithdrawnFromRaindex { .. }
        | EquityRedemptionEvent::TokensUnwrapped { .. }
        | EquityRedemptionEvent::UnwrapPending { .. }
        | EquityRedemptionEvent::UnwrapSubmitted { .. }
        | EquityRedemptionEvent::SendPending { .. }
        | EquityRedemptionEvent::SendSubmitted { .. }
        | EquityRedemptionEvent::TokensSent { .. }
        | EquityRedemptionEvent::Detected { .. }
        | EquityRedemptionEvent::Completed { .. }
        | EquityRedemptionEvent::ProviderCompletionRecovered { .. }
        | EquityRedemptionEvent::OperatorReconciled { .. } => None,
    }
}

/// Failure signal carried by a `TokenizedEquityMint` event, if any.
fn tokenized_equity_mint_failure(
    event: &TokenizedEquityMintEvent,
) -> Option<(FailureEventType, DateTime<Utc>)> {
    match event {
        TokenizedEquityMintEvent::MintRejected { rejected_at, .. } => {
            Some((FailureEventType::MintRejected, *rejected_at))
        }
        TokenizedEquityMintEvent::MintAcceptanceFailed { failed_at, .. } => {
            Some((FailureEventType::MintAcceptanceFailed, *failed_at))
        }
        TokenizedEquityMintEvent::WrappingFailed { failed_at, .. } => {
            Some((FailureEventType::WrappingFailed, *failed_at))
        }
        TokenizedEquityMintEvent::RaindexDepositFailed { failed_at, .. } => {
            Some((FailureEventType::RaindexDepositFailed, *failed_at))
        }
        TokenizedEquityMintEvent::MintRequested { .. }
        | TokenizedEquityMintEvent::MintAccepted { .. }
        | TokenizedEquityMintEvent::TokensReceived { .. }
        | TokenizedEquityMintEvent::WrapSubmitted { .. }
        | TokenizedEquityMintEvent::TokensWrapped { .. }
        | TokenizedEquityMintEvent::VaultDepositSubmitted { .. }
        | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
        | TokenizedEquityMintEvent::ProviderCompletionRecovered { .. }
        | TokenizedEquityMintEvent::OperatorReconciled { .. } => None,
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
    use std::collections::BTreeSet;

    use alloy::primitives::TxHash;
    use chrono::TimeZone;
    use serde_json::json;
    use uuid::Uuid;

    use st0x_event_sorcery::{DomainEvent, ReactorHarness};
    use st0x_execution::Symbol;
    use st0x_float_macro::float;

    use crate::equity_redemption::DetectionFailure;
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
        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));

        for offset in [300_i64, 100] {
            harness
                .receive::<OffchainOrder>(OffchainOrderId::new(), offchain_order_failed(offset))
                .await
                .unwrap();
        }

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].count, 2);
        // last_at must be the most recent occurrence, regardless of the
        // order the events were received in.
        assert_eq!(failures[0].last_at, timestamp(300));
    }

    #[tokio::test]
    async fn load_failure_events_records_rebalance_timestamps_per_variant() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));

        harness
            .receive::<UsdcRebalance>(
                UsdcRebalanceId(Uuid::new_v4()),
                UsdcRebalanceEvent::ConversionFailed {
                    reason: "rejected".to_string(),
                    failed_at: timestamp(100),
                },
            )
            .await
            .unwrap();
        // AttestationTimedOut carries timed_out_at, not failed_at: the
        // riskiest extraction path in the rebalance mapper.
        harness
            .receive::<UsdcRebalance>(
                UsdcRebalanceId(Uuid::new_v4()),
                UsdcRebalanceEvent::AttestationTimedOut {
                    burn_tx_hash: alloy::primitives::TxHash::random(),
                    retry_deadline_at: timestamp(50),
                    timed_out_at: timestamp(200),
                },
            )
            .await
            .unwrap();

        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 2);
        assert_eq!(
            failures[0].event_type,
            FailureEventType::AttestationTimedOut
        );
        assert_eq!(failures[0].count, 1);
        assert_eq!(failures[0].last_at, timestamp(200));
        assert_eq!(failures[1].event_type, FailureEventType::ConversionFailed);
        assert_eq!(failures[1].count, 1);
        assert_eq!(failures[1].last_at, timestamp(100));
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
            "INSERT INTO lifecycle_failure_event (aggregate_type, aggregate_id, event_type, occurred_at) \
             VALUES ('OffchainOrder', 'test-aggregate', 'OffchainOrderEvent::Failed', $1)",
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
        assert_eq!(
            failures[0].event_type,
            FailureEventType::OffchainOrderFailed
        );
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
            .record(
                OffchainOrder::AGGREGATE_TYPE,
                &OffchainOrderId::new().to_string(),
                Some((FailureEventType::OffchainOrderFailed, timestamp(100))),
            )
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

    #[tokio::test]
    async fn load_failure_events_skips_unrecognized_event_type_row() {
        let pool = setup_test_db().await;

        // Insert one row with a bogus event_type and one valid row.
        sqlx::query(
            "INSERT INTO lifecycle_failure_event (aggregate_type, aggregate_id, event_type, occurred_at) \
             VALUES ('OffchainOrder', 'test-aggregate', 'NotARealEvent::Nope', $1)",
        )
        .bind(timestamp(50).to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_failure_event (aggregate_type, aggregate_id, event_type, occurred_at) \
             VALUES ('OffchainOrder', 'test-aggregate', 'OffchainOrderEvent::Failed', $1)",
        )
        .bind(timestamp(100).to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();

        // The bogus row must be skipped; the valid row must appear.
        let failures = load_failure_events(&pool, &range()).await.unwrap();

        assert_eq!(failures.len(), 1);
        assert_eq!(
            failures[0].event_type,
            FailureEventType::OffchainOrderFailed
        );
        assert_eq!(failures[0].count, 1);
    }

    #[tokio::test]
    async fn mapper_functions_return_correct_failure_event_types() {
        // Exercises all four mapper functions (offchain_order_failure,
        // usdc_rebalance_failure, equity_redemption_failure,
        // tokenized_equity_mint_failure) and asserts that the returned
        // FailureEventType variant matches both the expected variant and its
        // as_str() discriminator -- restoring per-arm drift coverage.

        // offchain_order_failure
        let (event_type, _) = offchain_order_failure(&OffchainOrderEvent::Failed {
            error: "rejected".to_string(),
            failed_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::OffchainOrderFailed);
        assert_eq!(
            event_type.as_str(),
            OffchainOrderEvent::Failed {
                error: "rejected".to_string(),
                failed_at: timestamp(0),
            }
            .event_type()
        );

        // usdc_rebalance_failure -- BridgingFailed and DepositFailed are the
        // most prone to copy-paste error (two similar variant names).
        let (event_type, _) = usdc_rebalance_failure(&UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::BridgingFailed);
        assert_eq!(
            event_type.as_str(),
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: None,
                cctp_nonce: None,
                reason: "revert".to_string(),
                failed_at: timestamp(0),
            }
            .event_type()
        );

        let (event_type, _) = usdc_rebalance_failure(&UsdcRebalanceEvent::DepositFailed {
            deposit_ref: None,
            reason: "revert".to_string(),
            failed_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::DepositFailed);
        assert_eq!(
            event_type.as_str(),
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "revert".to_string(),
                failed_at: timestamp(0),
            }
            .event_type()
        );

        let (event_type, _) = usdc_rebalance_failure(&UsdcRebalanceEvent::AttestationTimedOut {
            burn_tx_hash: TxHash::random(),
            retry_deadline_at: timestamp(0),
            timed_out_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::AttestationTimedOut);

        // equity_redemption_failure -- all three failure arms.
        let (event_type, _) = equity_redemption_failure(&EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::RedemptionTransferFailed);
        assert_eq!(
            event_type.as_str(),
            EquityRedemptionEvent::TransferFailed {
                tx_hash: None,
                reason: None,
                failed_at: timestamp(0),
            }
            .event_type()
        );

        let (event_type, _) = equity_redemption_failure(&EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: timestamp(0),
        })
        .unwrap();
        assert_eq!(event_type, FailureEventType::RedemptionDetectionFailed);

        let (event_type, _) =
            equity_redemption_failure(&EquityRedemptionEvent::RedemptionRejected {
                reason: "rejected".to_string(),
                rejected_at: timestamp(0),
            })
            .unwrap();
        assert_eq!(event_type, FailureEventType::RedemptionRejected);

        // tokenized_equity_mint_failure -- all four failure arms.
        let (event_type, _) =
            tokenized_equity_mint_failure(&TokenizedEquityMintEvent::MintRejected {
                reason: "rejected".to_string(),
                rejected_at: timestamp(0),
            })
            .unwrap();
        assert_eq!(event_type, FailureEventType::MintRejected);

        let (event_type, _) =
            tokenized_equity_mint_failure(&TokenizedEquityMintEvent::MintAcceptanceFailed {
                reason: "rejected".to_string(),
                failed_at: timestamp(0),
            })
            .unwrap();
        assert_eq!(event_type, FailureEventType::MintAcceptanceFailed);

        let (event_type, _) =
            tokenized_equity_mint_failure(&TokenizedEquityMintEvent::WrappingFailed {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                reason: None,
                failed_at: timestamp(0),
            })
            .unwrap();
        assert_eq!(event_type, FailureEventType::WrappingFailed);
        assert_eq!(
            event_type.as_str(),
            TokenizedEquityMintEvent::WrappingFailed {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                reason: None,
                failed_at: timestamp(0),
            }
            .event_type()
        );

        let (event_type, _) =
            tokenized_equity_mint_failure(&TokenizedEquityMintEvent::RaindexDepositFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(0),
            })
            .unwrap();
        assert_eq!(event_type, FailureEventType::RaindexDepositFailed);
        assert_eq!(
            event_type.as_str(),
            TokenizedEquityMintEvent::RaindexDepositFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(0),
            }
            .event_type()
        );
    }

    /// Persists a real event into the store table so the replay path can fold
    /// it, serializing the event exactly as the event store does.
    async fn persist_event<E: serde::Serialize + DomainEvent>(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event: &E,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, ?, '1', ?, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event.event_type())
        .bind(serde_json::to_string(event).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    /// Stages a raw event row with an arbitrary payload -- the only way to stage
    /// a poison row that no real event could produce.
    async fn insert_raw_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, 'poison', '1', ?, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn count_failure_rows(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_failure_event")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Snapshots the failure table by identity so live and rebuilt state compare
    /// row-for-row.
    async fn fetch_failure_rows(pool: &SqlitePool) -> Vec<(String, String, String, String)> {
        sqlx::query_as(
            "SELECT aggregate_type, aggregate_id, event_type, occurred_at \
             FROM lifecycle_failure_event \
             ORDER BY aggregate_type, aggregate_id, event_type, occurred_at",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn fetch_checkpoint(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Option<i64> {
        sqlx::query_scalar(
            "SELECT last_sequence FROM lifecycle_failure_checkpoint \
             WHERE aggregate_type = ? AND aggregate_id = ?",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// Catch-up folds failures across all four subscribed streams, ignores
    /// non-failure events (which still advance the checkpoint), and records one
    /// row per failure.
    #[tokio::test]
    async fn catch_up_folds_failures_across_all_four_streams() {
        let pool = setup_test_db().await;
        let projection = LifecycleFailureProjection::new(pool.clone());

        let offchain_id = OffchainOrderId::new().to_string();
        let rebalance_id = UsdcRebalanceId(Uuid::new_v4()).to_string();
        let redemption_id = Uuid::new_v4().to_string();
        let mint_id = Uuid::new_v4().to_string();

        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &offchain_id,
            1,
            &offchain_order_failed(100),
        )
        .await;
        persist_event(
            &pool,
            UsdcRebalance::AGGREGATE_TYPE,
            &rebalance_id,
            1,
            &UsdcRebalanceEvent::WithdrawalFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(200),
            },
        )
        .await;
        // A non-failure event on the rebalance stream: advances the checkpoint
        // but creates no failure row.
        persist_event(
            &pool,
            UsdcRebalance::AGGREGATE_TYPE,
            &rebalance_id,
            2,
            &UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(250),
                withdrawal_tx: None,
            },
        )
        .await;
        persist_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &redemption_id,
            1,
            &EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::Timeout,
                failed_at: timestamp(300),
            },
        )
        .await;
        persist_event(
            &pool,
            TokenizedEquityMint::AGGREGATE_TYPE,
            &mint_id,
            1,
            &TokenizedEquityMintEvent::MintRejected {
                reason: "rejected".to_string(),
                rejected_at: timestamp(400),
            },
        )
        .await;

        let replayed = projection.catch_up().await.unwrap();
        assert_eq!(
            replayed, 5,
            "every event folds (four failures + one non-failure), all advancing checkpoints"
        );

        assert_eq!(
            count_failure_rows(&pool).await,
            4,
            "one failure row per stream; the non-failure event produces no row"
        );
        // The non-failure event still advanced the rebalance checkpoint to 2.
        assert_eq!(
            fetch_checkpoint(&pool, UsdcRebalance::AGGREGATE_TYPE, &rebalance_id).await,
            Some(2),
            "a non-failure event advances the checkpoint so it is not re-scanned"
        );
    }

    /// The production restart path: the live reactor records a failure (no
    /// checkpoint), and the next startup catch_up re-folds the same event from the
    /// log -- the dedup insert makes it a no-op, not a duplicate.
    #[tokio::test]
    async fn live_then_catch_up_does_not_duplicate_failures() {
        let pool = setup_test_db().await;

        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));
        let order_id = OffchainOrderId::new();
        let order_id_str = order_id.to_string();
        harness
            .receive::<OffchainOrder>(order_id, offchain_order_failed(100))
            .await
            .unwrap();
        assert_eq!(
            count_failure_rows(&pool).await,
            1,
            "the live reactor recorded the failure"
        );

        // The same event is in the log; catch_up re-folds it but the dedup insert
        // is a no-op.
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &order_id_str,
            1,
            &offchain_order_failed(100),
        )
        .await;
        let replayed = LifecycleFailureProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(replayed, 1, "catch_up folds the one logged event");
        assert_eq!(
            count_failure_rows(&pool).await,
            1,
            "dedup: the live failure is not duplicated by catch_up"
        );
    }

    /// Rebuilding from the event log alone reproduces the exact rows live
    /// processing produced.
    #[tokio::test]
    async fn rebuild_all_reproduces_live_state() {
        let pool = setup_test_db().await;

        let harness = ReactorHarness::new(LifecycleFailureProjection::new(pool.clone()));
        let order_id = OffchainOrderId::new();
        let order_id_str = order_id.to_string();
        let rebalance_id = UsdcRebalanceId(Uuid::new_v4());
        let rebalance_id_str = rebalance_id.to_string();
        let withdrawal_failed = UsdcRebalanceEvent::WithdrawalFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(200),
        };

        harness
            .receive::<OffchainOrder>(order_id, offchain_order_failed(100))
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(rebalance_id, withdrawal_failed.clone())
            .await
            .unwrap();

        // The same events in the log for the rebuild to fold.
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &order_id_str,
            1,
            &offchain_order_failed(100),
        )
        .await;
        persist_event(
            &pool,
            UsdcRebalance::AGGREGATE_TYPE,
            &rebalance_id_str,
            1,
            &withdrawal_failed,
        )
        .await;

        let live = fetch_failure_rows(&pool).await;
        assert_eq!(live.len(), 2, "two live failure rows");

        let replayed = LifecycleFailureProjection::new(pool.clone())
            .rebuild_all()
            .await
            .unwrap();
        assert_eq!(replayed, 2);

        let rebuilt = fetch_failure_rows(&pool).await;
        assert_eq!(
            live, rebuilt,
            "rebuilding from the event log reproduces the exact live failure rows"
        );
    }

    /// A per-aggregate checkpoint means a steady-state catch-up folds only the
    /// un-folded tail, never re-folding an aggregate already past its checkpoint.
    #[tokio::test]
    async fn catch_up_replays_only_events_past_the_checkpoint() {
        let pool = setup_test_db().await;
        let projection = LifecycleFailureProjection::new(pool.clone());

        let first_order = OffchainOrderId::new().to_string();
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &first_order,
            1,
            &offchain_order_failed(100),
        )
        .await;

        let first = projection.catch_up().await.unwrap();
        assert_eq!(first, 1, "the first catch-up folds the only event");
        assert_eq!(
            fetch_checkpoint(&pool, OffchainOrder::AGGREGATE_TYPE, &first_order).await,
            Some(1)
        );

        // A second failed order arrives; the first is already past its checkpoint.
        let second_order = OffchainOrderId::new().to_string();
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &second_order,
            1,
            &offchain_order_failed(200),
        )
        .await;

        let second = projection.catch_up().await.unwrap();
        assert_eq!(
            second, 1,
            "the second catch-up folds only the new aggregate's tail, not the first"
        );
        assert_eq!(count_failure_rows(&pool).await, 2);
    }

    /// A poison event (valid aggregate type, unparseable payload) is skipped with
    /// a warning rather than aborting catch_up; valid streams still fold.
    #[tokio::test]
    async fn catch_up_skips_unparseable_failure_payload() {
        let pool = setup_test_db().await;

        let valid_id = OffchainOrderId::new().to_string();
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &valid_id,
            1,
            &offchain_order_failed(100),
        )
        .await;
        insert_raw_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &Uuid::new_v4().to_string(),
            1,
            "not-valid-json",
        )
        .await;

        let replayed = LifecycleFailureProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 1,
            "the valid failure folds; the poison row is skipped, not propagated"
        );
        assert_eq!(count_failure_rows(&pool).await, 1);
    }

    /// Contiguous-sequence checkpoint: a poison row buried between valid events on
    /// the SAME aggregate caps that aggregate's checkpoint below the gap, so the
    /// poison (and the tail after it) is re-scanned on every catch-up rather than
    /// silently skipped past -- and the failure after the gap is still recorded.
    #[tokio::test]
    async fn catch_up_caps_checkpoint_below_intra_aggregate_poison_row() {
        let pool = setup_test_db().await;
        let aggregate_id = OffchainOrderId::new().to_string();

        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &aggregate_id,
            1,
            &offchain_order_failed(100),
        )
        .await;
        insert_raw_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &aggregate_id,
            2,
            "not-valid-json",
        )
        .await;
        persist_event(
            &pool,
            OffchainOrder::AGGREGATE_TYPE,
            &aggregate_id,
            3,
            &offchain_order_failed(300),
        )
        .await;

        let projection = LifecycleFailureProjection::new(pool.clone());

        let first = projection.catch_up().await.unwrap();
        assert_eq!(
            first, 2,
            "both valid failures (seq 1 and seq 3) fold; the poison row at seq 2 is skipped"
        );
        assert_eq!(
            count_failure_rows(&pool).await,
            2,
            "no silent loss: the failure after the poison row is still recorded"
        );
        assert_eq!(
            fetch_checkpoint(&pool, OffchainOrder::AGGREGATE_TYPE, &aggregate_id).await,
            Some(1),
            "the checkpoint stops at seq 1, below the poison at seq 2, not advancing to 3"
        );

        let second = projection.catch_up().await.unwrap();
        assert_eq!(
            second, 1,
            "the poison gap keeps seq 3 in the re-scanned tail rather than skipping past it"
        );
        assert_eq!(
            count_failure_rows(&pool).await,
            2,
            "dedup: re-folding seq 3 produces no duplicate row"
        );
        assert_eq!(
            fetch_checkpoint(&pool, OffchainOrder::AGGREGATE_TYPE, &aggregate_id).await,
            Some(1),
            "the checkpoint stays capped until the corrupt seq 2 payload is repaired"
        );
    }
}
