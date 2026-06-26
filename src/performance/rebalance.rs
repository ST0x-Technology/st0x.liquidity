//! Rebalance stage timing read model maintained forward-only by a reactor.
//!
//! [`RebalanceTimingProjection`] subscribes to the `UsdcRebalance` event
//! stream and maintains the `rebalance_stage_timing` table: one row per
//! operation holding the accumulated per-stage breakdown (conversion,
//! withdrawal, CCTP burn -> attestation -> mint, deposit). The report read
//! path ([`load_rebalance_timings`]) queries ONLY that table and never folds
//! the `events` table.
//!
//! Each operation's accumulated state is stored as a JSON [`StoredOperation`]
//! so the reactor can apply one event at a time (load -> apply -> upsert),
//! reproducing the same stage breakdown the old fold computed over a full
//! stream. The read path converts each stored operation into the dashboard DTO.
//!
//! Checkpointed catch-up: [`RebalanceTimingProjection::catch_up`] runs at
//! startup, before the live reactor consumes events, replaying everything past
//! each operation's persisted `last_sequence` checkpoint. The live reactor is
//! forward-only WHILE running, but an event it drops (a crash between an event
//! being persisted and this table being updated) is replayed on the next
//! startup -- so the read model converges on the event log instead of staying
//! best-effort until someone rebuilds. Catch-up reads only the un-folded tail,
//! so a restart does not re-fold the whole history -- except a poison row (an
//! unparseable event or a corrupt stored row), which is skipped and re-scanned
//! on every catch-up until a `rebuild_all` repairs it.
//!
//! Rebuildable: the fold is a pure function of event payloads (no
//! processing-time clock) and writes upsert by `operation_id`, so replaying the
//! event log reproduces the exact state the live reactor produced. Catch-up
//! replays only the tail since the last checkpoint;
//! [`RebalanceTimingProjection::rebuild_all`] truncates and re-folds every
//! stream. That replay-equals-live property is what makes this a valid CQRS/ES
//! projection: the event log stays the single source of truth.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use tracing::warn;

use st0x_dto::{
    AttestationSample, RebalanceOperationTiming, RebalanceStageName, RebalanceStageStats,
    RebalanceStageTiming, RebalanceTimingStatus, RebalanceTimings, StageOutcome,
};
use st0x_event_sorcery::{EntityList, EventSourced, Reactor, deps};
use st0x_finance::Usdc;

use super::{PerformanceError, ReportRange, latency_stats};
use crate::usdc_rebalance::{
    RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent, UsdcRebalanceId,
};

/// Stage-breakdown rows returned per report; the full operation count is
/// still reported via `total_operations`.
const MAX_OPERATION_REPORTS: usize = 100;

/// Load rebalance stage timings for operations started within `range`.
///
/// Reads ONLY the reactor-maintained `rebalance_stage_timing` table. Rows whose
/// stored timing is undeserializable are skipped with a warning rather than
/// failing the whole report.
pub(crate) async fn load_rebalance_timings(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<RebalanceTimings, PerformanceError> {
    let from = range.from.to_rfc3339();
    let to = range.to.to_rfc3339();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT operation_id, timing FROM rebalance_stage_timing \
         WHERE started_at >= ? AND started_at <= ?",
    )
    .bind(&from)
    .bind(&to)
    .fetch_all(pool)
    .await?;

    let mut skipped_operations: u32 = 0;
    let operations = rows
        .into_iter()
        .filter_map(|(operation_id, timing)| {
            match serde_json::from_str::<StoredOperation>(&timing) {
                Ok(stored) => Some(stored),
                Err(error) => {
                    warn!(%operation_id, %error, "Skipping undeserializable rebalance timing row");
                    skipped_operations = skipped_operations.saturating_add(1);
                    None
                }
            }
        })
        .map(WindowedOperation::from_stored)
        .collect();

    Ok(rebalance_timing_report(
        operations,
        range,
        skipped_operations,
    ))
}

/// A report operation paired with its observation time (`first_seen_at`), the
/// timestamp the SQL window/sort key uses. Mid-stream-first operations have a
/// `None` genuine `started_at` but always carry a `first_seen_at`, so windowing
/// and ordering by it keeps them visible instead of sorting them to the tail and
/// dropping them first on truncation.
struct WindowedOperation {
    first_seen_at: DateTime<Utc>,
    operation: RebalanceOperationTiming,
}

impl WindowedOperation {
    fn from_stored(stored: StoredOperation) -> Self {
        Self {
            first_seen_at: stored.first_seen_at,
            operation: stored.into_dto(),
        }
    }
}

/// Assemble the dashboard report from windowed operations.
///
/// `skipped_operations` is the count of read-model rows that failed to
/// deserialize upstream; it is surfaced verbatim so a malformed row is visible.
fn rebalance_timing_report(
    operations: Vec<WindowedOperation>,
    range: &ReportRange,
    skipped_operations: u32,
) -> RebalanceTimings {
    // Window by observation time (`first_seen_at`), not the genuine `started_at`:
    // an operation first observed mid-stream has no known start but a valid
    // observation time, so filtering by `first_seen_at` keeps it in range instead
    // of dropping it on a `None` start.
    let mut operations: Vec<WindowedOperation> = operations
        .into_iter()
        .filter(|windowed| range.contains(windowed.first_seen_at))
        .collect();

    let stage_summary = stage_summary(&operations);

    let mut attestation_trend: Vec<AttestationSample> = operations
        .iter()
        .flat_map(|windowed| &windowed.operation.stages)
        .filter(|stage| {
            stage.stage == RebalanceStageName::Attestation
                && stage.outcome == StageOutcome::Succeeded
        })
        .filter_map(|stage| {
            Some(AttestationSample {
                burned_at: stage.started_at,
                duration_ms: stage.duration_ms?,
            })
        })
        .collect();
    attestation_trend.sort_by_key(|sample| sample.burned_at);

    // Order by observation time (`first_seen_at`), not the genuine `started_at`:
    // a mid-stream-first operation has no genuine start but must still keep its
    // recency-ranked slot, otherwise it would sort to the tail and be the first
    // dropped when the row cap fires.
    operations.sort_unstable_by_key(|windowed| std::cmp::Reverse(windowed.first_seen_at));
    let total_operations = operations.len();
    operations.truncate(MAX_OPERATION_REPORTS);

    RebalanceTimings {
        operations: operations
            .into_iter()
            .map(|windowed| windowed.operation)
            .collect(),
        total_operations,
        skipped_operations,
        stage_summary,
        attestation_trend,
    }
}

/// Percentiles per stage over `Succeeded` stage runs only.
///
/// Returns a SPARSE list: a stage with no successful samples is omitted, not
/// emitted with a zero entry. Only `Succeeded` runs carry a duration meaningful
/// for percentiles -- `Failed` and `Unmeasured` runs are excluded.
fn stage_summary(operations: &[WindowedOperation]) -> Vec<RebalanceStageStats> {
    use RebalanceStageName::*;

    [Conversion, Withdrawal, Burn, Attestation, Mint, Deposit]
        .into_iter()
        .filter_map(|name| {
            let mut samples: Vec<i64> = operations
                .iter()
                .flat_map(|windowed| &windowed.operation.stages)
                .filter(|stage| stage.stage == name && stage.outcome == StageOutcome::Succeeded)
                .filter_map(|stage| stage.duration_ms)
                .collect();
            Some(RebalanceStageStats {
                stage: name,
                stats: latency_stats(&mut samples)?,
            })
        })
        .collect()
}

/// Reactor maintaining the rebalance stage-timing read model from live events.
pub(crate) struct RebalanceTimingProjection {
    pool: SqlitePool,
}

deps!(RebalanceTimingProjection, [UsdcRebalance]);

impl RebalanceTimingProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn load_operation_tx(
        tx: &mut Transaction<'_, Sqlite>,
        operation_id: &UsdcRebalanceId,
    ) -> Result<Option<StoredOperation>, ProjectionError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT timing FROM rebalance_stage_timing WHERE operation_id = ?")
                .bind(operation_id.to_string())
                .fetch_optional(&mut **tx)
                .await?;

        row.map(|(timing,)| serde_json::from_str::<StoredOperation>(&timing))
            .transpose()
            .map_err(Into::into)
    }

    async fn save_operation_tx(
        tx: &mut Transaction<'_, Sqlite>,
        operation: &StoredOperation,
        checkpoint: Option<i64>,
    ) -> Result<(), ProjectionError> {
        let timing = serde_json::to_string(operation)?;
        sqlx::query(
            "INSERT INTO rebalance_stage_timing (operation_id, started_at, timing, last_sequence) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(operation_id) DO UPDATE SET \
             started_at = excluded.started_at, timing = excluded.timing, \
             last_sequence = COALESCE(excluded.last_sequence, rebalance_stage_timing.last_sequence)",
        )
        .bind(operation.operation_id.to_string())
        // The `started_at` column orders and windows rows, so it tracks the
        // first OBSERVED event even when the genuine start is still unknown
        // (`StoredOperation::started_at` is `None`); without this a mid-stream
        // operation would have no sort/filter key.
        .bind(operation.first_seen_at.to_rfc3339())
        .bind(timing)
        // The event sequence just folded in -- set by replay/catch-up so a later
        // catch-up skips it. The live reactor has no access to the sequence and
        // passes `None`, so `COALESCE` leaves the prior checkpoint untouched.
        .bind(checkpoint)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn on_event(
        &self,
        operation_id: UsdcRebalanceId,
        event: UsdcRebalanceEvent,
    ) -> Result<(), ProjectionError> {
        // Load, apply, and save in one transaction so a save failure cannot
        // drop the event after a partial in-memory apply.
        let mut tx = self.pool.begin().await?;

        let mut operation = Self::load_operation_tx(&mut tx, &operation_id)
            .await?
            .unwrap_or_else(|| StoredOperation::new(operation_id.clone(), observed_at(&event)));
        operation.apply(&operation_id, &event);
        Self::save_operation_tx(&mut tx, &operation, None).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Catches the read model up to the event log, then returns events replayed.
    ///
    /// Replays only events past each operation's stored `last_sequence`
    /// checkpoint, so a restart re-folds the small tail emitted since the last
    /// catch-up rather than the whole event history. Run at startup, before the
    /// live reactor begins consuming events: this both seeds the table after a
    /// fresh deploy and recovers anything the forward-only live path dropped in
    /// the previous run, so the live path has no permanent drop window.
    pub(crate) async fn catch_up(&self) -> Result<u64, ProjectionError> {
        let mut tx = self.pool.begin().await?;

        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Rebuilds the entire `rebalance_stage_timing` table from the event log.
    ///
    /// Truncates the table and re-folds every stream from scratch via
    /// [`Self::replay_pending`] (an empty table has no checkpoints, so every
    /// event is past `COALESCE(last_sequence, 0)`). Use to repair a corrupted
    /// read model when an incremental [`Self::catch_up`] is not enough. Runs in
    /// one transaction: a failure mid-rebuild rolls back, never leaving a
    /// half-rebuilt table. Returns the number of events replayed.
    pub(crate) async fn rebuild_all(&self) -> Result<u64, ProjectionError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM rebalance_stage_timing")
            .execute(&mut *tx)
            .await?;
        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Folds every `UsdcRebalance` event past its operation's checkpoint into the
    /// read model, advancing each operation's `last_sequence` as it goes.
    ///
    /// The fold reads only event payloads (no processing-time clock) and writes
    /// upsert by `operation_id`, so replaying reproduces the exact state live
    /// processing produced. Operations are independent single-stream folds, so
    /// cross-aggregate ordering is irrelevant; within an operation the
    /// `ORDER BY ... sequence` clause preserves event order. The `LEFT JOIN`
    /// yields only un-folded events, so a steady-state catch-up reads just the
    /// tail rather than the whole history.
    ///
    /// A row whose aggregate id or event payload cannot be parsed -- or whose
    /// existing stored timing row is itself corrupt JSON -- is skipped with a
    /// warning rather than propagated: a single poison row must not brick startup
    /// (and the `rebuild_all` repair path, which calls this too). A corrupt stored
    /// row is recoverable via `rebuild_all`, which truncates first so it never
    /// reloads the bad row. The returned count reflects only the events that were
    /// successfully folded. Database errors still propagate and abort the
    /// transaction -- they are infrastructure failures, not poison rows.
    ///
    /// Once a stream hits a poison row, the rest of that stream is left unfolded
    /// for this pass: a later event must not advance the checkpoint past the
    /// poison (which would bury it) nor fold onto the indeterminate state left
    /// by the skipped event. Its checkpoint therefore holds at the last good
    /// sequence before the poison, so the poison -- and everything after it --
    /// is re-scanned (re-skipped, re-warned) on every catch-up until
    /// `rebuild_all` repairs it. The "reads just the tail" bound still holds for
    /// healthy streams; a poison stream stays visible rather than silently
    /// buried.
    async fn replay_pending(tx: &mut Transaction<'_, Sqlite>) -> Result<u64, ProjectionError> {
        let pending: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT event.aggregate_id, event.sequence, event.payload \
             FROM events event \
             LEFT JOIN rebalance_stage_timing timing \
               ON timing.operation_id = event.aggregate_id \
             WHERE event.aggregate_type = ? \
               AND event.sequence > COALESCE(timing.last_sequence, 0) \
             ORDER BY event.aggregate_id, event.sequence ASC",
        )
        .bind(UsdcRebalance::AGGREGATE_TYPE)
        .fetch_all(&mut **tx)
        .await?;

        let mut replayed: u64 = 0;
        // Streams that hit a poison row this pass. Events arrive ordered by
        // (aggregate_id, sequence), so once a stream is poisoned every later
        // event of it must be left unfolded: folding them would advance the
        // checkpoint past the poison (burying it) and fold onto an
        // indeterminate state (the skipped event's effect is missing).
        let mut poisoned_aggregates = HashSet::<String>::new();
        for (aggregate_id, sequence, payload) in pending {
            // Already poisoned earlier in this pass; the skip was logged then.
            if poisoned_aggregates.contains(&aggregate_id) {
                continue;
            }

            let operation_id: UsdcRebalanceId = match aggregate_id.parse() {
                Ok(operation_id) => operation_id,
                Err(error) => {
                    warn!(
                        %aggregate_id,
                        %error,
                        "Skipping rebalance event with an unparseable aggregate id"
                    );
                    poisoned_aggregates.insert(aggregate_id);
                    continue;
                }
            };
            let event: UsdcRebalanceEvent = match serde_json::from_str(&payload) {
                Ok(event) => event,
                Err(error) => {
                    warn!(
                        %operation_id,
                        sequence,
                        %error,
                        "Skipping unparseable rebalance event payload"
                    );
                    poisoned_aggregates.insert(aggregate_id);
                    continue;
                }
            };

            let mut operation = match Self::load_operation_tx(tx, &operation_id).await {
                Ok(Some(operation)) => operation,
                Ok(None) => StoredOperation::new(operation_id.clone(), observed_at(&event)),
                Err(ProjectionError::State(error)) => {
                    warn!(
                        %operation_id,
                        sequence,
                        %error,
                        "Skipping rebalance event: corrupt stored timing row (repair with view rebuild --all)"
                    );
                    poisoned_aggregates.insert(aggregate_id);
                    continue;
                }
                Err(error @ ProjectionError::Database(_)) => return Err(error),
            };
            operation.apply(&operation_id, &event);
            Self::save_operation_tx(tx, &operation, Some(sequence)).await?;

            replayed += 1;
        }

        Ok(replayed)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("rebalance stage-timing read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("rebalance operation timing (de)serialization failed")]
    State(#[from] serde_json::Error),
}

#[async_trait]
impl Reactor for RebalanceTimingProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move { self.on_event(id, event).await })
            .exhaustive()
            .await
    }
}

/// Accumulated per-operation timing state, persisted as JSON.
///
/// Mirrors the dashboard [`RebalanceOperationTiming`] DTO but with serde-
/// friendly domain types so the reactor can load, apply one event, and upsert.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOperation {
    operation_id: UsdcRebalanceId,
    direction: Option<RebalanceDirection>,
    amount: Option<Usdc>,
    /// Timestamp of the FIRST event this read model observed for the operation.
    /// Always set, even when the genuine start was missed. It backs the
    /// `started_at` SQL column so every row has a stable sort/window key.
    first_seen_at: DateTime<Utc>,
    /// Genuine operation start, seeded ONLY by a first-phase start event:
    /// `ConversionInitiated` (the AlpacaToBase pre-withdrawal conversion) or
    /// `WithdrawalSubmitting` / `Initiated` (the BaseToAlpaca withdrawal). Stays
    /// `None` when the operation was first observed mid-stream (e.g. at
    /// `BridgeAttestationReceived` after a deploy), leaving `total_ms`
    /// unmeasured rather than folding in an arbitrary mid-pipeline timestamp.
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    status: StoredStatus,
    /// Set when an `OperatorReconciled` resolved the operation out-of-band. Its
    /// `completed_at` is the manual reconciliation time, so the round-trip
    /// `total_ms` is suppressed to keep the operator-response window out of
    /// liquidity latency metrics. The operation still reports as `Completed`.
    operator_reconciled: bool,
    stages: Vec<StoredStage>,
}

/// Operation status, mirroring [`RebalanceTimingStatus`] with serde support.
//
// These leaf enums (`StoredStatus`, `StoredStageName`) mirror their DTO
// counterparts because the DTO enums derive `Serialize` + `TS` but not
// `Deserialize`; the persisted JSON must round-trip through the read model, so
// the reactor needs serde-deserializable stand-ins it converts into the DTO at
// read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStatus {
    InProgress,
    Completed,
    Failed,
}

/// One stage run, mirroring [`RebalanceStageTiming`] with serde support.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStage {
    stage: StoredStageName,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
    outcome: StoredStageOutcome,
}

/// Outcome of a stage run, mirroring [`StageOutcome`] with serde support.
//
// Same rationale as `StoredStatus`: the DTO `StageOutcome` derives
// `Serialize` + `TS` but not `Deserialize`, so the persisted state needs this
// deserializable stand-in. `InProgress` is read-model-internal: a stage that
// has opened but not closed has no DTO outcome yet, surfacing as `Unmeasured`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStageOutcome {
    InProgress,
    Succeeded,
    Failed,
    Unmeasured,
}

/// Stage identity, mirroring [`RebalanceStageName`] with serde support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStageName {
    Conversion,
    Withdrawal,
    Burn,
    Attestation,
    Mint,
    Deposit,
}

impl StoredOperation {
    fn new(operation_id: UsdcRebalanceId, first_seen_at: DateTime<Utc>) -> Self {
        Self {
            operation_id,
            direction: None,
            amount: None,
            first_seen_at,
            started_at: None,
            completed_at: None,
            status: StoredStatus::InProgress,
            operator_reconciled: false,
            stages: Vec::new(),
        }
    }

    /// Apply one event to the accumulated state, mirroring the per-event arms
    /// of the original full-stream fold.
    ///
    /// Idempotent under redelivery: every stage open goes through
    /// [`Self::open_if_closed`] and every close/record-unmeasured arm is a no-op
    /// when the target run is already in its terminal shape, so replaying a
    /// fully-applied event sequence yields the same stage list and durations.
    fn apply(&mut self, operation_id: &UsdcRebalanceId, event: &UsdcRebalanceEvent) {
        use StoredStageName::*;

        match event {
            UsdcRebalanceEvent::ConversionInitiated {
                direction,
                amount,
                initiated_at,
                ..
            } => {
                self.direction.get_or_insert(*direction);
                self.amount.get_or_insert(*amount);
                // `ConversionInitiated` is the genuine first phase only for
                // `AlpacaToBase` (the pre-withdrawal conversion). For
                // `BaseToAlpaca` it is the LAST phase (post-deposit), so seeding
                // a start from it would record a mid-pipeline timestamp when the
                // reactor first observes a `BaseToAlpaca` op here after a deploy.
                if *direction == RebalanceDirection::AlpacaToBase {
                    self.started_at.get_or_insert(*initiated_at);
                }
                self.open_once(Conversion, *initiated_at);
            }
            UsdcRebalanceEvent::ConversionConfirmed {
                direction,
                converted_at,
                ..
            } => {
                self.close(
                    operation_id,
                    Conversion,
                    *converted_at,
                    StoredStageOutcome::Succeeded,
                );
                if *direction == RebalanceDirection::BaseToAlpaca {
                    self.status = StoredStatus::Completed;
                    self.completed_at = Some(*converted_at);
                }
            }
            UsdcRebalanceEvent::ConversionFailed { failed_at, .. } => {
                self.close(
                    operation_id,
                    Conversion,
                    *failed_at,
                    StoredStageOutcome::Failed,
                );
                self.status = StoredStatus::Failed;
            }
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction,
                amount,
                submitting_at,
                ..
            } => {
                self.direction.get_or_insert(*direction);
                self.amount.get_or_insert(*amount);
                // The withdrawal is the genuine first phase only for
                // `BaseToAlpaca`. For `AlpacaToBase` it runs mid-pipeline (after
                // the initial conversion), so seeding a start from it would
                // record a mid-pipeline timestamp when the reactor first
                // observes an `AlpacaToBase` op here after a deploy.
                if *direction == RebalanceDirection::BaseToAlpaca {
                    self.started_at.get_or_insert(*submitting_at);
                }
                self.open_once(Withdrawal, *submitting_at);
            }
            UsdcRebalanceEvent::Initiated {
                direction,
                amount,
                initiated_at,
                ..
            } => {
                self.direction.get_or_insert(*direction);
                self.amount.get_or_insert(*amount);
                // Same direction gating as `WithdrawalSubmitting`: the
                // withdrawal is the genuine first phase only for `BaseToAlpaca`.
                if *direction == RebalanceDirection::BaseToAlpaca {
                    self.started_at.get_or_insert(*initiated_at);
                }
                self.open_once(Withdrawal, *initiated_at);
            }
            UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at, .. } => {
                self.close(
                    operation_id,
                    Withdrawal,
                    *confirmed_at,
                    StoredStageOutcome::Succeeded,
                );
            }
            UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. } => {
                self.close(
                    operation_id,
                    Withdrawal,
                    *failed_at,
                    StoredStageOutcome::Failed,
                );
                self.status = StoredStatus::Failed;
            }
            UsdcRebalanceEvent::BridgingSubmitting { submitting_at, .. } => {
                self.open_once(Burn, *submitting_at);
            }
            UsdcRebalanceEvent::BridgingInitiated { burned_at, .. } => {
                // Streams without a BridgingSubmitting intent (direct path or
                // pre-intent history) have no measurable burn duration; a
                // synthetic zero would drag the stage percentiles down.
                if self.open_run_index(Burn).is_some() {
                    self.close(
                        operation_id,
                        Burn,
                        *burned_at,
                        StoredStageOutcome::Succeeded,
                    );
                } else {
                    self.record_unmeasured(Burn, *burned_at);
                }
                self.open_once(Attestation, *burned_at);
            }
            UsdcRebalanceEvent::BridgeAttestationReceived { attested_at, .. } => {
                self.close(
                    operation_id,
                    Attestation,
                    *attested_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(Mint, *attested_at);
            }
            // Recoverable stall: the attestation stage simply stays open
            // until BridgeAttestationReceived or BridgingFailed.
            UsdcRebalanceEvent::AttestationTimedOut { .. } => {}
            UsdcRebalanceEvent::Bridged { minted_at, .. } => {
                self.close(
                    operation_id,
                    Mint,
                    *minted_at,
                    StoredStageOutcome::Succeeded,
                );
            }
            UsdcRebalanceEvent::BridgingFailed { failed_at, .. } => {
                self.close_open_stages(*failed_at, StoredStageOutcome::Failed);
                self.status = StoredStatus::Failed;
            }
            UsdcRebalanceEvent::BridgingCompletionRecovered { recovered_at, .. } => {
                self.close_open_stages(*recovered_at, StoredStageOutcome::Succeeded);
                // The mint demonstrably happened, but its true duration is
                // unknown (it was discovered after the fact).
                self.record_unmeasured(Mint, *recovered_at);
                self.status = StoredStatus::InProgress;
            }
            UsdcRebalanceEvent::DepositInitiated {
                deposit_initiated_at,
                ..
            } => {
                self.open_once(Deposit, *deposit_initiated_at);
            }
            UsdcRebalanceEvent::DepositConfirmed {
                direction,
                deposit_confirmed_at,
            } => {
                self.close(
                    operation_id,
                    Deposit,
                    *deposit_confirmed_at,
                    StoredStageOutcome::Succeeded,
                );
                if *direction == RebalanceDirection::AlpacaToBase {
                    self.status = StoredStatus::Completed;
                    self.completed_at = Some(*deposit_confirmed_at);
                }
            }
            UsdcRebalanceEvent::DepositFailed { failed_at, .. } => {
                self.close(
                    operation_id,
                    Deposit,
                    *failed_at,
                    StoredStageOutcome::Failed,
                );
                self.status = StoredStatus::Failed;
            }
            UsdcRebalanceEvent::OperatorReconciled {
                direction,
                amount,
                reconciled_at,
                ..
            } => {
                self.direction.get_or_insert(*direction);
                self.amount.get_or_insert(*amount);
                // Funds were settled out-of-band: the operation is resolved
                // at reconciliation time, but no pipeline stage ran -- the
                // failed Deposit stage stays failed so percentiles ignore it.
                // `reconciled_at` is a manual operator action (possibly hours
                // or days later), so flag the operation to exclude its
                // round-trip total_ms from latency metrics.
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*reconciled_at);
                self.operator_reconciled = true;
            }
        }
    }

    /// Open `stage` only if no run for it exists yet. Each pipeline stage runs
    /// at most once per operation, so this guards two cases: a redundant start
    /// marker (e.g. `Initiated` after `WithdrawalSubmitting`, while the run is
    /// still open) must not reset the clock, and a redelivered start event
    /// (whose run already closed) must not push a duplicate run.
    fn open_once(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        if self.has_run(stage) {
            return;
        }

        self.stages.push(StoredStage {
            stage,
            started_at: at,
            ended_at: None,
            duration_ms: None,
            outcome: StoredStageOutcome::InProgress,
        });
    }

    fn close(
        &mut self,
        operation_id: &UsdcRebalanceId,
        stage: StoredStageName,
        at: DateTime<Utc>,
        outcome: StoredStageOutcome,
    ) {
        let Some(index) = self.open_run_index(stage) else {
            // No OPEN run to close. Either a genuine end-without-start, or a
            // redelivered end event whose run already closed -- both are no-ops
            // here. The already-closed run keeps its recorded duration, so
            // redelivery is idempotent. Only warn when no run for the stage
            // exists at all (the true end-without-start case).
            if !self.has_run(stage) {
                warn!(
                    %operation_id,
                    ?stage,
                    "Stage end event without a matching start; skipping stage timing"
                );
            }

            return;
        };
        let run = &mut self.stages[index];
        run.ended_at = Some(at);
        // Clamped like the hedge pipeline samples: clock skew between event
        // sources must not push negative durations into percentiles.
        run.duration_ms = Some((at - run.started_at).num_milliseconds().max(0));
        run.outcome = outcome;
    }

    /// Close every stage open as of `at`, e.g. when bridging fails or recovers
    /// without per-stage end markers. A no-op when no stage is open, so a
    /// redelivered terminal event does not re-close already-closed runs.
    ///
    /// Only stages whose run started at or before `at` are closed. A restart
    /// catch-up re-folds the whole stream onto already-folded state, so the
    /// loaded state can already hold a stage opened by an event LATER than this
    /// terminal one; without the `started_at <= at` guard, re-applying an earlier
    /// `BridgingFailed`/`BridgingCompletionRecovered` would retroactively close
    /// that later stage and diverge from the live single-pass fold. The guard
    /// keeps the fold order-independent, so replay equals live.
    ///
    /// The guard equates "opened by a later event" with "started_at > at", which
    /// holds whenever a stream's event timestamps rise with sequence -- the normal
    /// case. A severe backward clock step (e.g. NTP) that stamps a later
    /// stage-open earlier than an earlier terminal event could still let a re-fold
    /// diverge; that residual is accepted here -- the impact is a single
    /// read-model latency metric, never financial state, and a full rebuild
    /// repairs it. Closing fully order-independently under such inversion would
    /// require threading the event `sequence` through the fold (an event-sorcery
    /// change deliberately left out of this PR's scope).
    fn close_open_stages(&mut self, at: DateTime<Utc>, outcome: StoredStageOutcome) {
        for run in self
            .stages
            .iter_mut()
            .filter(|run| run.ended_at.is_none() && run.started_at <= at)
        {
            run.ended_at = Some(at);
            run.duration_ms = Some((at - run.started_at).num_milliseconds().max(0));
            run.outcome = outcome;
        }
    }

    /// Record a stage that demonstrably happened but whose duration cannot
    /// be measured from the stream; excluded from percentile summaries. A no-op
    /// when any run for the stage already exists, so a redelivered event does
    /// not push a duplicate (whether the prior run was measured or unmeasured).
    fn record_unmeasured(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        if self.has_run(stage) {
            return;
        }

        self.stages.push(StoredStage {
            stage,
            started_at: at,
            ended_at: Some(at),
            duration_ms: None,
            outcome: StoredStageOutcome::Unmeasured,
        });
    }

    fn open_run_index(&self, stage: StoredStageName) -> Option<usize> {
        self.stages
            .iter()
            .rposition(|run| run.stage == stage && run.ended_at.is_none())
    }

    fn has_run(&self, stage: StoredStageName) -> bool {
        self.stages.iter().any(|run| run.stage == stage)
    }

    /// Convert the accumulated state into the dashboard DTO.
    fn into_dto(self) -> RebalanceOperationTiming {
        // Round-trip latency requires a genuine start and a completion; an
        // out-of-band operator reconciliation is excluded so the manual
        // response window never pollutes the metric.
        let total_ms = match (self.started_at, self.completed_at, self.operator_reconciled) {
            (Some(started_at), Some(completed_at), false) => {
                Some((completed_at - started_at).num_milliseconds().max(0))
            }
            _ => None,
        };
        let UsdcRebalanceId(operation_id) = self.operation_id;

        RebalanceOperationTiming {
            operation_id,
            direction: self.direction.map(Into::into),
            amount: self.amount,
            started_at: self.started_at,
            completed_at: self.completed_at,
            status: self.status.into(),
            stages: self.stages.into_iter().map(StoredStage::into_dto).collect(),
            total_ms,
        }
    }
}

impl StoredStage {
    fn into_dto(self) -> RebalanceStageTiming {
        RebalanceStageTiming {
            stage: self.stage.into(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.duration_ms,
            outcome: self.outcome.into(),
        }
    }
}

impl From<StoredStageOutcome> for StageOutcome {
    fn from(outcome: StoredStageOutcome) -> Self {
        match outcome {
            // An open stage has no terminal DTO outcome yet; surface it as
            // unmeasured so it is excluded from percentiles like any other
            // duration-less run.
            StoredStageOutcome::InProgress | StoredStageOutcome::Unmeasured => Self::Unmeasured,
            StoredStageOutcome::Succeeded => Self::Succeeded,
            StoredStageOutcome::Failed => Self::Failed,
        }
    }
}

impl From<StoredStatus> for RebalanceTimingStatus {
    fn from(status: StoredStatus) -> Self {
        match status {
            StoredStatus::InProgress => Self::InProgress,
            StoredStatus::Completed => Self::Completed,
            StoredStatus::Failed => Self::Failed,
        }
    }
}

impl From<StoredStageName> for RebalanceStageName {
    fn from(stage: StoredStageName) -> Self {
        match stage {
            StoredStageName::Conversion => Self::Conversion,
            StoredStageName::Withdrawal => Self::Withdrawal,
            StoredStageName::Burn => Self::Burn,
            StoredStageName::Attestation => Self::Attestation,
            StoredStageName::Mint => Self::Mint,
            StoredStageName::Deposit => Self::Deposit,
        }
    }
}

/// The timestamp at which the read model observed `event`, used to anchor a
/// freshly-seen operation's `first_seen_at` (the SQL window/sort key).
///
/// For `OperatorReconciled` this is `reconciled_at` -- when the reconciliation
/// actually happened -- NOT the original `initiated_at` (which may be weeks old).
/// A recently-reconciled old operation must fall inside recent query windows;
/// anchoring it to the stale `initiated_at` would hide exactly the out-of-band
/// recoveries operators want to see after a restart.
fn observed_at(event: &UsdcRebalanceEvent) -> DateTime<Utc> {
    match event {
        UsdcRebalanceEvent::ConversionInitiated { initiated_at, .. }
        | UsdcRebalanceEvent::Initiated { initiated_at, .. } => *initiated_at,
        UsdcRebalanceEvent::OperatorReconciled { reconciled_at, .. } => *reconciled_at,
        UsdcRebalanceEvent::ConversionConfirmed { converted_at, .. } => *converted_at,
        UsdcRebalanceEvent::ConversionFailed { failed_at, .. }
        | UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. }
        | UsdcRebalanceEvent::BridgingFailed { failed_at, .. }
        | UsdcRebalanceEvent::DepositFailed { failed_at, .. } => *failed_at,
        UsdcRebalanceEvent::WithdrawalSubmitting { submitting_at, .. }
        | UsdcRebalanceEvent::BridgingSubmitting { submitting_at, .. } => *submitting_at,
        UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at, .. } => *confirmed_at,
        UsdcRebalanceEvent::BridgingInitiated { burned_at, .. } => *burned_at,
        UsdcRebalanceEvent::BridgeAttestationReceived { attested_at, .. } => *attested_at,
        UsdcRebalanceEvent::AttestationTimedOut { timed_out_at, .. } => *timed_out_at,
        UsdcRebalanceEvent::Bridged { minted_at, .. } => *minted_at,
        UsdcRebalanceEvent::BridgingCompletionRecovered { recovered_at, .. } => *recovered_at,
        UsdcRebalanceEvent::DepositInitiated {
            deposit_initiated_at,
            ..
        } => *deposit_initiated_at,
        UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at,
            ..
        } => *deposit_confirmed_at,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, TxHash, fixed_bytes};
    use chrono::TimeZone;
    use uuid::Uuid;

    use st0x_dto::UsdcBridgeDirection;
    use st0x_event_sorcery::{ReactorHarness, StoreBuilder, test_store};
    use st0x_execution::ClientOrderId;
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{ReconcileReason, TransferRef, UsdcRebalanceCommand};

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    fn range() -> ReportRange {
        ReportRange {
            from: timestamp(0),
            to: timestamp(1_000_000),
        }
    }

    fn alpaca_to_base_happy_path() -> Vec<UsdcRebalanceEvent> {
        vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                filled_amount: Usdc::new(float!(1000)),
                converted_at: timestamp(10),
            },
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                from_block: 1,
                submitting_at: timestamp(20),
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                withdrawal_ref: TransferRef::OnchainTx(TxHash::random()),
                initiated_at: timestamp(21),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(50),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 2,
                submitting_at: timestamp(60),
                burn_amount: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(70),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![1],
                cctp_nonce: B256::random(),
                message: None,
                mint_scan_from_block: None,
                attested_at: timestamp(670),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                minted_at: timestamp(700),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(710),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_confirmed_at: timestamp(730),
            },
        ]
    }

    fn stage(
        operation: &RebalanceOperationTiming,
        name: RebalanceStageName,
    ) -> &RebalanceStageTiming {
        operation
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .unwrap()
    }

    /// Fold an event stream by applying each event to a fresh stored operation,
    /// exactly as the reactor would, retaining the observation time so it can be
    /// fed to [`rebalance_timing_report`] like the production load path does.
    fn fold_windowed(
        operation_id: &str,
        events: &[UsdcRebalanceEvent],
    ) -> Option<WindowedOperation> {
        let id = UsdcRebalanceId(Uuid::new_v5(&Uuid::NAMESPACE_OID, operation_id.as_bytes()));
        let first = events.first()?;
        let mut operation = StoredOperation::new(id.clone(), observed_at(first));

        for event in events {
            operation.apply(&id, event);
        }

        Some(WindowedOperation::from_stored(operation))
    }

    /// Fold an event stream into the dashboard DTO, exactly as the reactor would.
    fn fold_operation(
        operation_id: &str,
        events: &[UsdcRebalanceEvent],
    ) -> Option<RebalanceOperationTiming> {
        Some(fold_windowed(operation_id, events)?.operation)
    }

    /// Drives a sequence of `UsdcRebalance` events through the reactor, then
    /// loads the report from the read-model table.
    async fn run_through_reactor(
        events: &[UsdcRebalanceEvent],
    ) -> (SqlitePool, UsdcRebalanceId, RebalanceTimings) {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(pool.clone()));
        let operation_id = UsdcRebalanceId(Uuid::new_v4());

        for event in events {
            harness
                .receive::<UsdcRebalance>(operation_id.clone(), event.clone())
                .await
                .unwrap();
        }

        let report = load_rebalance_timings(&pool, &range()).await.unwrap();

        (pool, operation_id, report)
    }

    /// Snapshots the full `rebalance_stage_timing` table so live and rebuilt
    /// state can be compared row-for-row, including the accumulated timing JSON.
    async fn fetch_timing_rows(pool: &SqlitePool) -> Vec<(String, String, String)> {
        sqlx::query_as(
            "SELECT operation_id, started_at, timing FROM rebalance_stage_timing \
             ORDER BY operation_id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// The projection is only a valid CQRS/ES read model if replaying the event
    /// log through the reactor reproduces the state live processing produced.
    /// This drives two independent `UsdcRebalance` streams through the live
    /// reactor (so events are persisted AND the table is written), then rebuilds
    /// the table from the event log alone and asserts the two are byte-identical.
    #[tokio::test]
    async fn rebuild_all_reproduces_live_reactor_table_state() {
        let pool = setup_test_db().await;
        // Production wiring: the reactor is a registered query, so each `send`
        // persists the event to the store AND drives the live read-model write.
        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .with(std::sync::Arc::new(RebalanceTimingProjection::new(
                pool.clone(),
            )))
            .build(())
            .await
            .unwrap();

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        // Operation A: withdraw -> burn -> fail -> recover, five events spanning
        // multiple stages plus a failure and recovery.
        let recovered = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &recovered,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &recovered,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &recovered,
                UsdcRebalanceCommand::InitiateBridging { burn_tx },
            )
            .await
            .unwrap();
        store
            .send(
                &recovered,
                UsdcRebalanceCommand::FailBridging {
                    reason: "transient receipt error".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &recovered,
                UsdcRebalanceCommand::RecoverBridging {
                    mint_tx,
                    amount_received: Usdc::new(float!(399.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();

        // Operation B: a second, independent stream still mid-withdrawal. Proves
        // the rebuild keeps per-aggregate rows independent.
        let withdrawing = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &withdrawing,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();

        let live = fetch_timing_rows(&pool).await;
        assert_eq!(live.len(), 2, "two operations should produce two rows");

        let replayed = RebalanceTimingProjection::new(pool.clone())
            .rebuild_all()
            .await
            .unwrap();
        assert_eq!(replayed, 6, "all six persisted events should replay");

        let rebuilt = fetch_timing_rows(&pool).await;
        assert_eq!(
            live, rebuilt,
            "rebuilding from the event log must reproduce the exact live table state"
        );
    }

    /// Startup catch-up must replay only the events past each operation's
    /// checkpoint -- the property that lets a restart skip re-folding history.
    /// Persists events with no reactor attached (so the read model is behind,
    /// exactly what catch-up reconciles), then asserts the second catch-up folds
    /// only the newly-appended tail and converges on a full rebuild.
    #[tokio::test]
    async fn catch_up_replays_only_events_past_the_checkpoint() {
        let pool = setup_test_db().await;
        // No reactor registered: `send` persists events to the store but never
        // writes the read model, leaving it behind the log.
        let store = test_store::<UsdcRebalance>(pool.clone(), ());

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        // Operation A: three events.
        let operation_a = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &operation_a,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_a,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_a,
                UsdcRebalanceCommand::InitiateBridging { burn_tx },
            )
            .await
            .unwrap();

        // Operation B: one event, never touched again.
        let operation_b = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &operation_b,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();

        let projection = RebalanceTimingProjection::new(pool.clone());

        let first = projection.catch_up().await.unwrap();
        assert_eq!(first, 4, "first catch-up folds every persisted event");
        assert_eq!(
            fetch_timing_rows(&pool).await.len(),
            2,
            "both operations land"
        );

        // Two more events arrive on operation A; operation B stays current.
        store
            .send(
                &operation_a,
                UsdcRebalanceCommand::FailBridging {
                    reason: "transient receipt error".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_a,
                UsdcRebalanceCommand::RecoverBridging {
                    mint_tx,
                    amount_received: Usdc::new(float!(399.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();

        let second = projection.catch_up().await.unwrap();
        assert_eq!(
            second, 2,
            "second catch-up folds only operation A's new tail, not the history"
        );

        // Incremental catch-up converges on the same state as a from-scratch
        // rebuild: correct, not just cheap.
        let incremental = fetch_timing_rows(&pool).await;
        projection.rebuild_all().await.unwrap();
        let rebuilt = fetch_timing_rows(&pool).await;
        assert_eq!(
            incremental, rebuilt,
            "incremental catch-up must match a full rebuild"
        );
    }

    /// Reads an operation's persisted replay checkpoint (`NULL` -> `None`).
    async fn fetch_last_sequence(pool: &SqlitePool, operation_id: &UsdcRebalanceId) -> Option<i64> {
        let row: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT last_sequence FROM rebalance_stage_timing WHERE operation_id = ?",
        )
        .bind(operation_id.to_string())
        .fetch_optional(pool)
        .await
        .unwrap();

        row.and_then(|(last_sequence,)| last_sequence)
    }

    /// The production restart path: the live reactor writes rows with
    /// `last_sequence = NULL`, so the next startup `catch_up` re-folds the ENTIRE
    /// stream on top of the already-folded state, and that re-fold must reproduce
    /// the live state exactly. This drives an operation that recovers from a
    /// post-burn bridging failure and is left mid-deposit (Deposit stage open) --
    /// the case where a non-order-independent `close_open_stages` would
    /// retroactively fail the still-open Deposit when the earlier `BridgingFailed`
    /// is re-applied. Asserts the re-fold leaves the table byte-identical to live.
    #[tokio::test]
    async fn catch_up_refold_preserves_a_stage_opened_after_a_recovered_failure() {
        let pool = setup_test_db().await;
        // Production wiring: the reactor is a registered query, so each `send`
        // persists the event AND drives the live read-model write (last_sequence
        // stays NULL).
        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .with(std::sync::Arc::new(RebalanceTimingProjection::new(
                pool.clone(),
            )))
            .build(())
            .await
            .unwrap();

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let deposit_tx =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");

        // withdraw -> burn -> bridging fails -> recovered (un-fails to Bridged)
        // -> deposit initiated but NOT confirmed, so the Deposit stage stays open.
        let operation_id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "transient receipt error".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::RecoverBridging {
                    mint_tx,
                    amount_received: Usdc::new(float!(399.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(deposit_tx),
                },
            )
            .await
            .unwrap();

        // Live state: the Deposit stage is open (InProgress), still unmeasured.
        let live = fetch_timing_rows(&pool).await;

        // The live rows carry `last_sequence = NULL`, so catch_up re-folds all six
        // events on top of the already-folded state -- the routine restart path.
        let replayed = RebalanceTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 6,
            "every live-written event is re-folded (last_sequence was NULL)"
        );
        assert_eq!(
            fetch_last_sequence(&pool, &operation_id).await,
            Some(6),
            "catch-up advances the checkpoint from NULL to the last folded sequence"
        );

        let after = fetch_timing_rows(&pool).await;
        assert_eq!(
            live, after,
            "catch-up re-fold must reproduce the live state -- the open Deposit \
             stage must stay open, not be retroactively failed by the re-applied \
             BridgingFailed"
        );
    }

    /// The live reactor writes `last_sequence = NULL` (`checkpoint = None`), and
    /// the upsert's `COALESCE(excluded.last_sequence, ...)` must keep a checkpoint
    /// a prior catch-up already set. Otherwise a single live write after catch-up
    /// would null the checkpoint and the next restart would re-fold the whole
    /// history. Catches up to set the checkpoint, fires one more live event, and
    /// asserts the checkpoint survives.
    #[tokio::test]
    async fn live_write_preserves_the_catch_up_checkpoint() {
        let pool = setup_test_db().await;
        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .with(std::sync::Arc::new(RebalanceTimingProjection::new(
                pool.clone(),
            )))
            .build(())
            .await
            .unwrap();

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let operation_id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        let projection = RebalanceTimingProjection::new(pool.clone());
        projection.catch_up().await.unwrap();
        assert_eq!(
            fetch_last_sequence(&pool, &operation_id).await,
            Some(2),
            "catch-up checkpoints the operation at its last folded sequence"
        );

        // A live write (checkpoint = None) must not clobber the catch-up checkpoint.
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx },
            )
            .await
            .unwrap();
        assert_eq!(
            fetch_last_sequence(&pool, &operation_id).await,
            Some(2),
            "the live write's None checkpoint leaves the prior checkpoint untouched"
        );

        // Proof the checkpoint held: the next catch-up re-folds only the one event
        // past it, not the whole stream.
        let replayed = projection.catch_up().await.unwrap();
        assert_eq!(
            replayed, 1,
            "only the single event past the preserved checkpoint is re-folded"
        );
    }

    /// Inserts a raw `UsdcRebalance` event row straight into the event store --
    /// the only way to stage a poison row (malformed payload or aggregate id) that
    /// no domain command can produce.
    async fn insert_raw_rebalance_event(
        pool: &SqlitePool,
        aggregate_id: &str,
        sequence: i64,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, 'poison', '1', ?, '{}')",
        )
        .bind(UsdcRebalance::AGGREGATE_TYPE)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A poison row -- an unparseable aggregate id, an unparseable event payload,
    /// or a corrupt stored timing row -- must NOT abort catch_up (and so must not
    /// brick startup): the bad rows are skipped with a warning, valid streams
    /// still fold, and the returned count reflects only folded events. Drives a
    /// valid stream through a reactor-less store (events persisted, read model
    /// behind), then stages each poison class directly, since no domain API can
    /// produce them.
    #[tokio::test]
    async fn catch_up_skips_poison_rows_and_folds_the_rest() {
        let pool = setup_test_db().await;
        // No reactor: events persist but the read model stays behind, exactly what
        // catch_up reconciles.
        let store = test_store::<UsdcRebalance>(pool.clone(), ());

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        // A valid stream that must still fold despite the poison rows around it.
        let valid_id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &valid_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &valid_id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        // A stream whose stored timing row is corrupt JSON: catch_up must skip it
        // (recoverable via rebuild_all) rather than bricking startup.
        let corrupt_row_id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &corrupt_row_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO rebalance_stage_timing (operation_id, started_at, timing) \
             VALUES (?, ?, ?)",
        )
        .bind(corrupt_row_id.to_string())
        .bind(timestamp(0).to_rfc3339())
        .bind("not-valid-json")
        .execute(&pool)
        .await
        .unwrap();

        // A raw event row with an unparseable payload, and one with an unparseable
        // aggregate id.
        insert_raw_rebalance_event(&pool, &Uuid::new_v4().to_string(), 1, "not-valid-json").await;
        insert_raw_rebalance_event(&pool, "not-a-uuid", 1, "not-valid-json").await;

        // None of the poison rows abort catch_up; only the valid stream folds.
        let replayed = RebalanceTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 2,
            "only the two events of the valid stream fold; every poison row is skipped"
        );

        let rows = fetch_timing_rows(&pool).await;
        assert_eq!(
            rows.len(),
            2,
            "the valid fold and the untouched corrupt row remain; no poison-event rows are created"
        );

        let valid = valid_id.to_string();
        assert!(
            rows.iter()
                .any(|(operation_id, _, _)| operation_id == &valid),
            "the valid stream folded into a row"
        );

        // The corrupt row is left exactly as staged -- catch_up skipped it rather
        // than overwriting or repairing it.
        let corrupt = corrupt_row_id.to_string();
        let corrupt_row = rows
            .iter()
            .find(|(operation_id, _, _)| operation_id == &corrupt)
            .expect("the corrupt row remains in place");
        assert_eq!(
            corrupt_row.2, "not-valid-json",
            "catch_up does not touch the corrupt stored row"
        );

        // The valid stream is checkpointed; the skipped corrupt row is not (so it
        // stays visible and re-scanned until a rebuild repairs it).
        assert_eq!(
            fetch_last_sequence(&pool, &valid_id).await,
            Some(2),
            "the valid stream advances its checkpoint"
        );
        assert_eq!(
            fetch_last_sequence(&pool, &corrupt_row_id).await,
            None,
            "the skipped corrupt row is never checkpointed"
        );

        // A second catch-up converges: the valid stream is past its checkpoint and
        // the poison rows are re-skipped, so nothing new folds.
        let second = RebalanceTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            second, 0,
            "the valid stream is caught up and every poison row is re-skipped"
        );
    }

    /// A poison event in the MIDDLE of a stream must not let later valid events
    /// advance the checkpoint past it: that would bury the poison (the
    /// `sequence > last_sequence` filter would stop re-scanning it) and fold the
    /// tail onto the indeterminate state left by the skipped event. Stages a
    /// real `valid(1), poison(2), valid(3)` stream by corrupting the middle
    /// event's payload after the fact.
    #[tokio::test]
    async fn catch_up_holds_checkpoint_behind_a_mid_stream_poison_event() {
        let pool = setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());

        let amount = Usdc::new(float!(400.0));
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        // One stream with three valid events at sequences 1, 2, 3.
        let operation_id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &operation_id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx },
            )
            .await
            .unwrap();

        // Corrupt the middle event into a poison payload: now valid(1),
        // poison(2), valid(3) within the one stream.
        sqlx::query(
            "UPDATE events SET payload = 'not-valid-json' \
             WHERE aggregate_type = ? AND aggregate_id = ? AND sequence = 2",
        )
        .bind(UsdcRebalance::AGGREGATE_TYPE)
        .bind(operation_id.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let replayed = RebalanceTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 1,
            "only seq 1 folds; the poison at seq 2 stops the stream, so seq 3 is left unfolded"
        );

        // The checkpoint holds at the last good sequence before the poison, not
        // seq 3 -- otherwise the poison at seq 2 would be buried from re-scans.
        assert_eq!(
            fetch_last_sequence(&pool, &operation_id).await,
            Some(1),
            "the checkpoint stays at seq 1 so the poison at seq 2 stays visible"
        );

        // A second catch-up re-scans from seq 2, re-skips the poison, and folds
        // nothing new: seq 3 stays blocked behind the poison until a rebuild.
        let second = RebalanceTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            second, 0,
            "the poison keeps blocking the tail; nothing new folds"
        );
    }

    #[test]
    fn alpaca_to_base_completes_with_all_stage_durations() {
        let operation = fold_operation("op-1", &alpaca_to_base_happy_path()).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.direction, Some(UsdcBridgeDirection::AlpacaToBase));
        // The happy path opens with ConversionInitiated, which seeds the
        // genuine start.
        assert_eq!(operation.started_at, Some(timestamp(0)));
        assert_eq!(operation.completed_at, Some(timestamp(730)));
        assert_eq!(operation.total_ms, Some(730_000));

        use RebalanceStageName::*;
        assert_eq!(stage(&operation, Conversion).duration_ms, Some(10_000));
        assert_eq!(stage(&operation, Withdrawal).duration_ms, Some(30_000));
        assert_eq!(stage(&operation, Burn).duration_ms, Some(10_000));
        assert_eq!(stage(&operation, Attestation).duration_ms, Some(600_000));
        assert_eq!(stage(&operation, Mint).duration_ms, Some(30_000));
        assert_eq!(stage(&operation, Deposit).duration_ms, Some(20_000));
    }

    #[test]
    fn base_to_alpaca_completes_on_final_conversion() {
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(0),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(30),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::AlpacaId(Uuid::new_v4().into()),
                deposit_initiated_at: timestamp(40),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                deposit_confirmed_at: timestamp(100),
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(110),
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                filled_amount: Usdc::new(float!(500)),
                converted_at: timestamp(150),
            },
        ];

        let operation = fold_operation("op-2", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.direction, Some(UsdcBridgeDirection::BaseToAlpaca));
        assert_eq!(operation.completed_at, Some(timestamp(150)));
        assert_eq!(operation.total_ms, Some(150_000));
        // The mid-flow deposit confirmation must not complete the operation.
        assert_eq!(
            stage(&operation, RebalanceStageName::Deposit).duration_ms,
            Some(60_000)
        );
    }

    #[test]
    fn withdrawal_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(0),
            },
            UsdcRebalanceEvent::WithdrawalFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(5),
            },
        ];

        let operation = fold_operation("op-3", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(operation.completed_at, None);
        assert_eq!(operation.total_ms, None);
        let withdrawal = stage(&operation, RebalanceStageName::Withdrawal);
        assert_eq!(withdrawal.outcome, StageOutcome::Failed);
        assert_eq!(withdrawal.duration_ms, Some(5_000));
    }

    #[test]
    fn attestation_timeout_keeps_stage_open_until_received() {
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::AttestationTimedOut {
                burn_tx_hash: TxHash::random(),
                retry_deadline_at: timestamp(10_000),
                timed_out_at: timestamp(1_800),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![1],
                cctp_nonce: B256::random(),
                message: None,
                mint_scan_from_block: None,
                attested_at: timestamp(3_600),
            },
        ];

        let operation = fold_operation("op-4", &events).unwrap();

        let attestation = stage(&operation, RebalanceStageName::Attestation);
        assert_eq!(attestation.duration_ms, Some(3_600_000));
        assert_eq!(attestation.outcome, StageOutcome::Succeeded);
    }

    #[test]
    fn bridging_failure_then_recovery_returns_to_in_progress() {
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(TxHash::random()),
                cctp_nonce: None,
                reason: "poll exhausted".to_string(),
                failed_at: timestamp(100),
            },
            UsdcRebalanceEvent::BridgingCompletionRecovered {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                recovered_at: timestamp(200),
            },
        ];

        let operation = fold_operation("op-5", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::InProgress);
        // Burn (unmeasured: no submitting intent), Attestation, Mint
        // (unmeasured: discovered by recovery).
        assert_eq!(operation.stages.len(), 3);
        let burn = stage(&operation, RebalanceStageName::Burn);
        assert_eq!(burn.duration_ms, None);
        assert_eq!(burn.outcome, StageOutcome::Unmeasured);
        let attestation = stage(&operation, RebalanceStageName::Attestation);
        assert_eq!(attestation.outcome, StageOutcome::Failed);
        assert_eq!(attestation.duration_ms, Some(100_000));
        // The recovered mint happened but its duration is unknowable.
        let mint = stage(&operation, RebalanceStageName::Mint);
        assert_eq!(mint.duration_ms, None);
        assert_eq!(mint.outcome, StageOutcome::Unmeasured);
    }

    #[test]
    fn deposit_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "deposit revert".to_string(),
                failed_at: timestamp(20),
            },
        ];

        let operation = fold_operation("op-6", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        let deposit = stage(&operation, RebalanceStageName::Deposit);
        assert_eq!(deposit.outcome, StageOutcome::Failed);
        assert_eq!(deposit.duration_ms, Some(20_000));
    }

    #[test]
    fn operator_reconcile_completes_stranded_deposit_failure() {
        let events = vec![
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "deposit revert".to_string(),
                failed_at: timestamp(20),
            },
            UsdcRebalanceEvent::OperatorReconciled {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                reason: ReconcileReason::FundsMovedManually,
                initiated_at: timestamp(0),
                reconciled_at: timestamp(500),
            },
        ];

        let operation = fold_operation("op-7", &events).unwrap();

        // The operation is resolved, but the failed Deposit stage keeps its
        // failure so stage percentiles never absorb a manual recovery.
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(500)));
        // OperatorReconciled completion is out-of-band: total_ms is suppressed
        // so the operator-response window stays out of latency metrics. The
        // stream here also never carried a genuine start, so started_at is None.
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.total_ms, None);
        assert_eq!(
            operation.direction,
            Some(st0x_dto::UsdcBridgeDirection::AlpacaToBase)
        );
        let deposit = stage(&operation, RebalanceStageName::Deposit);
        assert_eq!(deposit.outcome, StageOutcome::Failed);
    }

    #[test]
    fn conversion_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::ConversionFailed {
                reason: "order rejected".to_string(),
                failed_at: timestamp(8),
            },
        ];

        let operation = fold_operation("op-7", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        let conversion = stage(&operation, RebalanceStageName::Conversion);
        assert_eq!(conversion.outcome, StageOutcome::Failed);
        assert_eq!(conversion.duration_ms, Some(8_000));
    }

    #[test]
    fn negative_stage_durations_clamp_to_zero() {
        // Clock skew: the confirmation timestamp precedes the submission.
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(10),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(5),
                withdrawal_tx: None,
            },
        ];

        let operation = fold_operation("op-8", &events).unwrap();

        let withdrawal = stage(&operation, RebalanceStageName::Withdrawal);
        assert_eq!(withdrawal.duration_ms, Some(0));
    }

    #[test]
    fn report_caps_operation_rows_but_counts_all() {
        let operations: Vec<WindowedOperation> = (0..=i64::try_from(MAX_OPERATION_REPORTS)
            .unwrap())
            .map(|index| {
                fold_windowed(
                    &format!("op-{index}"),
                    &[UsdcRebalanceEvent::WithdrawalSubmitting {
                        direction: RebalanceDirection::BaseToAlpaca,
                        amount: Usdc::new(float!(1)),
                        from_block: 1,
                        submitting_at: timestamp(index),
                    }],
                )
                .unwrap()
            })
            .collect();

        let report = rebalance_timing_report(operations, &range(), 0);

        assert_eq!(report.total_operations, MAX_OPERATION_REPORTS + 1);
        assert_eq!(report.operations.len(), MAX_OPERATION_REPORTS);
    }

    #[test]
    fn report_summarizes_stages_and_attestation_trend() {
        let first = fold_windowed("op-1", &alpaca_to_base_happy_path()).unwrap();

        let report = rebalance_timing_report(vec![first], &range(), 0);

        assert_eq!(report.total_operations, 1);
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.skipped_operations, 0);
        assert_eq!(report.attestation_trend.len(), 1);
        assert_eq!(report.attestation_trend[0].burned_at, timestamp(70));
        assert_eq!(report.attestation_trend[0].duration_ms, 600_000);

        let attestation_stats = report
            .stage_summary
            .iter()
            .find(|entry| entry.stage == RebalanceStageName::Attestation)
            .unwrap();
        assert_eq!(attestation_stats.stats.p50_ms, 600_000);
        assert_eq!(attestation_stats.stats.sample_count, 1);
    }

    #[test]
    fn report_excludes_operations_outside_range() {
        // The happy path is first observed at timestamp(0), which is outside the
        // narrow window, so windowing by `first_seen_at` excludes it entirely.
        let operation = fold_windowed("op-1", &alpaca_to_base_happy_path()).unwrap();
        let narrow = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };

        let report = rebalance_timing_report(vec![operation], &narrow, 0);

        assert_eq!(report.total_operations, 0);
        assert!(report.operations.is_empty());
        assert!(report.stage_summary.is_empty());
        assert!(report.attestation_trend.is_empty());
    }

    #[test]
    fn report_keeps_mid_stream_operation_with_unknown_start_in_window() {
        // A mid-stream-first operation has a `None` genuine start but a valid
        // observation time inside the window. It must be retained and windowed by
        // `first_seen_at`, not dropped because its `started_at` is unknown.
        let mid_stream = fold_windowed(
            "op-mid-stream-window",
            &[
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![1],
                    cctp_nonce: B256::random(),
                    message: None,
                    mint_scan_from_block: None,
                    attested_at: timestamp(150_000),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: TxHash::random(),
                    amount_received: Usdc::new(float!(999)),
                    fee_collected: Usdc::new(float!(1)),
                    minted_at: timestamp(150_030),
                },
            ],
        )
        .unwrap();
        // Sanity: the genuine start is unknown, only the observation time backs it.
        assert_eq!(mid_stream.operation.started_at, None);
        let narrow = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };

        let report = rebalance_timing_report(vec![mid_stream], &narrow, 0);

        assert_eq!(report.total_operations, 1);
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.operations[0].started_at, None);
        assert_eq!(report.operations[0].total_ms, None);
    }

    #[tokio::test]
    async fn load_rebalance_timings_reads_reactor_table() {
        let (_pool, operation_id, report) = run_through_reactor(&alpaca_to_base_happy_path()).await;

        assert_eq!(report.total_operations, 1);
        let operation = &report.operations[0];
        let UsdcRebalanceId(expected_id) = operation_id;
        assert_eq!(operation.operation_id, expected_id);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        // Stage timings must survive the per-event reactor -> DB -> read path.
        assert_eq!(operation.total_ms, Some(730_000));
        assert_eq!(
            stage(operation, RebalanceStageName::Attestation).duration_ms,
            Some(600_000)
        );
        assert_eq!(
            stage(operation, RebalanceStageName::Deposit).duration_ms,
            Some(20_000)
        );
    }

    #[tokio::test]
    async fn range_filter_excludes_out_of_range_reactor_written_operations() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(pool.clone()));
        let operation_id = UsdcRebalanceId(Uuid::new_v4());

        for event in alpaca_to_base_happy_path() {
            harness
                .receive::<UsdcRebalance>(operation_id.clone(), event)
                .await
                .unwrap();
        }

        let narrow = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };
        let report = load_rebalance_timings(&pool, &narrow).await.unwrap();

        assert_eq!(report.total_operations, 0);
        assert!(report.operations.is_empty());
    }

    #[tokio::test]
    async fn operator_reconcile_windowed_by_reconciled_at_not_initiated_at() {
        // A manual reconciliation done today for an operation initiated weeks ago
        // (the reactor's only observed event) must be windowed by reconciled_at,
        // not the stale initiated_at -- otherwise a recent out-of-band recovery
        // would vanish from a recent-window query right when operators need it.
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(pool.clone()));
        let operation_id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                operation_id.clone(),
                UsdcRebalanceEvent::OperatorReconciled {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000)),
                    reason: ReconcileReason::FundsMovedManually,
                    // initiated_at is far outside the window below.
                    initiated_at: timestamp(0),
                    // reconciled_at falls inside the window below.
                    reconciled_at: timestamp(150_000),
                },
            )
            .await
            .unwrap();

        let recent_window = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };
        let report = load_rebalance_timings(&pool, &recent_window).await.unwrap();

        // The operation is windowed by reconciled_at, so it appears.
        assert_eq!(report.total_operations, 1);
        let operation = &report.operations[0];
        let UsdcRebalanceId(expected_id) = operation_id;
        assert_eq!(operation.operation_id, expected_id);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        // No genuine start event was observed, so the round-trip stays
        // unmeasured even though completion is known.
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.completed_at, Some(timestamp(150_000)));
        assert_eq!(operation.total_ms, None);
    }

    #[test]
    fn operator_reconcile_as_only_event_has_no_start_and_no_total_ms() {
        // When the reactor only sees OperatorReconciled, there is no genuine
        // start event, so started_at stays None and the round-trip total_ms is
        // unmeasured -- doubly so, because operator-reconciled completion is an
        // out-of-band manual action that must never enter latency metrics.
        let events = vec![UsdcRebalanceEvent::OperatorReconciled {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            reason: ReconcileReason::FundsMovedManually,
            initiated_at: timestamp(100),
            reconciled_at: timestamp(600),
        }];

        let operation = fold_operation("op-reconcile-first", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.completed_at, Some(timestamp(600)));
        assert_eq!(operation.total_ms, None);
    }

    #[test]
    fn operator_reconcile_excluded_from_total_ms_even_with_genuine_start() {
        // A fully-tracked operation that ends in OperatorReconciled has a known
        // start, yet its round-trip total_ms is still suppressed so the
        // operator-response window (initiated_at -> reconciled_at) never folds
        // into the liquidity latency metric. The genuine first phase for
        // AlpacaToBase is the pre-withdrawal conversion, so the stream opens with
        // ConversionInitiated to seed a real start.
        let events = vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(10),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "deposit revert".to_string(),
                failed_at: timestamp(20),
            },
            UsdcRebalanceEvent::OperatorReconciled {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                reason: ReconcileReason::FundsMovedManually,
                initiated_at: timestamp(0),
                reconciled_at: timestamp(90_000),
            },
        ];

        let operation = fold_operation("op-reconcile-tracked", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, Some(timestamp(0)));
        assert_eq!(operation.completed_at, Some(timestamp(90_000)));
        // Despite a known start and completion, the manual reconciliation keeps
        // this out of round-trip latency.
        assert_eq!(operation.total_ms, None);
    }

    #[test]
    fn mid_stream_first_event_leaves_start_and_total_ms_unmeasured() {
        // The reactor first observes the operation at BridgeAttestationReceived
        // (e.g. it started before a deploy). No genuine start event is ever
        // seen, so started_at must stay None and total_ms unmeasured rather than
        // adopting the mid-pipeline attestation timestamp as the start.
        let events = vec![
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![1],
                cctp_nonce: B256::random(),
                message: None,
                mint_scan_from_block: None,
                attested_at: timestamp(670),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                minted_at: timestamp(700),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(710),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_confirmed_at: timestamp(730),
            },
        ];

        let operation = fold_operation("op-mid-stream", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.completed_at, Some(timestamp(730)));
        assert_eq!(operation.total_ms, None);
        // The mint stage still measures from attestation -> Bridged.
        assert_eq!(
            stage(&operation, RebalanceStageName::Mint).duration_ms,
            Some(30_000)
        );
    }

    #[test]
    fn alpaca_to_base_first_observed_at_withdrawal_leaves_start_unmeasured() {
        // For AlpacaToBase the genuine first phase is the pre-withdrawal
        // conversion (ConversionInitiated). The withdrawal runs mid-pipeline, so
        // if the reactor first observes the operation at WithdrawalSubmitting
        // (e.g. after a deploy that missed the conversion), started_at must stay
        // None rather than adopt the mid-pipeline withdrawal timestamp.
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                from_block: 1,
                submitting_at: timestamp(20),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(50),
                withdrawal_tx: None,
            },
        ];

        let operation = fold_operation("op-a2b-mid-withdrawal", &events).unwrap();

        assert_eq!(operation.direction, Some(UsdcBridgeDirection::AlpacaToBase));
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.total_ms, None);
        // The withdrawal stage still measures since both endpoints were seen.
        assert_eq!(
            stage(&operation, RebalanceStageName::Withdrawal).duration_ms,
            Some(30_000)
        );
    }

    #[test]
    fn base_to_alpaca_first_observed_at_conversion_leaves_start_unmeasured() {
        // For BaseToAlpaca the genuine first phase is the withdrawal. The
        // post-deposit conversion (ConversionInitiated) runs LAST, so if the
        // reactor first observes the operation at ConversionInitiated (e.g. after
        // a deploy that missed the earlier phases), started_at must stay None
        // rather than adopt the late-pipeline conversion timestamp.
        let events = vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(110),
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                filled_amount: Usdc::new(float!(500)),
                converted_at: timestamp(150),
            },
        ];

        let operation = fold_operation("op-b2a-mid-conversion", &events).unwrap();

        assert_eq!(operation.direction, Some(UsdcBridgeDirection::BaseToAlpaca));
        // BaseToAlpaca completes on the final conversion, so it is Completed with
        // a known completed_at -- but no genuine start was observed, so the
        // round-trip total_ms stays unmeasured.
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.completed_at, Some(timestamp(150)));
        assert_eq!(operation.total_ms, None);
        // The conversion stage still measures since both endpoints were seen.
        assert_eq!(
            stage(&operation, RebalanceStageName::Conversion).duration_ms,
            Some(40_000)
        );
    }

    #[test]
    fn redelivering_full_event_sequence_is_idempotent() {
        // Replaying an already-applied sequence (event redelivery) must not
        // double-push stages or change any duration.
        let events = alpaca_to_base_happy_path();
        let id = UsdcRebalanceId(Uuid::new_v4());

        let mut once = StoredOperation::new(id.clone(), observed_at(&events[0]));
        for event in &events {
            once.apply(&id, event);
        }

        let mut twice = once.clone();
        for event in &events {
            twice.apply(&id, event);
        }

        let once_dto = once.into_dto();
        let twice_dto = twice.into_dto();

        assert_eq!(once_dto.stages.len(), twice_dto.stages.len());
        assert_eq!(once_dto.total_ms, twice_dto.total_ms);
        for (single, redelivered) in once_dto.stages.iter().zip(twice_dto.stages.iter()) {
            assert_eq!(single.stage, redelivered.stage);
            assert_eq!(single.started_at, redelivered.started_at);
            assert_eq!(single.ended_at, redelivered.ended_at);
            assert_eq!(single.duration_ms, redelivered.duration_ms);
            assert_eq!(single.outcome, redelivered.outcome);
        }
    }

    #[tokio::test]
    async fn load_rebalance_timings_counts_skipped_undeserializable_rows() {
        // A malformed `timing` JSON row that survives the SQL window must be
        // skipped from `operations` but surfaced via `skipped_operations` rather
        // than vanishing silently. This drives the real DB read path so the
        // `serde_json::from_str` skip-and-increment branch is exercised.
        let pool = setup_test_db().await;

        // Seed one well-formed operation through the reactor so the report is not
        // empty, proving the malformed row is dropped while the good row remains.
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(pool.clone()));
        let good_id = UsdcRebalanceId(Uuid::new_v4());
        for event in alpaca_to_base_happy_path() {
            harness
                .receive::<UsdcRebalance>(good_id.clone(), event)
                .await
                .unwrap();
        }

        // Insert a row with a valid in-window `started_at` but unparseable timing.
        sqlx::query(
            "INSERT INTO rebalance_stage_timing (operation_id, started_at, timing) \
             VALUES (?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(timestamp(5).to_rfc3339())
        .bind("not-valid-json")
        .execute(&pool)
        .await
        .unwrap();

        let report = load_rebalance_timings(&pool, &range()).await.unwrap();

        assert_eq!(report.skipped_operations, 1);
        // Only the well-formed operation survives; the malformed row is absent.
        assert_eq!(report.total_operations, 1);
        assert_eq!(report.operations.len(), 1);
        let UsdcRebalanceId(expected_id) = good_id;
        assert_eq!(report.operations[0].operation_id, expected_id);
    }

    #[test]
    fn stage_summary_excludes_failed_withdrawal_from_percentiles() {
        // A failed withdrawal stage must not contaminate the Withdrawal percentiles.
        let failed_withdrawal = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(0),
            },
            UsdcRebalanceEvent::WithdrawalFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(5),
            },
        ];
        let successful_withdrawal = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 2,
                submitting_at: timestamp(100),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(130),
                withdrawal_tx: None,
            },
        ];

        let failed_op = fold_windowed("op-fail", &failed_withdrawal).unwrap();
        let success_op = fold_windowed("op-success", &successful_withdrawal).unwrap();
        let report = rebalance_timing_report(vec![failed_op, success_op], &range(), 0);

        let withdrawal_stats = report
            .stage_summary
            .iter()
            .find(|entry| entry.stage == RebalanceStageName::Withdrawal)
            .unwrap();
        // Only the successful withdrawal (30_000 ms) must appear in the sample.
        assert_eq!(withdrawal_stats.stats.sample_count, 1);
        assert_eq!(withdrawal_stats.stats.p50_ms, 30_000);
    }

    #[test]
    fn attestation_trend_excludes_failed_attestation_in_bridging_recovery() {
        // An operation where attestation fails (BridgingFailed) and is later
        // recovered must have its failed attestation excluded from the trend.
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(TxHash::random()),
                cctp_nonce: None,
                reason: "poll exhausted".to_string(),
                failed_at: timestamp(100),
            },
            UsdcRebalanceEvent::BridgingCompletionRecovered {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                recovered_at: timestamp(200),
            },
        ];

        let operation = fold_windowed("op-bridging-recovery", &events).unwrap();
        let report = rebalance_timing_report(vec![operation], &range(), 0);

        // The failed attestation stage must not appear in the trend.
        assert!(report.attestation_trend.is_empty());
    }

    #[test]
    fn bridging_recovery_then_deposit_completes_operation() {
        // After BridgingCompletionRecovered the operation returns to InProgress.
        // Subsequent DepositInitiated + DepositConfirmed (AlpacaToBase) must
        // complete the operation with a correct total_ms.
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(TxHash::random()),
                cctp_nonce: None,
                reason: "poll exhausted".to_string(),
                failed_at: timestamp(100),
            },
            UsdcRebalanceEvent::BridgingCompletionRecovered {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                recovered_at: timestamp(200),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(210),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_confirmed_at: timestamp(250),
            },
        ];

        let operation = fold_operation("op-recovery-complete", &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(250)));
        // The stream opens at BridgingInitiated (no genuine start event), so
        // started_at stays None and the round-trip total_ms is unmeasured;
        // per-stage durations are still measured where their endpoints are known.
        assert_eq!(operation.started_at, None);
        assert_eq!(operation.total_ms, None);
        assert_eq!(
            stage(&operation, RebalanceStageName::Deposit).duration_ms,
            Some(40_000)
        );
        assert_eq!(
            stage(&operation, RebalanceStageName::Deposit).outcome,
            StageOutcome::Succeeded
        );
    }

    #[tokio::test]
    async fn reactor_redelivery_does_not_double_count_stages() {
        // Drive the full sequence through the reactor, then redeliver every
        // event. The persisted read-model row must be byte-identical in stage
        // shape: no doubled stages, no changed durations.
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(pool.clone()));
        let operation_id = UsdcRebalanceId(Uuid::new_v4());
        let events = alpaca_to_base_happy_path();

        for event in &events {
            harness
                .receive::<UsdcRebalance>(operation_id.clone(), event.clone())
                .await
                .unwrap();
        }

        let first_pass = load_rebalance_timings(&pool, &range()).await.unwrap();

        for event in &events {
            harness
                .receive::<UsdcRebalance>(operation_id.clone(), event.clone())
                .await
                .unwrap();
        }

        let second_pass = load_rebalance_timings(&pool, &range()).await.unwrap();

        assert_eq!(first_pass.total_operations, 1);
        assert_eq!(second_pass.total_operations, 1);
        let before = &first_pass.operations[0];
        let after = &second_pass.operations[0];
        assert_eq!(before.stages.len(), after.stages.len());
        assert_eq!(before.total_ms, after.total_ms);
        for (single, redelivered) in before.stages.iter().zip(after.stages.iter()) {
            assert_eq!(single.stage, redelivered.stage);
            assert_eq!(single.duration_ms, redelivered.duration_ms);
            assert_eq!(single.outcome, redelivered.outcome);
        }
    }
}
