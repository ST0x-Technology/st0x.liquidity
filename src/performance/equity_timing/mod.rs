//! Equity mint/redemption stage timing read model maintained forward-only by
//! a reactor.
//!
//! [`EquityTimingProjection`] subscribes to the `TokenizedEquityMint` and
//! `EquityRedemption` event streams and maintains the `equity_stage_timing`
//! table: one row per operation holding the accumulated per-stage breakdown
//! (mint: acceptance -> receipt -> wrap -> deposit; redemption: withdraw ->
//! unwrap -> send -> detection -> completion). The report read path
//! ([`load_equity_timings`]) queries ONLY that table and never folds the
//! `events` table.
//!
//! Structured identically to [`super::rebalance::RebalanceTimingProjection`]
//! (its module doc explains the checkpointed catch-up / rebuildable-projection
//! design in full; not repeated here), but subscribes to TWO aggregate types
//! via one [`deps!`] call -- mirroring
//! [`super::reliability::LifecycleFailureProjection`], which already
//! subscribes to four. `operation_id` (a `Uuid`) is unique across mint and
//! redemption aggregates (independently generated), so both operation kinds
//! share one read-model table keyed by `operation_id` alone, exactly like
//! [`super::rebalance`]'s single-aggregate table -- no composite
//! `(aggregate_type, aggregate_id)` key is needed the way
//! `LifecycleFailureProjection`'s checkpoint table uses one.

mod mint;
mod redemption;

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use st0x_dto::{
    EquityOperationKind, EquityOperationTiming, EquityStageName, EquityStageStats,
    EquityStageTiming, RebalanceTimingStatus, StageOutcome,
};
use st0x_event_sorcery::{EntityList, EventSourced, Reactor, deps};
use st0x_execution::Symbol;
use st0x_finance::FractionalShares;
use st0x_tokenization::IssuerRequestId;

use super::{PerformanceError, ReportRange, latency_stats};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent, RedemptionAggregateId};
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
use mint::mint_observed_at;
use redemption::redemption_observed_at;

/// Stage-breakdown rows returned per report; the full operation count is
/// still reported via `total_operations`. Matches
/// `rebalance::MAX_OPERATION_REPORTS`.
const MAX_OPERATION_REPORTS: usize = 100;

/// Load equity mint/redemption stage timings for operations started within
/// `range`.
///
/// Reads ONLY the reactor-maintained `equity_stage_timing` table. Rows whose
/// stored timing is undeserializable are skipped with a warning rather than
/// failing the whole report.
pub(crate) async fn load_equity_timings(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<EquityTimings, PerformanceError> {
    let from = range.from.to_rfc3339();
    let to = range.to.to_rfc3339();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT operation_id, timing FROM equity_stage_timing \
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
                    warn!(%operation_id, %error, "Skipping undeserializable equity timing row");
                    skipped_operations = skipped_operations.saturating_add(1);
                    None
                }
            }
        })
        .map(WindowedOperation::from_stored)
        .collect();

    Ok(equity_timing_report(operations, range, skipped_operations))
}

// Re-exported so callers (`src/api.rs`) don't need a separate `st0x_dto`
// import purely to name this endpoint's response type.
pub(crate) use st0x_dto::EquityTimings;

/// A report operation paired with its observation time (`first_seen_at`), the
/// timestamp the SQL window/sort key uses. Mid-stream-first operations have a
/// `None` genuine `started_at` but always carry a `first_seen_at`, so windowing
/// and ordering by it keeps them visible instead of sorting them to the tail
/// and dropping them first on truncation.
struct WindowedOperation {
    first_seen_at: DateTime<Utc>,
    operation: EquityOperationTiming,
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
fn equity_timing_report(
    operations: Vec<WindowedOperation>,
    range: &ReportRange,
    skipped_operations: u32,
) -> EquityTimings {
    // Window by observation time (`first_seen_at`), not the genuine
    // `started_at`: an operation first observed mid-stream has no known
    // start but a valid observation time, so filtering by `first_seen_at`
    // keeps it in range instead of dropping it on a `None` start.
    let mut operations: Vec<WindowedOperation> = operations
        .into_iter()
        .filter(|windowed| range.contains(windowed.first_seen_at))
        .collect();

    let stage_summary = stage_summary(&operations);

    // Order by observation time (`first_seen_at`), not the genuine
    // `started_at`: a mid-stream-first operation has no genuine start but
    // must still keep its recency-ranked slot, otherwise it would sort to
    // the tail and be the first dropped when the row cap fires.
    operations.sort_unstable_by_key(|windowed| std::cmp::Reverse(windowed.first_seen_at));
    let total_operations = operations.len();
    operations.truncate(MAX_OPERATION_REPORTS);

    EquityTimings {
        operations: operations
            .into_iter()
            .map(|windowed| windowed.operation)
            .collect(),
        total_operations,
        skipped_operations,
        stage_summary,
    }
}

/// Percentiles per stage over `Succeeded` stage runs only.
///
/// Returns a SPARSE list: a stage with no successful samples is omitted, not
/// emitted with a zero entry. Only `Succeeded` runs carry a duration
/// meaningful for percentiles -- `Failed` and `Unmeasured` runs are excluded.
fn stage_summary(operations: &[WindowedOperation]) -> Vec<EquityStageStats> {
    use EquityStageName::*;

    [
        MintAcceptance,
        MintReceipt,
        MintWrap,
        MintDeposit,
        RedemptionWithdraw,
        RedemptionUnwrap,
        RedemptionSend,
        RedemptionDetection,
        RedemptionCompletion,
    ]
    .into_iter()
    .filter_map(|name| {
        let mut samples: Vec<i64> = operations
            .iter()
            .flat_map(|windowed| &windowed.operation.stages)
            .filter(|stage| stage.stage == name && stage.outcome == StageOutcome::Succeeded)
            .filter_map(|stage| stage.duration_ms)
            .collect();
        Some(EquityStageStats {
            stage: name,
            stats: latency_stats(&mut samples)?,
        })
    })
    .collect()
}

/// Reactor maintaining the equity mint/redemption stage-timing read model
/// from live events.
pub(crate) struct EquityTimingProjection {
    pool: SqlitePool,
}

deps!(
    EquityTimingProjection,
    [TokenizedEquityMint, EquityRedemption]
);

impl EquityTimingProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn load_operation_tx(
        tx: &mut Transaction<'_, Sqlite>,
        operation_id: Uuid,
    ) -> Result<Option<StoredOperation>, ProjectionError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT timing FROM equity_stage_timing WHERE operation_id = ?")
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
            "INSERT INTO equity_stage_timing (operation_id, started_at, timing, last_sequence) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(operation_id) DO UPDATE SET \
             started_at = excluded.started_at, timing = excluded.timing, \
             last_sequence = COALESCE(excluded.last_sequence, equity_stage_timing.last_sequence)",
        )
        .bind(operation.operation_id.to_string())
        // The `started_at` column orders and windows rows, so it tracks the
        // first OBSERVED event even when the genuine start is still unknown
        // (`StoredOperation::started_at` is `None`); without this a
        // mid-stream operation would have no sort/filter key.
        .bind(operation.first_seen_at.to_rfc3339())
        .bind(timing)
        // The event sequence just folded in -- set by replay/catch-up so a
        // later catch-up skips it. The live reactor has no access to the
        // sequence and passes `None`, so `COALESCE` leaves the prior
        // checkpoint untouched.
        .bind(checkpoint)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn on_mint_event(
        &self,
        id: IssuerRequestId,
        event: TokenizedEquityMintEvent,
    ) -> Result<(), ProjectionError> {
        let IssuerRequestId(operation_id) = id;

        // Load, apply, and save in one transaction so a save failure cannot
        // drop the event after a partial in-memory apply.
        let mut tx = self.pool.begin().await?;

        let mut operation = Self::load_operation_tx(&mut tx, operation_id)
            .await?
            .unwrap_or_else(|| {
                StoredOperation::new(operation_id, StoredKind::Mint, mint_observed_at(&event))
            });
        operation.apply_mint(&event);
        Self::save_operation_tx(&mut tx, &operation, None).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn on_redemption_event(
        &self,
        id: RedemptionAggregateId,
        event: EquityRedemptionEvent,
    ) -> Result<(), ProjectionError> {
        let RedemptionAggregateId(operation_id) = id;

        let mut tx = self.pool.begin().await?;

        let mut operation = Self::load_operation_tx(&mut tx, operation_id)
            .await?
            .unwrap_or_else(|| {
                StoredOperation::new(
                    operation_id,
                    StoredKind::Redeem,
                    redemption_observed_at(&event),
                )
            });
        // The live reactor has no access to the event's sequence (see
        // `on_mint_event`'s `checkpoint: None`), so it can never legitimately
        // observe a legacy genesis event (see `apply_redemption`'s
        // `VaultWithdrawSubmitted`/`WithdrawnFromRaindex` arms) -- the
        // aggregate command that emits one directly no longer exists, so
        // `None` safely falls back to the mid-stream-first path there.
        operation.apply_redemption(&event, None);
        Self::save_operation_tx(&mut tx, &operation, None).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Catches the read model up to the event log, then returns events replayed.
    ///
    /// See `RebalanceTimingProjection::catch_up`'s doc for the full rationale
    /// (identical here): run at startup, before the live reactor begins
    /// consuming events.
    pub(crate) async fn catch_up(&self) -> Result<u64, ProjectionError> {
        let mut tx = self.pool.begin().await?;

        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Rebuilds the entire `equity_stage_timing` table from the event log.
    ///
    /// See `RebalanceTimingProjection::rebuild_all`'s doc for the full
    /// rationale (identical here).
    pub(crate) async fn rebuild_all(&self) -> Result<u64, ProjectionError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM equity_stage_timing")
            .execute(&mut *tx)
            .await?;
        let replayed = Self::replay_pending(&mut tx).await?;

        tx.commit().await?;

        Ok(replayed)
    }

    /// Folds every `TokenizedEquityMint`/`EquityRedemption` event past its
    /// operation's checkpoint into the read model, advancing each operation's
    /// `last_sequence` as it goes.
    ///
    /// Mirrors `RebalanceTimingProjection::replay_pending` exactly (see its
    /// doc for the full poison-row/order-independence rationale), extended
    /// to two aggregate types the same way
    /// `LifecycleFailureProjection::replay_pending` folds four: the query
    /// binds both `AGGREGATE_TYPE` constants via `IN (?, ?)`, and the loop
    /// dispatches which event type to deserialize based on the returned
    /// `aggregate_type` column.
    async fn replay_pending(tx: &mut Transaction<'_, Sqlite>) -> Result<u64, ProjectionError> {
        let pending: Vec<(String, String, i64, String)> = sqlx::query_as(
            "SELECT event.aggregate_type, event.aggregate_id, event.sequence, event.payload \
             FROM events event \
             LEFT JOIN equity_stage_timing timing \
               ON timing.operation_id = event.aggregate_id \
             WHERE event.aggregate_type IN (?, ?) \
               AND event.sequence > COALESCE(timing.last_sequence, 0) \
             ORDER BY event.aggregate_id, event.sequence ASC",
        )
        .bind(TokenizedEquityMint::AGGREGATE_TYPE)
        .bind(EquityRedemption::AGGREGATE_TYPE)
        .fetch_all(&mut **tx)
        .await?;

        let mut replayed: u64 = 0;
        // Streams that hit a poison row this pass. Events arrive ordered by
        // (aggregate_id, sequence), so once a stream is poisoned every later
        // event of it must be left unfolded: folding them would advance the
        // checkpoint past the poison (burying it) and fold onto an
        // indeterminate state (the skipped event's effect is missing).
        let mut poisoned_aggregates = HashSet::<String>::new();
        for (aggregate_type, aggregate_id, sequence, payload) in pending {
            // Already poisoned earlier in this pass; the skip was logged then.
            if poisoned_aggregates.contains(&aggregate_id) {
                continue;
            }

            let operation_id: Uuid = match aggregate_id.parse() {
                Ok(operation_id) => operation_id,
                Err(error) => {
                    warn!(
                        %aggregate_id,
                        %error,
                        "Skipping equity timing event with an unparseable aggregate id"
                    );
                    poisoned_aggregates.insert(aggregate_id);
                    continue;
                }
            };

            let fold_result = if aggregate_type == TokenizedEquityMint::AGGREGATE_TYPE {
                Self::replay_mint_event(tx, operation_id, sequence, &payload).await
            } else if aggregate_type == EquityRedemption::AGGREGATE_TYPE {
                Self::replay_redemption_event(tx, operation_id, sequence, &payload).await
            } else {
                // Unreachable given the query's `IN` filter, but handled
                // exhaustively rather than assumed away.
                warn!(%aggregate_type, "equity timing replay saw an unsubscribed aggregate type; ignoring");
                Ok(false)
            };

            match fold_result {
                Ok(true) => replayed += 1,
                Ok(false) => {}
                Err(ReplayError::Poison) => {
                    poisoned_aggregates.insert(aggregate_id);
                }
                Err(ReplayError::Database(error)) => return Err(error.into()),
            }
        }

        Ok(replayed)
    }

    /// Deserializes and folds one mint event during replay. Returns `Ok(true)`
    /// on success, `Err(ReplayError::Poison)` for an unparseable payload or
    /// corrupt stored row (logged here), or `Err(ReplayError::Database(_))`
    /// for an infrastructure failure that must abort the whole pass.
    async fn replay_mint_event(
        tx: &mut Transaction<'_, Sqlite>,
        operation_id: Uuid,
        sequence: i64,
        payload: &str,
    ) -> Result<bool, ReplayError> {
        let event: TokenizedEquityMintEvent = match serde_json::from_str(payload) {
            Ok(event) => event,
            Err(error) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping unparseable mint event payload"
                );
                return Err(ReplayError::Poison);
            }
        };

        let mut operation = match Self::load_operation_tx(tx, operation_id).await {
            Ok(Some(operation)) => operation,
            Ok(None) => {
                StoredOperation::new(operation_id, StoredKind::Mint, mint_observed_at(&event))
            }
            Err(ProjectionError::State(error)) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping mint event: corrupt stored timing row (repair with view rebuild --all)"
                );
                return Err(ReplayError::Poison);
            }
            Err(ProjectionError::Database(error)) => return Err(ReplayError::Database(error)),
        };
        operation.apply_mint(&event);
        match Self::save_operation_tx(tx, &operation, Some(sequence)).await {
            Ok(()) => {}
            Err(ProjectionError::Database(error)) => return Err(ReplayError::Database(error)),
            Err(ProjectionError::State(error)) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping mint event: failed to serialize stored timing row"
                );
                return Err(ReplayError::Poison);
            }
        }

        Ok(true)
    }

    /// Redemption counterpart to [`Self::replay_mint_event`]; same contract.
    async fn replay_redemption_event(
        tx: &mut Transaction<'_, Sqlite>,
        operation_id: Uuid,
        sequence: i64,
        payload: &str,
    ) -> Result<bool, ReplayError> {
        let event: EquityRedemptionEvent = match serde_json::from_str(payload) {
            Ok(event) => event,
            Err(error) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping unparseable redemption event payload"
                );
                return Err(ReplayError::Poison);
            }
        };

        let mut operation = match Self::load_operation_tx(tx, operation_id).await {
            Ok(Some(operation)) => operation,
            Ok(None) => StoredOperation::new(
                operation_id,
                StoredKind::Redeem,
                redemption_observed_at(&event),
            ),
            Err(ProjectionError::State(error)) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping redemption event: corrupt stored timing row (repair with view rebuild --all)"
                );
                return Err(ReplayError::Poison);
            }
            Err(ProjectionError::Database(error)) => return Err(ReplayError::Database(error)),
        };
        operation.apply_redemption(&event, Some(sequence));
        match Self::save_operation_tx(tx, &operation, Some(sequence)).await {
            Ok(()) => {}
            Err(ProjectionError::Database(error)) => return Err(ReplayError::Database(error)),
            Err(ProjectionError::State(error)) => {
                warn!(
                    %operation_id,
                    sequence,
                    %error,
                    "Skipping redemption event: failed to serialize stored timing row"
                );
                return Err(ReplayError::Poison);
            }
        }

        Ok(true)
    }
}

/// Outcome of folding one event during replay, distinct from [`ProjectionError`]
/// so a poison row (bad payload / corrupt stored state) can be handled by the
/// caller (skip + continue) while a database error still propagates and
/// aborts the transaction.
enum ReplayError {
    Poison,
    Database(sqlx::Error),
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("equity stage-timing read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("equity operation timing (de)serialization failed")]
    State(#[from] serde_json::Error),
}

#[async_trait]
impl Reactor for EquityTimingProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move { self.on_mint_event(id, event).await })
            .on(|id, event| async move { self.on_redemption_event(id, event).await })
            .exhaustive()
            .await
    }
}

/// Accumulated per-operation timing state, persisted as JSON.
///
/// Mirrors the dashboard [`EquityOperationTiming`] DTO but with serde-friendly
/// domain types so the reactor can load, apply one event, and upsert. Shared
/// by both mint and redemption operations (distinguished by `kind`) since
/// `operation_id` is unique across both.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOperation {
    operation_id: Uuid,
    kind: StoredKind,
    /// `None` only when the read model first observed the operation
    /// mid-stream (its genesis event, which alone carries the symbol/quantity,
    /// was never seen) -- mirrors `rebalance::StoredOperation::direction`'s
    /// optionality for the same reason.
    symbol: Option<Symbol>,
    quantity: Option<FractionalShares>,
    /// Timestamp of the FIRST event this read model observed for the
    /// operation. Always set, even when the genuine start was missed. It
    /// backs the `started_at` SQL column so every row has a stable
    /// sort/window key.
    first_seen_at: DateTime<Utc>,
    /// Genuine operation start, seeded ONLY by the operation's genesis event
    /// (`MintRequested` for mint, `VaultWithdrawPending` for redemption --
    /// both are the aggregate's own `initialize()` event, so this is always
    /// the true first phase, unlike USDC's direction-gated seeding). Stays
    /// `None` when the operation was first observed mid-stream.
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    status: StoredStatus,
    /// Set when an `OperatorReconciled` resolved the operation out-of-band.
    /// Its `completed_at` is the manual reconciliation time, so the
    /// round-trip `total_ms` is suppressed to keep the operator-response
    /// window out of latency metrics. The operation still reports as
    /// `Completed`.
    operator_reconciled: bool,
    stages: Vec<StoredStage>,
}

/// Which pipeline an operation belongs to, mirroring [`EquityOperationKind`]
/// with serde support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredKind {
    Mint,
    Redeem,
}

/// Operation status, mirroring [`RebalanceTimingStatus`] with serde support.
/// Reused across both `rebalance` and `equity_timing` read models (both DTOs
/// share `RebalanceTimingStatus`), but each module keeps its own serde
/// stand-in since the DTO enum derives `Serialize` + `TS` but not
/// `Deserialize` (the persisted JSON must round-trip through the read model).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStatus {
    InProgress,
    Completed,
    Failed,
}

/// One stage run, mirroring [`EquityStageTiming`] with serde support.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStage {
    stage: StoredStageName,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
    outcome: StoredStageOutcome,
}

/// Outcome of a stage run, mirroring [`StageOutcome`] with serde support.
/// `InProgress` is read-model-internal: a stage that has opened but not
/// closed has no DTO outcome yet, surfacing as `Unmeasured`.
///
/// `Placeholder` is also read-model-internal, distinct from `Unmeasured`: it
/// positively marks a run [`StoredOperation::push_closed_stage`] created
/// because a stage-closing event arrived with no open run for it (see
/// [`StoredOperation::record_unmeasured`]) -- a shape
/// [`StoredOperation::upgrade_placeholder_start`] can safely recognize and
/// backfill with the real start time once it arrives (typically via
/// catch-up's full-stream replay). A run the recovery scrub loops (mint/
/// redemption `ProviderCompletionRecovered`, `recover_receipt_stage`'s
/// already-closed-run branch) flip back from `Failed` is deliberately left as
/// plain `Unmeasured`, NOT `Placeholder`: that run's `started_at`/`ended_at`
/// came from a genuine (if now-superseded) open/close pair, so it must never
/// be mistaken for an upgradeable placeholder even if it happens to share the
/// same `started_at == ended_at` shape (e.g. a stage opened and failed at the
/// exact same timestamp). Both variants surface as `StageOutcome::Unmeasured`
/// on the dashboard -- the distinction is purely internal bookkeeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStageOutcome {
    InProgress,
    Succeeded,
    Failed,
    Unmeasured,
    Placeholder,
}

/// Stage identity, mirroring [`EquityStageName`] with serde support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredStageName {
    MintAcceptance,
    MintReceipt,
    MintWrap,
    MintDeposit,
    RedemptionWithdraw,
    RedemptionUnwrap,
    RedemptionSend,
    RedemptionDetection,
    RedemptionCompletion,
}

impl StoredOperation {
    fn new(operation_id: Uuid, kind: StoredKind, first_seen_at: DateTime<Utc>) -> Self {
        Self {
            operation_id,
            kind,
            symbol: None,
            quantity: None,
            first_seen_at,
            started_at: None,
            completed_at: None,
            status: StoredStatus::InProgress,
            operator_reconciled: false,
            stages: Vec::new(),
        }
    }

    /// Open `stage` only if no run for it exists yet. Each pipeline stage
    /// runs at most once per operation, so this guards a redelivered start
    /// event (whose run is already open or already closed) from resetting
    /// the clock or pushing a duplicate run.
    ///
    /// An existing `Placeholder` run (see [`Self::upgrade_placeholder_start`])
    /// is the one exception: it is recoverable, not "already handled", so it
    /// is upgraded in place with the real start time instead of being left
    /// untouched.
    fn open_once(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        if self.upgrade_placeholder_start(stage, at) {
            return;
        }

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

    /// Recovers a `Placeholder` run for `stage` (pushed by
    /// [`Self::record_unmeasured`] when a stage-closing event arrived with no
    /// open run for it -- e.g. because the live reactor's fold of the real
    /// start event failed and the event-sorcery reactor-bridge logged and
    /// continued past it) by backfilling its `started_at` with the real
    /// start time now arriving (typically via catch-up's full-stream
    /// replay) and recomputing `duration_ms`/`outcome` from it.
    ///
    /// A placeholder is identified by its dedicated `StoredStageOutcome`
    /// variant, NOT by shape (`started_at == ended_at` with
    /// `duration_ms: None`): a genuinely-measured run never has that shape,
    /// but a run the recovery scrub loops rescrub from `Failed` back to plain
    /// `Unmeasured` (e.g. [`Self::recover_receipt_stage`]'s
    /// already-closed-run branch) CAN coincidentally share it, if the stage's
    /// open and its terminal-failure close carried the exact same timestamp.
    /// Matching on shape would then wrongly upgrade that scrubbed run on
    /// redelivery instead of leaving it `Unmeasured`, breaking fold
    /// idempotency -- so this matches the positive `Placeholder` marker
    /// instead, which the scrub loops never produce. Returns `true` if a
    /// placeholder was found and upgraded, so [`Self::open_once`] knows not
    /// to push a fresh run.
    fn upgrade_placeholder_start(
        &mut self,
        stage: StoredStageName,
        real_started_at: DateTime<Utc>,
    ) -> bool {
        let Some(run) = self
            .stages
            .iter_mut()
            .find(|run| run.stage == stage && run.outcome == StoredStageOutcome::Placeholder)
        else {
            return false;
        };

        run.started_at = real_started_at;
        if let Some(ended_at) = run.ended_at {
            run.duration_ms = Some((ended_at - real_started_at).num_milliseconds().max(0));
            run.outcome = StoredStageOutcome::Succeeded;
        }

        true
    }

    fn close(&mut self, stage: StoredStageName, at: DateTime<Utc>, outcome: StoredStageOutcome) {
        let Some(index) = self.open_run_index(stage) else {
            // No OPEN run to close. Either a genuine end-without-start, or a
            // redelivered end event whose run already closed -- both are
            // no-ops here. The already-closed run keeps its recorded
            // duration, so redelivery is idempotent. Only warn when no run
            // for the stage exists at all (the true end-without-start case).
            if !self.has_run(stage) {
                warn!(
                    operation_id = %self.operation_id,
                    ?stage,
                    "Stage end event without a matching start; skipping stage timing"
                );
            }

            return;
        };
        let run = &mut self.stages[index];
        run.ended_at = Some(at);
        // Clamped like the USDC rebalance fold: clock skew between event
        // sources must not push negative durations into percentiles.
        run.duration_ms = Some((at - run.started_at).num_milliseconds().max(0));
        run.outcome = outcome;
    }

    /// Close every stage open as of `at`, e.g. an operator force-fail or a
    /// provider-completion recovery with no per-stage end marker. A no-op
    /// when no stage is open, so a redelivered terminal event does not
    /// re-close already-closed runs.
    ///
    /// Only stages whose run started at or before `at` are closed -- see
    /// `rebalance::StoredOperation::close_open_stages`'s doc for the full
    /// order-independence rationale (identical here).
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

    /// Push a stage run that never had a genuine open/close pair (its
    /// `started_at`/`ended_at` are both `at`, `duration_ms` is unmeasurable).
    /// A no-op when any run for the stage already exists, so a redelivered
    /// event does not push a duplicate. Shared by [`Self::record_unmeasured`]
    /// and [`Self::fail_unopened_stage`], which differ only in `outcome`.
    fn push_closed_stage(
        &mut self,
        stage: StoredStageName,
        at: DateTime<Utc>,
        outcome: StoredStageOutcome,
    ) {
        if self.has_run(stage) {
            return;
        }

        self.stages.push(StoredStage {
            stage,
            started_at: at,
            ended_at: Some(at),
            duration_ms: None,
            outcome,
        });
    }

    /// Record a stage that demonstrably happened but whose duration cannot be
    /// measured from the stream; excluded from percentile summaries.
    ///
    /// Pushed with the `Placeholder` outcome (not plain `Unmeasured`) so
    /// [`Self::upgrade_placeholder_start`] can positively identify it as
    /// recoverable if the stage's real start event arrives later (typically
    /// via catch-up's full-stream replay) -- see that method's doc for why
    /// shape alone cannot make this distinction.
    fn record_unmeasured(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        self.push_closed_stage(stage, at, StoredStageOutcome::Placeholder);
    }

    /// Close `stage` if a run is open for it, else record it as
    /// [`Self::record_unmeasured`]. Every stage-closing event in the pipeline
    /// can plausibly be the projection's first-ever observation of an
    /// operation (deploy/restart backfill, or a missed live-reactor
    /// interval): without this guard, `close` would find no open run and
    /// silently drop the stage entirely instead of recording that it
    /// demonstrably happened.
    fn close_or_record_unmeasured(
        &mut self,
        stage: StoredStageName,
        at: DateTime<Utc>,
        outcome: StoredStageOutcome,
    ) {
        if self.open_run_index(stage).is_some() {
            self.close(stage, at, outcome);
        } else {
            self.record_unmeasured(stage, at);
        }
    }

    /// Record a stage that never opened as failed, e.g. a terminal transfer
    /// failure arriving between two stages: the prior stage already closed
    /// successfully, but the next stage's start event never fired.
    fn fail_unopened_stage(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        self.push_closed_stage(stage, at, StoredStageOutcome::Failed);
    }

    /// Recover `stage` as `Unmeasured`, e.g. mint's `ProviderCompletionRecovered`.
    /// Handles both shapes recovery can find the stage in: no run yet (the
    /// failure predated the stage opening, e.g. `MintRejected`) falls back to
    /// [`Self::record_unmeasured`]; an already-closed run (the failure closed
    /// it `Failed`, e.g. `MintAcceptanceFailed`) has its outcome flipped to
    /// `Unmeasured` instead of staying permanently `Failed`, since recovery
    /// proves the stage actually succeeded even though its true duration is
    /// now unknown.
    fn recover_receipt_stage(&mut self, stage: StoredStageName, at: DateTime<Utc>) {
        match self.stages.iter_mut().find(|run| run.stage == stage) {
            Some(run) => {
                run.outcome = StoredStageOutcome::Unmeasured;
                run.duration_ms = None;
            }
            None => self.record_unmeasured(stage, at),
        }
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
    fn into_dto(self) -> EquityOperationTiming {
        // Round-trip latency requires a genuine start and a completion; an
        // out-of-band operator reconciliation is excluded so the manual
        // response window never pollutes the metric.
        let total_ms = match (self.started_at, self.completed_at, self.operator_reconciled) {
            (Some(started_at), Some(completed_at), false) => {
                Some((completed_at - started_at).num_milliseconds().max(0))
            }
            _ => None,
        };

        EquityOperationTiming {
            operation_id: self.operation_id,
            kind: self.kind.into(),
            symbol: self.symbol,
            quantity: self.quantity,
            started_at: self.started_at,
            completed_at: self.completed_at,
            status: self.status.into(),
            stages: self.stages.into_iter().map(StoredStage::into_dto).collect(),
            total_ms,
        }
    }
}

impl StoredStage {
    fn into_dto(self) -> EquityStageTiming {
        EquityStageTiming {
            stage: self.stage.into(),
            started_at: self.started_at,
            ended_at: self.ended_at,
            duration_ms: self.duration_ms,
            outcome: self.outcome.into(),
        }
    }
}

impl From<StoredKind> for EquityOperationKind {
    fn from(kind: StoredKind) -> Self {
        match kind {
            StoredKind::Mint => Self::Mint,
            StoredKind::Redeem => Self::Redeem,
        }
    }
}

impl From<StoredStageOutcome> for StageOutcome {
    fn from(outcome: StoredStageOutcome) -> Self {
        match outcome {
            // An open stage has no terminal DTO outcome yet, and a
            // `Placeholder` (see its own doc) is internal recoverable
            // bookkeeping with no genuine duration -- both surface as
            // unmeasured so they are excluded from percentiles like any
            // other duration-less run.
            StoredStageOutcome::InProgress
            | StoredStageOutcome::Unmeasured
            | StoredStageOutcome::Placeholder => Self::Unmeasured,
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

impl From<StoredStageName> for EquityStageName {
    fn from(stage: StoredStageName) -> Self {
        match stage {
            StoredStageName::MintAcceptance => Self::MintAcceptance,
            StoredStageName::MintReceipt => Self::MintReceipt,
            StoredStageName::MintWrap => Self::MintWrap,
            StoredStageName::MintDeposit => Self::MintDeposit,
            StoredStageName::RedemptionWithdraw => Self::RedemptionWithdraw,
            StoredStageName::RedemptionUnwrap => Self::RedemptionUnwrap,
            StoredStageName::RedemptionSend => Self::RedemptionSend,
            StoredStageName::RedemptionDetection => Self::RedemptionDetection,
            StoredStageName::RedemptionCompletion => Self::RedemptionCompletion,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256};
    use chrono::TimeZone;
    use std::sync::Arc;
    use uuid::Uuid;

    use st0x_dto::EquityOperationKind;
    use st0x_event_sorcery::{ReactorHarness, StoreBuilder};
    use st0x_float_macro::float;
    use st0x_tokenization::{TokenizationRequestId, issuer_request_id};

    use super::*;
    use crate::equity_redemption::redemption_aggregate_id;
    use crate::test_utils::setup_test_db;
    use crate::tokenized_equity_mint::TokenizedEquityMintCommand;

    pub(super) fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    pub(super) fn range() -> ReportRange {
        ReportRange {
            from: timestamp(0),
            to: timestamp(1_000_000),
        }
    }

    pub(super) fn symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    pub(super) fn stage(
        operation: &EquityOperationTiming,
        name: EquityStageName,
    ) -> &EquityStageTiming {
        operation
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .unwrap()
    }

    pub(super) fn mint_happy_path() -> Vec<TokenizedEquityMintEvent> {
        vec![
            TokenizedEquityMintEvent::MintRequested {
                symbol: symbol(),
                quantity: float!(5),
                wallet: Address::repeat_byte(0x11),
                requested_at: timestamp(0),
            },
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id: issuer_request_id("op"),
                tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
                accepted_at: timestamp(10),
            },
            TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::random(),
                shares_minted: U256::from(5_000_000_000_000_000_000_u128),
                fees: None,
                received_at: timestamp(30),
            },
            TokenizedEquityMintEvent::WrapSubmitted {
                wrap_tx_hash: TxHash::random(),
                submitted_at: timestamp(35),
            },
            TokenizedEquityMintEvent::TokensWrapped {
                wrap_tx_hash: TxHash::random(),
                wrapped_shares: U256::from(5_000_000_000_000_000_000_u128),
                wrapped_at: timestamp(50),
                wrap_block: Some(1),
            },
            TokenizedEquityMintEvent::VaultDepositSubmitted {
                vault_deposit_tx_hash: TxHash::random(),
                submitted_at: timestamp(55),
            },
            TokenizedEquityMintEvent::DepositedIntoRaindex {
                vault_deposit_tx_hash: TxHash::random(),
                deposited_at: timestamp(70),
            },
        ]
    }

    pub(super) fn redemption_happy_path() -> Vec<EquityRedemptionEvent> {
        vec![
            EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(0),
            },
            EquityRedemptionEvent::VaultWithdrawSubmitted {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                tx_hash: TxHash::random(),
                submitted_at: timestamp(5),
            },
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                actual_wrapped_amount: Some(U256::from(5_000_000_000_000_000_000_u128)),
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: Some(1),
                withdrawn_at: timestamp(20),
            },
            EquityRedemptionEvent::UnwrapPending {
                pending_at: timestamp(25),
            },
            EquityRedemptionEvent::UnwrapSubmitted {
                unwrap_tx_hash: TxHash::random(),
                submitted_at: timestamp(30),
            },
            EquityRedemptionEvent::TokensUnwrapped {
                quantity: Some(float!(5)),
                underlying_token: Address::repeat_byte(0x33),
                unwrap_tx_hash: TxHash::random(),
                unwrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                unwrap_block: Some(2),
                unwrapped_at: timestamp(45),
            },
            EquityRedemptionEvent::SendPending {
                pending_at: timestamp(50),
            },
            EquityRedemptionEvent::TokensSent {
                redemption_wallet: Address::repeat_byte(0x44),
                redemption_tx: TxHash::random(),
                sent_at: timestamp(60),
            },
            EquityRedemptionEvent::Detected {
                tokenization_request_id: TokenizationRequestId::try_new("tok-2").unwrap(),
                detected_at: timestamp(90),
            },
            EquityRedemptionEvent::Completed {
                completed_at: timestamp(100),
            },
        ]
    }

    pub(super) fn fold_mint(events: &[TokenizedEquityMintEvent]) -> EquityOperationTiming {
        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Mint,
            mint_observed_at(&events[0]),
        );
        for event in events {
            operation.apply_mint(event);
        }
        operation.into_dto()
    }

    /// Folds redemption events the way the live reactor does: no known
    /// sequence. Most fixtures slice `redemption_happy_path()` from the
    /// middle to simulate a mid-stream first observation, so this must NOT
    /// derive a sequence from the slice's own position (that would wrongly
    /// look like a legacy genesis to `apply_redemption`) -- use
    /// [`fold_redemption_from_genesis`] for fixtures that are genuinely a
    /// full stream starting at the aggregate's real sequence 1.
    pub(super) fn fold_redemption(events: &[EquityRedemptionEvent]) -> EquityOperationTiming {
        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Redeem,
            redemption_observed_at(&events[0]),
        );
        for event in events {
            operation.apply_redemption(event, None);
        }
        operation.into_dto()
    }

    /// Folds a full redemption stream with the real 1-based sequence threaded
    /// through, mirroring what `replay_redemption_event` does on catch-up.
    /// Only valid when `events[0]` is genuinely the aggregate's sequence-1
    /// event (never a slice starting mid-stream).
    pub(super) fn fold_redemption_from_genesis(
        events: &[EquityRedemptionEvent],
    ) -> EquityOperationTiming {
        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Redeem,
            redemption_observed_at(&events[0]),
        );
        for (index, event) in events.iter().enumerate() {
            let sequence = i64::try_from(index + 1).unwrap();
            operation.apply_redemption(event, Some(sequence));
        }
        operation.into_dto()
    }

    #[tokio::test]
    async fn reactor_round_trip_mint_matches_direct_fold() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(EquityTimingProjection::new(pool.clone()));
        let operation_id = issuer_request_id("reactor-mint");

        for event in mint_happy_path() {
            harness
                .receive::<TokenizedEquityMint>(operation_id.clone(), event)
                .await
                .unwrap();
        }

        let report = load_equity_timings(&pool, &range()).await.unwrap();
        assert_eq!(report.operations.len(), 1);
        let operation = &report.operations[0];
        assert_eq!(operation.kind, EquityOperationKind::Mint);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.total_ms, Some(70_000));
    }

    #[tokio::test]
    async fn reactor_round_trip_redemption_matches_direct_fold() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(EquityTimingProjection::new(pool.clone()));
        let operation_id = redemption_aggregate_id("reactor-redemption");

        for event in redemption_happy_path() {
            harness
                .receive::<EquityRedemption>(operation_id.clone(), event)
                .await
                .unwrap();
        }

        let report = load_equity_timings(&pool, &range()).await.unwrap();
        assert_eq!(report.operations.len(), 1);
        let operation = &report.operations[0];
        assert_eq!(operation.kind, EquityOperationKind::Redeem);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.total_ms, Some(100_000));
    }

    #[tokio::test]
    async fn catch_up_replays_events_the_live_reactor_never_saw() {
        let pool = setup_test_db().await;

        // Seed events directly through a store with NO projection attached,
        // simulating history that accumulated before this projection existed.
        // `RequestMint`'s `initialize()` calls `services.tokenizer.request_mint`,
        // so the panicking stub's tokenizer is swapped for a working mock --
        // mirrors `simulated_transfers.rs`'s `FixtureTokenizer` wiring pattern.
        let mut services = crate::rebalancing::equity::EquityTransferServices::panicking();
        services.tokenizer = Arc::new(st0x_tokenization::mock::MockTokenizer::new());
        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .build(services)
            .await
            .unwrap();
        let operation_id = issuer_request_id("catch-up-mint");
        mint_store
            .send(
                &operation_id,
                TokenizedEquityMintCommand::RequestMintAt {
                    issuer_request_id: operation_id.clone(),
                    symbol: symbol(),
                    quantity: float!(5),
                    wallet: Address::repeat_byte(0x11),
                    requested_at: timestamp(0),
                },
            )
            .await
            .unwrap();

        let projection = EquityTimingProjection::new(pool.clone());
        let replayed = projection.catch_up().await.unwrap();
        // `RequestMintAt` against a mock tokenizer that returns `Pending` (not
        // `Rejected`) persists both `MintRequested` and `MintAccepted`.
        assert_eq!(
            replayed, 2,
            "catch_up must replay exactly the two pre-existing events"
        );

        let report = load_equity_timings(&pool, &range()).await.unwrap();
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.operations[0].kind, EquityOperationKind::Mint);
        assert_eq!(
            report.operations[0].status,
            RebalanceTimingStatus::InProgress
        );

        // A second catch_up must be a no-op (nothing new past the checkpoint).
        let replayed_again = projection.catch_up().await.unwrap();
        assert_eq!(replayed_again, 0);
    }

    /// Inserts a raw event row straight into the event store -- the only way
    /// to stage a poison row (malformed payload or aggregate id) that no
    /// domain command can produce. Also used to seed valid streams for
    /// catch-up tests without going through a real command store: replay
    /// only deserializes the JSON payload and never re-validates aggregate
    /// state transitions, so a raw insert is indistinguishable from one
    /// produced by a real command.
    async fn insert_raw_equity_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, \
              metadata) \
             VALUES (?, ?, ?, 'test-event', '1', ?, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Snapshots the full `equity_stage_timing` table so poison-row handling
    /// can be asserted row-for-row, mirroring
    /// `rebalance::tests::fetch_timing_rows`.
    async fn fetch_equity_timing_rows(pool: &SqlitePool) -> Vec<(String, String, String)> {
        sqlx::query_as(
            "SELECT operation_id, started_at, timing FROM equity_stage_timing \
             ORDER BY operation_id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Reads an operation's persisted replay checkpoint (`NULL` -> `None`),
    /// mirroring `rebalance::tests::fetch_last_sequence`.
    async fn fetch_equity_last_sequence(pool: &SqlitePool, operation_id: &str) -> Option<i64> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT last_sequence FROM equity_stage_timing WHERE operation_id = ?")
                .bind(operation_id)
                .fetch_optional(pool)
                .await
                .unwrap();

        row.and_then(|(last_sequence,)| last_sequence)
    }

    /// A poison row -- an unparseable aggregate id, an unparseable event
    /// payload, or a corrupt stored timing row -- must NOT abort catch_up
    /// (and so must not brick startup): the bad rows are skipped with a
    /// warning, valid streams still fold, and the returned count reflects
    /// only folded events. Ports `rebalance`'s
    /// `catch_up_skips_poison_rows_and_folds_the_rest`.
    #[tokio::test]
    async fn catch_up_skips_poison_rows_and_folds_the_rest() {
        let pool = setup_test_db().await;

        // A valid redemption stream that must still fold despite the poison
        // rows around it.
        let valid_id = Uuid::new_v4();
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &valid_id.to_string(),
            1,
            &serde_json::to_string(&EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(0),
            })
            .unwrap(),
        )
        .await;
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &valid_id.to_string(),
            2,
            &serde_json::to_string(&EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                actual_wrapped_amount: Some(U256::from(5_000_000_000_000_000_000_u128)),
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: Some(1),
                withdrawn_at: timestamp(20),
            })
            .unwrap(),
        )
        .await;

        // A stream whose stored timing row is corrupt JSON: catch_up must
        // skip it (recoverable via rebuild_all) rather than bricking startup.
        let corrupt_row_id = Uuid::new_v4();
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &corrupt_row_id.to_string(),
            1,
            &serde_json::to_string(&EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(0),
            })
            .unwrap(),
        )
        .await;
        sqlx::query(
            "INSERT INTO equity_stage_timing (operation_id, started_at, timing) \
             VALUES (?, ?, ?)",
        )
        .bind(corrupt_row_id.to_string())
        .bind(timestamp(0).to_rfc3339())
        .bind("not-valid-json")
        .execute(&pool)
        .await
        .unwrap();

        // A raw event row with an unparseable payload, and one with an
        // unparseable aggregate id.
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &Uuid::new_v4().to_string(),
            1,
            "not-valid-json",
        )
        .await;
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            "not-a-uuid",
            1,
            "not-valid-json",
        )
        .await;

        // None of the poison rows abort catch_up; only the valid stream folds.
        let replayed = EquityTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 2,
            "only the two events of the valid stream fold; every poison row is skipped"
        );

        let rows = fetch_equity_timing_rows(&pool).await;
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

        // The corrupt row is left exactly as staged -- catch_up skipped it
        // rather than overwriting or repairing it.
        let corrupt = corrupt_row_id.to_string();
        let corrupt_row = rows
            .iter()
            .find(|(operation_id, _, _)| operation_id == &corrupt)
            .expect("the corrupt row remains in place");
        assert_eq!(
            corrupt_row.2, "not-valid-json",
            "catch_up does not touch the corrupt stored row"
        );

        // The valid stream is checkpointed; the skipped corrupt row is not
        // (so it stays visible and re-scanned until a rebuild repairs it).
        assert_eq!(
            fetch_equity_last_sequence(&pool, &valid).await,
            Some(2),
            "the valid stream advances its checkpoint"
        );
        assert_eq!(
            fetch_equity_last_sequence(&pool, &corrupt).await,
            None,
            "the skipped corrupt row is never checkpointed"
        );

        // A second catch-up converges: the valid stream is past its
        // checkpoint and the poison rows are re-skipped, so nothing new
        // folds.
        let second = EquityTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            second, 0,
            "the valid stream is caught up and every poison row is re-skipped"
        );
    }

    /// A poison event in the MIDDLE of a stream must not let later valid
    /// events advance the checkpoint past it: that would bury the poison
    /// (the `sequence > last_sequence` filter would stop re-scanning it) and
    /// fold the tail onto the indeterminate state left by the skipped event.
    /// Ports `rebalance`'s
    /// `catch_up_holds_checkpoint_behind_a_mid_stream_poison_event`.
    #[tokio::test]
    async fn catch_up_holds_checkpoint_behind_a_mid_stream_poison_event() {
        let pool = setup_test_db().await;

        let operation_id = Uuid::new_v4();
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &operation_id.to_string(),
            1,
            &serde_json::to_string(&EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(0),
            })
            .unwrap(),
        )
        .await;
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &operation_id.to_string(),
            2,
            &serde_json::to_string(&EquityRedemptionEvent::VaultWithdrawSubmitted {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                tx_hash: TxHash::random(),
                submitted_at: timestamp(5),
            })
            .unwrap(),
        )
        .await;
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &operation_id.to_string(),
            3,
            &serde_json::to_string(&EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                actual_wrapped_amount: Some(U256::from(5_000_000_000_000_000_000_u128)),
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: Some(1),
                withdrawn_at: timestamp(20),
            })
            .unwrap(),
        )
        .await;

        // Corrupt the middle event into a poison payload: now valid(1),
        // poison(2), valid(3) within the one stream.
        sqlx::query(
            "UPDATE events SET payload = 'not-valid-json' \
             WHERE aggregate_type = ? AND aggregate_id = ? AND sequence = 2",
        )
        .bind(EquityRedemption::AGGREGATE_TYPE)
        .bind(operation_id.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let replayed = EquityTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 1,
            "only seq 1 folds; the poison at seq 2 stops the stream, so seq 3 is left unfolded"
        );

        // The checkpoint holds at the last good sequence before the poison,
        // not seq 3 -- otherwise the poison at seq 2 would be buried from
        // re-scans.
        assert_eq!(
            fetch_equity_last_sequence(&pool, &operation_id.to_string()).await,
            Some(1),
            "the checkpoint stays at seq 1 so the poison at seq 2 stays visible"
        );

        // A second catch-up re-scans from seq 2, re-skips the poison, and
        // folds nothing new: seq 3 stays blocked behind the poison until a
        // rebuild.
        let second = EquityTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            second, 0,
            "the poison keeps blocking the tail; nothing new folds"
        );
    }

    /// A malformed `timing` JSON row that survives the SQL window must be
    /// skipped from `operations` but surfaced via `skipped_operations`
    /// rather than vanishing silently. Ports `rebalance`'s
    /// `load_rebalance_timings_counts_skipped_undeserializable_rows`.
    #[tokio::test]
    async fn load_equity_timings_counts_skipped_undeserializable_rows() {
        let pool = setup_test_db().await;

        // Seed one well-formed mint operation through the reactor so the
        // report is not empty, proving the malformed row is dropped while
        // the good row remains.
        let harness = ReactorHarness::new(EquityTimingProjection::new(pool.clone()));
        let good_id = issuer_request_id("good-mint");
        for event in mint_happy_path() {
            harness
                .receive::<TokenizedEquityMint>(good_id.clone(), event)
                .await
                .unwrap();
        }

        // Insert a row with a valid in-window `started_at` but unparseable
        // timing.
        sqlx::query(
            "INSERT INTO equity_stage_timing (operation_id, started_at, timing) \
             VALUES (?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(timestamp(5).to_rfc3339())
        .bind("not-valid-json")
        .execute(&pool)
        .await
        .unwrap();

        let report = load_equity_timings(&pool, &range()).await.unwrap();

        assert_eq!(report.skipped_operations, 1);
        // Only the well-formed operation survives; the malformed row is
        // absent.
        assert_eq!(report.total_operations, 1);
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.operations[0].kind, EquityOperationKind::Mint);
    }

    /// The `aggregate_type` dispatch (mint vs redemption) has no
    /// single-aggregate-type analogue in `rebalance`'s catch-up -- this seeds
    /// one stream of each kind with no projection attached and asserts one
    /// `catch_up` call folds both, each into its own operation with the
    /// correct kind and an independently-tracked checkpoint.
    #[tokio::test]
    async fn catch_up_replays_both_mint_and_redemption_streams_independently() {
        let pool = setup_test_db().await;

        let mint_id = Uuid::new_v4();
        insert_raw_equity_event(
            &pool,
            TokenizedEquityMint::AGGREGATE_TYPE,
            &mint_id.to_string(),
            1,
            &serde_json::to_string(&TokenizedEquityMintEvent::MintRequested {
                symbol: symbol(),
                quantity: float!(5),
                wallet: Address::repeat_byte(0x11),
                requested_at: timestamp(0),
            })
            .unwrap(),
        )
        .await;

        let redemption_id = Uuid::new_v4();
        insert_raw_equity_event(
            &pool,
            EquityRedemption::AGGREGATE_TYPE,
            &redemption_id.to_string(),
            1,
            &serde_json::to_string(&EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(0),
            })
            .unwrap(),
        )
        .await;

        let replayed = EquityTimingProjection::new(pool.clone())
            .catch_up()
            .await
            .unwrap();
        assert_eq!(
            replayed, 2,
            "one event from each aggregate type folds in a single catch_up pass"
        );

        let report = load_equity_timings(&pool, &range()).await.unwrap();
        assert_eq!(report.operations.len(), 2);

        let mint_operation = report
            .operations
            .iter()
            .find(|operation| operation.operation_id == mint_id)
            .expect("the mint stream folded into its own operation");
        assert_eq!(mint_operation.kind, EquityOperationKind::Mint);

        let redemption_operation = report
            .operations
            .iter()
            .find(|operation| operation.operation_id == redemption_id)
            .expect("the redemption stream folded into its own operation");
        assert_eq!(redemption_operation.kind, EquityOperationKind::Redeem);

        // Each stream's checkpoint is tracked independently.
        assert_eq!(
            fetch_equity_last_sequence(&pool, &mint_id.to_string()).await,
            Some(1)
        );
        assert_eq!(
            fetch_equity_last_sequence(&pool, &redemption_id.to_string()).await,
            Some(1)
        );
    }

    #[test]
    fn stage_summary_is_sparse_and_only_counts_succeeded_runs() {
        let mint = fold_mint(&mint_happy_path());
        let report = equity_timing_report(
            vec![WindowedOperation {
                first_seen_at: mint.started_at.unwrap(),
                operation: mint,
            }],
            &range(),
            0,
        );

        // Only the four mint stages have succeeded samples; no redemption
        // stage should appear.
        let names: Vec<EquityStageName> = report.stage_summary.iter().map(|s| s.stage).collect();
        assert!(names.contains(&EquityStageName::MintAcceptance));
        assert!(!names.contains(&EquityStageName::RedemptionWithdraw));
    }

    #[test]
    fn report_excludes_operations_outside_range() {
        // Mirrors `rebalance::tests::report_excludes_operations_outside_range`:
        // `equity_timing_report`'s own `first_seen_at` windowing is a pure-
        // function-level filter, independent of `load_equity_timings`'s SQL
        // `WHERE` clause -- exercise it directly so a regression (e.g. an
        // inverted condition) is caught even outside the DB-backed path.
        let mint = fold_mint(&mint_happy_path());
        let narrow = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };

        let report = equity_timing_report(
            vec![WindowedOperation {
                first_seen_at: mint.started_at.unwrap(),
                operation: mint,
            }],
            &narrow,
            0,
        );

        assert_eq!(report.total_operations, 0);
        assert!(report.operations.is_empty());
        assert!(report.stage_summary.is_empty());
    }

    #[test]
    fn mint_stage_duration_clamps_negative_clock_skew_to_zero() {
        // Mirrors `rebalance::tests::negative_stage_durations_clamp_to_zero`:
        // the close-out event's timestamp arrives before the stage-open
        // event's timestamp (clock skew between event sources), so the naive
        // `ended_at - started_at` would be negative. `close`'s `.max(0)` clamp
        // must keep it from leaking a negative sample into percentiles.
        let events = [
            TokenizedEquityMintEvent::MintRequested {
                symbol: symbol(),
                quantity: float!(5),
                wallet: Address::repeat_byte(0x11),
                requested_at: timestamp(10),
            },
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id: issuer_request_id("op"),
                tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
                accepted_at: timestamp(0),
            },
        ];
        let operation = fold_mint(&events);

        let acceptance = stage(&operation, EquityStageName::MintAcceptance);
        assert_eq!(acceptance.outcome, StageOutcome::Succeeded);
        assert_eq!(acceptance.duration_ms, Some(0));
    }

    #[test]
    fn redemption_stage_duration_clamps_negative_clock_skew_to_zero() {
        // Redemption counterpart, exercising `close`'s identical clamp via a
        // stage-close event timestamped before the stage-open event.
        let events = [
            EquityRedemptionEvent::VaultWithdrawPending {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                pending_at: timestamp(10),
            },
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                actual_wrapped_amount: Some(U256::from(5_000_000_000_000_000_000_u128)),
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: Some(1),
                withdrawn_at: timestamp(0),
            },
        ];
        let operation = fold_redemption(&events);

        let withdraw = stage(&operation, EquityStageName::RedemptionWithdraw);
        assert_eq!(withdraw.outcome, StageOutcome::Succeeded);
        assert_eq!(withdraw.duration_ms, Some(0));
    }
}
