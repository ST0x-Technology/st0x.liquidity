//! Verifies that migrations apply cleanly to a real (prod/staging) database
//! and that every currently persisted event still replays under the
//! CURRENT aggregate code.
//!
//! Catches cases where an event or aggregate shape change breaks legacy
//! data that no migration has repaired yet. Never mutates the database it
//! is pointed at: everything runs against a disposable `VACUUM INTO` copy
//! in a scratch temp directory. Safe to point at a live database (once the
//! writer is stopped) or a downloaded snapshot -- see
//! `src/bin/verify-migrations.rs` for the CLI entry point used both as a
//! pre-deploy gate and for manual testing while developing a migration.

use std::fmt;
use std::path::Path;
use std::str::FromStr;

use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use thiserror::Error;

use st0x_event_sorcery::{EventSourced, load_all_ids, load_entity};

use crate::equity_redemption::EquityRedemption;
use crate::inventory::snapshot::InventorySnapshot;
use crate::offchain::order::OffchainOrder;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::unwrapped_equity_recovery::aggregate::UnwrappedEquityRecovery;
use crate::usdc_rebalance::UsdcRebalance;
use crate::vault_registry::VaultRegistry;
use crate::wrapped_equity_recovery::aggregate::WrappedEquityRecovery;

/// One aggregate instance that failed to replay under current code.
#[derive(Debug)]
pub struct ReplayFailure {
    pub aggregate_id: String,
    pub error: String,
}

/// Replay results for every persisted instance of one aggregate type.
#[derive(Debug)]
pub struct AggregateReplayReport {
    pub aggregate_type: String,
    pub total: usize,
    pub failures: Vec<ReplayFailure>,
}

impl AggregateReplayReport {
    fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }
}

/// Full result of verifying migrations and event replay against a database
/// copy.
#[derive(Debug)]
pub struct VerificationReport {
    pub replay_reports: Vec<AggregateReplayReport>,
}

impl VerificationReport {
    pub fn has_failures(&self) -> bool {
        self.replay_reports
            .iter()
            .any(AggregateReplayReport::has_failures)
    }
}

impl fmt::Display for VerificationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "Migrations applied cleanly.")?;
        writeln!(formatter, "Aggregate replay check:")?;
        for report in &self.replay_reports {
            writeln!(
                formatter,
                "  {}: {} aggregate(s), {} failure(s)",
                report.aggregate_type,
                report.total,
                report.failures.len()
            )?;
            for failure in &report.failures {
                writeln!(
                    formatter,
                    "    - aggregate_id={}: {}",
                    failure.aggregate_id, failure.error
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("failed to open source database at {path}")]
    OpenSource {
        path: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to snapshot source database into scratch copy")]
    Vacuum(#[source] sqlx::Error),
    #[error("failed to open scratch database copy")]
    OpenScratch(#[source] sqlx::Error),
    #[error("migrations failed to apply")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("failed to clear stale snapshots before replay check")]
    ClearSnapshots(#[source] sqlx::Error),
    #[error("failed to create scratch directory")]
    ScratchDir(#[from] std::io::Error),
}

/// Verifies migrations and event replay against a copy of `source_db_path`.
///
/// `source_db_path` is opened read-only and copied via `VACUUM INTO` into a
/// scratch temp file before anything runs against it -- the source is never
/// modified. Safe to point at a live database (after its writer is stopped)
/// or a downloaded snapshot.
pub async fn verify_migrations(
    source_db_path: &Path,
) -> Result<VerificationReport, VerificationError> {
    let source_options = SqliteConnectOptions::new()
        .filename(source_db_path)
        .read_only(true);
    let source_pool = SqlitePool::connect_with(source_options)
        .await
        .map_err(|source| VerificationError::OpenSource {
            path: source_db_path.display().to_string(),
            source,
        })?;

    let scratch_dir = tempfile::tempdir()?;
    let scratch_path = scratch_dir.path().join("verify-migrations-scratch.db");

    sqlx::query("VACUUM INTO ?1")
        .bind(scratch_path.display().to_string())
        .execute(&source_pool)
        .await
        .map_err(VerificationError::Vacuum)?;
    source_pool.close().await;

    let scratch_options = SqliteConnectOptions::new().filename(&scratch_path);
    let scratch_pool = SqlitePool::connect_with(scratch_options)
        .await
        .map_err(VerificationError::OpenScratch)?;

    sqlx::migrate!()
        .set_ignore_missing(true)
        .run(&scratch_pool)
        .await?;

    clear_snapshots(&scratch_pool).await?;

    let replay_reports = run_replay_checks(&scratch_pool).await;

    scratch_pool.close().await;

    Ok(VerificationReport { replay_reports })
}

/// Historical snapshots reflect the aggregate shape at the time they were
/// taken. If no events have appended since, `load_entity` returns the
/// cached snapshot directly and never touches the underlying events --
/// masking exactly the "old event no longer deserializes under current
/// code" bug this check exists to catch. Clearing snapshots forces every
/// aggregate to replay from its full raw event history, the same as what
/// happens in production when a `SCHEMA_VERSION` bump clears stale
/// snapshots on deploy.
async fn clear_snapshots(pool: &SqlitePool) -> Result<(), VerificationError> {
    sqlx::query("DELETE FROM snapshots")
        .execute(pool)
        .await
        .map_err(VerificationError::ClearSnapshots)?;

    Ok(())
}

async fn run_replay_checks(pool: &SqlitePool) -> Vec<AggregateReplayReport> {
    let mut reports = vec![
        check_replay::<Position>(pool).await,
        check_replay::<OnChainTrade>(pool).await,
        check_replay::<OffchainOrder>(pool).await,
        check_replay::<VaultRegistry>(pool).await,
        check_replay::<InventorySnapshot>(pool).await,
        check_replay::<UsdcRebalance>(pool).await,
        check_replay::<TokenizedEquityMint>(pool).await,
        check_replay::<EquityRedemption>(pool).await,
        check_replay::<WrappedEquityRecovery>(pool).await,
        check_replay::<UnwrappedEquityRecovery>(pool).await,
    ];

    reports.extend(uncovered_aggregate_reports(pool, &reports).await);

    reports
}

/// The event store itself is the single source of truth for which aggregate
/// types exist, so any persisted `aggregate_type` the checks above did not
/// cover fails the gate closed instead of silently shipping unverified
/// replay. This is the drift guard for the hand-maintained list in
/// [`run_replay_checks`]: adding a new `EventSourced` aggregate and
/// forgetting to extend the list surfaces here on the first deploy where
/// that aggregate has any events — the exact moment its legacy shapes start
/// needing verification.
async fn uncovered_aggregate_reports(
    pool: &SqlitePool,
    covered_reports: &[AggregateReplayReport],
) -> Vec<AggregateReplayReport> {
    let persisted_types: Vec<String> = match sqlx::query_scalar(
        "SELECT DISTINCT aggregate_type FROM events ORDER BY aggregate_type",
    )
    .fetch_all(pool)
    .await
    {
        Ok(persisted_types) => persisted_types,
        Err(error) => {
            return vec![AggregateReplayReport {
                aggregate_type: "*".to_string(),
                total: 0,
                failures: vec![ReplayFailure {
                    aggregate_id: "*".to_string(),
                    error: format!("failed to enumerate persisted aggregate types: {error}"),
                }],
            }];
        }
    };

    persisted_types
        .into_iter()
        .filter(|persisted| {
            !covered_reports
                .iter()
                .any(|report| report.aggregate_type == *persisted)
        })
        .map(|uncovered| AggregateReplayReport {
            aggregate_type: uncovered,
            total: 0,
            failures: vec![ReplayFailure {
                aggregate_id: "*".to_string(),
                error: "aggregate type is present in the event store but not \
                        covered by the replay checks; add it to \
                        run_replay_checks so its legacy events are verified \
                        before deploy"
                    .to_string(),
            }],
        })
        .collect()
}

async fn check_replay<Entity>(pool: &SqlitePool) -> AggregateReplayReport
where
    Entity: EventSourced,
    <Entity::Id as FromStr>::Err: fmt::Debug,
{
    let ids = match load_all_ids::<Entity>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            return AggregateReplayReport {
                aggregate_type: Entity::AGGREGATE_TYPE.to_string(),
                total: 0,
                failures: vec![ReplayFailure {
                    aggregate_id: "*".to_string(),
                    error: format!("failed to enumerate aggregate ids: {error}"),
                }],
            };
        }
    };

    let mut failures = Vec::new();
    for id in &ids {
        match load_entity::<Entity>(pool, id).await {
            Ok(Some(_)) => {}
            Ok(None) => failures.push(ReplayFailure {
                aggregate_id: id.to_string(),
                error: "replayed to empty state".to_string(),
            }),
            Err(error) => failures.push(ReplayFailure {
                aggregate_id: id.to_string(),
                error: error.to_string(),
            }),
        }
    }

    AggregateReplayReport {
        aggregate_type: Entity::AGGREGATE_TYPE.to_string(),
        total: ids.len(),
        failures,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use st0x_config::ExecutionThreshold;
    use st0x_execution::{ClientOrderId, FractionalShares, Positive, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::position::PositionEvent;
    use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};

    const A_USDC_REBALANCE_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    const REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS: &str = include_str!(
        "../migrations/20260701223808_repair_legacy_usdc_conversion_confirmed_events.sql"
    );

    async fn migrated_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        event_version: &str,
        payload: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, ?, ?, ?, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(event_version)
        .bind(payload.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_snapshot(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO snapshots \
             (aggregate_type, aggregate_id, last_sequence, payload, timestamp) \
             VALUES (?, ?, 1, '{}', '2026-01-01T00:00:00Z')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .execute(pool)
        .await
        .unwrap();
    }

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(1))).unwrap())
    }

    async fn insert_position_initialized(pool: &SqlitePool, symbol: &str) {
        let event = PositionEvent::Initialized {
            symbol: Symbol::new(symbol).unwrap(),
            threshold: one_share_threshold(),
            initialized_at: Utc::now(),
        };
        insert_event(
            pool,
            "Position",
            symbol,
            1,
            "PositionEvent::Initialized",
            "1.0",
            serde_json::to_value(&event).unwrap(),
        )
        .await;
    }

    fn find_report<'reports>(
        reports: &'reports [AggregateReplayReport],
        aggregate_type: &str,
    ) -> &'reports AggregateReplayReport {
        reports
            .iter()
            .find(|report| report.aggregate_type == aggregate_type)
            .unwrap()
    }

    /// The drift guard for the hand-maintained replay list: an aggregate type
    /// persisted in the event store but absent from `run_replay_checks` must
    /// fail the deploy gate closed — otherwise a newly added `EventSourced`
    /// aggregate silently ships with unverified legacy-event replay.
    #[tokio::test]
    async fn uncovered_persisted_aggregate_type_fails_the_gate() {
        let pool = migrated_pool().await;
        insert_position_initialized(&pool, "AAPL").await;
        insert_event(
            &pool,
            "BrandNewAggregate",
            "some-id",
            1,
            "BrandNewAggregateEvent::Created",
            "1.0",
            json!({"Created": {}}),
        )
        .await;

        let reports = run_replay_checks(&pool).await;

        let uncovered = find_report(&reports, "BrandNewAggregate");
        assert_eq!(uncovered.failures.len(), 1);
        assert_eq!(uncovered.failures[0].aggregate_id, "*");
        assert!(
            uncovered.failures[0]
                .error
                .contains("not covered by the replay checks"),
            "the failure must name the drift, got: {}",
            uncovered.failures[0].error
        );

        // The covered aggregates keep passing: the guard adds a failure, it
        // does not poison the rest of the report.
        let position_report = find_report(&reports, "Position");
        assert!(position_report.failures.is_empty());
    }

    /// A store containing only covered aggregate types produces no synthetic
    /// uncovered-type failures — the guard stays silent on a healthy store.
    #[tokio::test]
    async fn covered_only_store_produces_no_uncovered_reports() {
        let pool = migrated_pool().await;
        insert_position_initialized(&pool, "AAPL").await;

        let reports = run_replay_checks(&pool).await;

        assert!(
            reports.iter().all(|report| report.failures.is_empty()),
            "no synthetic failures expected for a fully covered store: {reports:?}"
        );
    }

    #[tokio::test]
    async fn replays_a_well_formed_position_cleanly() {
        let pool = migrated_pool().await;
        insert_position_initialized(&pool, "AAPL").await;

        let reports = run_replay_checks(&pool).await;

        let position_report = find_report(&reports, "Position");
        assert_eq!(position_report.total, 1);
        assert!(position_report.failures.is_empty());
    }

    #[tokio::test]
    async fn reports_a_failure_for_an_unparseable_event() {
        let pool = migrated_pool().await;
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            1,
            "UsdcRebalanceEvent::SomeVariantThatNoLongerExists",
            "1.0",
            json!({"garbage": "payload"}),
        )
        .await;

        let reports = run_replay_checks(&pool).await;

        let usdc_report = find_report(&reports, "UsdcRebalance");
        assert_eq!(usdc_report.total, 1);
        assert_eq!(usdc_report.failures.len(), 1);
        assert_eq!(usdc_report.failures[0].aggregate_id, A_USDC_REBALANCE_ID);
    }

    /// The prod failure this migration repairs is `UsdcRebalance` *hydration*,
    /// not a SQLite field-shape mismatch: a legacy `ConversionConfirmed`
    /// carrying the pre-split `filled_amount` field no longer deserializes into
    /// the current `ConversionAmounts` (source + received) shape, so the whole
    /// aggregate fails to replay. This seeds that exact legacy stream, proves it
    /// breaks replay before the repair, then proves the repair migration makes
    /// it hydrate cleanly under current code -- exercising the replay path the
    /// deploy gate depends on, which the pure JSON-shape assertions in
    /// `tests/migrations.rs` never reach.
    #[tokio::test]
    async fn repair_migration_makes_legacy_conversion_confirmed_replay() {
        let pool = migrated_pool().await;

        // The originating event in its current shape so the stream is valid up
        // to the conversion confirmation.
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            1,
            "UsdcRebalanceEvent::ConversionInitiated",
            "2.0",
            serde_json::to_value(&UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(1082.711862)),
                order_id: ClientOrderId::from_uuid(Uuid::from_u128(1)),
                initiated_at: Utc::now(),
            })
            .unwrap(),
        )
        .await;

        // The confirmation in its *legacy* shape: a single `filled_amount`
        // string instead of the current source/received split. This is exactly
        // what prod persisted before the model change and cannot deserialize
        // under the current event schema.
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            2,
            "UsdcRebalanceEvent::ConversionConfirmed",
            "1.0",
            json!({
                "ConversionConfirmed": {
                    "direction": "BaseToAlpaca",
                    "filled_amount": "1082.711862",
                    "converted_at": "2026-07-01T19:58:41.907Z"
                }
            }),
        )
        .await;

        let before_reports = run_replay_checks(&pool).await;
        let before = find_report(&before_reports, "UsdcRebalance");
        assert_eq!(before.total, 1);
        assert_eq!(before.failures.len(), 1);
        assert_eq!(before.failures[0].aggregate_id, A_USDC_REBALANCE_ID);

        sqlx::raw_sql(REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS)
            .execute(&pool)
            .await
            .unwrap();

        let after_reports = run_replay_checks(&pool).await;
        let after = find_report(&after_reports, "UsdcRebalance");
        assert_eq!(after.total, 1);
        assert!(after.failures.is_empty());
    }

    #[tokio::test]
    async fn empty_database_replays_cleanly_for_every_aggregate_type() {
        let pool = migrated_pool().await;

        let reports = run_replay_checks(&pool).await;

        assert_eq!(reports.len(), 10);
        for report in &reports {
            assert_eq!(report.total, 0, "{}", report.aggregate_type);
            assert!(report.failures.is_empty(), "{}", report.aggregate_type);
        }
    }

    /// Guards the exact bug class this check exists to catch: a snapshot
    /// taken under an old aggregate/event shape would otherwise let
    /// `load_entity` skip straight to the cached (and now stale-shaped)
    /// state without ever touching the underlying events, silently masking
    /// legacy data that no longer replays under current code.
    #[tokio::test]
    async fn clear_snapshots_removes_all_rows() {
        let pool = migrated_pool().await;
        insert_snapshot(&pool, "Position", "AAPL").await;
        insert_snapshot(&pool, "UsdcRebalance", A_USDC_REBALANCE_ID).await;

        clear_snapshots(&pool).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn verify_migrations_never_mutates_the_source_and_covers_every_aggregate_type() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_path = source_dir.path().join("source.db");

        let setup_pool = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&source_path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        sqlx::migrate!().run(&setup_pool).await.unwrap();
        insert_position_initialized(&setup_pool, "AAPL").await;
        setup_pool.close().await;

        let bytes_before = std::fs::read(&source_path).unwrap();

        let report = verify_migrations(&source_path).await.unwrap();

        let bytes_after = std::fs::read(&source_path).unwrap();
        assert_eq!(bytes_before, bytes_after, "source database was mutated");

        assert!(!report.has_failures());
        assert_eq!(report.replay_reports.len(), 10);
        assert_eq!(find_report(&report.replay_reports, "Position").total, 1);
    }

    #[tokio::test]
    async fn verify_migrations_fails_clearly_on_a_missing_source() {
        let error = verify_migrations(Path::new("/nonexistent/path/does-not-exist.db"))
            .await
            .unwrap_err();

        assert!(matches!(error, VerificationError::OpenSource { .. }));
    }
}
