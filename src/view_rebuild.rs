//! Operator rebuild of a materialized view or read model by replaying its
//! event streams from scratch: the escape hatch for a view corrupted by a lost
//! update. One definition shared by the `stox view rebuild` CLI and the ops API
//! route, so the set of rebuildable views and their single-id support cannot
//! drift between the two.
//!
//! Rebuilds replace each affected aggregate view atomically: row deletion and
//! event replay share one SQLite transaction, so a replay failure preserves the
//! previous materialized rows. Callers must still exclude concurrent
//! `Store::send` projection writers. The live ops API does so through projection
//! maintenance; direct database callers, including the legacy `st0x-cli`, must
//! run only while the bot is stopped.

use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, Sqlite, SqlitePool, Transaction};
use thiserror::Error;

use st0x_event_sorcery::{EventSourced, LifecycleError, Projection, ProjectionError, Table};
use st0x_execution::{EmptySymbolError, Symbol};

use crate::offchain::order::{OffchainOrder, OffchainOrderId};
use crate::performance::equity_timing::EquityTimingProjection;
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::performance::reliability::LifecycleFailureProjection;
use crate::portfolio_snapshot::PortfolioSnapshotProjection;
use crate::position::Position;
use crate::vault_registry::{ParseVaultRegistryIdError, VaultRegistry, VaultRegistryId};

/// A view or read model an operator may rebuild. Kebab-cased for both the
/// CLI value and the wire (`position`, `offchain-order`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RebuildableView {
    /// Position aggregate (position_view)
    Position,
    /// Offchain order aggregate (offchain_order_view)
    OffchainOrder,
    /// Vault registry aggregate (vault_registry_view)
    VaultRegistry,
    /// Rebalance stage-timing read model (rebalance_stage_timing). Replays every
    /// `UsdcRebalance` event stream through the reactor fold. Whole model only.
    RebalanceTiming,
    /// Equity mint/redemption stage-timing read model (equity_stage_timing).
    /// Replays every `TokenizedEquityMint`/`EquityRedemption` event stream
    /// through the reactor fold. Whole model only.
    EquityTiming,
    /// Lifecycle-failure read model (lifecycle_failure_event). Replays every
    /// failure across all four subscribed streams through the reactor fold.
    /// Whole model only.
    LifecycleFailure,
    /// Daily portfolio snapshot read model. Replays captures and every audited
    /// historical-mark correction. Whole model only.
    PortfolioSnapshot,
}

impl RebuildableView {
    /// The kebab-case name used on the CLI and the wire.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Position => "position",
            Self::OffchainOrder => "offchain-order",
            Self::VaultRegistry => "vault-registry",
            Self::RebalanceTiming => "rebalance-timing",
            Self::EquityTiming => "equity-timing",
            Self::LifecycleFailure => "lifecycle-failure",
            Self::PortfolioSnapshot => "portfolio-snapshot",
        }
    }

    /// Whether one aggregate's view can be rebuilt on its own. The per-aggregate
    /// views can; the reactor-fold read models replay the whole model.
    pub const fn supports_single_id(self) -> bool {
        match self {
            Self::Position | Self::OffchainOrder | Self::VaultRegistry => true,
            Self::RebalanceTiming
            | Self::EquityTiming
            | Self::LifecycleFailure
            | Self::PortfolioSnapshot => false,
        }
    }
}

impl std::fmt::Display for RebuildableView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Which rows to rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildScope {
    /// One aggregate's view, by its id as the operator typed it.
    Id(String),
    /// Every row of the view or read model.
    All,
}

/// What a rebuild did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRebuilt {
    pub view: RebuildableView,
    pub scope: RebuildScope,
    /// Events replayed, reported by the read models; the per-aggregate views
    /// do not count.
    pub replayed: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ViewRebuildError {
    /// A single-id rebuild was requested for a read model that only replays
    /// the whole model.
    #[error("{view} rebuild replays the whole read model and cannot rebuild a single id")]
    WholeModelOnly { view: RebuildableView },
    #[error("invalid position id {id:?}: {source}")]
    InvalidPositionId {
        id: String,
        #[source]
        source: EmptySymbolError,
    },
    #[error("invalid offchain-order id {id:?}: {source}")]
    InvalidOffchainOrderId {
        id: String,
        #[source]
        source: uuid::Error,
    },
    #[error("invalid vault-registry id {id:?}: {source}")]
    InvalidVaultRegistryId {
        id: String,
        #[source]
        source: ParseVaultRegistryIdError,
    },
    /// The requested aggregate id has no event stream to replay.
    #[error("{view} has no event stream for id {id:?}")]
    NoEventStream { view: RebuildableView, id: String },
    #[error(transparent)]
    Position(ProjectionError<Position>),
    #[error(transparent)]
    OffchainOrder(ProjectionError<OffchainOrder>),
    #[error(transparent)]
    VaultRegistry(ProjectionError<VaultRegistry>),
    #[error(transparent)]
    RebalanceTiming(crate::performance::rebalance::ProjectionError),
    #[error(transparent)]
    EquityTiming(crate::performance::equity_timing::ProjectionError),
    #[error(transparent)]
    LifecycleFailure(crate::performance::reliability::FailureProjectionError),
    #[error(transparent)]
    PortfolioSnapshot(crate::portfolio_snapshot::projection::ProjectionError),
}

impl ViewRebuildError {
    /// Whether the failure is a request error rather than a store failure.
    pub const fn is_caller_error(&self) -> bool {
        matches!(
            self,
            Self::WholeModelOnly { .. }
                | Self::InvalidPositionId { .. }
                | Self::InvalidOffchainOrderId { .. }
                | Self::InvalidVaultRegistryId { .. }
                | Self::NoEventStream { .. }
        )
    }
}

/// Local mirror of event-sorcery's private lifecycle state. The serialized
/// shape must remain identical because the framework deserializes these rows.
#[derive(Default, Serialize)]
#[serde(bound = "")]
enum RebuiltLifecycle<Entity>
where
    Entity: EventSourced,
{
    #[default]
    Uninitialized,
    Live(Entity),
    Failed {
        error: LifecycleError<Entity>,
        last_valid_entity: Option<Box<Entity>>,
    },
}

impl<Entity> RebuiltLifecycle<Entity>
where
    Entity: EventSourced,
{
    fn apply(&mut self, event: Entity::Event) {
        *self = match std::mem::take(self) {
            Self::Uninitialized => Entity::originate(&event).map_or_else(
                || Self::Failed {
                    error: LifecycleError::EventCantOriginate { event },
                    last_valid_entity: None,
                },
                Self::Live,
            ),
            Self::Live(entity) => match Entity::evolve(&entity, &event) {
                Ok(Some(next)) => Self::Live(next),
                Ok(None) => Self::Failed {
                    error: LifecycleError::UnexpectedEvent {
                        entity: Box::new(entity.clone()),
                        event,
                    },
                    last_valid_entity: Some(Box::new(entity)),
                },
                Err(error) => Self::Failed {
                    error: LifecycleError::Apply(error),
                    last_valid_entity: Some(Box::new(entity)),
                },
            },
            Self::Failed {
                error,
                last_valid_entity,
            } => Self::Failed {
                error: LifecycleError::AlreadyFailed {
                    failure: Box::new(error),
                    event,
                },
                last_valid_entity,
            },
        };
    }
}

/// Transaction-aware rebuild operations missing from event-sorcery's public
/// projection API. Keeping the transaction caller-owned makes deletion and
/// replay one atomic operation.
trait TransactionalProjection<Entity>
where
    Entity: EventSourced<Materialized = Table>,
{
    async fn rebuild_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &Entity::Id,
    ) -> Result<bool, ProjectionError<Entity>>;

    async fn rebuild_all_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), ProjectionError<Entity>>;
}

impl<Entity> TransactionalProjection<Entity> for Projection<Entity>
where
    Entity: EventSourced<Materialized = Table>,
{
    async fn rebuild_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &Entity::Id,
    ) -> Result<bool, ProjectionError<Entity>> {
        let Table(table) = Entity::PROJECTION;
        let view_id = id.to_string();

        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE view_id = ?1"
        )))
        .bind(&view_id)
        .execute(&mut **transaction)
        .await?;

        replay_projection::<Entity>(transaction, table, &view_id).await
    }

    async fn rebuild_all_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), ProjectionError<Entity>> {
        let Table(table) = Entity::PROJECTION;
        let aggregate_ids: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT aggregate_id FROM events \
             WHERE aggregate_type = ?1 ORDER BY aggregate_id",
        )
        .bind(Entity::AGGREGATE_TYPE)
        .fetch_all(&mut **transaction)
        .await?;

        sqlx::query(AssertSqlSafe(format!("DELETE FROM {table}")))
            .execute(&mut **transaction)
            .await?;

        for aggregate_id in aggregate_ids {
            let rebuilt = replay_projection::<Entity>(transaction, table, &aggregate_id).await?;
            debug_assert!(
                rebuilt,
                "an aggregate id selected from events must be replayable"
            );
        }

        Ok(())
    }
}

async fn replay_projection<Entity>(
    transaction: &mut Transaction<'_, Sqlite>,
    table: &str,
    aggregate_id: &str,
) -> Result<bool, ProjectionError<Entity>>
where
    Entity: EventSourced<Materialized = Table>,
{
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT sequence, payload FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
         ORDER BY sequence ASC",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(aggregate_id)
    .fetch_all(&mut **transaction)
    .await?;

    let Some((max_sequence, _)) = events.last() else {
        return Ok(false);
    };
    let max_sequence = *max_sequence;
    let mut lifecycle = RebuiltLifecycle::<Entity>::default();

    for (_, payload) in events {
        let event: Entity::Event =
            serde_json::from_str(&payload).map_err(|source| ProjectionError::Serde {
                aggregate_id: aggregate_id.to_owned(),
                source,
            })?;
        lifecycle.apply(event);
    }

    let payload = serde_json::to_string(&lifecycle).map_err(|source| ProjectionError::Serde {
        aggregate_id: aggregate_id.to_owned(),
        source,
    })?;

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {table} (view_id, version, payload) VALUES (?1, ?2, ?3)"
    )))
    .bind(aggregate_id)
    .bind(max_sequence)
    .bind(payload)
    .execute(&mut **transaction)
    .await?;

    Ok(true)
}

async fn rebuild_projection<Entity>(
    pool: &SqlitePool,
    id: &Entity::Id,
) -> Result<bool, ProjectionError<Entity>>
where
    Entity: EventSourced<Materialized = Table>,
{
    let projection = Projection::<Entity>::sqlite(pool.clone());
    let mut transaction = pool.begin().await?;
    let rebuilt = projection.rebuild_in(&mut transaction, id).await?;
    if !rebuilt {
        transaction.rollback().await?;
        return Ok(false);
    }
    transaction.commit().await?;
    Ok(true)
}

async fn rebuild_all_projections<Entity>(pool: &SqlitePool) -> Result<(), ProjectionError<Entity>>
where
    Entity: EventSourced<Materialized = Table>,
{
    let projection = Projection::<Entity>::sqlite(pool.clone());
    let mut transaction = pool.begin().await?;
    projection.rebuild_all_in(&mut transaction).await?;
    transaction.commit().await?;
    Ok(())
}

async fn event_stream_exists<Entity>(
    pool: &SqlitePool,
    id: &Entity::Id,
) -> Result<bool, ProjectionError<Entity>>
where
    Entity: EventSourced<Materialized = Table>,
{
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2)",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(id.to_string())
    .fetch_one(pool)
    .await?)
}

/// A validated rebuild target. Single-aggregate variants carry their parsed,
/// view-specific id, so executing one cannot discover a caller error after a
/// projection-maintenance pause has begun.
#[derive(Debug)]
pub enum ParsedRebuildScope {
    /// One position projection.
    Position { requested_id: String, id: Symbol },
    /// One offchain-order projection.
    OffchainOrder {
        requested_id: String,
        id: OffchainOrderId,
    },
    /// One vault-registry projection.
    VaultRegistry {
        requested_id: String,
        id: VaultRegistryId,
    },
    /// Every row of the selected view or read model.
    All(RebuildableView),
}

/// Validates a requested scope and parses a single aggregate id into the type
/// its projection uses. This performs no store access.
pub fn parse_rebuild_scope(
    view: RebuildableView,
    scope: RebuildScope,
) -> Result<ParsedRebuildScope, ViewRebuildError> {
    match (view, scope) {
        (RebuildableView::Position, RebuildScope::Id(requested_id)) => {
            let id =
                requested_id
                    .parse()
                    .map_err(|source| ViewRebuildError::InvalidPositionId {
                        id: requested_id.clone(),
                        source,
                    })?;
            Ok(ParsedRebuildScope::Position { requested_id, id })
        }
        (RebuildableView::OffchainOrder, RebuildScope::Id(requested_id)) => {
            let id = requested_id.parse().map_err(|source| {
                ViewRebuildError::InvalidOffchainOrderId {
                    id: requested_id.clone(),
                    source,
                }
            })?;
            Ok(ParsedRebuildScope::OffchainOrder { requested_id, id })
        }
        (RebuildableView::VaultRegistry, RebuildScope::Id(requested_id)) => {
            let id = requested_id.parse().map_err(|source| {
                ViewRebuildError::InvalidVaultRegistryId {
                    id: requested_id.clone(),
                    source,
                }
            })?;
            Ok(ParsedRebuildScope::VaultRegistry { requested_id, id })
        }
        (
            RebuildableView::RebalanceTiming
            | RebuildableView::EquityTiming
            | RebuildableView::LifecycleFailure
            | RebuildableView::PortfolioSnapshot,
            RebuildScope::Id(_),
        ) => Err(ViewRebuildError::WholeModelOnly { view }),
        (view, RebuildScope::All) => Ok(ParsedRebuildScope::All(view)),
    }
}

/// Checks that a parsed single-aggregate target has an event stream to replay.
/// The live API calls this before pausing projection writers.
pub async fn validate_rebuild_scope(
    pool: &SqlitePool,
    scope: ParsedRebuildScope,
) -> Result<ParsedRebuildScope, ViewRebuildError> {
    let exists = match &scope {
        ParsedRebuildScope::Position { id, .. } => event_stream_exists::<Position>(pool, id)
            .await
            .map_err(ViewRebuildError::Position)?,
        ParsedRebuildScope::OffchainOrder { id, .. } => {
            event_stream_exists::<OffchainOrder>(pool, id)
                .await
                .map_err(ViewRebuildError::OffchainOrder)?
        }
        ParsedRebuildScope::VaultRegistry { id, .. } => {
            event_stream_exists::<VaultRegistry>(pool, id)
                .await
                .map_err(ViewRebuildError::VaultRegistry)?
        }
        ParsedRebuildScope::All(_) => return Ok(scope),
    };

    if exists {
        return Ok(scope);
    }

    let (view, id) = match scope {
        ParsedRebuildScope::Position { requested_id, .. } => {
            (RebuildableView::Position, requested_id)
        }
        ParsedRebuildScope::OffchainOrder { requested_id, .. } => {
            (RebuildableView::OffchainOrder, requested_id)
        }
        ParsedRebuildScope::VaultRegistry { requested_id, .. } => {
            (RebuildableView::VaultRegistry, requested_id)
        }
        ParsedRebuildScope::All(_) => unreachable!("whole-view scopes return before validation"),
    };
    Err(ViewRebuildError::NoEventStream { view, id })
}

/// Executes a previously validated rebuild by deleting the affected rows and
/// replaying the event log. The caller must exclude concurrent projection
/// writers.
pub async fn execute_rebuild_view(
    pool: &SqlitePool,
    scope: ParsedRebuildScope,
) -> Result<ViewRebuilt, ViewRebuildError> {
    let (view, scope, replayed) = match scope {
        ParsedRebuildScope::Position { requested_id, id } => {
            let rebuilt = rebuild_projection::<Position>(pool, &id)
                .await
                .map_err(ViewRebuildError::Position)?;
            if !rebuilt {
                return Err(ViewRebuildError::NoEventStream {
                    view: RebuildableView::Position,
                    id: requested_id,
                });
            }
            (
                RebuildableView::Position,
                RebuildScope::Id(requested_id),
                None,
            )
        }
        ParsedRebuildScope::OffchainOrder { requested_id, id } => {
            let rebuilt = rebuild_projection::<OffchainOrder>(pool, &id)
                .await
                .map_err(ViewRebuildError::OffchainOrder)?;
            if !rebuilt {
                return Err(ViewRebuildError::NoEventStream {
                    view: RebuildableView::OffchainOrder,
                    id: requested_id,
                });
            }
            (
                RebuildableView::OffchainOrder,
                RebuildScope::Id(requested_id),
                None,
            )
        }
        ParsedRebuildScope::VaultRegistry { requested_id, id } => {
            let rebuilt = rebuild_projection::<VaultRegistry>(pool, &id)
                .await
                .map_err(ViewRebuildError::VaultRegistry)?;
            if !rebuilt {
                return Err(ViewRebuildError::NoEventStream {
                    view: RebuildableView::VaultRegistry,
                    id: requested_id,
                });
            }
            (
                RebuildableView::VaultRegistry,
                RebuildScope::Id(requested_id),
                None,
            )
        }
        ParsedRebuildScope::All(view) => {
            let replayed = match view {
                RebuildableView::Position => {
                    rebuild_all_projections::<Position>(pool)
                        .await
                        .map_err(ViewRebuildError::Position)?;
                    None
                }
                RebuildableView::OffchainOrder => {
                    rebuild_all_projections::<OffchainOrder>(pool)
                        .await
                        .map_err(ViewRebuildError::OffchainOrder)?;
                    None
                }
                RebuildableView::VaultRegistry => {
                    rebuild_all_projections::<VaultRegistry>(pool)
                        .await
                        .map_err(ViewRebuildError::VaultRegistry)?;
                    None
                }
                RebuildableView::RebalanceTiming => Some(
                    RebalanceTimingProjection::new(pool.clone())
                        .rebuild_all()
                        .await
                        .map_err(ViewRebuildError::RebalanceTiming)?,
                ),
                RebuildableView::EquityTiming => Some(
                    EquityTimingProjection::new(pool.clone())
                        .rebuild_all()
                        .await
                        .map_err(ViewRebuildError::EquityTiming)?,
                ),
                RebuildableView::LifecycleFailure => Some(
                    LifecycleFailureProjection::new(pool.clone())
                        .rebuild_all()
                        .await
                        .map_err(ViewRebuildError::LifecycleFailure)?,
                ),
                RebuildableView::PortfolioSnapshot => Some(
                    PortfolioSnapshotProjection::new(pool.clone())
                        .rebuild_all()
                        .await
                        .map_err(ViewRebuildError::PortfolioSnapshot)?,
                ),
            };
            (view, RebuildScope::All, replayed)
        }
    };

    Ok(ViewRebuilt {
        view,
        scope,
        replayed,
    })
}

/// Validates and executes a rebuild for direct database callers that do not
/// manage projection maintenance separately.
pub async fn rebuild_view(
    pool: &SqlitePool,
    view: RebuildableView,
    scope: RebuildScope,
) -> Result<ViewRebuilt, ViewRebuildError> {
    let scope = parse_rebuild_scope(view, scope)?;
    let scope = validate_rebuild_scope(pool, scope).await?;
    execute_rebuild_view(pool, scope).await
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_config::ExecutionThreshold;

    use super::*;
    use crate::position::PositionEvent;
    use crate::test_utils::{persist_event, setup_test_db};

    #[test]
    fn read_models_refuse_a_single_id_during_parsing() {
        for view in [
            RebuildableView::RebalanceTiming,
            RebuildableView::EquityTiming,
            RebuildableView::LifecycleFailure,
            RebuildableView::PortfolioSnapshot,
        ] {
            let error = parse_rebuild_scope(view, RebuildScope::Id("x".to_owned())).unwrap_err();
            assert!(
                matches!(error, ViewRebuildError::WholeModelOnly { view: refused } if refused == view),
                "{view}: {error}"
            );
            assert!(!view.supports_single_id());
        }
    }

    #[test]
    fn a_malformed_id_is_a_caller_error_during_parsing() {
        let error = parse_rebuild_scope(
            RebuildableView::VaultRegistry,
            RebuildScope::Id("no-delimiter".to_owned()),
        )
        .unwrap_err();
        assert!(
            matches!(error, ViewRebuildError::InvalidVaultRegistryId { .. }),
            "{error}"
        );
        // The typed source chain is preserved, not flattened into a string.
        assert!(
            std::error::Error::source(&error).is_some(),
            "the parse error must remain the source",
        );
    }

    #[tokio::test]
    async fn missing_event_stream_is_a_caller_error_and_preserves_the_view_row() {
        let pool = setup_test_db().await;
        sqlx::query(
            "INSERT INTO position_view (view_id, version, payload) VALUES ('AAPL', 7, '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let parsed = parse_rebuild_scope(
            RebuildableView::Position,
            RebuildScope::Id("AAPL".to_owned()),
        )
        .unwrap();
        let error = validate_rebuild_scope(&pool, parsed).await.unwrap_err();
        assert!(
            matches!(
                &error,
                ViewRebuildError::NoEventStream {
                    view: RebuildableView::Position,
                    id,
                } if id == "AAPL"
            ),
            "{error}"
        );
        assert!(error.is_caller_error());

        // Execution defends the same invariant and rolls its deletion back if a
        // caller bypasses preflight validation.
        let parsed = parse_rebuild_scope(
            RebuildableView::Position,
            RebuildScope::Id("AAPL".to_owned()),
        )
        .unwrap();
        let error = execute_rebuild_view(&pool, parsed).await.unwrap_err();
        assert!(
            matches!(&error, ViewRebuildError::NoEventStream { .. }),
            "{error}"
        );

        let row: (i64, String) =
            sqlx::query_as("SELECT version, payload FROM position_view WHERE view_id = 'AAPL'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row, (7, "{}".to_owned()));
    }

    #[tokio::test]
    async fn aggregate_rebuild_matches_framework_projection_output() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap_or_else(|error| panic!("{error}"));
        let symbol_id = symbol.to_string();
        persist_event::<Position>(
            &pool,
            &symbol_id,
            1,
            &PositionEvent::Initialized {
                symbol,
                threshold: ExecutionThreshold::whole_share(),
                initialized_at: Utc::now(),
            },
        )
        .await;
        Projection::<Position>::sqlite(pool.clone())
            .catch_up()
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        let framework_row: (i64, String) =
            sqlx::query_as("SELECT version, payload FROM position_view WHERE view_id = ?1")
                .bind(&symbol_id)
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|error| panic!("{error}"));

        rebuild_view(
            &pool,
            RebuildableView::Position,
            RebuildScope::Id(symbol_id.clone()),
        )
        .await
        .unwrap_or_else(|error| panic!("{error}"));

        let rebuilt_row: (i64, String) =
            sqlx::query_as("SELECT version, payload FROM position_view WHERE view_id = ?1")
                .bind(&symbol_id)
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(rebuilt_row, framework_row);
    }

    #[tokio::test]
    async fn aggregate_rebuilds_roll_back_when_event_deserialization_fails() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap_or_else(|error| panic!("{error}"));
        let symbol_id = symbol.to_string();
        persist_event::<Position>(
            &pool,
            &symbol_id,
            1,
            &PositionEvent::Initialized {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                initialized_at: Utc::now(),
            },
        )
        .await;
        Projection::<Position>::sqlite(pool.clone())
            .catch_up()
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        let before: (i64, String) =
            sqlx::query_as("SELECT version, payload FROM position_view WHERE view_id = ?1")
                .bind(&symbol_id)
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|error| panic!("{error}"));

        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, 2, 'Corrupt', '1', '{', '{}')",
        )
        .bind(Position::AGGREGATE_TYPE)
        .bind(&symbol_id)
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("{error}"));

        for scope in [RebuildScope::Id(symbol_id.clone()), RebuildScope::All] {
            let error = rebuild_view(&pool, RebuildableView::Position, scope)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    ViewRebuildError::Position(ProjectionError::Serde { .. })
                ),
                "{error}"
            );

            let after: (i64, String) =
                sqlx::query_as("SELECT version, payload FROM position_view WHERE view_id = ?1")
                    .bind(&symbol_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(after, before);
        }
    }

    #[tokio::test]
    async fn an_empty_store_rebuilds_every_view_to_nothing() {
        let pool = setup_test_db().await;
        for view in [
            RebuildableView::Position,
            RebuildableView::OffchainOrder,
            RebuildableView::VaultRegistry,
            RebuildableView::RebalanceTiming,
            RebuildableView::EquityTiming,
            RebuildableView::LifecycleFailure,
            RebuildableView::PortfolioSnapshot,
        ] {
            let rebuilt = rebuild_view(&pool, view, RebuildScope::All)
                .await
                .unwrap_or_else(|error| panic!("{view}: {error}"));
            assert_eq!(rebuilt.view, view);
            assert_eq!(rebuilt.scope, RebuildScope::All);
            assert_eq!(
                rebuilt.replayed,
                (!view.supports_single_id()).then_some(0),
                "{view}: read models report a replay count, per-aggregate views do not"
            );
        }
    }
}
