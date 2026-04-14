//! Backend-agnostic projection for loading entity state from
//! materialized views.
//!
//! Generic over the view repository implementation, allowing any
//! backend (SQLite, Postgres, in-memory) that implements
//! `ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`.

use async_trait::async_trait;
use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use sqlite_es::SqliteViewRepository;
use sqlx::SqlitePool;
use sqlx::sqlite::Sqlite;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::EventSourced;
use crate::dependency::{Cons, Dependent, EntityList, Nil};
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::reactor::Reactor;

/// A materialized view table name.
///
/// Used with [`EventSourced::PROJECTION`] to declare that an
/// entity has a materialized view, and with
/// [`Projection::sqlite`] to create the projection.
#[derive(Debug, Clone, Copy)]
pub struct Table(pub &'static str);

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// A column name for view table queries.
///
/// Used with [`Projection::filter`] to query materialized views
/// by generated column values. Column existence is validated
/// against the table schema at query time to catch stale
/// generated columns early.
#[derive(Debug, Clone)]
pub struct Column(pub &'static str);

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Errors from [`Projection`] query operations.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionError<Entity: EventSourced> {
    #[error(
        "operation requires a SQLite-backed projection \
         created via Projection::sqlite()"
    )]
    NotSqliteBacked,
    #[error("column '{column}' does not exist in table '{table}'")]
    ColumnNotFound { column: Column, table: String },
    #[error(
        "generated column '{column}' has all NULL values in \
         table '{table}' ({row_count} rows) - likely stale \
         JSON path in migration"
    )]
    StaleColumn {
        column: Column,
        table: String,
        row_count: i64,
    },
    #[error("serde failed for aggregate '{aggregate_id}': {source}")]
    Serde {
        aggregate_id: String,
        source: serde_json::Error,
    },
    #[error(
        "event sequence gap for aggregate '{aggregate_id}': \
         expected {expected} events after version {view_version}, \
         found {actual}"
    )]
    EventSequenceGap {
        aggregate_id: String,
        view_version: i64,
        expected: i64,
        actual: usize,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Lifecycle(Box<LifecycleError<Entity>>),
}

impl<Entity: EventSourced> From<LifecycleError<Entity>> for ProjectionError<Entity> {
    fn from(error: LifecycleError<Entity>) -> Self {
        Self::Lifecycle(Box::new(error))
    }
}

/// SQLite view repository that hides [`Lifecycle`] from
/// Projection's public type signature.
///
/// Without this newtype, the default `Repo` parameter would be
/// `SqliteViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`,
/// leaking the `pub(crate)` `Lifecycle` type to external
/// consumers. This wraps the underlying repository so that
/// `Projection<Position>` expands to
/// `Projection<Position, SqliteProjectionRepo<Position>>` - no
/// `Lifecycle` visible.
pub struct SqliteProjectionRepo<Entity: EventSourced> {
    inner: SqliteViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
}

#[async_trait]
impl<Entity: EventSourced> ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>
    for SqliteProjectionRepo<Entity>
{
    async fn load(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<Lifecycle<Entity>>, PersistenceError> {
        self.inner.load(aggregate_id).await
    }

    async fn load_with_context(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<(Lifecycle<Entity>, ViewContext)>, PersistenceError> {
        self.inner.load_with_context(aggregate_id).await
    }

    async fn update_view(
        &self,
        view: Lifecycle<Entity>,
        context: ViewContext,
    ) -> Result<(), PersistenceError> {
        self.inner.update_view(view, context).await
    }
}

/// Materialized view of an event-sourced entity.
///
/// Provides [`load`](Self::load) to retrieve a single entity by
/// ID, [`load_all`](Self::load_all) to retrieve all live
/// entities, and [`filter`](Self::filter) for typed
/// column-filtered queries. Backed by SQLite in production; the
/// `Repo` parameter defaults to [`SqliteProjectionRepo`] so
/// consumers write `Projection<Position>`.
///
/// Constructed via [`sqlite`](Self::sqlite) during wiring or
/// directly in CLI/test code.
pub struct Projection<Entity: EventSourced, Repo = SqliteProjectionRepo<Entity>> {
    repo: Arc<Repo>,
    pool: Option<SqlitePool>,
    table_name: Option<String>,
    _entity: PhantomData<Entity>,
}

impl<Entity: EventSourced<Materialized = Table>> Projection<Entity> {
    /// Creates a SQLite-backed projection for an entity that
    /// declares `type Materialized = Table`.
    ///
    /// Uses `Entity::PROJECTION` directly to determine the view
    /// table name. Only callable on entities with a materialized
    /// view.
    pub fn sqlite(pool: SqlitePool) -> Self {
        let Table(table) = Entity::PROJECTION;
        let repo = Arc::new(SqliteProjectionRepo {
            inner: SqliteViewRepository::<Lifecycle<Entity>, Lifecycle<Entity>>::new(
                pool.clone(),
                table.to_string(),
            ),
        });

        Self {
            repo,
            pool: Some(pool),
            table_name: Some(table.to_string()),
            _entity: PhantomData,
        }
    }

    /// Load all live entities from the view table.
    ///
    /// Returns every entity in `Live` state, skipping
    /// non-live aggregates with a warning.
    pub async fn load_all(&self) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError<Entity>>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (pool, table) = self.sqlite_backing()?;

        let query = format!(
            "SELECT view_id, payload FROM {table}
             ORDER BY view_id ASC"
        );

        let rows: Vec<(String, String)> = sqlx::query_as(&query).fetch_all(pool).await?;

        Ok(Self::parse_rows(rows))
    }

    /// Load all live entities where a generated column matches
    /// a typed value.
    ///
    /// The value can be any domain type that implements
    /// `sqlx::Type<Sqlite>` and `sqlx::Encode<Sqlite>` (e.g.,
    /// `OrderStatus`), so callers pass typed values rather than
    /// raw strings.
    ///
    /// Validates that the column exists in the table schema
    /// and has at least one non-NULL value (catches stale
    /// generated columns whose JSON paths drifted from the
    /// actual serialization format).
    pub async fn filter<V>(
        &self,
        column: Column,
        value: &V,
    ) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError<Entity>>
    where
        for<'q> &'q V: sqlx::Encode<'q, Sqlite>,
        V: sqlx::Type<Sqlite> + Send + Sync,
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (pool, table) = self.sqlite_backing()?;
        validate_column(pool, table, &column).await?;

        let column_name = column.0;
        let query = format!(
            "SELECT view_id, payload FROM {table}
             WHERE {column_name} = ?1
             ORDER BY view_id ASC"
        );

        let rows: Vec<(String, String)> =
            sqlx::query_as(&query).bind(value).fetch_all(pool).await?;

        Ok(Self::parse_rows(rows))
    }

    /// Replays any events the view missed due to a crash between
    /// event persistence and view update.
    ///
    /// For each view row, compares its `version` against the max
    /// event `sequence` for that aggregate. If behind, fetches the
    /// missed events and applies them incrementally.
    ///
    /// On a normal startup (no crash), this is a cheap version
    /// comparison query with no replay.
    pub async fn catch_up(&self) -> Result<(), ProjectionError<Entity>> {
        let (pool, table) = self.sqlite_backing()?;
        let aggregate_type = <Lifecycle<Entity> as Aggregate>::aggregate_type();

        // Drive from events table (LEFT JOIN) so we also detect aggregates
        // with persisted events but no view row (crash before initial view write).
        // view_version is NULL when the view row is missing.
        let stale_aggregates: Vec<(String, Option<i64>, i64)> = sqlx::query_as(&format!(
            "SELECT e.aggregate_id, v.version, e.max_seq \
             FROM ( \
                 SELECT aggregate_id, MAX(sequence) as max_seq \
                 FROM events \
                 WHERE aggregate_type = ?1 \
                 GROUP BY aggregate_id \
             ) e \
             LEFT JOIN {table} v ON v.view_id = e.aggregate_id \
             WHERE v.version IS NULL OR e.max_seq > v.version"
        ))
        .bind(&aggregate_type)
        .fetch_all(pool)
        .await?;

        for (aggregate_id, view_version, max_seq) in &stale_aggregates {
            let view_version = view_version.unwrap_or(0);

            self.replay_missed_events(
                pool,
                table,
                &aggregate_type,
                aggregate_id,
                view_version,
                *max_seq,
            )
            .await?;
        }

        Ok(())
    }

    /// Rebuild a single view by deleting its row and replaying
    /// all events from scratch via `catch_up`.
    ///
    /// Use as an escape hatch when a view becomes corrupted due
    /// to lost updates.
    pub async fn rebuild(&self, id: &Entity::Id) -> Result<(), ProjectionError<Entity>>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (pool, table) = self.sqlite_backing()?;
        let view_id = id.to_string();

        info!(%view_id, %table, "Rebuilding view from event history");

        sqlx::query(&format!("DELETE FROM {table} WHERE view_id = ?1"))
            .bind(&view_id)
            .execute(pool)
            .await?;

        self.catch_up().await
    }

    /// Rebuild all views by deleting every row and replaying
    /// all events from scratch via `catch_up`.
    pub async fn rebuild_all(&self) -> Result<(), ProjectionError<Entity>>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (pool, table) = self.sqlite_backing()?;

        info!(%table, "Rebuilding all views from event history");

        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(pool)
            .await?;

        self.catch_up().await
    }

    async fn replay_missed_events(
        &self,
        pool: &SqlitePool,
        table: &str,
        aggregate_type: &str,
        view_id: &str,
        view_version: i64,
        max_seq: i64,
    ) -> Result<(), ProjectionError<Entity>> {
        let behind = max_seq - view_version;

        info!(
            %view_id, %view_version, %max_seq, %behind, %aggregate_type,
            "View is behind, replaying missed events"
        );

        let mut lifecycle = match self.repo.load_with_context(view_id).await? {
            Some((lifecycle, _context)) => lifecycle,
            // No view row exists -- start from scratch
            None => Lifecycle::default(),
        };

        let missed_payloads: Vec<(String,)> = sqlx::query_as(
            "SELECT payload FROM events \
             WHERE aggregate_type = ?1 \
               AND aggregate_id = ?2 \
               AND sequence > ?3 \
             ORDER BY sequence ASC",
        )
        .bind(aggregate_type)
        .bind(view_id)
        .bind(view_version)
        .fetch_all(pool)
        .await?;

        let actual = missed_payloads.len();
        // behind is always positive (SQL WHERE max_seq > version), safe to compare
        if !usize::try_from(behind).is_ok_and(|expected| actual == expected) {
            return Err(ProjectionError::EventSequenceGap {
                aggregate_id: view_id.to_string(),
                view_version,
                expected: behind,
                actual,
            });
        }

        for (payload_json,) in &missed_payloads {
            let event: Entity::Event = serde_json::from_str(payload_json).map_err(|source| {
                ProjectionError::<Entity>::Serde {
                    aggregate_id: view_id.to_string(),
                    source,
                }
            })?;

            lifecycle.apply(event);
        }

        let payload = serde_json::to_string(&lifecycle).map_err(|source| ProjectionError::<
            Entity,
        >::Serde {
            aggregate_id: view_id.to_string(),
            source,
        })?;

        // Write directly with version = max_seq, bypassing the view repo's
        // optimistic lock (which expects version + 1 increments). This is
        // safe because catch_up runs once at startup before the main loop.
        sqlx::query(&format!(
            "INSERT INTO {table} (view_id, version, payload) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(view_id) DO UPDATE SET version = ?2, payload = ?3"
        ))
        .bind(view_id)
        .bind(max_seq)
        .bind(&payload)
        .execute(pool)
        .await?;

        info!(%view_id, %behind, "View caught up successfully");

        Ok(())
    }

    fn sqlite_backing(&self) -> Result<(&SqlitePool, &str), ProjectionError<Entity>> {
        let pool = self.pool.as_ref().ok_or(ProjectionError::NotSqliteBacked)?;

        let table = self
            .table_name
            .as_deref()
            .ok_or(ProjectionError::NotSqliteBacked)?;

        Ok((pool, table))
    }

    fn parse_rows(rows: Vec<(String, String)>) -> Vec<(Entity::Id, Entity)>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        rows.into_iter()
            .filter_map(|(view_id, payload)| {
                let id: Entity::Id = match view_id.parse() {
                    Ok(id) => id,
                    Err(error) => {
                        warn!(view_id, ?error, "Failed to parse view ID");
                        return None;
                    }
                };

                let lifecycle: Lifecycle<Entity> = match serde_json::from_str(&payload) {
                    Ok(lifecycle) => lifecycle,
                    Err(error) => {
                        warn!(%id, ?error, "Failed to deserialize view payload");
                        return None;
                    }
                };

                if let Lifecycle::Live(entity) = lifecycle {
                    Some((id, entity))
                } else {
                    warn!(%id, "Skipping non-live aggregate in view");
                    None
                }
            })
            .collect()
    }
}

// TODO: Projection's Repo parameter ideally encodes a
// higher-kinded type - `Repo<Lifecycle<Entity>, Lifecycle<Entity>>`
// - so the struct definition captures the relationship between
// Repo and Entity without naming Lifecycle in bounds. Rust lacks
// native HKT support, but GAT-based workarounds (e.g., a
// `RepoFamily` trait with `type Repo<V, A>`) can emulate this.
// For now we suppress the warning; proper decoupling deferred to
// when this crate is extracted from the workspace.
#[allow(private_bounds)]
impl<Entity: EventSourced, Repo> Projection<Entity, Repo>
where
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
{
    #[cfg(test)]
    pub(crate) fn new(repo: Arc<Repo>) -> Self {
        Self {
            repo,
            pool: None,
            table_name: None,
            _entity: PhantomData,
        }
    }

    /// Load a single entity by ID from the materialized view.
    ///
    /// Delegates to the underlying view repository, then unwraps
    /// the internal `Lifecycle` wrapper. Returns `None` if the
    /// entity doesn't exist or hasn't been initialized yet.
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, ProjectionError<Entity>> {
        let view_id = id.to_string();

        match self.repo.load(&view_id).await {
            Ok(Some(lifecycle)) => Ok(lifecycle.into_result()?),
            Ok(None) => Ok(None),
            Err(error) => Err(error)?,
        }
    }
}

impl<Entity: EventSourced, Repo> Clone for Projection<Entity, Repo> {
    fn clone(&self) -> Self {
        Self {
            repo: Arc::clone(&self.repo),
            pool: self.pool.clone(),
            table_name: self.table_name.clone(),
            _entity: PhantomData,
        }
    }
}

// Same private_bounds suppression as above -- see HKD TODO.
#[allow(private_bounds)]
impl<Entity, Repo> Dependent for Projection<Entity, Repo>
where
    Entity: EventSourced + 'static,
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>> + Send + Sync,
{
    type Dependencies = Cons<Entity, Nil>;
}

// Same private_bounds suppression as above -- see HKD TODO.
#[async_trait]
#[allow(private_bounds)]
impl<Entity, Repo> Reactor for Projection<Entity, Repo>
where
    Entity: EventSourced + 'static,
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>> + Send + Sync,
{
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (id, event) = event.into_inner();
        let view_id = id.to_string();

        // Retry with exponential backoff: 10ms, 20ms, 40ms, ... capped at 1s.
        // 10 retries gives ~4s of total retry budget, enough for any realistic
        // burst of concurrent writers on the same aggregate.
        let max_retries = 10u32;

        for attempt in 0..=max_retries {
            let (mut lifecycle, context) = match self.repo.load_with_context(&view_id).await {
                Ok(Some(pair)) => pair,
                Ok(None) => (Lifecycle::default(), ViewContext::new(view_id.clone(), 0)),
                Err(error) => {
                    warn!(%view_id, ?error, "Failed to load view for update");
                    return Ok(());
                }
            };

            lifecycle.apply(event.clone());

            match self.repo.update_view(lifecycle, context).await {
                Ok(()) => return Ok(()),
                Err(PersistenceError::OptimisticLockError) if attempt < max_retries => {
                    let delay_ms = 10u64 * (1u64 << attempt.min(6));
                    warn!(
                        %view_id, attempt = attempt + 1, max_retries, delay_ms,
                        "Optimistic lock conflict, retrying view update"
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(PersistenceError::OptimisticLockError) => {
                    error!(
                        %view_id, max_retries,
                        "View update lost: optimistic lock conflict persisted after all retries"
                    );
                    return Ok(());
                }
                Err(error) => {
                    warn!(%view_id, ?error, "Failed to save view update");
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}

/// Validates that a column exists in the table schema and returns
/// an error if all values are NULL (indicates a stale generated
/// column).
async fn validate_column<Entity: EventSourced>(
    pool: &SqlitePool,
    table: &str,
    column: &Column,
) -> Result<(), ProjectionError<Entity>> {
    let column_name = column.0;

    let columns: Vec<(String,)> =
        sqlx::query_as(&format!("SELECT name FROM pragma_table_xinfo('{table}')"))
            .fetch_all(pool)
            .await?;

    if !columns.iter().any(|(name,)| name == column_name) {
        return Err(ProjectionError::ColumnNotFound {
            column: column.clone(),
            table: table.to_string(),
        });
    }

    let row_count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await?;

    if row_count.0 > 0 {
        let non_null_count: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM {table}
             WHERE {column_name} IS NOT NULL"
        ))
        .fetch_one(pool)
        .await?;

        if non_null_count.0 == 0 {
            return Err(ProjectionError::StaleColumn {
                column: column.clone(),
                table: table.to_string(),
                row_count: row_count.0,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::*;
    use crate::Nil;
    use crate::lifecycle::Never;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntity {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvent;

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "TestEvent".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for TestEntity {
        type Id = String;
        type Event = TestEvent;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "TestEntity";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &TestEvent) -> Option<Self> {
            Some(Self {
                name: "test".to_string(),
            })
        }

        fn evolve(entity: &Self, _event: &TestEvent) -> Result<Option<Self>, Never> {
            Ok(Some(entity.clone()))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<TestEvent>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<TestEvent>, Never> {
            Ok(vec![])
        }
    }

    /// In-memory view repository for testing Projection::load.
    struct InMemoryRepo {
        views: RwLock<HashMap<String, Lifecycle<TestEntity>>>,
    }

    impl InMemoryRepo {
        fn with(entries: Vec<(&str, Lifecycle<TestEntity>)>) -> Self {
            let mut views = HashMap::new();
            for (aggregate_id, lifecycle) in entries {
                views.insert(aggregate_id.to_string(), lifecycle);
            }
            Self {
                views: RwLock::new(views),
            }
        }
    }

    #[async_trait]
    impl ViewRepository<Lifecycle<TestEntity>, Lifecycle<TestEntity>> for InMemoryRepo {
        async fn load(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<Lifecycle<TestEntity>>, PersistenceError> {
            Ok(self.views.read().await.get(aggregate_id).cloned())
        }

        async fn load_with_context(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<(Lifecycle<TestEntity>, ViewContext)>, PersistenceError> {
            let view = self.views.read().await.get(aggregate_id).cloned();
            Ok(view.map(|lifecycle| {
                let context = ViewContext::new(aggregate_id.to_string(), 0);
                (lifecycle, context)
            }))
        }

        async fn update_view(
            &self,
            view: Lifecycle<TestEntity>,
            context: ViewContext,
        ) -> Result<(), PersistenceError> {
            self.views
                .write()
                .await
                .insert(context.view_instance_id, view);
            Ok(())
        }
    }

    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::dependency::OneOf;
    use crate::reactor::Reactor;

    /// In-memory view repository that returns `OptimisticLockError`
    /// a configurable number of times before succeeding.
    struct ConflictingRepo {
        views: RwLock<HashMap<String, (Lifecycle<TestEntity>, i64)>>,
        remaining_conflicts: AtomicU32,
    }

    impl ConflictingRepo {
        fn new(conflicts: u32) -> Self {
            Self {
                views: RwLock::new(HashMap::new()),
                remaining_conflicts: AtomicU32::new(conflicts),
            }
        }

        fn with_entity(self, aggregate_id: &str, entity: TestEntity) -> Self {
            let mut views = self.views.into_inner();
            views.insert(aggregate_id.to_string(), (Lifecycle::Live(entity), 1));
            Self {
                views: RwLock::new(views),
                ..self
            }
        }
    }

    #[async_trait]
    impl ViewRepository<Lifecycle<TestEntity>, Lifecycle<TestEntity>> for ConflictingRepo {
        async fn load(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<Lifecycle<TestEntity>>, PersistenceError> {
            Ok(self
                .views
                .read()
                .await
                .get(aggregate_id)
                .map(|(lifecycle, _version)| lifecycle.clone()))
        }

        async fn load_with_context(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<(Lifecycle<TestEntity>, ViewContext)>, PersistenceError> {
            let guard = self.views.read().await;
            Ok(guard.get(aggregate_id).map(|(lifecycle, version)| {
                let context = ViewContext::new(aggregate_id.to_string(), *version);
                (lifecycle.clone(), context)
            }))
        }

        async fn update_view(
            &self,
            view: Lifecycle<TestEntity>,
            context: ViewContext,
        ) -> Result<(), PersistenceError> {
            let remaining = self.remaining_conflicts.load(Ordering::SeqCst);

            if remaining > 0 {
                self.remaining_conflicts
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(PersistenceError::OptimisticLockError);
            }

            let new_version = context.version + 1;
            self.views
                .write()
                .await
                .insert(context.view_instance_id, (view, new_version));

            Ok(())
        }
    }

    #[tokio::test]
    async fn react_retries_on_optimistic_lock_conflict() {
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo = ConflictingRepo::new(2).with_entity("id-1", entity);
        let projection = Projection::new(Arc::new(repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        projection.react(event).await.unwrap();

        // evolve clones the entity unchanged, but the update was persisted
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(
            result,
            Some(TestEntity {
                name: "original".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn react_gives_up_after_max_retries() {
        // 4 conflicts exceeds the max of 3 retries (attempts 0..=3)
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo = Arc::new(ConflictingRepo::new(4).with_entity("id-1", entity.clone()));
        let projection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        // Should not panic -- react swallows the error
        projection.react(event).await.unwrap();

        // All 4 attempts were made (counter decremented from 4 to 0)
        assert_eq!(repo.remaining_conflicts.load(Ordering::SeqCst), 0);

        // View should still have the original entity (update never succeeded)
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(entity));
    }

    #[tokio::test]
    async fn react_succeeds_without_conflict() {
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo = ConflictingRepo::new(0).with_entity("id-1", entity);
        let projection = Projection::new(Arc::new(repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        projection.react(event).await.unwrap();

        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(
            result,
            Some(TestEntity {
                name: "original".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn load_live_entity_returns_some() {
        let entity = TestEntity {
            name: "alice".to_string(),
        };
        let repo = InMemoryRepo::with(vec![("id-1", Lifecycle::Live(entity.clone()))]);
        let projection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await.unwrap();

        assert_eq!(result, Some(entity));
    }

    #[tokio::test]
    async fn load_missing_view_returns_none() {
        let repo = InMemoryRepo::with(vec![]);
        let projection = Projection::new(Arc::new(repo));

        let result = projection.load(&"nonexistent".to_string()).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn load_uninitialized_returns_none() {
        let repo = InMemoryRepo::with(vec![("id-1", Lifecycle::Uninitialized)]);
        let projection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn load_failed_returns_error() {
        let error = LifecycleError::EventCantOriginate { event: TestEvent };
        let repo = InMemoryRepo::with(vec![(
            "id-1",
            Lifecycle::Failed {
                error: error.clone(),
                last_valid_entity: None,
            },
        )]);
        let projection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await;

        assert!(matches!(
            result.unwrap_err(),
            ProjectionError::Lifecycle(boxed)
                if matches!(*boxed, LifecycleError::EventCantOriginate { .. })
        ));
    }

    // -- catch_up tests --

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Counter {
        value: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Created { initial: i64 },
        Incremented,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "CounterEvent::Created".to_string(),
                Self::Incremented => "CounterEvent::Incremented".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Event = CounterEvent;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Table;

        const AGGREGATE_TYPE: &'static str = "Counter";
        const PROJECTION: Table = Table("counter_view");
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &CounterEvent) -> Option<Self> {
            match event {
                CounterEvent::Created { initial } => Some(Self { value: *initial }),
                CounterEvent::Incremented => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, Never> {
            match event {
                CounterEvent::Created { .. } => Ok(None),
                CounterEvent::Incremented => Ok(Some(Self {
                    value: entity.value + 1,
                })),
            }
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<CounterEvent>, Never> {
            Ok(vec![])
        }

        async fn transition(
            &self,
            _command: (),
            _services: &(),
        ) -> Result<Vec<CounterEvent>, Never> {
            Ok(vec![])
        }
    }

    async fn setup_catch_up_db() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::query(
            "CREATE TABLE events ( \
                 aggregate_type TEXT NOT NULL, \
                 aggregate_id TEXT NOT NULL, \
                 sequence BIGINT NOT NULL, \
                 event_type TEXT NOT NULL, \
                 event_version TEXT NOT NULL, \
                 payload TEXT NOT NULL, \
                 metadata TEXT NOT NULL, \
                 PRIMARY KEY (aggregate_type, aggregate_id, sequence) \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE counter_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Also need schema_registry for Projection::sqlite
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_registry ( \
                 aggregate_type TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        pool
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_id: &str,
        sequence: i64,
        event: &CounterEvent,
    ) {
        let payload = serde_json::to_string(event).unwrap();
        let event_type = DomainEvent::event_type(event);

        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind("Counter")
        .bind(aggregate_id)
        .bind(sequence)
        .bind(&event_type)
        .bind(&payload)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_stale_view(pool: &SqlitePool, view_id: &str, version: i64, counter: &Counter) {
        let lifecycle = Lifecycle::Live(counter.clone());
        let payload = serde_json::to_string(&lifecycle).unwrap();

        sqlx::query("INSERT INTO counter_view (view_id, version, payload) VALUES (?1, ?2, ?3)")
            .bind(view_id)
            .bind(version)
            .bind(&payload)
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn catch_up_replays_missed_events() {
        let pool = setup_catch_up_db().await;

        // Insert 3 events: Created(0), Incremented, Incremented
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        // View is stale at version 1 (only saw Created)
        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));
    }

    #[tokio::test]
    async fn catch_up_skips_up_to_date_views() {
        let pool = setup_catch_up_db().await;

        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 5 }).await;

        // View is up to date at version 1
        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 5 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 5 }));
    }

    #[tokio::test]
    async fn catch_up_with_no_events_is_noop() {
        let pool = setup_catch_up_db().await;

        let projection = Projection::<Counter>::sqlite(pool);

        projection.catch_up().await.unwrap();
    }

    #[tokio::test]
    async fn catch_up_is_idempotent() {
        let pool = setup_catch_up_db().await;

        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter>::sqlite(pool.clone());

        projection.catch_up().await.unwrap();
        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));

        // Verify version is correct (should be 3, matching max event sequence)
        let (version,): (i64,) =
            sqlx::query_as("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(version, 3);
    }

    #[tokio::test]
    async fn catch_up_rebuilds_missing_view_row() {
        let pool = setup_catch_up_db().await;

        // Events exist but no view row (crash before initial view write)
        insert_event(
            &pool,
            "counter-1",
            1,
            &CounterEvent::Created { initial: 10 },
        )
        .await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;

        let projection = Projection::<Counter>::sqlite(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 11 }));
    }

    #[tokio::test]
    async fn catch_up_detects_sequence_gap() {
        let pool = setup_catch_up_db().await;

        // Insert events with a gap: seq 1 and seq 3 (missing seq 2)
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        // View at version 1 expects 2 missed events (3 - 1), but only 1 exists
        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        let error = projection.catch_up().await.unwrap_err();
        assert!(
            matches!(
                error,
                ProjectionError::EventSequenceGap {
                    expected: 2,
                    actual: 1,
                    ..
                }
            ),
            "expected EventSequenceGap, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn catch_up_fails_on_malformed_payload() {
        let pool = setup_catch_up_db().await;

        // Insert a valid first event, then a malformed payload at seq 2
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;

        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind("Counter")
        .bind("counter-1")
        .bind(2_i64)
        .bind("CounterEvent::Incremented")
        .bind("not valid json {{{")
        .execute(&pool)
        .await
        .unwrap();

        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        let error = projection.catch_up().await.unwrap_err();
        assert!(
            matches!(error, ProjectionError::Serde { .. }),
            "expected Serde error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn rebuild_replays_all_events_from_scratch() {
        let pool = setup_catch_up_db().await;

        // Insert events and a corrupted view (value should be 2, not 99)
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        insert_stale_view(&pool, "counter-1", 3, &Counter { value: 99 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        // catch_up would not fix this because version matches max_seq
        projection.rebuild(&"counter-1".to_string()).await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));
    }

    #[tokio::test]
    async fn rebuild_all_replays_all_aggregates() {
        let pool = setup_catch_up_db().await;

        // Two aggregates with corrupted views
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(
            &pool,
            "counter-2",
            1,
            &CounterEvent::Created { initial: 10 },
        )
        .await;

        insert_stale_view(&pool, "counter-1", 2, &Counter { value: 99 }).await;
        insert_stale_view(&pool, "counter-2", 1, &Counter { value: 99 }).await;

        let projection = Projection::<Counter>::sqlite(pool);

        projection.rebuild_all().await.unwrap();

        let result1 = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result1, Some(Counter { value: 1 }));

        let result2 = projection.load(&"counter-2".to_string()).await.unwrap();
        assert_eq!(result2, Some(Counter { value: 10 }));
    }
}
