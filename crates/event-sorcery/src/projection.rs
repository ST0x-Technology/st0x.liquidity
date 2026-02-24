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
use tracing::warn;

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
    pub(crate) fn sqlite(pool: SqlitePool) -> Self {
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

        let (mut lifecycle, context) = match self.repo.load_with_context(&view_id).await {
            Ok(Some(pair)) => pair,
            Ok(None) => (Lifecycle::default(), ViewContext::new(view_id.clone(), 0)),
            Err(error) => {
                warn!(%view_id, ?error, "Failed to load view for update");
                return Ok(());
            }
        };

        lifecycle.apply(event.clone());

        if let Err(error) = self.repo.update_view(lifecycle, context).await {
            warn!(%view_id, ?error, "Failed to save view update");
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

        const AGGREGATE_TYPE: &'static str = "TestEntity";
        type Materialized = Nil;
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
}
