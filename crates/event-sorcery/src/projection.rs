//! Backend-agnostic projection for loading entity state from
//! materialized views.
//!
//! Generic over the view repository implementation, allowing any
//! backend (SQLite, Postgres, in-memory) that implements
//! `ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`.

use async_trait::async_trait;
use cqrs_es::persist::{GenericQuery, PersistenceError, ViewContext, ViewRepository};
use sqlite_es::SqliteViewRepository;
use sqlx::SqlitePool;
use sqlx::sqlite::Sqlite;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

use crate::EventSourced;
use crate::lifecycle::{Lifecycle, LifecycleError};

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
pub enum ProjectionError {
    #[error("entity has no materialized view (PROJECTION = None)")]
    NoTable,
    #[error(
        "operation requires a SQLite-backed projection \
         created via Projection::sqlite()"
    )]
    NotSqliteBacked,
    #[error("column '{column}' does not exist in table '{table}'")]
    ColumnNotFound { column: Column, table: String },
    #[error(
        "generated column '{column}' has all NULL values in \
         table '{table}' ({row_count} rows) — likely stale \
         JSON path in migration"
    )]
    StaleColumn {
        column: Column,
        table: String,
        row_count: i64,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// SQLite view repository that hides [`Lifecycle`] from
/// Projection's public type signature.
///
/// Without this newtype, the default `Repo` parameter would be
/// `SqliteViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`,
/// leaking the `pub(crate)` `Lifecycle` type to external
/// consumers. This wraps the underlying repository so that
/// `Projection<Position>` expands to
/// `Projection<Position, SqliteProjectionRepo<Position>>` — no
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

impl<Entity: EventSourced> Projection<Entity> {
    /// Creates a SQLite-backed projection using
    /// [`EventSourced::PROJECTION`].
    ///
    /// Returns `Err(ProjectionError::NoTable)` if the entity has
    /// no materialized view configured (`PROJECTION = None`).
    pub fn sqlite(pool: SqlitePool) -> Result<Self, ProjectionError> {
        let Table(table) = Entity::PROJECTION.ok_or(ProjectionError::NoTable)?;
        let repo = Arc::new(SqliteProjectionRepo {
            inner: SqliteViewRepository::<Lifecycle<Entity>, Lifecycle<Entity>>::new(
                pool.clone(),
                table.to_string(),
            ),
        });

        Ok(Self {
            repo,
            pool: Some(pool),
            table_name: Some(table.to_string()),
            _entity: PhantomData,
        })
    }

    /// Load all live entities from the view table.
    ///
    /// Returns every entity in `Live` state, skipping
    /// non-live aggregates with a warning.
    pub async fn load_all(&self) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError>
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
    ) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError>
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

    fn sqlite_backing(&self) -> Result<(&SqlitePool, &str), ProjectionError> {
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

                match lifecycle {
                    Lifecycle::Live(entity) => Some((id, entity)),
                    _ => {
                        warn!(%id, "Skipping non-live aggregate in view");
                        None
                    }
                }
            })
            .collect()
    }
}

impl<Entity: EventSourced, Repo> Projection<Entity, Repo>
where
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
{
    /// Creates a projection from any view repository backend.
    pub fn new(repo: Arc<Repo>) -> Self {
        Self {
            repo,
            pool: None,
            table_name: None,
            _entity: PhantomData,
        }
    }

    /// Load the current entity state from the materialized view.
    ///
    /// Returns:
    /// - `Ok(Some(entity))` if the entity is live
    /// - `Ok(None)` if the entity has not been initialized
    /// - `Err(error)` if the entity is in a failed lifecycle state
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, LifecycleError<Entity>>
    where
        Entity::Error: Clone,
    {
        let aggregate_id = id.to_string();
        let lifecycle = match self.repo.load(&aggregate_id).await {
            Ok(Some(lifecycle)) => lifecycle,
            Ok(None) | Err(_) => return Ok(None),
        };

        match lifecycle {
            Lifecycle::Live(entity) => Ok(Some(entity)),
            Lifecycle::Uninitialized => Ok(None),
            Lifecycle::Failed { error, .. } => Err(error),
        }
    }

    /// Construct a GenericQuery for wiring with StoreBuilder.
    ///
    /// GenericQuery implements `cqrs_es::Query<Lifecycle<Entity>>`
    /// natively via `View::update`, so it can be registered
    /// directly with the CQRS framework.
    pub(crate) fn inner_query(&self) -> GenericQuery<Repo, Lifecycle<Entity>, Lifecycle<Entity>> {
        GenericQuery::new(Arc::clone(&self.repo))
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

/// Validates that a column exists in the table schema and returns
/// an error if all values are NULL (indicates a stale generated
/// column).
async fn validate_column(
    pool: &SqlitePool,
    table: &str,
    column: &Column,
) -> Result<(), ProjectionError> {
    let column_name = column.0;

    let columns: Vec<(String,)> =
        sqlx::query_as(&format!("SELECT name FROM pragma_table_info('{table}')"))
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
        const PROJECTION: Option<Table> = None;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &TestEvent) -> Option<Self> {
            Some(Self {
                name: "test".to_string(),
            })
        }

        fn evolve(_event: &TestEvent, state: &Self) -> Result<Option<Self>, Never> {
            Ok(Some(state.clone()))
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
        let error = LifecycleError::Uninitialized;
        let repo = InMemoryRepo::with(vec![(
            "id-1",
            Lifecycle::Failed {
                error: error.clone(),
                last_valid_state: None,
            },
        )]);
        let projection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await;

        assert_eq!(result.unwrap_err(), error);
    }
}
