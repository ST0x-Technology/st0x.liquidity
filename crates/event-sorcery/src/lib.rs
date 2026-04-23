//! A safer, more ergonomic interface for event-sourced entities
//! on top of cqrs-es.
//!
//! # Why this exists
//!
//! cqrs-es provides the `Aggregate` trait, but it has several
//! sharp edges that have caused production bugs:
//!
//! - **Infallible `apply`**: `Aggregate::apply(&mut self, event)`
//!   returns nothing. Financial applications cannot panic on
//!   arithmetic overflow, so every aggregate needs a wrapper to
//!   capture errors without panicking. Every aggregate in the
//!   codebase had identical boilerplate for this.
//!
//! - **Stringly-typed aggregate IDs**: `cqrs.execute("some-id",
//!   cmd)` takes `&str`, making it trivial to pass the wrong ID.
//!   This has caused production bugs.
//!
//! - **No schema versioning**: When aggregate or view schemas
//!   change, stale snapshots and views cause silent data
//!   corruption. Manual database intervention is required.
//!
//! - **Flat command handling**: A single `handle` method receives
//!   all commands regardless of lifecycle state. Implementors
//!   must manually match on (lifecycle_state, command) tuples,
//!   making it easy to accidentally reference state during
//!   initialization or forget to handle a case.
//!
//! # Design
//!
//! [`EventSourced`] replaces direct `Aggregate` usage. Domain
//! types implement `EventSourced`, and [`Lifecycle`] provides a
//! blanket `Aggregate` impl that bridges to cqrs-es. Consumers
//! interact through [`Store`], which enforces typed IDs and hides
//! cqrs-es internals.
//!
//! ```text
//! Domain type          Adapter             cqrs-es
//! +--------------+     +----------------+  +------------+
//! | impl         | --> | Lifecycle      |  | Aggregate  |
//! | EventSourced |     | (blanket impl) |--| trait      |
//! +--------------+     +----------------+  +------------+
//!                             |
//!                      +------+------+
//!                      | Store       |
//!                      | (typed IDs, |
//!                      |  send())    |
//!                      +-------------+
//! ```
//!
//! # Naming
//!
//! Method names follow two themes to distinguish their purpose:
//!
//! **Event-side** (replaying events to reconstruct state) uses
//! evolution-themed names:
//! - [`originate`](EventSourced::originate) -- create initial
//!   state from the first event
//! - [`evolve`](EventSourced::evolve) -- derive new state from
//!   subsequent events
//!
//! **Command-side** (processing commands to produce events) uses
//! state-machine names:
//! - [`initialize`](EventSourced::initialize) -- handle a
//!   command when no state exists yet
//! - [`transition`](EventSourced::transition) -- handle a
//!   command against existing state
//!
//! The asymmetry is intentional: commands express intent,
//! events express facts. Different verbs for different
//! semantics.
//!
//! cqrs-es names (`Aggregate`, `Query`, `View`, `DomainEvent`)
//! are deliberately avoided in our public API to make it
//! immediately obvious whether code belongs to this crate or
//! to cqrs-es.

pub(crate) mod dependency;
mod lifecycle;
mod projection;
mod reactor;
mod schema_registry;
#[cfg(any(test, feature = "test-support"))]
mod testing;
mod wire;

use async_trait::async_trait;
pub use cqrs_es::AggregateError;
pub use cqrs_es::DomainEvent;
use cqrs_es::EventStore;
use cqrs_es::persist::PersistedEventStore;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlite_es::{SqliteCqrs, SqliteEventRepository};
use sqlx::SqlitePool;
use std::fmt::{Debug, Display};
use std::str::FromStr;

#[doc(hidden)]
pub use dependency::Cons;
pub use dependency::Nil;
pub use dependency::{Dependent, EntityList, Fold, HasEntity, OneOf};
use lifecycle::Lifecycle;
pub use lifecycle::{LifecycleError, Never};
pub use projection::{Column, Projection, ProjectionError, SqliteProjectionRepo, Table};
pub use reactor::Reactor;
pub use schema_registry::{ReconcileError, Reconciler, SchemaRegistry};
#[cfg(any(test, feature = "test-support"))]
pub use testing::{
    ReactorHarness, SpyReactor, TestHarness, TestResult, TestStore, replay, test_store,
};
pub use wire::StoreBuilder;

/// The core abstraction for event-sourced domain entities.
///
/// Implement this trait on your domain type (e.g., `Position`,
/// `OffchainOrder`) to get a complete event-sourcing setup:
/// [`Lifecycle`] provides a blanket `Aggregate` impl, and
/// [`Store`] provides type-safe command dispatch.
///
/// # Associated types
///
/// - `Id`: The strongly-typed aggregate identifier. Prevents
///   mixing up IDs between different entity types at compile
///   time. Converted to string at the cqrs-es boundary only.
/// - `Event`: Domain events that drive state changes. Must be
///   `Eq` so lifecycle error states can carry typed events.
/// - `Command`: Instructions that produce events. A single
///   command type is used for both initialization and
///   transitions -- the lifecycle routes based on state.
/// - `Error`: Domain-specific errors from command handling or
///   event application (e.g., arithmetic overflow). For
///   entities with infallible operations, use [`Never`].
/// - `Services`: External dependencies injected into command
///   handlers (e.g., `Arc<dyn OrderPlacer>`). Use `()` when
///   no services are needed.
///
/// # Constants
///
/// - `AGGREGATE_TYPE`: Stable identifier for the event store.
///   Must not change after events are persisted.
/// - `SCHEMA_VERSION`: Bump when the entity's state, event, or
///   view schema changes. On startup, the wiring infrastructure
///   detects version mismatches and automatically clears stale
///   snapshots and replays views.
///
/// # Event-side methods
///
/// These reconstruct state from the event log during replay.
/// They are called by the blanket `Aggregate::apply` impl on
/// [`Lifecycle`], never by application code directly.
///
/// - `originate`: Attempt to create initial state from an
///   event. Returns `Some(state)` for genesis events, `None`
///   for events that require existing state.
/// - `evolve`: Attempt to derive new state from an event
///   applied to existing state. Returns `Ok(Some(new_state))`
///   on success, `Ok(None)` if the event doesn't apply to the
///   current state (mismatch), or `Err` for domain failures
///   like arithmetic overflow.
///
/// # Command-side methods
///
/// These process commands to produce events. They are called by
/// the blanket `Aggregate::handle` impl on [`Lifecycle`], which
/// routes commands based on lifecycle state.
///
/// - `initialize`: Handle a command when the entity doesn't
///   exist yet. Has no `&self` parameter, preventing accidental
///   reference to existing state during creation.
/// - `transition`: Handle a command against existing state.
///   Receives `&self` (the domain type, not `Lifecycle`), so
///   the handler only deals with live state.
#[async_trait]
pub trait EventSourced: Clone + Debug + Send + Sync + Sized + Serialize + DeserializeOwned {
    /// Aggregate identity type, used as the key in the event store.
    type Id: Debug + Display + FromStr + Clone + Send + Sync;
    /// Domain event type emitted by commands and applied during replay.
    type Event: DomainEvent;
    /// Command type that drives state transitions.
    type Command: Send + Sync;
    /// Domain error type returned by command handlers and event
    /// application.
    type Error: DomainError;
    /// External dependencies injected into command handlers (e.g.
    /// API clients, order placers).
    type Services: Send + Sync;
    /// Whether this entity has a materialized view.
    ///
    /// Set to `Table` with `PROJECTION = Table("view_name")` for
    /// entities with materialized views. Set to `Nil` with
    /// `PROJECTION = Nil` for entities without views.
    ///
    /// [`StoreBuilder::build()`] uses this to auto-wire projections:
    /// `Table` entities return `(Store, Projection)`, `Nil` entities
    /// return just `Store`.
    type Materialized;

    /// Unique string identifying this aggregate type in the event
    /// store. Must be stable across deployments.
    const AGGREGATE_TYPE: &'static str;
    /// Projection table name (for `Table` entities) or `Nil`.
    const PROJECTION: Self::Materialized;
    /// Schema version for migration reconciliation. Bump when the
    /// event schema changes.
    const SCHEMA_VERSION: u64;

    /// Create initial state from a genesis event.
    ///
    /// Returns `Some(state)` if this event creates the entity,
    /// `None` if it requires existing state. Returning `None`
    /// causes [`Lifecycle`] to enter a `Failed` state with a
    /// [`LifecycleError::EventCantOriginate`].
    fn originate(event: &Self::Event) -> Option<Self>;

    /// Derive new entity from an event applied to the current one.
    ///
    /// - `Ok(Some(new_entity))` -- event applied successfully
    /// - `Ok(None)` -- event doesn't apply to current entity
    ///   (becomes [`LifecycleError::UnexpectedEvent`])
    /// - `Err(error)` -- domain error during application
    ///   (becomes [`LifecycleError::Apply`])
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error>;

    /// Handle a command when the entity doesn't exist yet.
    ///
    /// No `&self` -- impossible to accidentally reference
    /// existing state during creation.
    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    /// Handle a command against existing state.
    ///
    /// `&self` is the domain type directly, not `Lifecycle`.
    /// The handler only deals with live state; lifecycle routing
    /// is handled by the blanket `Aggregate` impl.
    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;
}

/// Type-safe command dispatch for an event-sourced entity.
///
/// Wraps `SqliteCqrs<Lifecycle<Entity>>` and enforces that
/// commands are addressed to the correct entity type via
/// strongly-typed IDs. This prevents a class of bugs where
/// string aggregate IDs are mixed up between different entity
/// types.
///
/// # Usage
///
/// ```ignore
/// let positions: Store<Position> = /* built by StoreBuilder */;
///
/// // Typed ID -- can't accidentally pass an OffchainOrderId
/// let symbol = Symbol::new("AAPL").unwrap();
/// positions.send(&symbol, PositionCommand::AcknowledgeFill { .. }).await?;
/// ```
///
/// Produced by [`StoreBuilder::build()`] during conductor
/// startup. The builder handles CQRS framework construction,
/// query wiring, and schema reconciliation, returning a
/// ready-to-use `Store`.
pub struct Store<Entity: EventSourced> {
    cqrs: SqliteCqrs<Lifecycle<Entity>>,
    event_store: PersistedEventStore<SqliteEventRepository, Lifecycle<Entity>>,
}

impl<Entity: EventSourced> Store<Entity> {
    /// Wrap an existing `SqliteCqrs` framework.
    ///
    /// Prefer using `StoreBuilder::build()` which handles wiring
    /// and reconciliation. This constructor exists for cases
    /// where direct construction is needed (e.g., tests).
    pub(crate) fn new(cqrs: SqliteCqrs<Lifecycle<Entity>>, pool: SqlitePool) -> Self {
        let repo = SqliteEventRepository::new(pool);
        let event_store = PersistedEventStore::new_event_store(repo);
        Self { cqrs, event_store }
    }

    /// Send a command to the entity identified by `id`.
    ///
    /// The command is routed based on the entity's lifecycle
    /// state:
    /// - Uninitialized -> `Entity::initialize`
    /// - Live -> `Entity::transition`
    /// - Failed -> returns the stored error
    pub async fn send(
        &self,
        id: &Entity::Id,
        command: Entity::Command,
    ) -> Result<(), SendError<Entity>> {
        self.cqrs.execute(&id.to_string(), command).await
    }

    /// Load an entity's current state directly from the event store.
    ///
    /// Replays events to reconstruct aggregate state. No query
    /// processors are dispatched.
    ///
    /// Returns:
    /// - `Ok(Some(entity))` if the entity is live
    /// - `Ok(None)` if the entity has not been initialized
    /// - `Err` if the entity is in a failed lifecycle state or on infrastructure error
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, SendError<Entity>> {
        let context = self.event_store.load_aggregate(&id.to_string()).await?;

        Ok(context.aggregate.into_result()?)
    }

    /// Reconstruct an entity's state from events without needing
    /// a full `Store` (no services or CQRS framework required).
    ///
    /// Useful in test/CLI contexts where you only need to read
    /// aggregate state and never send commands.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn load_from_pool(
        pool: SqlitePool,
        id: &Entity::Id,
    ) -> Result<Option<Entity>, SendError<Entity>> {
        let repo = SqliteEventRepository::new(pool);
        let event_store =
            PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_event_store(repo);
        let context = event_store.load_aggregate(&id.to_string()).await?;

        Ok(context.aggregate.into_result()?)
    }
}

/// Error returned by [`Store::send`] and [`Store::load`].
///
/// Wraps the cqrs-es `AggregateError` containing a
/// `LifecycleError` so that consumers don't import from cqrs-es
/// or lifecycle directly.
pub type SendError<Entity> = AggregateError<LifecycleError<Entity>>;

impl<Entity: EventSourced> From<LifecycleError<Entity>> for SendError<Entity> {
    fn from(error: LifecycleError<Entity>) -> Self {
        Self::UserError(error)
    }
}

/// Bounds required for domain error types used with
/// [`EventSourced`].
///
/// [`LifecycleError`] stores the entity's error in its `Apply`
/// variant and derives `Clone`, `Serialize`, `Deserialize`,
/// `PartialEq`, and `Eq`. This trait captures those bounds in
/// one place so implementors see a single meaningful name
/// instead of a long bound list.
pub trait DomainError:
    std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

impl<T> DomainError for T where
    T: std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

/// Load a single entity by replaying events from the store.
///
/// Creates a lightweight, temporary event store - no CQRS framework, no
/// query processors. Suitable for read-only access from contexts that
/// don't own a [`Store`] (e.g., dashboard transfer loading).
///
/// # Errors
///
/// Returns `SendError` if event store loading or lifecycle
/// reconstruction fails.
pub async fn load_entity<Entity: EventSourced>(
    pool: &SqlitePool,
    id: &Entity::Id,
) -> Result<Option<Entity>, SendError<Entity>> {
    let repo = SqliteEventRepository::new(pool.clone());
    let event_store =
        PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_event_store(repo);

    let context = event_store.load_aggregate(&id.to_string()).await?;

    Ok(context.aggregate.into_result()?)
}

/// Load all aggregate IDs for a given entity type.
///
/// Queries the events table for distinct aggregate IDs. Used
/// by dashboard transfer loading to enumerate all transfer
/// aggregates without requiring access to a [`Store`].
///
/// Returns an error if any stored aggregate ID fails to parse,
/// since that indicates data corruption or a schema mismatch.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or if stored aggregate
/// IDs fail to parse.
pub async fn load_all_ids<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT aggregate_id FROM events \
         WHERE aggregate_type = ?1 \
         ORDER BY aggregate_id ASC",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    tracing::warn!(
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Load aggregate IDs with pagination, newest first (by highest rowid).
///
/// Returns up to `limit` IDs starting from `offset`, ordered by most
/// recently created aggregate first (based on the maximum rowid of each
/// aggregate's events).
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or unparseable IDs.
pub async fn load_ids_paginated<Entity: EventSourced>(
    pool: &SqlitePool,
    limit: usize,
    offset: usize,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM events \
         WHERE aggregate_type = ?1 \
         GROUP BY aggregate_id \
         ORDER BY MAX(rowid) DESC \
         LIMIT ?2 OFFSET ?3",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(i64::try_from(limit)?)
    .bind(i64::try_from(offset)?)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    tracing::warn!(
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID (paginated)"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Count the total number of distinct aggregates of this type.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database failure or numeric conversion error.
pub async fn count_aggregates<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<usize, LoadAllIdsError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(DISTINCT aggregate_id) FROM events \
         WHERE aggregate_type = ?1",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_one(pool)
    .await?;

    Ok(usize::try_from(row.0)?)
}

/// Errors that can occur when loading all aggregate IDs.
#[derive(Debug, thiserror::Error)]
pub enum LoadAllIdsError {
    #[error("Database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error(
        "Found unparseable aggregate IDs for {aggregate_type}: \
         {ids:?}"
    )]
    InvalidIds {
        aggregate_type: &'static str,
        ids: Vec<String>,
    },
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use super::*;

    /// Numeric-only ID that rejects non-numeric strings.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NumericId(u64);

    impl Display for NumericId {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{}", self.0)
        }
    }

    impl FromStr for NumericId {
        type Err = std::num::ParseIntError;

        fn from_str(value: &str) -> Result<Self, Self::Err> {
            value.parse::<u64>().map(NumericId)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Widget {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum WidgetEvent {
        Created { name: String },
    }

    impl DomainEvent for WidgetEvent {
        fn event_type(&self) -> String {
            "WidgetEvent::Created".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("widget error")]
    struct WidgetError;

    enum WidgetCommand {
        Create { name: String },
    }

    #[async_trait]
    impl EventSourced for Widget {
        type Id = NumericId;
        type Event = WidgetEvent;
        type Command = WidgetCommand;
        type Error = WidgetError;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Widget";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
            }
        }

        fn evolve(_entity: &Self, _event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            Ok(None)
        }

        async fn initialize(
            command: WidgetCommand,
            _services: &(),
        ) -> Result<Vec<WidgetEvent>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => Ok(vec![WidgetEvent::Created { name }]),
            }
        }

        async fn transition(
            &self,
            _command: WidgetCommand,
            _services: &(),
        ) -> Result<Vec<WidgetEvent>, WidgetError> {
            Ok(vec![])
        }
    }

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_event(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, 1, 'WidgetEvent::Created', '1.0', ?3, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(r#"{"Created":{"name":"test-widget"}}"#)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_entity_replays_events_into_entity() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "test-widget".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();

        let widget = entity.expect("entity should exist after event replay");
        assert_eq!(widget.name, "test-widget");
    }

    #[tokio::test]
    async fn load_entity_returns_none_when_no_events() {
        let pool = test_pool().await;

        let entity = load_entity::<Widget>(&pool, &NumericId(999)).await.unwrap();

        assert!(entity.is_none(), "expected None for nonexistent aggregate");
    }

    #[tokio::test]
    async fn load_all_ids_returns_parsed_ids() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "10").await;
        insert_event(&pool, "Widget", "20").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(10), NumericId(20)]);
    }

    #[tokio::test]
    async fn load_all_ids_returns_empty_when_no_events() {
        let pool = test_pool().await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn load_all_ids_errors_on_unparseable_id() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "42").await;
        insert_event(&pool, "Widget", "not-a-number").await;

        let error = load_all_ids::<Widget>(&pool)
            .await
            .expect_err("should fail when an ID cannot parse");

        match error {
            LoadAllIdsError::InvalidIds {
                aggregate_type,
                ids,
            } => {
                assert_eq!(aggregate_type, "Widget");
                assert_eq!(ids, vec!["not-a-number"]);
            }
            LoadAllIdsError::Sql(sql_error) => {
                panic!("expected InvalidIds, got Sql: {sql_error}")
            }
            LoadAllIdsError::NumericConversion(conv_error) => {
                panic!("expected InvalidIds, got NumericConversion: {conv_error}")
            }
        }
    }

    #[tokio::test]
    async fn load_all_ids_ignores_other_aggregate_types() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "1").await;
        insert_event(&pool, "OtherAggregate", "should-be-excluded").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(1)]);
    }
}
