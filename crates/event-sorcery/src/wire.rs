//! CQRS framework construction via [`StoreBuilder`].
//!
//! All CQRS framework construction must go through
//! [`StoreBuilder`], which reconciles schema versions and
//! registers reactors. Direct CQRS framework construction is
//! blocked via clippy's `disallowed-methods`; `StoreBuilder`
//! contains the narrow escape hatch.
//!
//! # Registering reactors
//!
//! Use [`.with()`](StoreBuilder::with) to register a reactor
//! with a builder. For single-entity reactors, wrap in
//! `Arc::new()`. For multi-entity reactors, clone the same
//! `Arc` into each builder.
//!
//! # Auto-wired projections
//!
//! `build()` dispatches on `Entity::Materialized` via a type
//! parameter that defaults to `Entity::Materialized`:
//!
//! - `Table` entities: auto-creates and wires a [`Projection`],
//!   returning `(Arc<Store>, Arc<Projection>)`.
//! - `Nil` entities: returns `Arc<Store>`.
//!
//! This eliminates the footgun of forgetting to wire a
//! projection -- if the entity declares a table, the projection
//! is always present.
//!
//! Exhaustive entity handling is enforced by the reactor's
//! [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain at compile
//! time, not by the wiring infrastructure.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use cqrs_es::persist::PersistedEventStore;
use cqrs_es::persist::PersistenceError;
use cqrs_es::{CqrsFramework, Query};
use sqlx::SqlitePool;

use crate::Nil;
use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::projection::{Projection, ProjectionError, Table};
use crate::reactor::Reactor;
use crate::schema_registry::{ReconcileError, Reconciler};
use crate::sqlite_event_repository::SqliteEventRepository;
use crate::{CompactionPolicy, EventSourced, SqliteCqrs, Store};

/// Builder for a single CQRS framework.
///
/// Parameterized on an [`EventSourced`] entity type. The
/// `Materialized` type parameter defaults to
/// `Entity::Materialized` and determines the `build()` return
/// type: `Table` returns `(Arc<Store>, Arc<Projection>)`, `Nil`
/// returns `Arc<Store>`.
///
/// Register reactors via [`.with()`](Self::with), then call
/// [`.build()`](Self::build) to construct the framework.
pub struct StoreBuilder<Entity: EventSourced, Materialized = <Entity as EventSourced>::Materialized>
{
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    _materialized: std::marker::PhantomData<Materialized>,
}

impl<Entity: EventSourced> StoreBuilder<Entity> {
    /// Creates a new builder for the given entity type.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
            _materialized: std::marker::PhantomData,
        }
    }

    /// Registers a reactor with this CQRS framework.
    ///
    /// The reactor must declare `Entity` in its dependency list
    /// (via [`deps!`](crate::deps)). For multi-entity reactors,
    /// clone the same `Arc` into each relevant builder.
    #[must_use]
    pub fn with<R>(mut self, reactor: Arc<R>) -> Self
    where
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.queries.push(Box::new(ReactorBridge { reactor }));
        self
    }
}

fn sqlite_snapshot_cqrs<Entity: EventSourced>(
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    services: Entity::Services,
) -> SqliteCqrs<Entity> {
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
        repo,
        Entity::SNAPSHOT_SIZE,
    );
    #[allow(clippy::disallowed_methods)]
    CqrsFramework::new(store, queries, services)
}

/// Projected entities: auto-creates and wires a [`Projection`],
/// returning `(Arc<Store>, Arc<Projection>)`.
impl<Entity: EventSourced<Materialized = Table> + 'static> StoreBuilder<Entity, Table>
where
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
{
    pub async fn build(
        mut self,
        services: Entity::Services,
    ) -> Result<(Arc<Store<Entity>>, Arc<Projection<Entity>>), ReconcileError> {
        // Projected entities must retain all events so that
        // `catch_up`/`rebuild_all` can replay the full history.
        // Compacted aggregates lose events after snapshot, making
        // projection rebuilds silently incomplete.
        const {
            assert!(
                matches!(Entity::COMPACTION_POLICY, CompactionPolicy::Retain),
                "CompactAfterSnapshot entities must not have table projections -- \
                 rebuild_all only reads the events table and would miss \
                 compacted snapshot-only aggregates"
            );
        }

        Reconciler::new(self.pool.clone())
            .reconcile::<Entity>()
            .await?;

        let projection = Arc::new(Projection::sqlite(self.pool.clone()));

        // Replay any events the view missed due to a crash between
        // event persistence and view update. Runs before registering
        // the projection as a reactor so no concurrent writes can
        // interfere.
        projection.catch_up().await.map_err(|error| match error {
            ProjectionError::Sqlx(sqlx_error) => ReconcileError::from(sqlx_error),
            ProjectionError::Persistence(persistence_error) => {
                ReconcileError::from(persistence_error)
            }
            other => ReconcileError::Persistence(PersistenceError::UnknownError(Box::new(other))),
        })?;

        // The materialized view is the source for reconnect snapshots.
        // Update it before side-effect reactors emit notifications so a
        // client that reconnects after a broadcast cannot read a stale view.
        self.queries.insert(
            0,
            Box::new(ReactorBridge {
                reactor: projection.clone(),
            }),
        );

        let cqrs = sqlite_snapshot_cqrs(self.pool.clone(), self.queries, services);
        Ok((Arc::new(Store::new(cqrs, self.pool)), projection))
    }
}

/// Non-projected entities: returns just `Store`.
impl<Entity: EventSourced<Materialized = Nil>> StoreBuilder<Entity, Nil> {
    pub async fn build(
        self,
        services: Entity::Services,
    ) -> Result<Arc<Store<Entity>>, ReconcileError> {
        Reconciler::new(self.pool.clone())
            .reconcile::<Entity>()
            .await?;

        let cqrs = sqlite_snapshot_cqrs(self.pool.clone(), self.queries, services);
        Ok(Arc::new(Store::new(cqrs, self.pool)))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::dependency::EntityList;
    use crate::deps;
    use crate::lifecycle::Never;

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateA;

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateB;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventA;

    impl DomainEvent for EventA {
        fn event_type(&self) -> String {
            "EventA".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventB;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct ProjectedAggregate {
        value: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum ProjectedEvent {
        Created { value: i64 },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ProjectedCommand {
        Create { value: i64 },
    }

    impl DomainEvent for EventB {
        fn event_type(&self) -> String {
            "EventB".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    impl DomainEvent for ProjectedEvent {
        fn event_type(&self) -> String {
            "ProjectedEvent::Created".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for AggregateA {
        type Id = String;
        type Event = EventA;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "AggregateA";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventA) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl EventSourced for AggregateB {
        type Id = String;
        type Event = EventB;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "AggregateB";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventB) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventB) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl EventSourced for ProjectedAggregate {
        type Id = String;
        type Event = ProjectedEvent;
        type Command = ProjectedCommand;
        type Error = Never;
        type Services = ();
        type Materialized = Table;

        const AGGREGATE_TYPE: &'static str = "ProjectedAggregate";
        const PROJECTION: Table = Table("projected_aggregate_view");
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &ProjectedEvent) -> Option<Self> {
            match event {
                ProjectedEvent::Created { value } => Some(Self { value: *value }),
            }
        }

        fn evolve(_entity: &Self, _event: &ProjectedEvent) -> Result<Option<Self>, Never> {
            Ok(None)
        }

        async fn initialize(
            command: ProjectedCommand,
            _services: &(),
        ) -> Result<Vec<ProjectedEvent>, Never> {
            match command {
                ProjectedCommand::Create { value } => Ok(vec![ProjectedEvent::Created { value }]),
            }
        }

        async fn transition(
            &self,
            _command: ProjectedCommand,
            _services: &(),
        ) -> Result<Vec<ProjectedEvent>, Never> {
            Ok(vec![])
        }
    }

    struct MultiEntityReactor;

    deps!(MultiEntityReactor, [AggregateA, AggregateB]);

    #[async_trait]
    impl Reactor for MultiEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            event
                .on(|_id, _event| async {})
                .on(|_id, _event| async {})
                .exhaustive()
                .await;
            Ok(())
        }
    }

    struct SingleEntityReactor;

    deps!(SingleEntityReactor, [AggregateA]);

    #[async_trait]
    impl Reactor for SingleEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, _event) = event.into_inner();
            Ok(())
        }
    }

    struct ProjectionReadingReactor {
        pool: SqlitePool,
        sender: tokio::sync::mpsc::Sender<Option<i64>>,
    }

    deps!(ProjectionReadingReactor, [ProjectedAggregate]);

    #[async_trait]
    impl Reactor for ProjectionReadingReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            event
                .on(|id, _event| async move {
                    let value = load_projected_value(&self.pool, &id).await;
                    self.sender.send(value).await.unwrap();
                })
                .exhaustive()
                .await;

            Ok(())
        }
    }

    async fn load_projected_value(pool: &SqlitePool, id: &str) -> Option<i64> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM projected_aggregate_view WHERE view_id = ?")
                .bind(id)
                .fetch_optional(pool)
                .await
                .unwrap();

        payload.and_then(|payload| {
            match serde_json::from_str::<Lifecycle<ProjectedAggregate>>(&payload).unwrap() {
                Lifecycle::Live(entity) => Some(entity.value),
                Lifecycle::Uninitialized | Lifecycle::Failed { .. } => None,
            }
        })
    }

    async fn setup_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn single_entity_wiring() {
        let pool = setup_pool().await;

        let _store = StoreBuilder::<AggregateA>::new(pool.clone())
            .with(Arc::new(SingleEntityReactor))
            .build(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn multi_entity_wiring() {
        let pool = setup_pool().await;

        let multi = Arc::new(MultiEntityReactor);
        let single = Arc::new(SingleEntityReactor);

        let _store_a = StoreBuilder::<AggregateA>::new(pool.clone())
            .with(multi.clone())
            .with(single)
            .build(())
            .await
            .unwrap();

        let _store_b = StoreBuilder::<AggregateB>::new(pool.clone())
            .with(multi)
            .build(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn projected_entities_update_views_before_registered_reactors() {
        let pool = setup_pool().await;
        sqlx::query(
            "CREATE TABLE projected_aggregate_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let reactor = Arc::new(ProjectionReadingReactor {
            pool: pool.clone(),
            sender,
        });
        let (store, _projection) = StoreBuilder::<ProjectedAggregate>::new(pool.clone())
            .with(reactor)
            .build(())
            .await
            .unwrap();

        store
            .send(
                &"projected-1".to_string(),
                ProjectedCommand::Create { value: 42 },
            )
            .await
            .unwrap();

        let value_seen_by_reactor =
            tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
                .await
                .expect("registered reactor should receive the projected event")
                .expect("registered reactor should receive the projected event");
        assert_eq!(
            value_seen_by_reactor,
            Some(42),
            "registered reactors must observe the updated projection"
        );
    }
}
