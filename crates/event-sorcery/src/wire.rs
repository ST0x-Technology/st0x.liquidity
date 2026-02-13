//! Type-safe CQRS wiring infrastructure.
//!
//! This module prevents a class of bugs where query processors are
//! created but never registered with their CQRS frameworks, causing
//! events to persist without updating materialized views.
//!
//! # Type-Level Tracking
//!
//! The system uses phantom types to track wiring dependencies at
//! compile time:
//!
//! - [`Cons<E, Tail>`] - A type-level linked list cell. `E` is the
//!   [`EventSourced`] entity that must be wired next, `Tail` is the
//!   remaining list.
//!
//! - [`Nil`] - Empty list, indicating all dependencies satisfied.
//!
//! - [`Unwired<Q, Deps>`] - Wraps a query processor `Q` with
//!   its wiring dependencies. The
//!   [`into_inner`](Unwired::into_inner) method is only
//!   available when `Deps = Nil`, enforcing complete wiring.
//!
//! # Examples
//!
//! ## Single-entity queries (simple case)
//!
//! ```ignore
//! // Query only needs wiring to Position entity
//! type ViewDeps = Cons<Position, Nil>;
//! let view: Unwired<PositionView, ViewDeps> =
//!     Unwired::new(view);
//!
//! // Build and discard -- query deps satisfied after wiring
//! let position_store = StoreBuilder::<Position>::new(pool)
//!     .wire(view)
//!     .build(());
//! ```
//!
//! ## Multi-entity queries
//!
//! ```ignore
//! // Query requiring wiring to Position, then Mint entities
//! type TriggerDeps = Cons<Position, Cons<TokenizedEquityMint, Nil>>;
//! let trigger: Unwired<Trigger, TriggerDeps> =
//!     Unwired::new(trigger);
//!
//! // Build Position store -- get trigger back for further wiring
//! let (position_store, (trigger, ())) =
//!     StoreBuilder::<Position>::new(pool.clone())
//!         .wire(trigger)
//!         .build(());
//! // trigger: Unwired<Trigger, Cons<TokenizedEquityMint, Nil>>
//!
//! // Build Mint store -- trigger's last dependency
//! let mint_store =
//!     StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
//!         .wire(trigger)
//!         .build(());
//! ```
//!
//! # Clippy Enforcement
//!
//! Direct calls to `sqlite_es::sqlite_cqrs` are blocked via
//! clippy's `disallowed-methods`. All CQRS construction must go
//! through [`StoreBuilder`], which contains the single `#[allow]`
//! escape hatch.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use cqrs_es::Query;
use cqrs_es::persist::ViewRepository;
use sqlx::SqlitePool;

use crate::Reactor;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::projection::Projection;
use crate::schema_registry::Reconciler;
use crate::{EventSourced, Store};

/// Type-level cons cell for building linked lists of entities.
///
/// Forms a compile-time linked list:
/// `Cons<Position, Cons<TokenizedEquityMint, Nil>>`.
pub struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

/// Type-level empty list (nil).
///
/// Terminal element for type-level lists. When an [`Unwired`]
/// reaches this state, [`into_inner`](Unwired::into_inner)
/// becomes available.
pub struct Nil;

/// A query processor with compile-time wiring dependencies.
///
/// Wraps an `Arc<Processor>` and tracks which entities it still
/// needs to be wired to via the `Deps` phantom type. The inner
/// Arc is only extractable via [`into_inner`](Self::into_inner)
/// when `Deps = Nil`.
///
/// # Type Parameter Evolution
///
/// Each call to [`StoreBuilder::wire`] consumes this and returns a
/// new instance with the head entity removed from `Deps`:
///
/// ```text
/// Unwired<Processor, Cons<A, Cons<B, Nil>>>
///     --wire to A-->
/// Unwired<Processor, Cons<B, Nil>>
///     --wire to B-->
/// Unwired<Processor, Nil>
///     --into_inner-->
/// Arc<Processor>
/// ```
#[must_use = "query must be wired via StoreBuilder, \
              then extracted with into_inner"]
pub struct Unwired<Processor, Deps> {
    pub(crate) query: Arc<Processor>,
    pub(crate) _deps: PhantomData<Deps>,
}

impl<Processor, Deps> Unwired<Processor, Deps> {
    /// Creates a new unwired query with the given dependencies.
    ///
    /// The `Deps` type parameter should encode all
    /// [`EventSourced`] entities this query needs to be wired to
    /// before it can be used.
    pub fn new(query: Processor) -> Self {
        Self {
            query: Arc::new(query),
            _deps: PhantomData,
        }
    }
}

impl<Processor> Unwired<Processor, Nil> {
    /// Extracts the inner Arc. Only available when all
    /// dependencies are satisfied.
    ///
    /// This method's availability is the compile-time proof that
    /// all required entities have been wired via
    /// [`StoreBuilder::wire`].
    pub fn into_inner(self) -> Arc<Processor> {
        self.query
    }
}

/// The wired-output accumulator: a cons-cell pairing a
/// partially-wired processor with the previously accumulated
/// wired results.
type WiredOutput<Processor, Tail, Rest> = (Unwired<Processor, Tail>, Rest);

/// Builder for a single CQRS framework with type-tracked query
/// wiring.
///
/// Parameterized on an [`EventSourced`] entity type. Internally
/// constructs `SqliteCqrs<Lifecycle<Entity>>` but returns
/// [`Store<Entity>`], keeping Lifecycle as an implementation
/// detail.
///
/// Accumulates query processors via [`wire`](Self::wire) calls,
/// tracking them at the type level in the `Wired` parameter.
/// Call [`build`](Self::build) to construct the framework and
/// get back wired queries for continued wiring.
pub struct StoreBuilder<Entity: EventSourced, Wired = ()> {
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    wired: Wired,
}

impl<Entity: EventSourced> StoreBuilder<Entity, ()> {
    /// Creates a new builder for the given entity type.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
            wired: (),
        }
    }

    /// Adds a query processor without type-level tracking.
    ///
    /// Use this for CLI code where compile-time wiring
    /// guarantees aren't needed. Production code should use
    /// [`wire`](StoreBuilder::wire) with [`Unwired`] instead.
    #[must_use]
    pub fn with_query(mut self, query: impl Query<Lifecycle<Entity>> + 'static) -> Self {
        self.queries.push(Box::new(query));
        self
    }

    /// Adds a reactor without type-level tracking.
    ///
    /// Like [`with_query`](Self::with_query) but accepts a
    /// [`Reactor<Entity>`] instead of a raw `Query<Lifecycle<Entity>>`.
    #[must_use]
    pub fn with_reactor(self, reactor: impl Reactor<Entity> + 'static) -> Self
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.with_query(ReactorBridge(reactor))
    }

    /// Adds a projection without type-level tracking.
    ///
    /// Registers the projection's internal `GenericQuery` with the
    /// CQRS framework so that it receives event updates.
    #[must_use]
    pub fn with_projection<Repo>(self, projection: &Projection<Entity, Repo>) -> Self
    where
        Entity: 'static,
        Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>> + Send + Sync + 'static,
    {
        self.with_query(projection.inner_query())
    }

    /// Builds the CQRS framework when no queries were wired.
    ///
    /// Reconciles the entity's schema version before constructing
    /// the framework, clearing stale snapshots if the version
    /// changed.
    ///
    /// Returns a [`Store`] for type-safe command dispatch.
    pub async fn build(self, services: Entity::Services) -> Result<Store<Entity>, anyhow::Error> {
        Reconciler::new(self.pool.clone())
            .reconcile::<Entity>()
            .await?;

        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool, self.queries, services);
        Ok(Store::new(cqrs))
    }
}

impl<Entity: EventSourced, Wired> StoreBuilder<Entity, Wired> {
    /// Wires a query processor to this CQRS framework.
    ///
    /// Consumes the [`Unwired`] and returns a new builder
    /// with:
    /// - The query added to the internal processors list
    /// - An updated `Unwired` (with `Entity` removed from
    ///   dependencies) added to the wired tuple for return at
    ///   build time
    ///
    /// # Type Evolution
    ///
    /// The input query must have `Entity` as its next dependency:
    /// `Unwired<Processor, Cons<Entity, Tail>>`. The
    /// returned query will be
    /// `Unwired<Processor, Tail>`.
    pub fn wire<Processor, Tail>(
        mut self,
        query: Unwired<Processor, Cons<Entity, Tail>>,
    ) -> StoreBuilder<Entity, WiredOutput<Processor, Tail, Wired>>
    where
        Processor: Send + Sync + 'static,
        Arc<Processor>: Query<Lifecycle<Entity>>,
    {
        self.queries.push(Box::new(query.query.clone()));

        StoreBuilder {
            pool: self.pool,
            queries: self.queries,
            wired: (
                Unwired {
                    query: query.query,
                    _deps: PhantomData,
                },
                self.wired,
            ),
        }
    }

    /// Wires a reactor to this CQRS framework.
    ///
    /// Like [`wire`](Self::wire) but for processors that
    /// implement [`Reactor<Entity>`] instead of
    /// `Query<Lifecycle<Entity>>`.
    pub fn wire_reactor<Processor, Tail>(
        mut self,
        query: Unwired<Processor, Cons<Entity, Tail>>,
    ) -> StoreBuilder<Entity, WiredOutput<Processor, Tail, Wired>>
    where
        Processor: Send + Sync + 'static,
        Arc<Processor>: Reactor<Entity>,
        <Entity::Id as FromStr>::Err: Debug + Send + Sync,
    {
        self.queries
            .push(Box::new(ReactorBridge(query.query.clone())));

        StoreBuilder {
            pool: self.pool,
            queries: self.queries,
            wired: (
                Unwired {
                    query: query.query,
                    _deps: PhantomData,
                },
                self.wired,
            ),
        }
    }

    /// Wires a projection to this CQRS framework with type-level
    /// tracking.
    pub fn wire_projection<Repo, Tail>(
        mut self,
        projection: &Unwired<Projection<Entity, Repo>, Cons<Entity, Tail>>,
    ) -> StoreBuilder<Entity, WiredOutput<Projection<Entity, Repo>, Tail, Wired>>
    where
        Entity: 'static,
        Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>> + Send + Sync + 'static,
    {
        self.queries.push(Box::new(projection.query.inner_query()));

        StoreBuilder {
            pool: self.pool,
            queries: self.queries,
            wired: (
                Unwired {
                    query: Arc::clone(&projection.query),
                    _deps: PhantomData,
                },
                self.wired,
            ),
        }
    }
}

impl<Entity: EventSourced, Head, Tail> StoreBuilder<Entity, (Head, Tail)> {
    /// Builds the CQRS framework, returning a [`Store`] with all
    /// wired queries.
    ///
    /// Reconciles the entity's schema version before constructing
    /// the framework. Destructure the returned tuple to continue
    /// wiring to other builders or extract via
    /// [`Unwired::into_inner`].
    pub async fn build(
        self,
        services: Entity::Services,
    ) -> Result<(Store<Entity>, (Head, Tail)), anyhow::Error> {
        Reconciler::new(self.pool.clone())
            .reconcile::<Entity>()
            .await?;

        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool, self.queries, services);
        Ok((Store::new(cqrs), self.wired))
    }
}

/// Test-only escape hatch for creating CQRS frameworks directly.
///
/// Tests often need CQRS with specific configurations that don't
/// fit the production wiring pattern. Use this instead of
/// `sqlite_es::sqlite_cqrs`.
#[cfg(any(test, feature = "test-support"))]
pub fn test_store<Entity: EventSourced>(
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    services: Entity::Services,
) -> Store<Entity> {
    #[allow(clippy::disallowed_methods)]
    let cqrs = sqlite_es::sqlite_cqrs(pool, queries, services);
    Store::new(cqrs)
}

/// Like [`test_store`] but also accepts [`Reactor`] implementations.
///
/// Reactors are bridged to cqrs-es queries via [`ReactorBridge`]
/// and appended after the provided queries.
#[cfg(any(test, feature = "test-support"))]
pub fn test_store_with_reactors<Entity: EventSourced + 'static>(
    pool: SqlitePool,
    mut queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    reactors: Vec<Box<dyn Reactor<Entity>>>,
    services: Entity::Services,
) -> Store<Entity>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let reactor_queries: Vec<Box<dyn Query<Lifecycle<Entity>>>> = reactors
        .into_iter()
        .map(|reactor| Box::new(ReactorBridge(reactor)) as Box<dyn Query<Lifecycle<Entity>>>)
        .collect();
    queries.extend(reactor_queries);

    #[allow(clippy::disallowed_methods)]
    let cqrs = sqlite_es::sqlite_cqrs(pool, queries, services);
    Store::new(cqrs)
}

/// In-memory event store for unit tests.
///
/// Provides the same typed-ID interface as [`Store`] but backed
/// by an in-memory store instead of SQLite. Also exposes
/// [`load`](Self::load) for inspecting aggregate state after
/// commands, which production [`Store`] intentionally omits
/// (use projections instead).
#[cfg(any(test, feature = "test-support"))]
pub struct TestStore<Entity: EventSourced> {
    mem_store: cqrs_es::mem_store::MemStore<Lifecycle<Entity>>,
    cqrs:
        cqrs_es::CqrsFramework<Lifecycle<Entity>, cqrs_es::mem_store::MemStore<Lifecycle<Entity>>>,
}

#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
impl<Entity: EventSourced> TestStore<Entity>
where
    Lifecycle<Entity>: cqrs_es::Aggregate<
            Command = Entity::Command,
            Event = Entity::Event,
            Error = crate::lifecycle::LifecycleError<Entity>,
            Services = Entity::Services,
        >,
{
    /// Send a command to the entity identified by `id`.
    pub async fn send(
        &self,
        id: &Entity::Id,
        command: Entity::Command,
    ) -> Result<(), crate::SendError<Entity>> {
        self.execute(&id.to_string(), command).await
    }

    /// Send a command using a raw string aggregate ID.
    ///
    /// Prefer [`send`](Self::send) with typed IDs. This exists
    /// for tests that haven't been migrated to typed IDs yet.
    pub async fn execute(
        &self,
        aggregate_id: &str,
        command: Entity::Command,
    ) -> Result<(), crate::SendError<Entity>> {
        self.cqrs.execute(aggregate_id, command).await
    }

    /// Load the lifecycle state of an entity by typed ID.
    pub async fn load(&self, id: &Entity::Id) -> Lifecycle<Entity> {
        self.load_by_id(&id.to_string()).await
    }

    /// Load the lifecycle state using a raw string aggregate ID.
    ///
    /// Prefer [`load`](Self::load) with typed IDs. This exists
    /// for tests that haven't been migrated to typed IDs yet.
    #[expect(
        clippy::unwrap_used,
        reason = "test-only helper, panicking on error is fine"
    )]
    pub async fn load_by_id(&self, aggregate_id: &str) -> Lifecycle<Entity> {
        use cqrs_es::EventStore;

        self.mem_store
            .load_aggregate(aggregate_id)
            .await
            .unwrap()
            .aggregate
    }
}

/// Create an in-memory [`TestStore`] for fast, isolated unit
/// tests.
///
/// Unlike [`test_store`] which uses SQLite, this is purely
/// in-memory. Accepts [`Reactor`] impls which are internally
/// bridged to cqrs-es queries via [`ReactorBridge`].
#[cfg(any(test, feature = "test-support"))]
pub fn test_mem_store<Entity: EventSourced + 'static>(
    reactors: Vec<Box<dyn Reactor<Entity>>>,
    services: Entity::Services,
) -> TestStore<Entity>
where
    Lifecycle<Entity>: cqrs_es::Aggregate<
            Command = Entity::Command,
            Event = Entity::Event,
            Error = crate::lifecycle::LifecycleError<Entity>,
            Services = Entity::Services,
        >,
    <Entity::Id as FromStr>::Err: Debug,
{
    let queries: Vec<Box<dyn Query<Lifecycle<Entity>>>> = reactors
        .into_iter()
        .map(|reactor| Box::new(ReactorBridge(reactor)) as Box<dyn Query<Lifecycle<Entity>>>)
        .collect();

    let mem_store = cqrs_es::mem_store::MemStore::default();
    #[allow(clippy::disallowed_methods)]
    let cqrs = cqrs_es::CqrsFramework::new(mem_store.clone(), queries, services);
    TestStore { mem_store, cqrs }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use serde::{Deserialize, Serialize};

    use super::*;
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

    impl DomainEvent for EventB {
        fn event_type(&self) -> String {
            "EventB".to_string()
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

        const AGGREGATE_TYPE: &'static str = "AggregateA";
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_event: &EventA, _state: &Self) -> Result<Option<Self>, Never> {
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

        const AGGREGATE_TYPE: &'static str = "AggregateB";
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventB) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_event: &EventB, _state: &Self) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }
    }

    struct MultiEntityQuery {
        name: &'static str,
    }

    #[async_trait]
    impl Query<Lifecycle<AggregateA>> for Arc<MultiEntityQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA>>]) {}
    }

    #[async_trait]
    impl Query<Lifecycle<AggregateB>> for Arc<MultiEntityQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateB>>]) {}
    }

    struct SingleEntityQuery;

    #[async_trait]
    impl Query<Lifecycle<AggregateA>> for Arc<SingleEntityQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA>>]) {}
    }

    #[test]
    fn single_entity_query_wiring() {
        type Deps = Cons<AggregateA, Nil>;
        let query: Unwired<SingleEntityQuery, Deps> = Unwired::new(SingleEntityQuery);

        // Simulate wiring (no actual pool needed for type-level test)
        let query: Unwired<SingleEntityQuery, Nil> = Unwired {
            query: query.query,
            _deps: PhantomData,
        };

        let _arc: Arc<SingleEntityQuery> = query.into_inner();
    }

    #[test]
    fn multi_entity_query_wiring_sequence() {
        type Deps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        let query: Unwired<MultiEntityQuery, Deps> =
            Unwired::new(MultiEntityQuery { name: "test" });

        // After wiring to A, only B remains
        let query: Unwired<MultiEntityQuery, Cons<AggregateB, Nil>> = Unwired {
            query: query.query,
            _deps: PhantomData,
        };

        // After wiring to B, Nil
        let query: Unwired<MultiEntityQuery, Nil> = Unwired {
            query: query.query,
            _deps: PhantomData,
        };

        let arc = query.into_inner();
        assert_eq!(arc.name, "test");
    }

    #[tokio::test]
    async fn full_wiring_flow_with_builders() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        type MultiDeps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        type SingleDeps = Cons<AggregateA, Nil>;

        let multi: Unwired<MultiEntityQuery, MultiDeps> =
            Unwired::new(MultiEntityQuery { name: "multi" });
        let single: Unwired<SingleEntityQuery, SingleDeps> = Unwired::new(SingleEntityQuery);

        // Build AggregateA Store
        let (_store_a, (single, (multi, ()))) = StoreBuilder::<AggregateA>::new(pool.clone())
            .wire(multi)
            .wire(single)
            .build(())
            .await
            .unwrap();

        let _single_arc: Arc<SingleEntityQuery> = single.into_inner();

        // Build AggregateB Store
        let (_store_b, (multi, ())) = StoreBuilder::<AggregateB>::new(pool.clone())
            .wire(multi)
            .build(())
            .await
            .unwrap();

        let _multi_arc: Arc<MultiEntityQuery> = multi.into_inner();
    }
}
