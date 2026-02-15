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
//! ## Single-entity reactors (simple case)
//!
//! ```ignore
//! // Reactor only needs wiring to Position entity
//! type ViewDeps = Cons<Position, Nil>;
//! let view: Unwired<PositionView, ViewDeps> =
//!     Unwired::new(view);
//!
//! // Build and discard -- deps satisfied after wiring
//! let position_store = StoreBuilder::<Position>::new(pool)
//!     .wire(view)
//!     .build(());
//! ```
//!
//! ## Multi-entity reactors
//!
//! ```ignore
//! // Reactor requiring wiring to Position, then Mint entities
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
use sqlx::SqlitePool;

use crate::Reactor;
use crate::lifecycle::{Lifecycle, ReactorBridge};
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
    pub(crate) inner: Arc<Processor>,
    pub(crate) _deps: PhantomData<Deps>,
}

impl<Processor, Deps> Unwired<Processor, Deps> {
    /// Creates a new unwired query with the given dependencies.
    ///
    /// The `Deps` type parameter should encode all
    /// [`EventSourced`] entities this query needs to be wired to
    /// before it can be used.
    pub fn new(processor: Processor) -> Self {
        Self {
            inner: Arc::new(processor),
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
        self.inner
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
    pub(crate) fn with_query(mut self, query: impl Query<Lifecycle<Entity>> + 'static) -> Self {
        self.queries.push(Box::new(query));
        self
    }

    /// Adds a reactor or projection without type-level tracking.
    ///
    /// Use this for CLI code where compile-time wiring
    /// guarantees aren't needed. Production code should use
    /// [`wire`](StoreBuilder::wire) with [`Unwired`] instead.
    #[must_use]
    pub fn with(self, reactor: impl Reactor<Entity> + 'static) -> Self
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.with_query(ReactorBridge(reactor))
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
        let cqrs = sqlite_es::sqlite_cqrs(self.pool.clone(), self.queries, services);
        Ok(Store::new(cqrs, self.pool))
    }
}

impl<Entity: EventSourced, Wired> StoreBuilder<Entity, Wired> {
    /// Wires a reactor or projection to this CQRS framework with
    /// type-level dependency tracking.
    ///
    /// Consumes the [`Unwired`] processor and returns a new
    /// builder with:
    /// - The processor (bridged to cqrs-es `Query`) added to
    ///   the internal processors list
    /// - An updated `Unwired` (with `Entity` removed from
    ///   dependencies) added to the wired tuple for return at
    ///   build time
    ///
    /// # Type Evolution
    ///
    /// The input must have `Entity` as its next dependency:
    /// `Unwired<R, Cons<Entity, Tail>>`. The returned value
    /// will be `Unwired<R, Tail>`.
    pub fn wire<R, Tail>(
        mut self,
        processor: Unwired<R, Cons<Entity, Tail>>,
    ) -> StoreBuilder<Entity, WiredOutput<R, Tail, Wired>>
    where
        R: Send + Sync + 'static,
        Arc<R>: Reactor<Entity>,
        <Entity::Id as FromStr>::Err: Debug + Send + Sync,
    {
        self.queries
            .push(Box::new(ReactorBridge(processor.inner.clone())));

        StoreBuilder {
            pool: self.pool,
            queries: self.queries,
            wired: (
                Unwired {
                    inner: processor.inner,
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
        let cqrs = sqlite_es::sqlite_cqrs(self.pool.clone(), self.queries, services);
        Ok((Store::new(cqrs, self.pool), self.wired))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::Table;
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
        const PROJECTION: Option<Table> = None;
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

        const AGGREGATE_TYPE: &'static str = "AggregateB";
        const PROJECTION: Option<Table> = None;
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

    struct MultiEntityReactor {
        name: &'static str,
    }

    #[async_trait]
    impl Reactor<AggregateA> for Arc<MultiEntityReactor> {
        async fn react(&self, _id: &String, _event: &EventA) {}
    }

    #[async_trait]
    impl Reactor<AggregateB> for Arc<MultiEntityReactor> {
        async fn react(&self, _id: &String, _event: &EventB) {}
    }

    struct SingleEntityReactor;

    #[async_trait]
    impl Reactor<AggregateA> for Arc<SingleEntityReactor> {
        async fn react(&self, _id: &String, _event: &EventA) {}
    }

    #[test]
    fn single_entity_reactor_wiring() {
        type Deps = Cons<AggregateA, Nil>;
        let reactor: Unwired<SingleEntityReactor, Deps> = Unwired::new(SingleEntityReactor);

        // Simulate wiring (no actual pool needed for type-level test)
        let reactor: Unwired<SingleEntityReactor, Nil> = Unwired {
            inner: reactor.inner,
            _deps: PhantomData,
        };

        let _arc: Arc<SingleEntityReactor> = reactor.into_inner();
    }

    #[test]
    fn multi_entity_reactor_wiring_sequence() {
        type Deps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        let reactor: Unwired<MultiEntityReactor, Deps> =
            Unwired::new(MultiEntityReactor { name: "test" });

        // After wiring to A, only B remains
        let reactor: Unwired<MultiEntityReactor, Cons<AggregateB, Nil>> = Unwired {
            inner: reactor.inner,
            _deps: PhantomData,
        };

        // After wiring to B, Nil
        let reactor: Unwired<MultiEntityReactor, Nil> = Unwired {
            inner: reactor.inner,
            _deps: PhantomData,
        };

        let arc = reactor.into_inner();
        assert_eq!(arc.name, "test");
    }

    #[tokio::test]
    async fn full_wiring_flow_with_builders() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        type MultiDeps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        type SingleDeps = Cons<AggregateA, Nil>;

        let multi: Unwired<MultiEntityReactor, MultiDeps> =
            Unwired::new(MultiEntityReactor { name: "multi" });
        let single: Unwired<SingleEntityReactor, SingleDeps> = Unwired::new(SingleEntityReactor);

        // Build AggregateA Store
        let (_store_a, (single, (multi, ()))) = StoreBuilder::<AggregateA>::new(pool.clone())
            .wire(multi)
            .wire(single)
            .build(())
            .await
            .unwrap();

        let _single_arc: Arc<SingleEntityReactor> = single.into_inner();

        // Build AggregateB Store
        let (_store_b, (multi, ())) = StoreBuilder::<AggregateB>::new(pool.clone())
            .wire(multi)
            .build(())
            .await
            .unwrap();

        let _multi_arc: Arc<MultiEntityReactor> = multi.into_inner();
    }
}
