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
//! - [`UnwiredQuery<Q, Deps>`] - Wraps a query processor `Q` with
//!   its wiring dependencies. The
//!   [`into_inner`](UnwiredQuery::into_inner) method is only
//!   available when `Deps = Nil`, enforcing complete wiring.
//!
//! # Examples
//!
//! ## Single-entity queries (simple case)
//!
//! ```ignore
//! // Query only needs wiring to Position entity
//! type ViewDeps = Cons<Position, Nil>;
//! let view: UnwiredQuery<PositionView, ViewDeps> =
//!     UnwiredQuery::new(view);
//!
//! // Build and discard -- query deps satisfied after wiring
//! let position_store = CqrsBuilder::<Position>::new(pool)
//!     .wire(view)
//!     .build(());
//! ```
//!
//! ## Multi-entity queries
//!
//! ```ignore
//! // Query requiring wiring to Position, then Mint entities
//! type TriggerDeps = Cons<Position, Cons<TokenizedEquityMint, Nil>>;
//! let trigger: UnwiredQuery<Trigger, TriggerDeps> =
//!     UnwiredQuery::new(trigger);
//!
//! // Build Position store -- get trigger back for further wiring
//! let (position_store, (trigger, ())) =
//!     CqrsBuilder::<Position>::new(pool.clone())
//!         .wire(trigger)
//!         .build(());
//! // trigger: UnwiredQuery<Trigger, Cons<TokenizedEquityMint, Nil>>
//!
//! // Build Mint store -- trigger's last dependency
//! let mint_store =
//!     CqrsBuilder::<TokenizedEquityMint>::new(pool.clone())
//!         .wire(trigger)
//!         .build(());
//! ```
//!
//! # Clippy Enforcement
//!
//! Direct calls to `sqlite_es::sqlite_cqrs` are blocked via
//! clippy's `disallowed-methods`. All CQRS construction must go
//! through [`CqrsBuilder`], which contains the single `#[allow]`
//! escape hatch.

use std::marker::PhantomData;
use std::sync::Arc;

use cqrs_es::Query;
use sqlx::SqlitePool;

use crate::event_sourced::{EventSourced, Store};
use crate::lifecycle::Lifecycle;

use super::schema_registry::Reconciler;

/// Type-level cons cell for building linked lists of entities.
///
/// Forms a compile-time linked list:
/// `Cons<Position, Cons<TokenizedEquityMint, Nil>>`.
pub(crate) struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

/// Type-level empty list (nil).
///
/// Terminal element for type-level lists. When an [`UnwiredQuery`]
/// reaches this state, [`into_inner`](UnwiredQuery::into_inner)
/// becomes available.
pub(crate) struct Nil;

/// A query processor with compile-time wiring dependencies.
///
/// Wraps an `Arc<Processor>` and tracks which entities it still
/// needs to be wired to via the `Deps` phantom type. The inner
/// Arc is only extractable via [`into_inner`](Self::into_inner)
/// when `Deps = Nil`.
///
/// # Type Parameter Evolution
///
/// Each call to [`CqrsBuilder::wire`] consumes this and returns a
/// new instance with the head entity removed from `Deps`:
///
/// ```text
/// UnwiredQuery<Processor, Cons<A, Cons<B, Nil>>>
///     --wire to A-->
/// UnwiredQuery<Processor, Cons<B, Nil>>
///     --wire to B-->
/// UnwiredQuery<Processor, Nil>
///     --into_inner-->
/// Arc<Processor>
/// ```
#[must_use = "query must be wired via CqrsBuilder, \
              then extracted with into_inner"]
pub(crate) struct UnwiredQuery<Processor, Deps> {
    query: Arc<Processor>,
    _deps: PhantomData<Deps>,
}

impl<Processor, Deps> UnwiredQuery<Processor, Deps> {
    /// Creates a new unwired query with the given dependencies.
    ///
    /// The `Deps` type parameter should encode all
    /// [`EventSourced`] entities this query needs to be wired to
    /// before it can be used.
    pub(crate) fn new(query: Processor) -> Self {
        Self {
            query: Arc::new(query),
            _deps: PhantomData,
        }
    }
}

impl<Processor> UnwiredQuery<Processor, Nil> {
    /// Extracts the inner Arc. Only available when all
    /// dependencies are satisfied.
    ///
    /// This method's availability is the compile-time proof that
    /// all required entities have been wired via
    /// [`CqrsBuilder::wire`].
    pub(crate) fn into_inner(self) -> Arc<Processor> {
        self.query
    }
}

/// Builder for a single CQRS framework with type-tracked query
/// wiring.
///
/// Parameterized on an [`EventSourced`] entity type. Internally
/// constructs `SqliteCqrs<Lifecycle<E>>` but returns
/// [`Store<E>`], keeping Lifecycle as an implementation detail.
///
/// Accumulates query processors via [`wire`](Self::wire) calls,
/// tracking them at the type level in the `Wired` parameter.
/// Call [`build`](Self::build) to construct the framework and
/// get back wired queries for continued wiring.
pub(crate) struct CqrsBuilder<E: EventSourced, Wired = ()> {
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<E>>>>,
    wired: Wired,
}

impl<E: EventSourced> CqrsBuilder<E, ()> {
    /// Creates a new builder for entity type `E`.
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
            wired: (),
        }
    }

    /// Builds the CQRS framework when no queries were wired.
    ///
    /// Reconciles the entity's schema version before constructing
    /// the framework, clearing stale snapshots if the version
    /// changed.
    ///
    /// Returns a [`Store`] for type-safe command dispatch.
    pub(crate) async fn build(self, services: E::Services) -> Result<Store<E>, anyhow::Error> {
        Reconciler::new(self.pool.clone()).reconcile::<E>().await?;

        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool, self.queries, services);
        Ok(Store::new(cqrs))
    }
}

impl<E: EventSourced, W> CqrsBuilder<E, W> {
    /// Wires a query processor to this CQRS framework.
    ///
    /// Consumes the [`UnwiredQuery`] and returns a new builder
    /// with:
    /// - The query added to the internal processors list
    /// - An updated `UnwiredQuery` (with `E` removed from
    ///   dependencies) added to the `Wired` tuple for return at
    ///   build time
    ///
    /// # Type Evolution
    ///
    /// The input query must have `E` as its next dependency:
    /// `UnwiredQuery<Q, Cons<E, Tail>>`. The returned query in
    /// the tuple will be `UnwiredQuery<Q, Tail>`.
    pub(crate) fn wire<Q, Tail>(
        mut self,
        query: UnwiredQuery<Q, Cons<E, Tail>>,
    ) -> CqrsBuilder<E, (UnwiredQuery<Q, Tail>, W)>
    where
        Q: Send + Sync + 'static,
        Arc<Q>: Query<Lifecycle<E>>,
    {
        self.queries.push(Box::new(query.query.clone()));

        CqrsBuilder {
            pool: self.pool,
            queries: self.queries,
            wired: (
                UnwiredQuery {
                    query: query.query,
                    _deps: PhantomData,
                },
                self.wired,
            ),
        }
    }
}

impl<E: EventSourced, H, T> CqrsBuilder<E, (H, T)> {
    /// Builds the CQRS framework, returning a [`Store`] with all
    /// wired queries.
    ///
    /// Reconciles the entity's schema version before constructing
    /// the framework. Destructure the returned tuple to continue
    /// wiring to other builders or extract via
    /// [`UnwiredQuery::into_inner`].
    pub(crate) async fn build(
        self,
        services: E::Services,
    ) -> Result<(Store<E>, (H, T)), anyhow::Error> {
        Reconciler::new(self.pool.clone()).reconcile::<E>().await?;

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
#[cfg(test)]
pub(crate) fn test_cqrs<E: EventSourced>(
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<E>>>>,
    services: E::Services,
) -> Store<E> {
    #[allow(clippy::disallowed_methods)]
    let cqrs = sqlite_es::sqlite_cqrs(pool, queries, services);
    Store::new(cqrs)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::{DomainEvent, EventEnvelope};

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
        type Id = &'static str;
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
        type Id = &'static str;
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

    // Serde imports needed for derive on test aggregates
    use serde::{Deserialize, Serialize};

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
        let query: UnwiredQuery<SingleEntityQuery, Deps> = UnwiredQuery::new(SingleEntityQuery);

        // Simulate wiring (no actual pool needed for type-level test)
        let query: UnwiredQuery<SingleEntityQuery, Nil> = UnwiredQuery {
            query: query.query,
            _deps: PhantomData,
        };

        let _arc: Arc<SingleEntityQuery> = query.into_inner();
    }

    #[test]
    fn multi_entity_query_wiring_sequence() {
        type Deps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        let query: UnwiredQuery<MultiEntityQuery, Deps> =
            UnwiredQuery::new(MultiEntityQuery { name: "test" });

        // After wiring to A, only B remains
        let query: UnwiredQuery<MultiEntityQuery, Cons<AggregateB, Nil>> = UnwiredQuery {
            query: query.query,
            _deps: PhantomData,
        };

        // After wiring to B, Nil
        let query: UnwiredQuery<MultiEntityQuery, Nil> = UnwiredQuery {
            query: query.query,
            _deps: PhantomData,
        };

        let arc = query.into_inner();
        assert_eq!(arc.name, "test");
    }

    #[tokio::test]
    async fn full_wiring_flow_with_builders() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        type MultiDeps = Cons<AggregateA, Cons<AggregateB, Nil>>;
        type SingleDeps = Cons<AggregateA, Nil>;

        let multi: UnwiredQuery<MultiEntityQuery, MultiDeps> =
            UnwiredQuery::new(MultiEntityQuery { name: "multi" });
        let single: UnwiredQuery<SingleEntityQuery, SingleDeps> =
            UnwiredQuery::new(SingleEntityQuery);

        // Build AggregateA Store
        let (_store_a, (single, (multi, ()))) = CqrsBuilder::<AggregateA>::new(pool.clone())
            .wire(multi)
            .wire(single)
            .build(())
            .await
            .unwrap();

        let _single_arc: Arc<SingleEntityQuery> = single.into_inner();

        // Build AggregateB Store
        let (_store_b, (multi, ())) = CqrsBuilder::<AggregateB>::new(pool.clone())
            .wire(multi)
            .build(())
            .await
            .unwrap();

        let _multi_arc: Arc<MultiEntityQuery> = multi.into_inner();
    }
}
