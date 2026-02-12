//! Type-safe CQRS wiring infrastructure.
//!
//! This module prevents a class of bugs where query processors are created but
//! never registered with their CQRS frameworks, causing events to persist
//! without updating materialized views.
//!
//! # Type-Level Tracking
//!
//! The system uses phantom types to track wiring dependencies at compile time:
//!
//! - [`Cons<A, Tail>`] - A type-level linked list cell. `A` is the aggregate
//!   that must be wired next, `Tail` is the remaining list.
//!
//! - [`Nil`] - Empty list, indicating all dependencies satisfied.
//!
//! - [`UnwiredQuery<Q, Deps>`] - Wraps a query processor `Q` with its
//!   wiring dependencies. The [`into_inner`](UnwiredQuery::into_inner) method
//!   is only available when `Deps = Nil`, enforcing complete wiring.
//!
//! # Examples
//!
//! ## Single-aggregate queries (simple case)
//!
//! ```ignore
//! // Query only needs wiring to Position aggregate
//! type ViewDeps = Cons<PositionAgg, Nil>;
//! let view: UnwiredQuery<PositionView, ViewDeps> = UnwiredQuery::new(view);
//!
//! // Build and discard - query dependencies satisfied after this wiring
//! let position_cqrs = CqrsBuilder::new(pool)
//!     .wire(view)
//!     .build(());
//! ```
//!
//! ## Multi-aggregate queries
//!
//! ```ignore
//! // Query requiring wiring to Position, then Mint aggregates
//! type TriggerDeps = Cons<PositionAgg, Cons<MintAgg, Nil>>;
//! let trigger: UnwiredQuery<Trigger, TriggerDeps> =
//!     UnwiredQuery::new(trigger);
//!
//! // Build Position CQRS - use build to get trigger back
//! let (position_cqrs, (trigger, ())) = CqrsBuilder::new(pool.clone())
//!     .wire(trigger)
//!     .build(());
//! // trigger is now: UnwiredQuery<Trigger, Cons<MintAgg, Nil>>
//!
//! // Build Mint CQRS - trigger's last dependency, can use simple build
//! let mint_cqrs = CqrsBuilder::new(pool.clone())
//!     .wire(trigger)
//!     .build(());
//! ```
//!
//! # Clippy Enforcement
//!
//! Direct calls to `sqlite_es::sqlite_cqrs` are blocked via clippy's
//! `disallowed-methods`. All CQRS construction must go through
//! [`CqrsBuilder`], which contains the single `#[allow]` escape hatch.

use std::marker::PhantomData;
use std::sync::Arc;

use cqrs_es::Aggregate;
use cqrs_es::Query;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;

/// Type-level cons cell for building linked lists of aggregates.
///
/// Forms a compile-time linked list:
/// `Cons<Agg1, Cons<Agg2, Nil>>`.
pub(crate) struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

/// Type-level empty list (nil).
///
/// Terminal element for type-level lists. When an [`UnwiredQuery`]
/// reaches this state, [`into_inner`](UnwiredQuery::into_inner)
/// becomes available.
pub(crate) struct Nil;

/// A query processor with compile-time wiring dependencies.
///
/// Wraps an `Arc<Q>` and tracks which aggregates it still needs to
/// be wired to via the `Deps` phantom type. The inner Arc is only
/// extractable via [`into_inner`](Self::into_inner) when
/// `Deps = Nil`.
///
/// # Type Parameter Evolution
///
/// Each call to [`CqrsBuilder::wire`] consumes this and returns a
/// new instance with the head aggregate removed from `Deps`:
///
/// ```text
/// UnwiredQuery<Q, Cons<A, Cons<B, Nil>>>
///     --wire to A-->
/// UnwiredQuery<Q, Cons<B, Nil>>
///     --wire to B-->
/// UnwiredQuery<Q, Nil>
///     --into_inner-->
/// Arc<Q>
/// ```
#[must_use = "query must be wired via CqrsBuilder, \
              then extracted with into_inner"]
pub(crate) struct UnwiredQuery<Q, Deps> {
    query: Arc<Q>,
    _deps: PhantomData<Deps>,
}

impl<Q, Deps> UnwiredQuery<Q, Deps> {
    /// Creates a new unwired query with the given dependencies.
    ///
    /// The `Deps` type parameter should encode all aggregates this
    /// query needs to be wired to before it can be used.
    pub(crate) fn new(query: Q) -> Self {
        Self {
            query: Arc::new(query),
            _deps: PhantomData,
        }
    }
}

impl<Q> UnwiredQuery<Q, Nil> {
    /// Extracts the inner Arc. Only available when all dependencies
    /// are satisfied.
    ///
    /// This method's availability is the compile-time proof that all
    /// required aggregates have been wired via [`CqrsBuilder::wire`].
    pub(crate) fn into_inner(self) -> Arc<Q> {
        self.query
    }
}

/// Builder for a single CQRS framework with type-tracked query
/// wiring.
///
/// Accumulates query processors via [`wire`](Self::wire) calls,
/// tracking them at the type level in the `Wired` parameter.
/// Call [`build`](Self::build) to construct the framework and get
/// back wired queries for continued wiring.
pub(crate) struct CqrsBuilder<A: Aggregate, Wired = ()> {
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<A>>>,
    wired: Wired,
}

impl<A: Aggregate> CqrsBuilder<A, ()> {
    /// Creates a new builder for aggregate type `A`.
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
            wired: (),
        }
    }

    /// Builds the CQRS framework when no queries were wired.
    pub(crate) fn build<S>(self, services: S) -> SqliteCqrs<A>
    where
        A: Aggregate<Services = S>,
    {
        #[allow(clippy::disallowed_methods)]
        sqlite_es::sqlite_cqrs(self.pool, self.queries, services)
    }
}

impl<A: Aggregate, W> CqrsBuilder<A, W> {
    /// Wires a query processor to this CQRS framework.
    ///
    /// Consumes the [`UnwiredQuery`] and returns a new builder with:
    /// - The query added to the internal processors list
    /// - An updated `UnwiredQuery` (with `A` removed from
    ///   dependencies) added to the `Wired` tuple for return at
    ///   build time
    ///
    /// # Type Evolution
    ///
    /// The input query must have `A` as its next dependency:
    /// `UnwiredQuery<Q, Cons<A, Tail>>`. The returned query in the
    /// tuple will be `UnwiredQuery<Q, Tail>`.
    pub(crate) fn wire<Q, Tail>(
        mut self,
        query: UnwiredQuery<Q, Cons<A, Tail>>,
    ) -> CqrsBuilder<A, (UnwiredQuery<Q, Tail>, W)>
    where
        Q: Send + Sync + 'static,
        Arc<Q>: Query<A>,
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

impl<A: Aggregate, H, T> CqrsBuilder<A, (H, T)> {
    /// Builds the CQRS framework, returning it with all wired
    /// queries.
    ///
    /// Destructure the returned tuple to continue wiring to other
    /// builders or extract via [`UnwiredQuery::into_inner`].
    pub(crate) fn build<S>(self, services: S) -> (SqliteCqrs<A>, (H, T))
    where
        A: Aggregate<Services = S>,
    {
        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool, self.queries, services);
        (cqrs, self.wired)
    }
}

/// Test-only escape hatch for creating CQRS frameworks directly.
///
/// Tests often need CQRS with specific configurations that don't
/// fit the production wiring pattern. Use this instead of
/// `sqlite_es::sqlite_cqrs`.
#[cfg(test)]
pub(crate) fn test_cqrs<A: Aggregate>(
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> SqliteCqrs<A> {
    #[allow(clippy::disallowed_methods)]
    sqlite_es::sqlite_cqrs(pool, queries, services)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::lifecycle::{EventSourced, Lifecycle};

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

    impl EventSourced for AggregateA {
        type Event = EventA;
    }

    impl EventSourced for AggregateB {
        type Event = EventB;
    }

    #[async_trait]
    impl cqrs_es::Aggregate for Lifecycle<AggregateA> {
        type Command = ();
        type Event = EventA;
        type Error = std::convert::Infallible;
        type Services = ();

        fn aggregate_type() -> String {
            "AggregateA".to_string()
        }

        async fn handle(
            &self,
            _cmd: Self::Command,
            _svc: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[async_trait]
    impl cqrs_es::Aggregate for Lifecycle<AggregateB> {
        type Command = ();
        type Event = EventB;
        type Error = std::convert::Infallible;
        type Services = ();

        fn aggregate_type() -> String {
            "AggregateB".to_string()
        }

        async fn handle(
            &self,
            _cmd: Self::Command,
            _svc: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    struct MultiAggregateQuery {
        name: &'static str,
    }

    #[async_trait]
    impl Query<Lifecycle<AggregateA>> for Arc<MultiAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA>>]) {}
    }

    #[async_trait]
    impl Query<Lifecycle<AggregateB>> for Arc<MultiAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateB>>]) {}
    }

    struct SingleAggregateQuery;

    #[async_trait]
    impl Query<Lifecycle<AggregateA>> for Arc<SingleAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA>>]) {}
    }

    type AggA = Lifecycle<AggregateA>;
    type AggB = Lifecycle<AggregateB>;

    #[test]
    fn single_aggregate_query_wiring() {
        type Deps = Cons<AggA, Nil>;
        let query: UnwiredQuery<SingleAggregateQuery, Deps> =
            UnwiredQuery::new(SingleAggregateQuery);

        // Simulate wiring (no actual pool needed for type-level test)
        let query: UnwiredQuery<SingleAggregateQuery, Nil> = UnwiredQuery {
            query: query.query,
            _deps: PhantomData,
        };

        let _arc: Arc<SingleAggregateQuery> = query.into_inner();
    }

    #[test]
    fn multi_aggregate_query_wiring_sequence() {
        type Deps = Cons<AggA, Cons<AggB, Nil>>;
        let query: UnwiredQuery<MultiAggregateQuery, Deps> =
            UnwiredQuery::new(MultiAggregateQuery { name: "test" });

        // After wiring to A, only B remains
        let query: UnwiredQuery<MultiAggregateQuery, Cons<AggB, Nil>> = UnwiredQuery {
            query: query.query,
            _deps: PhantomData,
        };

        // After wiring to B, Nil
        let query: UnwiredQuery<MultiAggregateQuery, Nil> = UnwiredQuery {
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

        type MultiDeps = Cons<AggA, Cons<AggB, Nil>>;
        type SingleDeps = Cons<AggA, Nil>;

        let multi: UnwiredQuery<MultiAggregateQuery, MultiDeps> =
            UnwiredQuery::new(MultiAggregateQuery { name: "multi" });
        let single: UnwiredQuery<SingleAggregateQuery, SingleDeps> =
            UnwiredQuery::new(SingleAggregateQuery);

        // Build AggregateA CQRS
        let (_cqrs_a, (single, (multi, ()))) = CqrsBuilder::<AggA>::new(pool.clone())
            .wire(multi)
            .wire(single)
            .build(());

        let _single_arc: Arc<SingleAggregateQuery> = single.into_inner();

        // Build AggregateB CQRS
        let (_cqrs_b, (multi, ())) = CqrsBuilder::<AggB>::new(pool.clone()).wire(multi).build(());

        let _multi_arc: Arc<MultiAggregateQuery> = multi.into_inner();
    }
}
