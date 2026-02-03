//! Type-safe CQRS wiring infrastructure.
//!
//! This module prevents a class of bugs where query processors are created but
//! never registered with their CQRS frameworks, causing events to persist
//! without updating materialized views.
//!
//! # Type-Level Tracking
//!
//! The system uses phantom types to track wiring requirements at compile time:
//!
//! - [`Requires<A, Rest>`] - A type-level list indicating aggregate `A` must be
//!   wired, with `Rest` being the remaining requirements (another `Requires` or
//!   [`Done`]).
//!
//! - [`Done`] - Terminal marker indicating all requirements satisfied.
//!
//! - [`UnwiredQuery<Q, Required>`] - Wraps a query processor `Q` with its
//!   wiring requirements. The [`into_inner`](UnwiredQuery::into_inner) method
//!   is only available when `Required = Done`, enforcing complete wiring.
//!
//! # Examples
//!
//! ## Single-aggregate queries (simple case)
//!
//! ```ignore
//! // Query only needs wiring to Position aggregate
//! type ViewReqs = Requires<PositionAgg, Done>;
//! let view: UnwiredQuery<PositionView, ViewReqs> = UnwiredQuery::new(view);
//!
//! // Build and discard - query requirements satisfied after this wiring
//! let position_cqrs = CqrsBuilder::new(pool)
//!     .wire(view)
//!     .build(());
//! ```
//!
//! ## Multi-aggregate queries
//!
//! ```ignore
//! // Query requiring wiring to Position, then Mint aggregates
//! type TriggerReqs = Requires<PositionAgg, Requires<MintAgg, Done>>;
//! let trigger: UnwiredQuery<Trigger, TriggerReqs> = UnwiredQuery::new(trigger);
//!
//! // Build Position CQRS - use build_with_queries to get trigger back
//! let (position_cqrs, (trigger, ())) = CqrsBuilder::new(pool.clone())
//!     .wire(trigger)
//!     .build_with_queries(());
//! // trigger is now: UnwiredQuery<Trigger, Requires<MintAgg, Done>>
//!
//! // Build Mint CQRS - trigger's last requirement, can use simple build
//! let mint_cqrs = CqrsBuilder::new(pool.clone())
//!     .wire(trigger)
//!     .build(());
//! ```
//!
//! # Clippy Enforcement
//!
//! Direct calls to `sqlite_es::sqlite_cqrs` are blocked via clippy's
//! `disallowed-methods`. All CQRS construction must go through [`CqrsBuilder`],
//! which contains the single `#[allow]` escape hatch.

use std::marker::PhantomData;
use std::sync::Arc;

use cqrs_es::Aggregate;
use cqrs_es::Query;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;

/// Type-level marker: aggregate `A` must be wired, then `Rest`.
///
/// Forms a compile-time linked list of requirements. Use with [`Done`] as the
/// terminal: `Requires<Agg1, Requires<Agg2, Done>>`.
pub(crate) struct Requires<A, Rest>(PhantomData<(A, Rest)>);

/// Type-level marker: all wiring requirements satisfied.
///
/// Terminal element for the [`Requires`] list. When an [`UnwiredQuery`] reaches
/// this state, [`into_inner`](UnwiredQuery::into_inner) becomes available.
pub(crate) struct Done;

/// A query processor with compile-time wiring requirements.
///
/// Wraps an `Arc<Q>` and tracks which aggregates it still needs to be wired to
/// via the `Required` phantom type. The inner Arc is only extractable via
/// [`into_inner`](Self::into_inner) when `Required = Done`.
///
/// # Type Parameter Evolution
///
/// Each call to [`CqrsBuilder::wire`] consumes this and returns a new instance
/// with the head aggregate removed from `Required`:
///
/// ```text
/// UnwiredQuery<Q, Requires<A, Requires<B, Done>>>
///     --wire to A-->
/// UnwiredQuery<Q, Requires<B, Done>>
///     --wire to B-->
/// UnwiredQuery<Q, Done>
///     --into_inner-->
/// Arc<Q>
/// ```
#[must_use = "query must be wired via CqrsBuilder, then extracted with into_inner"]
pub(crate) struct UnwiredQuery<Q, Required> {
    query: Arc<Q>,
    _required: PhantomData<Required>,
}

impl<Q, Required> UnwiredQuery<Q, Required> {
    /// Creates a new unwired query with the given requirements.
    ///
    /// The `Required` type parameter should encode all aggregates this query
    /// needs to be wired to before it can be used.
    pub(crate) fn new(query: Q) -> Self {
        Self {
            query: Arc::new(query),
            _required: PhantomData,
        }
    }

    /// Creates from an existing Arc with the given requirements.
    pub(crate) fn from_arc(query: Arc<Q>) -> Self {
        Self {
            query,
            _required: PhantomData,
        }
    }
}

impl<Q> UnwiredQuery<Q, Done> {
    /// Extracts the inner Arc. Only available when all requirements are met.
    ///
    /// This method's availability is the compile-time proof that all required
    /// aggregates have been wired via [`CqrsBuilder::wire`].
    pub(crate) fn into_inner(self) -> Arc<Q> {
        self.query
    }
}

/// Builder for a single CQRS framework with type-tracked query wiring.
///
/// Accumulates query processors via [`wire`](Self::wire) calls, tracking them
/// at the type level in the `Wired` parameter.
///
/// # Build Methods
///
/// - [`build`](Self::build) - Returns just the CQRS framework. Use when all
///   wired queries only target this aggregate.
///
/// - [`build_with_queries`](Self::build_with_queries) - Returns `(SqliteCqrs,
///   Wired)` tuple. Use when queries need continued wiring to other aggregates.
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
}

impl<A: Aggregate, W> CqrsBuilder<A, W> {
    /// Wires a query processor to this CQRS framework.
    ///
    /// Consumes the [`UnwiredQuery`] and returns a new builder with:
    /// - The query added to the internal processors list
    /// - An updated `UnwiredQuery` (with `A` removed from requirements) added
    ///   to the `Wired` tuple for return at build time
    ///
    /// # Type Evolution
    ///
    /// The input query must have `A` as its next requirement:
    /// `UnwiredQuery<Q, Requires<A, Rest>>`. The returned query in the tuple
    /// will be `UnwiredQuery<Q, Rest>`.
    pub(crate) fn wire<Q, Rest>(
        mut self,
        query: UnwiredQuery<Q, Requires<A, Rest>>,
    ) -> CqrsBuilder<A, (UnwiredQuery<Q, Rest>, W)>
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
                    _required: PhantomData,
                },
                self.wired,
            ),
        }
    }

    /// Builds the CQRS framework, returning it with all wired queries.
    ///
    /// Use this when queries need continued wiring to other aggregates. The
    /// `Wired` tuple contains each query passed to [`wire`](Self::wire), with
    /// their types updated to reflect the wiring. Destructure to continue
    /// wiring to other builders or extract via [`UnwiredQuery::into_inner`].
    ///
    /// For simpler cases where queries don't need further wiring, use
    /// [`build`](Self::build) instead.
    pub(crate) fn build_with_queries<S>(self, services: S) -> (SqliteCqrs<A>, W)
    where
        A: Aggregate<Services = S>,
    {
        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool, self.queries, services);
        (cqrs, self.wired)
    }

    /// Builds the CQRS framework, discarding the wired queries.
    ///
    /// Use this when all wired queries only target this aggregate (their
    /// requirements become `Done` after this wiring). For queries that need
    /// continued wiring to other aggregates, use
    /// [`build_with_queries`](Self::build_with_queries) instead.
    pub(crate) fn build<S>(self, services: S) -> SqliteCqrs<A>
    where
        A: Aggregate<Services = S>,
    {
        self.build_with_queries(services).0
    }
}

/// Test-only escape hatch for creating CQRS frameworks directly.
///
/// Tests often need CQRS with specific configurations that don't fit the
/// production wiring pattern. Use this instead of `sqlite_es::sqlite_cqrs`.
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
    use crate::lifecycle::{Lifecycle, Never};

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct AggregateA;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct AggregateB;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct EventA;

    impl DomainEvent for EventA {
        fn event_type(&self) -> String {
            "EventA".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    impl cqrs_es::Aggregate for Lifecycle<AggregateA, Never> {
        type Command = ();
        type Event = EventA;
        type Error = Never;
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
    impl cqrs_es::Aggregate for Lifecycle<AggregateB, Never> {
        type Command = ();
        type Event = EventB;
        type Error = Never;
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
    impl Query<Lifecycle<AggregateA, Never>> for Arc<MultiAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA, Never>>]) {}
    }

    #[async_trait]
    impl Query<Lifecycle<AggregateB, Never>> for Arc<MultiAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateB, Never>>]) {}
    }

    struct SingleAggregateQuery;

    #[async_trait]
    impl Query<Lifecycle<AggregateA, Never>> for Arc<SingleAggregateQuery> {
        async fn dispatch(&self, _: &str, _: &[EventEnvelope<Lifecycle<AggregateA, Never>>]) {}
    }

    type AggA = Lifecycle<AggregateA, Never>;
    type AggB = Lifecycle<AggregateB, Never>;

    #[test]
    fn single_aggregate_query_wiring() {
        // Query that only needs wiring to AggregateA
        type Reqs = Requires<AggA, Done>;
        let query: UnwiredQuery<SingleAggregateQuery, Reqs> =
            UnwiredQuery::new(SingleAggregateQuery);

        // Cannot call into_inner yet - would be compile error:
        // let _ = query.into_inner();

        // Simulate wiring (no actual pool needed for type-level test)
        let query: UnwiredQuery<SingleAggregateQuery, Done> = UnwiredQuery {
            query: query.query,
            _required: PhantomData,
        };

        // Now we can extract
        let _arc: Arc<SingleAggregateQuery> = query.into_inner();
    }

    #[test]
    fn multi_aggregate_query_wiring_sequence() {
        // Query requiring wiring to A and B
        type Reqs = Requires<AggA, Requires<AggB, Done>>;
        let query: UnwiredQuery<MultiAggregateQuery, Reqs> =
            UnwiredQuery::new(MultiAggregateQuery { name: "test" });

        // After wiring to A, only B remains
        let query: UnwiredQuery<MultiAggregateQuery, Requires<AggB, Done>> = UnwiredQuery {
            query: query.query,
            _required: PhantomData,
        };

        // After wiring to B, Done
        let query: UnwiredQuery<MultiAggregateQuery, Done> = UnwiredQuery {
            query: query.query,
            _required: PhantomData,
        };

        let arc = query.into_inner();
        assert_eq!(arc.name, "test");
    }

    #[tokio::test]
    async fn full_wiring_flow_with_builders() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        // Create queries with their full requirements
        type MultiReqs = Requires<AggA, Requires<AggB, Done>>;
        type SingleReqs = Requires<AggA, Done>;

        let multi: UnwiredQuery<MultiAggregateQuery, MultiReqs> =
            UnwiredQuery::new(MultiAggregateQuery { name: "multi" });
        let single: UnwiredQuery<SingleAggregateQuery, SingleReqs> =
            UnwiredQuery::new(SingleAggregateQuery);

        // Build AggregateA CQRS - multi needs further wiring, single doesn't
        // Use build_with_queries to get multi back for continued wiring
        let (cqrs_a, (single, (multi, ()))) = CqrsBuilder::<AggA>::new(pool.clone())
            .wire(multi)
            .wire(single)
            .build_with_queries(());

        // single is now Done (only needed AggA)
        let _single_arc: Arc<SingleAggregateQuery> = single.into_inner();

        // Build AggregateB CQRS - multi's last requirement, use simple build
        let _cqrs_b = CqrsBuilder::<AggB>::new(pool.clone()).wire(multi).build(());

        // Both CQRS frameworks are ready
        drop(cqrs_a);
    }
}
