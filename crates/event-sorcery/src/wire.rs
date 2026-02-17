//! CQRS framework construction via [`StoreBuilder`].
//!
//! All CQRS framework construction must go through
//! [`StoreBuilder`], which reconciles schema versions and
//! registers reactors. Direct calls to `sqlite_es::sqlite_cqrs`
//! are blocked via clippy's `disallowed-methods`; `StoreBuilder`
//! contains the single `#[allow]` escape hatch.
//!
//! # Registering reactors
//!
//! Use [`.with()`](StoreBuilder::with) to register a reactor
//! with a builder. For single-entity reactors, wrap in
//! `Arc::new()`. For multi-entity reactors, clone the same
//! `Arc` into each builder:
//!
//! ```ignore
//! let trigger = Arc::new(RebalancingTrigger::new(...));
//!
//! let position = StoreBuilder::<Position>::new(pool.clone())
//!     .with(Arc::new(position_view))
//!     .with(trigger.clone())
//!     .build(())
//!     .await?;
//!
//! let mint = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
//!     .with(trigger.clone())
//!     .build(services)
//!     .await?;
//! ```
//!
//! Exhaustive entity handling is enforced by the reactor's
//! [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain at compile
//! time, not by the wiring infrastructure.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use cqrs_es::Query;
use sqlx::SqlitePool;

use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::reactor::Reactor;
use crate::schema_registry::{ReconcileError, Reconciler};
use crate::{EventSourced, Store};

/// Builder for a single CQRS framework.
///
/// Parameterized on an [`EventSourced`] entity type. Internally
/// constructs `SqliteCqrs<Lifecycle<Entity>>` but returns
/// [`Store<Entity>`], keeping Lifecycle as an implementation
/// detail.
///
/// Register reactors via [`.with()`](Self::with), then call
/// [`.build()`](Self::build) to construct the framework.
pub struct StoreBuilder<Entity: EventSourced> {
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
}

impl<Entity: EventSourced> StoreBuilder<Entity> {
    /// Creates a new builder for the given entity type.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
        }
    }

    /// Registers a reactor with this CQRS framework.
    ///
    /// The reactor must declare `Entity` in its dependency list
    /// (via [`deps!`](crate::deps)). For multi-entity reactors,
    /// clone the same `Arc` into each relevant builder.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Single-entity reactor
    /// StoreBuilder::<Position>::new(pool.clone())
    ///     .with(Arc::new(position_view))
    ///     .build(())
    ///     .await?;
    ///
    /// // Multi-entity reactor (shared across builders)
    /// let trigger = Arc::new(RebalancingTrigger::new(...));
    /// StoreBuilder::<Position>::new(pool.clone())
    ///     .with(trigger.clone())
    ///     .build(())
    ///     .await?;
    /// StoreBuilder::<Mint>::new(pool.clone())
    ///     .with(trigger.clone())
    ///     .build(services)
    ///     .await?;
    /// ```
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

    /// Builds the CQRS framework.
    ///
    /// Reconciles the entity's schema version before constructing
    /// the framework, clearing stale snapshots if the version
    /// changed.
    ///
    /// Returns a [`Store`] for type-safe command dispatch.
    pub async fn build(self, services: Entity::Services) -> Result<Store<Entity>, ReconcileError> {
        Reconciler::new(self.pool.clone())
            .reconcile::<Entity>()
            .await?;

        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(self.pool.clone(), self.queries, services);
        Ok(Store::new(cqrs, self.pool))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::Table;
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

    #[tokio::test]
    async fn single_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let _store = StoreBuilder::<AggregateA>::new(pool.clone())
            .with(Arc::new(SingleEntityReactor))
            .build(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn multi_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

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
}
