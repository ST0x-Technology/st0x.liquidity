//! Backend-agnostic projection for loading entity state from
//! materialized views.
//!
//! Generic over the view repository implementation, allowing any
//! backend (SQLite, Postgres, in-memory) that implements
//! `ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`.

use cqrs_es::persist::{GenericQuery, ViewRepository};
use sqlite_es::SqliteViewRepository;
use std::sync::Arc;

use crate::EventSourced;
use crate::lifecycle::{Lifecycle, LifecycleError};

/// SQLite-backed projection -- the concrete type used in
/// production wiring.
pub type SqliteProjection<Entity> =
    Projection<Entity, SqliteViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>>;

/// Materialized view of an event-sourced entity.
///
/// Provides [`load`](Self::load) to retrieve the current entity
/// state. The `Repo` type parameter determines the storage
/// backend (e.g., `SqliteViewRepository` for SQLite).
///
/// Constructed via [`new`](Self::new) during wiring in
/// [`StoreBuilder`](crate::StoreBuilder) or directly in CLI/test code.
pub struct Projection<
    Entity: EventSourced,
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
> {
    inner: Arc<GenericQuery<Repo, Lifecycle<Entity>, Lifecycle<Entity>>>,
}

impl<Entity: EventSourced, Repo> Projection<Entity, Repo>
where
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
{
    /// Creates a new projection from a view repository.
    pub fn new(repo: Arc<Repo>) -> Self {
        let query = GenericQuery::new(repo);
        Self {
            inner: Arc::new(query),
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
        let Some(lifecycle) = self.inner.load(&aggregate_id).await else {
            return Ok(None);
        };
        match lifecycle {
            Lifecycle::Live(entity) => Ok(Some(entity)),
            Lifecycle::Uninitialized => Ok(None),
            Lifecycle::Failed { error, .. } => Err(error),
        }
    }

    /// Access the inner GenericQuery for wiring with StoreBuilder.
    ///
    /// GenericQuery implements `cqrs_es::Query<Lifecycle<Entity>>`
    /// natively via `View::update`, so it can be registered directly
    /// with the CQRS framework.
    pub(crate) fn inner_query(
        &self,
    ) -> Arc<GenericQuery<Repo, Lifecycle<Entity>, Lifecycle<Entity>>> {
        Arc::clone(&self.inner)
    }
}

impl<Entity: EventSourced, Repo> Clone for Projection<Entity, Repo>
where
    Repo: ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
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
