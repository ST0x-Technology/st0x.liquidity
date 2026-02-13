//! Backend-agnostic projection for loading entity state from
//! materialized views.
//!
//! Generic over the view repository implementation, allowing any
//! backend (SQLite, Postgres, in-memory) that implements
//! `ViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>`.

use std::sync::Arc;

use cqrs_es::persist::{GenericQuery, ViewRepository};

use crate::EventSourced;
use crate::lifecycle::{Lifecycle, LifecycleError};

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
