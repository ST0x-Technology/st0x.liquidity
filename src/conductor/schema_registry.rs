//! Schema version registry for detecting stale snapshots.
//!
//! Tracks the last-known [`SCHEMA_VERSION`] for each aggregate type
//! in the event store. On startup, [`Reconciler::reconcile`] compares
//! the stored version against the current code version and clears
//! snapshots when they diverge.
//!
//! This is itself an event-sourced aggregate, so no migration is
//! needed -- versions are stored as events in the shared event store.
//! The view table is self-bootstrapping (created at runtime via
//! `CREATE TABLE IF NOT EXISTS`).
//!
//! [`SCHEMA_VERSION`]: crate::event_sourced::EventSourced::SCHEMA_VERSION

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use cqrs_es::persist::{GenericQuery, ViewRepository};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteCqrs, SqliteViewRepository};
use sqlx::SqlitePool;
use tracing::info;

use crate::event_sourced::EventSourced;
use crate::lifecycle::{Lifecycle, Never};

/// Singleton aggregate ID for the schema registry.
const REGISTRY_ID: &str = "schema";

/// View table name (self-bootstrapping, no migration needed).
const VIEW_TABLE: &str = "schema_registry_view";

/// Tracks schema versions for all aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SchemaRegistry {
    versions: BTreeMap<String, u64>,
}

impl SchemaRegistry {
    fn version_of(&self, name: &str) -> Option<u64> {
        self.versions.get(name).copied()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum SchemaRegistryEvent {
    VersionUpdated { name: String, version: u64 },
}

impl DomainEvent for SchemaRegistryEvent {
    fn event_type(&self) -> String {
        "SchemaRegistryEvent::VersionUpdated".to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SchemaRegistryCommand {
    Register { name: String, version: u64 },
}

#[async_trait]
impl EventSourced for SchemaRegistry {
    type Id = &'static str;
    type Event = SchemaRegistryEvent;
    type Command = SchemaRegistryCommand;
    type Error = Never;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "SchemaRegistry";
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut versions = BTreeMap::new();
        versions.insert(name.clone(), *version);
        Some(Self { versions })
    }

    fn evolve(
        event: &Self::Event,
        state: &Self,
    ) -> Result<Option<Self>, Self::Error> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut new_state = state.clone();
        new_state.versions.insert(name.clone(), *version);
        Ok(Some(new_state))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        Ok(vec![SchemaRegistryEvent::VersionUpdated { name, version }])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        if self.version_of(&name) == Some(version) {
            Ok(vec![])
        } else {
            Ok(vec![SchemaRegistryEvent::VersionUpdated { name, version }])
        }
    }
}

type ViewRepo = SqliteViewRepository<Lifecycle<SchemaRegistry>, Lifecycle<SchemaRegistry>>;
type Query = GenericQuery<ViewRepo, Lifecycle<SchemaRegistry>, Lifecycle<SchemaRegistry>>;

/// Handles schema version reconciliation at startup.
///
/// Owns the SchemaRegistry CQRS framework and view, providing
/// [`reconcile`](Self::reconcile) to check and update versions
/// for each aggregate type.
pub(crate) struct Reconciler {
    cqrs: SqliteCqrs<Lifecycle<SchemaRegistry>>,
    view: Arc<ViewRepo>,
    pool: SqlitePool,
}

impl Reconciler {
    /// Creates a new reconciler, bootstrapping the view table if
    /// needed.
    pub(crate) async fn new(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_registry_view (\
             view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, \
             payload JSON NOT NULL)",
        )
        .execute(&pool)
        .await?;

        let view = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            VIEW_TABLE.to_string(),
        ));
        let query: Query = GenericQuery::new(view.clone());

        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(pool.clone(), vec![Box::new(query)], ());

        Ok(Self { cqrs, view, pool })
    }

    /// Checks the stored schema version for `Entity` and clears
    /// snapshots if the version has changed.
    ///
    /// Returns `true` if snapshots were cleared (schema changed),
    /// `false` if versions matched.
    pub(crate) async fn reconcile<Entity: EventSourced>(
        &self,
    ) -> Result<bool, anyhow::Error> {
        let name = Entity::AGGREGATE_TYPE;
        let current_version = Entity::SCHEMA_VERSION;

        let stored_version = self
            .view
            .load(REGISTRY_ID)
            .await?
            .and_then(|lifecycle| match lifecycle {
                Lifecycle::Live(registry) => registry.version_of(name),
                _ => None,
            });

        let needs_clear = stored_version != Some(current_version);

        if needs_clear {
            sqlx::query("DELETE FROM snapshots WHERE aggregate_type = ?")
                .bind(name)
                .execute(&self.pool)
                .await?;

            info!(
                aggregate = name,
                old_version = ?stored_version,
                new_version = current_version,
                "Cleared stale snapshots for schema version change"
            );
        }

        self.cqrs
            .execute(
                REGISTRY_ID,
                SchemaRegistryCommand::Register {
                    name: name.to_string(),
                    version: current_version,
                },
            )
            .await?;

        Ok(needs_clear)
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::Aggregate;

    use super::*;

    #[tokio::test]
    async fn register_new_aggregate_emits_event() {
        let aggregate = Lifecycle::<SchemaRegistry>::default();

        let events = aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 1,
            }
        );
    }

    #[tokio::test]
    async fn register_same_version_is_noop() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let events = aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
            )
            .await
            .unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn register_new_version_emits_event() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let events = aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 2,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 2,
            }
        );
    }

    #[test]
    fn tracks_multiple_aggregates() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });
        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "OffchainOrder".to_string(),
            version: 3,
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(registry.version_of("Position"), Some(1));
        assert_eq!(registry.version_of("OffchainOrder"), Some(3));
        assert_eq!(registry.version_of("Unknown"), None);
    }

    #[tokio::test]
    async fn reconciler_detects_version_change() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool).await.unwrap();

        // First run: no stored version -> needs clear
        let cleared = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert!(cleared);

        // Second run: version matches -> no clear
        let cleared = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert!(!cleared);
    }
}
