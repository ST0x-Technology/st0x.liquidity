//! Test infrastructure for EventSourced entities.
//!
//! Provides [`replay`] for reconstructing entity state from events,
//! [`TestHarness`] for BDD-style command testing, and [`TestStore`]
//! for in-memory command dispatch with state inspection. All operate
//! at the EventSourced level, hiding Lifecycle/Aggregate internals.

use std::fmt::Debug;
use std::str::FromStr;

use cqrs_es::{Aggregate, CqrsFramework, EventStore, Query, mem_store};

use crate::Reactor;
use crate::lifecycle::{Lifecycle, LifecycleError, ReactorBridge};
use crate::{EventSourced, Store};

/// Replay events through EventSourced to reconstruct entity state.
///
/// Returns the entity if replay produces a live state, or the
/// lifecycle error if originate/evolve fails.
pub fn replay<Entity: EventSourced>(
    events: impl IntoIterator<Item = Entity::Event>,
) -> Result<Option<Entity>, LifecycleError<Entity>> {
    let mut lifecycle = Lifecycle::<Entity>::default();

    for event in events {
        lifecycle.apply(event);
    }

    lifecycle.into_result()
}

/// BDD-style test harness for EventSourced implementations.
///
/// # Example
///
/// ```ignore
/// TestHarness::<Position>::with(())
///     .given(vec![PositionEvent::Initialized { .. }])
///     .when(PositionCommand::AcknowledgeFill { .. })
///     .await
///     .then_expect_events(vec![PositionEvent::FillAcknowledged { .. }]);
/// ```
pub struct TestHarness<Entity: EventSourced> {
    services: Entity::Services,
    events: Vec<Entity::Event>,
}

impl<Entity: EventSourced> TestHarness<Entity> {
    /// Create a harness with the given services.
    pub fn with(services: Entity::Services) -> Self {
        Self {
            services,
            events: vec![],
        }
    }

    /// Set up prior events (given some history).
    #[must_use]
    pub fn given(mut self, events: Vec<Entity::Event>) -> Self {
        self.events = events;
        self
    }

    /// Set up with no prior events.
    #[must_use]
    pub fn given_no_previous_events(self) -> Self {
        self
    }

    /// Execute a command and return the result.
    pub async fn when(self, command: Entity::Command) -> TestResult<Entity> {
        let mut lifecycle = Lifecycle::<Entity>::default();
        for event in self.events {
            lifecycle.apply(event);
        }

        let result = lifecycle.handle(command, &self.services).await;

        TestResult { result }
    }
}

/// Result of a [`TestHarness::when`] invocation.
pub struct TestResult<Entity: EventSourced> {
    result: Result<Vec<Entity::Event>, LifecycleError<Entity>>,
}

#[expect(
    clippy::expect_used,
    reason = "test assertion helpers are meant to panic on failure"
)]
impl<Entity: EventSourced> TestResult<Entity>
where
    Entity::Event: PartialEq + std::fmt::Debug,
{
    /// Assert that the command produced exactly these events.
    pub fn then_expect_events(self, expected: &[Entity::Event]) {
        let events = self
            .result
            .expect("expected events but command returned error");
        assert_eq!(events, expected);
    }

    /// Assert that the command produced no events.
    pub fn then_expect_no_events(self) {
        let events = self
            .result
            .expect("expected no events but command returned error");
        assert!(events.is_empty(), "expected no events but got {events:?}");
    }

    /// Assert that the command failed with a LifecycleError, and
    /// return it for further assertions.
    pub fn then_expect_error(self) -> LifecycleError<Entity> {
        self.result
            .expect_err("expected error but command succeeded")
    }

    /// Return the events for custom assertions.
    pub fn events(self) -> Vec<Entity::Event> {
        self.result
            .expect("expected events but command returned error")
    }
}

/// Test-only escape hatch for creating CQRS frameworks directly.
///
/// Create a SQLite-backed Store with no reactors, for tests that
/// need persistence but no event processing side-effects.
pub fn test_store<Entity: EventSourced>(
    pool: sqlx::SqlitePool,
    services: Entity::Services,
) -> Store<Entity> {
    #[allow(clippy::disallowed_methods)]
    let cqrs = sqlite_es::sqlite_cqrs(pool, vec![], services);
    Store::new(cqrs)
}

/// In-memory event store for unit tests.
///
/// Provides the same typed-ID interface as [`Store`] but backed
/// by an in-memory store instead of SQLite. Also exposes
/// [`load`](Self::load) for inspecting aggregate state after
/// commands, which production [`Store`] intentionally omits
/// (use projections instead).
pub struct TestStore<Entity: EventSourced> {
    mem_store: mem_store::MemStore<Lifecycle<Entity>>,
    cqrs: CqrsFramework<Lifecycle<Entity>, mem_store::MemStore<Lifecycle<Entity>>>,
}

impl<Entity: EventSourced> TestStore<Entity> {
    /// Create an in-memory TestStore for fast, isolated unit tests.
    ///
    /// Accepts [`Reactor`] impls which are internally bridged to
    /// cqrs-es queries.
    pub fn new(reactors: Vec<Box<dyn Reactor<Entity>>>, services: Entity::Services) -> Self
    where
        Entity: 'static,
        <Entity::Id as FromStr>::Err: Debug,
    {
        let queries: Vec<Box<dyn Query<Lifecycle<Entity>>>> = reactors
            .into_iter()
            .map(|reactor| Box::new(ReactorBridge(reactor)) as Box<dyn Query<Lifecycle<Entity>>>)
            .collect();

        let mem_store = mem_store::MemStore::default();
        #[allow(clippy::disallowed_methods)]
        let cqrs = CqrsFramework::new(mem_store.clone(), queries, services);
        Self { mem_store, cqrs }
    }

    /// Send a command to the entity identified by `id`.
    pub async fn send(
        &self,
        id: &Entity::Id,
        command: Entity::Command,
    ) -> Result<(), crate::SendError<Entity>> {
        self.cqrs.execute(&id.to_string(), command).await
    }

    /// Load the entity state by typed ID.
    ///
    /// Returns:
    /// - `Ok(Some(entity))` if the entity is live
    /// - `Ok(None)` if the entity has not been initialized
    /// - `Err(error)` if the entity is in a failed lifecycle state
    #[expect(
        clippy::unwrap_used,
        reason = "test-only helper, panicking on error is fine"
    )]
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, LifecycleError<Entity>>
    where
        Entity: Clone,
    {
        self.mem_store
            .load_aggregate(&id.to_string())
            .await
            .unwrap()
            .aggregate
            .into_result()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::Table;

    /// Minimal counter entity for testing replay and harness.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Counter {
        value: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Created {
            initial: u32,
        },
        Incremented,
        /// Event that evolve rejects (returns None) to test mismatch.
        ResetToZero,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "Created".to_string(),
                Self::Incremented => "Incremented".to_string(),
                Self::ResetToZero => "ResetToZero".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    enum CounterError {
        #[error("overflow at {value}")]
        Overflow { value: u32 },
    }

    enum CounterCommand {
        Create { initial: u32 },
        Increment,
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Event = CounterEvent;
        type Command = CounterCommand;
        type Error = CounterError;
        type Services = ();

        const AGGREGATE_TYPE: &'static str = "Counter";
        const PROJECTION: Option<Table> = None;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &CounterEvent) -> Option<Self> {
            use CounterEvent::*;

            match event {
                Created { initial } => Some(Self { value: *initial }),
                _ => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, CounterError> {
            use CounterEvent::*;

            match event {
                Incremented => {
                    let next = entity.value.checked_add(1).ok_or(CounterError::Overflow {
                        value: entity.value,
                    })?;
                    Ok(Some(Self { value: next }))
                }
                Created { .. } | ResetToZero => Ok(None),
            }
        }

        async fn initialize(
            command: CounterCommand,
            _services: &(),
        ) -> Result<Vec<CounterEvent>, CounterError> {
            match command {
                CounterCommand::Create { initial } => Ok(vec![CounterEvent::Created { initial }]),
                CounterCommand::Increment => Ok(vec![]),
            }
        }

        async fn transition(
            &self,
            command: CounterCommand,
            _services: &(),
        ) -> Result<Vec<CounterEvent>, CounterError> {
            match command {
                CounterCommand::Create { .. } => Ok(vec![]),
                CounterCommand::Increment => Ok(vec![CounterEvent::Incremented]),
            }
        }
    }

    #[test]
    fn replay_valid_history_returns_live_entity() {
        let counter = replay::<Counter>(vec![
            CounterEvent::Created { initial: 10 },
            CounterEvent::Incremented,
            CounterEvent::Incremented,
        ])
        .unwrap()
        .unwrap();

        assert_eq!(counter.value, 12);
    }

    #[test]
    fn replay_empty_events_returns_none() {
        let result = replay::<Counter>(vec![]).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn replay_cant_originate_returns_error() {
        // Incremented is not a genesis event, so originate returns None
        let error = replay::<Counter>(vec![CounterEvent::Incremented]).unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn replay_unexpected_event_on_evolve_returns_error() {
        // ResetToZero causes evolve to return Ok(None)
        let error = replay::<Counter>(vec![
            CounterEvent::Created { initial: 5 },
            CounterEvent::ResetToZero,
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }

    #[tokio::test]
    async fn harness_given_history_then_command_produces_events() {
        TestHarness::<Counter>::with(())
            .given(vec![CounterEvent::Created { initial: 0 }])
            .when(CounterCommand::Increment)
            .await
            .then_expect_events(&[CounterEvent::Incremented]);
    }

    #[tokio::test]
    async fn harness_initialize_produces_genesis_event() {
        TestHarness::<Counter>::with(())
            .given_no_previous_events()
            .when(CounterCommand::Create { initial: 42 })
            .await
            .then_expect_events(&[CounterEvent::Created { initial: 42 }]);
    }

    #[tokio::test]
    async fn harness_on_failed_lifecycle_returns_error() {
        let error = TestHarness::<Counter>::with(())
            .given(vec![CounterEvent::Incremented])
            .when(CounterCommand::Increment)
            .await
            .then_expect_error();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }
}
