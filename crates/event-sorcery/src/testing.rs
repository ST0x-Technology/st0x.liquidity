//! Test infrastructure for EventSourced entities.
//!
//! Provides [`replay`] for reconstructing entity state from events
//! and [`TestHarness`] for BDD-style command testing. Both operate
//! at the EventSourced level, hiding Lifecycle/Aggregate internals.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use cqrs_es::Aggregate;

use crate::EventSourced;
use crate::lifecycle::{Lifecycle, LifecycleError};

/// Replay events through EventSourced to reconstruct entity state.
///
/// Returns the entity if replay produces a live state, or the
/// lifecycle error if originate/evolve fails.
pub fn replay<Entity: EventSourced>(
    events: impl IntoIterator<Item = Entity::Event>,
) -> Result<Entity, LifecycleError<Entity>> {
    let mut lifecycle = Lifecycle::<Entity>::default();

    for event in events {
        lifecycle.apply(event);
    }

    match lifecycle {
        Lifecycle::Live(entity) => Ok(entity),
        Lifecycle::Uninitialized => Err(LifecycleError::Uninitialized),
        Lifecycle::Failed { error, .. } => Err(error),
    }
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;

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
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &CounterEvent) -> Option<Self> {
            use CounterEvent::*;

            match event {
                Created { initial } => Some(Self { value: *initial }),
                _ => None,
            }
        }

        fn evolve(event: &CounterEvent, state: &Self) -> Result<Option<Self>, CounterError> {
            use CounterEvent::*;

            match event {
                Incremented => {
                    let next = state
                        .value
                        .checked_add(1)
                        .ok_or(CounterError::Overflow { value: state.value })?;
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
        .unwrap();

        assert_eq!(counter.value, 12);
    }

    #[test]
    fn replay_empty_events_returns_uninitialized() {
        let error = replay::<Counter>(vec![]).unwrap_err();

        assert!(matches!(error, LifecycleError::Uninitialized));
    }

    #[test]
    fn replay_mismatch_on_originate_returns_failed() {
        // Incremented is not a genesis event, so originate returns None
        let error = replay::<Counter>(vec![CounterEvent::Incremented]).unwrap_err();

        assert!(matches!(error, LifecycleError::Mismatch { .. }));
    }

    #[test]
    fn replay_mismatch_on_evolve_returns_failed() {
        // ResetToZero causes evolve to return Ok(None)
        let error = replay::<Counter>(vec![
            CounterEvent::Created { initial: 5 },
            CounterEvent::ResetToZero,
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::Mismatch { .. }));
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
        // Feed an event that causes Mismatch, then send a command
        let error = TestHarness::<Counter>::with(())
            .given(vec![CounterEvent::Incremented])
            .when(CounterCommand::Increment)
            .await
            .then_expect_error();

        assert!(matches!(error, LifecycleError::Mismatch { .. }));
    }
}
