//! Test infrastructure for EventSourced entities.
//!
//! Provides [`replay`] for reconstructing entity state from events
//! and [`TestHarness`] for BDD-style command testing. Both operate
//! at the EventSourced level, hiding Lifecycle/Aggregate internals.

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

impl<Entity: EventSourced> TestResult<Entity>
where
    Entity::Event: PartialEq + std::fmt::Debug,
{
    /// Assert that the command produced exactly these events.
    pub fn then_expect_events(self, expected: Vec<Entity::Event>) {
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
