//! Test infrastructure for EventSourced entities and Reactors.
//!
//! Provides [`replay`] for reconstructing entity state from events,
//! [`TestHarness`] for BDD-style command testing, [`TestStore`]
//! for in-memory command dispatch with state inspection,
//! [`ReactorHarness`] for ergonomic multi-entity reactor testing,
//! and [`SpyReactor`] for capturing dispatched events.
//! All operate at the EventSourced/Reactor level, hiding
//! Lifecycle/Aggregate internals.

use async_trait::async_trait;
use cqrs_es::{Aggregate, CqrsFramework, EventStore, Query, mem_store};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, LifecycleError, ReactorBridge};
use crate::reactor::Reactor;
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
    let cqrs = sqlite_es::sqlite_cqrs(pool.clone(), vec![], services);
    Store::new(cqrs, pool)
}

/// Test wrapper for [`Reactor`] implementations that hides
/// `OneOf::Here`/`OneOf::There` nesting.
///
/// Provides [`receive`](Self::receive) to send entity events to
/// the reactor using concrete `(Id, Event)` pairs. The correct
/// `OneOf` variant is constructed automatically via type inference
/// on the id and event types.
///
/// # Example
///
/// ```ignore
/// let (sender, mut receiver) = broadcast::channel(16);
/// let harness = ReactorHarness::new(EventBroadcaster::new(sender));
///
/// // No manual OneOf nesting -- depth inferred from types
/// harness.receive(mint_id, mint_event).await.unwrap();
/// harness.receive(redemption_id, redemption_event).await.unwrap();
///
/// // Assert on observable side effects
/// let msg = receiver.recv().await.unwrap();
/// ```
pub struct ReactorHarness<R> {
    reactor: R,
}

impl<R: Reactor> ReactorHarness<R> {
    /// Wrap a reactor for ergonomic testing.
    pub fn new(reactor: R) -> Self {
        Self { reactor }
    }

    /// Send an entity event to the reactor.
    ///
    /// For single-entity reactors, the entity type is inferred
    /// automatically. For multi-entity reactors, specify the
    /// entity type via turbofish:
    ///
    /// ```ignore
    /// harness.receive::<TokenizedEquityMint>(id, event).await?;
    /// harness.receive::<EquityRedemption>(id, event).await?;
    /// ```
    ///
    /// Requires [`register_entities!`] to be called for the
    /// reactor's entity list.
    pub async fn receive<Entity: EventSourced>(
        &self,
        id: Entity::Id,
        event: Entity::Event,
    ) -> Result<(), R::Error>
    where
        R::Dependencies: HasEntity<Entity>,
    {
        let injected = <R::Dependencies as HasEntity<Entity>>::inject(id, event);
        self.reactor.react(injected).await
    }

    /// Access the inner reactor for inspecting state.
    pub fn inner(&self) -> &R {
        &self.reactor
    }
}

/// Single-entity spy reactor that captures all dispatched events.
///
/// Provides a generic event-capturing reactor so tests don't need
/// to define their own ad-hoc reactors with `deps!`, `EntityList`,
/// `into_inner()`, and `Ok(())` boilerplate.
///
/// # Example
///
/// ```ignore
/// let spy = SpyReactor::<UsdcRebalance>::new();
/// let store = TestStore::with_reactor(spy.clone());
///
/// store.send(&id, SomeCommand).await.unwrap();
///
/// let events = spy.events().await;
/// assert_eq!(events.len(), 1);
/// ```
type EventLog<Entity> = Arc<
    Mutex<
        Vec<(
            <Entity as EventSourced>::Id,
            <Entity as EventSourced>::Event,
        )>,
    >,
>;

pub struct SpyReactor<Entity: EventSourced> {
    events: EventLog<Entity>,
}

impl<Entity: EventSourced> SpyReactor<Entity> {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return a snapshot of all captured `(Id, Event)` pairs.
    pub async fn events(&self) -> Vec<(Entity::Id, Entity::Event)>
    where
        Entity::Id: Clone,
        Entity::Event: Clone,
    {
        self.events.lock().await.clone()
    }
}

impl<Entity: EventSourced> Default for SpyReactor<Entity> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Entity: EventSourced> Clone for SpyReactor<Entity> {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
        }
    }
}

impl<Entity: EventSourced + 'static> crate::Dependent for SpyReactor<Entity> {
    type Dependencies = crate::deps![Entity];
}

#[async_trait]
impl<Entity: EventSourced + 'static> Reactor for SpyReactor<Entity>
where
    Entity::Id: Clone,
    Entity::Event: Clone,
{
    type Error = crate::lifecycle::Never;

    async fn react(
        &self,
        event: <Self::Dependencies as crate::EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (id, event) = event.into_inner();
        self.events.lock().await.push((id, event));
        Ok(())
    }
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
    pub fn new(services: Entity::Services) -> Self
    where
        Entity: 'static,
        <Entity::Id as FromStr>::Err: Debug,
    {
        Self::build(vec![], services)
    }

    /// Create an in-memory TestStore with a reactor that receives
    /// dispatched events.
    ///
    /// When `Entity::Services` is `()`, use the single-argument
    /// overload instead: `TestStore::with_reactor(spy)`.
    pub fn with_reactor_and<R>(reactor: Arc<R>, services: Entity::Services) -> Self
    where
        Entity: 'static,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug + Send + Sync,
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
    {
        let query: Box<dyn Query<Lifecycle<Entity>>> = Box::new(ReactorBridge { reactor });
        Self::build(vec![query], services)
    }

    fn build(queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>, services: Entity::Services) -> Self
    where
        Entity: 'static,
        <Entity::Id as FromStr>::Err: Debug,
    {
        let mem_store = mem_store::MemStore::default();
        #[allow(clippy::disallowed_methods)]
        let cqrs = CqrsFramework::new(mem_store.clone(), queries, services);
        Self { mem_store, cqrs }
    }
}

impl<Entity: EventSourced> TestStore<Entity>
where
    Entity::Services: Default,
{
    /// Create an in-memory TestStore with a reactor, using
    /// `Default` services (typically `()`).
    pub fn with_reactor<R>(reactor: Arc<R>) -> Self
    where
        Entity: 'static,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug + Send + Sync,
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
    {
        Self::with_reactor_and(reactor, Entity::Services::default())
    }
}

impl<Entity: EventSourced> TestStore<Entity> {
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
    use crate::Nil;

    // Required for ReactorHarness::receive to resolve HasEntity<Counter>.
    crate::register_entities!(Counter);

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
        type Materialized = Nil;
        const PROJECTION: Nil = Nil;
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

    #[tokio::test]
    async fn test_store_send_and_load() {
        let store = TestStore::<Counter>::new(());
        let id = "counter-1".to_string();

        store
            .send(&id, CounterCommand::Create { initial: 5 })
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert_eq!(entity.value, 5);
    }

    #[tokio::test]
    async fn test_store_load_nonexistent_returns_none() {
        let store = TestStore::<Counter>::new(());

        let result = store.load(&"nonexistent".to_string()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_store_multiple_commands() {
        let store = TestStore::<Counter>::new(());
        let id = "counter-1".to_string();

        store
            .send(&id, CounterCommand::Create { initial: 0 })
            .await
            .unwrap();
        store.send(&id, CounterCommand::Increment).await.unwrap();
        store.send(&id, CounterCommand::Increment).await.unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert_eq!(entity.value, 2);
    }

    #[tokio::test]
    async fn spy_reactor_captures_events() {
        let spy = SpyReactor::<Counter>::new();
        let store = TestStore::<Counter>::with_reactor(Arc::new(spy.clone()));
        let id = "counter-1".to_string();

        store
            .send(&id, CounterCommand::Create { initial: 42 })
            .await
            .unwrap();

        let captured = spy.events().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, "counter-1");
        assert_eq!(captured[0].1, CounterEvent::Created { initial: 42 });
    }

    #[tokio::test]
    async fn spy_reactor_captures_multiple_events() {
        let spy = SpyReactor::<Counter>::new();
        let store = TestStore::<Counter>::with_reactor(Arc::new(spy.clone()));
        let id = "counter-1".to_string();

        store
            .send(&id, CounterCommand::Create { initial: 0 })
            .await
            .unwrap();
        store.send(&id, CounterCommand::Increment).await.unwrap();

        let captured = spy.events().await;
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[1].1, CounterEvent::Incremented);
    }

    #[tokio::test]
    async fn reactor_harness_dispatches_to_spy() {
        let spy = SpyReactor::<Counter>::new();
        let harness = ReactorHarness::new(spy.clone());

        harness
            .receive::<Counter>("test-id".to_string(), CounterEvent::Created { initial: 7 })
            .await
            .unwrap();

        let captured = spy.events().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, "test-id");
        assert_eq!(captured[0].1, CounterEvent::Created { initial: 7 });
    }
}
