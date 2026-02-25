//! Lifecycle adapter for event-sourced entities.
//!
//! Wraps domain entities in a state machine that tracks whether
//! they are uninitialized, live, or failed. Provides a blanket
//! `Aggregate` impl that delegates to [`EventSourced`] methods,
//! eliminating per-entity boilerplate.
//!
//! See the [crate root](crate) for the full design rationale.

use async_trait::async_trait;
use cqrs_es::{Aggregate, EventEnvelope, Query, View};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, warn};

use crate::EventSourced;
use crate::dependency::HasEntity;
use crate::reactor::Reactor;

/// Adapter that bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// Wraps a domain entity and tracks whether it has been
/// initialized, is live, or has entered an error state. The
/// blanket `Aggregate` impl delegates to `EventSourced` methods
/// and translates between the two interfaces.
///
/// Application code should not construct or match on `Lifecycle`
/// directly in most cases. Interact through
/// [`Store::send`](crate::Store::send) for commands and through
/// views for queries.
///
/// # State machine
///
/// ```text
/// Uninitialized --originate(entity)--> Live(entity)
/// Uninitialized --originate(None)----> Failed { EventCantOriginate }
///
/// Live(entity) --evolve(Ok(Some))----> Live(new_entity)
/// Live(entity) --evolve(Ok(None))----> Failed { UnexpectedEvent }
/// Live(entity) --evolve(Err(e))------> Failed { Apply(e) }
///
/// Failed { .. } ---- any event ------> Failed { AlreadyFailed }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
// Override serde's inferred bounds. Without this, serde derives
// `Entity: Serialize + Deserialize` bounds, but Entity's serde
// impls are already guaranteed by the EventSourced supertrait.
// The empty bound avoids redundant constraints that confuse the
// compiler when Entity has complex associated types.
#[serde(bound = "")]
pub(crate) enum Lifecycle<Entity: EventSourced> {
    Uninitialized,
    Live(Entity),
    Failed {
        error: LifecycleError<Entity>,
        last_valid_entity: Option<Box<Entity>>,
    },
}

impl<Entity: EventSourced> Lifecycle<Entity> {
    pub(crate) fn into_result(self) -> Result<Option<Entity>, LifecycleError<Entity>> {
        match self {
            Self::Live(entity) => Ok(Some(entity)),
            Self::Uninitialized => Ok(None),
            Self::Failed { error, .. } => Err(error),
        }
    }
}

impl<Entity: EventSourced> Default for Lifecycle<Entity> {
    fn default() -> Self {
        Self::Uninitialized
    }
}

/// Errors from lifecycle state management.
///
/// These are infrastructure-level errors produced by
/// [`Lifecycle`]'s blanket `Aggregate` impl, not by domain
/// code directly. Domain errors are wrapped in the [`Apply`]
/// variant.
///
/// The error carries typed state and event information rather
/// than opaque debug strings, enabling meaningful error
/// handling and debugging.
///
/// [`Apply`]: LifecycleError::Apply
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
// Same override as Lifecycle above -- serde would infer
// `Entity::Error: Serialize + Deserialize` bounds that are
// already guaranteed by EventSourced's DomainError supertrait.
#[serde(bound = "")]
pub enum LifecycleError<Entity: EventSourced> {
    #[error("event '{event:?}' cannot originate entity")]
    EventCantOriginate { event: Entity::Event },
    #[error("event '{event:?}' not applicable to entity '{entity:?}'")]
    UnexpectedEvent {
        entity: Box<Entity>,
        event: Entity::Event,
    },
    #[error("event '{event:?}' applied to already-failed lifecycle")]
    AlreadyFailed {
        failure: Box<LifecycleError<Entity>>,
        event: Entity::Event,
    },
    #[error(transparent)]
    Apply(Entity::Error),
}

/// Uninhabited error type for entities with infallible
/// operations.
///
/// Similar to `std::convert::Infallible` but derives
/// `Serialize`/`Deserialize` for cqrs-es compatibility.
/// Use as `type Error = Never` on entities where neither
/// command handling nor event application can fail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("never")]
pub enum Never {}

/// Bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// This blanket impl eliminates per-entity boilerplate. All
/// command routing (uninitialized -> `initialize`, live ->
/// `transition`) and event application (uninitialized ->
/// `originate`, live -> `evolve`) is handled here.
///
/// The `apply` method uses `std::mem::take` to move out of
/// `&mut self`, avoiding unnecessary clones when transitioning
/// between lifecycle states.
#[async_trait]
impl<Entity> Aggregate for Lifecycle<Entity>
where
    Entity: EventSourced,
    Entity::Event: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + PartialEq,
{
    type Command = Entity::Command;
    type Event = Entity::Event;
    type Error = LifecycleError<Entity>;
    type Services = Entity::Services;

    fn aggregate_type() -> String {
        Entity::AGGREGATE_TYPE.to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match self {
            Self::Uninitialized => Entity::initialize(command, services)
                .await
                .map_err(LifecycleError::Apply),

            Self::Live(entity) => entity
                .transition(command, services)
                .await
                .map_err(LifecycleError::Apply),

            Self::Failed { error, .. } => Err(error.clone()),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = match std::mem::take(self) {
            Self::Uninitialized => Entity::originate(&event).map_or_else(
                || {
                    let err = LifecycleError::EventCantOriginate { event };
                    error!("lifecycle failed during originate: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: None,
                    }
                },
                Self::Live,
            ),

            Self::Live(entity) => match Entity::evolve(&entity, &event) {
                Ok(Some(new_entity)) => Self::Live(new_entity),
                Ok(None) => {
                    let err = LifecycleError::UnexpectedEvent {
                        entity: Box::new(entity.clone()),
                        event,
                    };
                    error!("lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: Some(Box::new(entity)),
                    }
                }
                Err(domain_err) => {
                    let err = LifecycleError::Apply(domain_err);
                    error!("lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: Some(Box::new(entity)),
                    }
                }
            },

            Self::Failed {
                error,
                last_valid_entity,
            } => {
                let err = LifecycleError::AlreadyFailed {
                    failure: Box::new(error),
                    event,
                };
                error!("lifecycle already failed, ignoring event: {err}");
                Self::Failed {
                    error: err,
                    last_valid_entity,
                }
            }
        };
    }
}

/// Allows any `Lifecycle<Entity>` to serve as its own
/// materialized view by replaying events through `apply`.
impl<Entity> View<Self> for Lifecycle<Entity>
where
    Self: Aggregate,
    Entity: EventSourced,
{
    fn update(&mut self, event: &EventEnvelope<Self>) {
        self.apply(event.payload.clone());
    }
}

/// Enables sharing a single query processor across multiple
/// CQRS frameworks via `Arc`.
#[async_trait]
impl<QueryImpl, Entity> Query<Lifecycle<Entity>> for Arc<QueryImpl>
where
    QueryImpl: Query<Lifecycle<Entity>> + Send + Sync,
    Entity: EventSourced,
    Lifecycle<Entity>: Aggregate,
{
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<Lifecycle<Entity>>]) {
        QueryImpl::dispatch(self, aggregate_id, events).await;
    }
}

/// Bridges a [`Reactor`](crate::Reactor) to
/// `cqrs_es::Query<Lifecycle<Entity>>`.
///
/// Parses the stringly-typed aggregate ID into `Entity::Id`,
/// injects the entity's (Id, Event) pair into the reactor's
/// computed event type via [`HasEntity`], and dispatches it.
pub(crate) struct ReactorBridge<R> {
    pub(crate) reactor: Arc<R>,
}

#[async_trait]
impl<R, Entity> Query<Lifecycle<Entity>> for ReactorBridge<R>
where
    R: Reactor,
    R::Dependencies: HasEntity<Entity>,
    Entity: EventSourced,
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
    Lifecycle<Entity>: Aggregate<Event = Entity::Event>,
{
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<Lifecycle<Entity>>]) {
        let Ok(typed_id) = aggregate_id.parse::<Entity::Id>() else {
            warn!(
                aggregate_id = aggregate_id,
                aggregate_type = Entity::AGGREGATE_TYPE,
                "Failed to parse aggregate ID in reactor bridge"
            );
            return;
        };

        for envelope in events {
            let injected = <R::Dependencies as HasEntity<Entity>>::inject(
                typed_id.clone(),
                envelope.payload.clone(),
            );

            if let Err(error) = self.reactor.react(injected).await {
                error!(
                    ?error,
                    aggregate_id = aggregate_id,
                    aggregate_type = Entity::AGGREGATE_TYPE,
                    "Reactor failed to handle event"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    use super::*;
    use crate::{EventSourced, Nil};

    /// Test entity: a simple counter with controllable error behavior.
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
        /// evolve returns Ok(None) for this, triggering Mismatch.
        Invalid,
        /// evolve returns Err for this, triggering Apply.
        Broken,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            format!("{self:?}")
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("domain error")]
    struct CounterError;

    enum CounterCommand {
        Create { initial: u32 },
        Increment,
        Fail,
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Event = CounterEvent;
        type Command = CounterCommand;
        type Error = CounterError;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Counter";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &CounterEvent) -> Option<Self> {
            match event {
                CounterEvent::Created { initial } => Some(Self { value: *initial }),
                _ => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, CounterError> {
            use CounterEvent::*;
            match event {
                Broken => Err(CounterError),
                Created { .. } | Invalid => Ok(None),
                Incremented => Ok(Some(Self {
                    value: entity.value + 1,
                })),
            }
        }

        async fn initialize(
            command: CounterCommand,
            _services: &(),
        ) -> Result<Vec<CounterEvent>, CounterError> {
            use CounterCommand::*;
            match command {
                Create { initial } => Ok(vec![CounterEvent::Created { initial }]),
                Increment => Ok(vec![CounterEvent::Incremented]),
                Fail => Err(CounterError),
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
                CounterCommand::Fail => Err(CounterError),
            }
        }
    }

    #[test]
    fn originate_success_transitions_to_live() {
        let mut lifecycle = Lifecycle::<Counter>::default();

        lifecycle.apply(CounterEvent::Created { initial: 10 });

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 10 })));
    }

    #[test]
    fn originate_none_transitions_to_failed_mismatch() {
        let mut lifecycle = Lifecycle::<Counter>::default();

        lifecycle.apply(CounterEvent::Incremented);

        assert!(matches!(
            lifecycle,
            Lifecycle::Failed {
                error: LifecycleError::EventCantOriginate { .. },
                last_valid_entity: None,
            }
        ));
    }

    #[test]
    fn evolve_success_stays_live_with_new_state() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Incremented);

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 6 })));
    }

    #[test]
    fn evolve_mismatch_transitions_to_failed_with_last_valid_entity() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Invalid);

        match lifecycle {
            Lifecycle::Failed {
                error: LifecycleError::UnexpectedEvent { .. },
                last_valid_entity: Some(last),
            } => assert_eq!(*last, Counter { value: 5 }),
            other => panic!("expected Failed with UnexpectedEvent, got {other:?}"),
        }
    }

    #[test]
    fn evolve_domain_error_transitions_to_failed_apply() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Broken);

        match lifecycle {
            Lifecycle::Failed {
                error: LifecycleError::Apply(CounterError),
                last_valid_entity: Some(last),
            } => assert_eq!(*last, Counter { value: 5 }),
            other => panic!("expected Failed with Apply, got {other:?}"),
        }
    }

    #[test]
    fn failed_state_is_sticky() {
        let prior_error = LifecycleError::EventCantOriginate {
            event: CounterEvent::Incremented,
        };

        let mut lifecycle = Lifecycle::<Counter>::Failed {
            error: prior_error,
            last_valid_entity: None,
        };

        lifecycle.apply(CounterEvent::Created { initial: 99 });

        assert!(matches!(
            lifecycle,
            Lifecycle::Failed {
                error: LifecycleError::AlreadyFailed { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn handle_uninitialized_delegates_to_initialize() {
        let lifecycle = Lifecycle::<Counter>::default();

        let events = lifecycle
            .handle(CounterCommand::Create { initial: 42 }, &())
            .await
            .unwrap();

        assert_eq!(events, vec![CounterEvent::Created { initial: 42 }]);
    }

    #[tokio::test]
    async fn handle_live_delegates_to_transition() {
        let lifecycle = Lifecycle::Live(Counter { value: 0 });

        let events = lifecycle
            .handle(CounterCommand::Increment, &())
            .await
            .unwrap();

        assert_eq!(events, vec![CounterEvent::Incremented]);
    }

    #[tokio::test]
    async fn handle_maps_domain_error_to_lifecycle_apply() {
        let lifecycle = Lifecycle::Live(Counter { value: 0 });

        let error = lifecycle
            .handle(CounterCommand::Fail, &())
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::Apply(CounterError)));
    }

    #[tokio::test]
    async fn handle_failed_returns_stored_error() {
        let stored_error = LifecycleError::EventCantOriginate {
            event: CounterEvent::Incremented,
        };
        let lifecycle = Lifecycle::<Counter>::Failed {
            error: stored_error.clone(),
            last_valid_entity: None,
        };

        let error = lifecycle
            .handle(CounterCommand::Increment, &())
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn view_update_applies_event_to_lifecycle() {
        let mut lifecycle = Lifecycle::<Counter>::default();
        let envelope = EventEnvelope {
            aggregate_id: "test".to_string(),
            sequence: 1,
            payload: CounterEvent::Created { initial: 7 },
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 7 })));
    }
}
