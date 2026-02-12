//! Lifecycle adapter for event-sourced entities.
//!
//! Wraps domain entities in a state machine that tracks whether
//! they are uninitialized, live, or failed. Provides a blanket
//! `Aggregate` impl that delegates to [`EventSourced`] methods,
//! eliminating per-entity boilerplate.
//!
//! See [`event_sourced`](crate::event_sourced) for the full
//! design rationale.

use async_trait::async_trait;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, Query, View};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tracing::error;

use crate::event_sourced::EventSourced;

/// Adapter that bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// Wraps a domain entity and tracks whether it has been
/// initialized, is live, or has entered an error state. The
/// blanket `Aggregate` impl delegates to `EventSourced` methods
/// and translates between the two interfaces.
///
/// Application code should not construct or match on `Lifecycle`
/// directly in most cases. Interact through
/// [`Store::send`](crate::event_sourced::Store::send) for
/// commands and through views for queries.
///
/// # State machine
///
/// ```text
/// Uninitialized --originate(event)--> Live(state)
/// Uninitialized --originate(None)---> Failed { Mismatch }
///
/// Live(state) --evolve(Ok(Some))--> Live(new_state)
/// Live(state) --evolve(Ok(None))--> Failed { Mismatch }
/// Live(state) --evolve(Err(e))----> Failed { Apply(e) }
///
/// Failed { .. } ---- any event ----> Failed { .. } (unchanged)
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound = "")]
pub(crate) enum Lifecycle<Entity: EventSourced> {
    Uninitialized,
    Live(Entity),
    Failed {
        error: LifecycleError<Entity>,
        last_valid_state: Option<Box<Entity>>,
    },
}

impl<Entity: EventSourced> Lifecycle<Entity> {
    pub(crate) fn live(&self) -> Result<&Entity, LifecycleError<Entity>>
    where
        Entity::Error: Clone,
    {
        match self {
            Self::Live(inner) => Ok(inner),
            Self::Uninitialized => Err(LifecycleError::Uninitialized),
            Self::Failed { error, .. } => Err(error.clone()),
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[serde(bound = "")]
pub(crate) enum LifecycleError<Entity: EventSourced> {
    #[error("operation on uninitialized state")]
    Uninitialized,

    #[error("initialization on already-live state")]
    AlreadyInitialized,

    #[error("event '{event:?}' not applicable to state '{state:?}'")]
    Mismatch {
        state: Box<Lifecycle<Entity>>,
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
pub(crate) enum Never {}

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

            Self::Live(state) => state
                .transition(command, services)
                .await
                .map_err(LifecycleError::Apply),

            Self::Failed { error, .. } => Err(error.clone()),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = match std::mem::take(self) {
            Self::Uninitialized => match Entity::originate(&event) {
                Some(state) => Self::Live(state),
                None => {
                    let err = LifecycleError::Mismatch {
                        state: Box::new(Self::Uninitialized),
                        event,
                    };
                    error!("lifecycle failed during originate: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_state: None,
                    }
                }
            },

            Self::Live(state) => match Entity::evolve(&event, &state) {
                Ok(Some(new_state)) => Self::Live(new_state),
                Ok(None) => {
                    let err = LifecycleError::Mismatch {
                        state: Box::new(Self::Live(state.clone())),
                        event,
                    };
                    error!("lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_state: Some(Box::new(state)),
                    }
                }
                Err(domain_err) => {
                    let err = LifecycleError::Apply(domain_err);
                    error!("lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_state: Some(Box::new(state)),
                    }
                }
            },

            failed @ Self::Failed { .. } => failed,
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
