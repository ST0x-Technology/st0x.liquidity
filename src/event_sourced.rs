//! A safer, more ergonomic interface for event-sourced entities
//! on top of cqrs-es.
//!
//! # Why this exists
//!
//! cqrs-es provides the `Aggregate` trait, but it has several
//! sharp edges that have caused production bugs:
//!
//! - **Infallible `apply`**: `Aggregate::apply(&mut self, event)`
//!   returns nothing. Financial applications cannot panic on
//!   arithmetic overflow, so every aggregate needs a wrapper to
//!   capture errors without panicking. Every aggregate in the
//!   codebase had identical boilerplate for this.
//!
//! - **Stringly-typed aggregate IDs**: `cqrs.execute("some-id",
//!   cmd)` takes `&str`, making it trivial to pass the wrong ID.
//!   This has caused production bugs.
//!
//! - **No schema versioning**: When aggregate or view schemas
//!   change, stale snapshots and views cause silent data
//!   corruption. Manual database intervention is required.
//!
//! - **Flat command handling**: A single `handle` method receives
//!   all commands regardless of lifecycle state. Implementors
//!   must manually match on (lifecycle_state, command) tuples,
//!   making it easy to accidentally reference state during
//!   initialization or forget to handle a case.
//!
//! # Design
//!
//! [`EventSourced`] replaces direct `Aggregate` usage. Domain
//! types implement `EventSourced`, and [`Lifecycle`] provides a
//! blanket `Aggregate` impl that bridges to cqrs-es. Consumers
//! interact through [`Store`], which enforces typed IDs and hides
//! cqrs-es internals.
//!
//! ```text
//! Domain type          Adapter             cqrs-es
//! +--------------+     +----------------+  +------------+
//! | impl         | --> | Lifecycle      |  | Aggregate  |
//! | EventSourced |     | (blanket impl) |--| trait      |
//! +--------------+     +----------------+  +------------+
//!                             |
//!                      +------+------+
//!                      | Store       |
//!                      | (typed IDs, |
//!                      |  send())    |
//!                      +-------------+
//! ```
//!
//! # Naming
//!
//! Method names follow two themes to distinguish their purpose:
//!
//! **Event-side** (replaying events to reconstruct state) uses
//! evolution-themed names:
//! - [`originate`](EventSourced::originate) -- create initial
//!   state from the first event
//! - [`evolve`](EventSourced::evolve) -- derive new state from
//!   subsequent events
//!
//! **Command-side** (processing commands to produce events) uses
//! state-machine names:
//! - [`initialize`](EventSourced::initialize) -- handle a
//!   command when no state exists yet
//! - [`transition`](EventSourced::transition) -- handle a
//!   command against existing state
//!
//! The asymmetry is intentional: commands express intent,
//! events express facts. Different verbs for different
//! semantics.
//!
//! cqrs-es names (`Aggregate`, `Query`, `View`, `DomainEvent`)
//! are deliberately avoided in our public API to make it
//! immediately obvious whether code belongs to this crate or
//! to cqrs-es.

use async_trait::async_trait;
use cqrs_es::persist::GenericQuery;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, Query, View};
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteCqrs, SqliteViewRepository};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tracing::error;

/// The core abstraction for event-sourced domain entities.
///
/// Implement this trait on your domain type (e.g., `Position`,
/// `OffchainOrder`) to get a complete event-sourcing setup:
/// [`Lifecycle`] provides a blanket `Aggregate` impl, and
/// [`Store`] provides type-safe command dispatch.
///
/// # Associated types
///
/// - `Id`: The strongly-typed aggregate identifier. Prevents
///   mixing up IDs between different entity types at compile
///   time. Converted to string at the cqrs-es boundary only.
/// - `Event`: Domain events that drive state changes. Must be
///   `Eq` so lifecycle error states can carry typed events.
/// - `Command`: Instructions that produce events. A single
///   command type is used for both initialization and
///   transitions -- the lifecycle routes based on state.
/// - `Error`: Domain-specific errors from command handling or
///   event application (e.g., arithmetic overflow). For
///   entities with infallible operations, use [`Never`].
/// - `Services`: External dependencies injected into command
///   handlers (e.g., `Arc<dyn OrderPlacer>`). Use `()` when
///   no services are needed.
///
/// # Constants
///
/// - `AGGREGATE_TYPE`: Stable identifier for the event store.
///   Must not change after events are persisted.
/// - `SCHEMA_VERSION`: Bump when the entity's state, event, or
///   view schema changes. On startup, the wiring infrastructure
///   detects version mismatches and automatically clears stale
///   snapshots and replays views.
///
/// # Event-side methods
///
/// These reconstruct state from the event log during replay.
/// They are called by the blanket `Aggregate::apply` impl on
/// [`Lifecycle`], never by application code directly.
///
/// - `originate`: Attempt to create initial state from an
///   event. Returns `Some(state)` for genesis events, `None`
///   for events that require existing state.
/// - `evolve`: Attempt to derive new state from an event
///   applied to existing state. Returns `Ok(Some(new_state))`
///   on success, `Ok(None)` if the event doesn't apply to the
///   current state (mismatch), or `Err` for domain failures
///   like arithmetic overflow.
///
/// # Command-side methods
///
/// These process commands to produce events. They are called by
/// the blanket `Aggregate::handle` impl on [`Lifecycle`], which
/// routes commands based on lifecycle state.
///
/// - `initialize`: Handle a command when the entity doesn't
///   exist yet. Has no `&self` parameter, preventing accidental
///   reference to existing state during creation.
/// - `transition`: Handle a command against existing state.
///   Receives `&self` (the domain type, not `Lifecycle`), so
///   the handler only deals with live state.
pub(crate) trait EventSourced:
    Clone + Debug + Send + Sync + Sized + Serialize + DeserializeOwned
{
    type Id: Display;
    type Event: DomainEvent + Eq;
    type Command: Send + Sync;
    type Error: std::error::Error;
    type Services: Send + Sync;

    const AGGREGATE_TYPE: &'static str;
    const SCHEMA_VERSION: u64;

    /// Create initial state from a genesis event.
    ///
    /// Returns `Some(state)` if this event creates the entity,
    /// `None` if it requires existing state. Returning `None`
    /// causes [`Lifecycle`] to enter a `Failed` state with a
    /// [`LifecycleError::Mismatch`].
    fn originate(event: &Self::Event) -> Option<Self>;

    /// Derive new state from an event applied to existing state.
    ///
    /// - `Ok(Some(new_state))` -- event applied successfully
    /// - `Ok(None)` -- event doesn't apply to current state
    ///   (becomes [`LifecycleError::Mismatch`])
    /// - `Err(error)` -- domain error during application
    ///   (becomes [`LifecycleError::Apply`])
    fn evolve(
        event: &Self::Event,
        state: &Self,
    ) -> Result<Option<Self>, Self::Error>;

    /// Handle a command when the entity doesn't exist yet.
    ///
    /// No `&self` -- impossible to accidentally reference
    /// existing state during creation.
    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    /// Handle a command against existing state.
    ///
    /// `&self` is the domain type directly, not `Lifecycle`.
    /// The handler only deals with live state; lifecycle routing
    /// is handled by the blanket `Aggregate` impl.
    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;
}

/// Adapter that bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// Wraps a domain entity and tracks whether it has been
/// initialized, is live, or has entered an error state. The
/// blanket `Aggregate` impl delegates to `EventSourced` methods
/// and translates between the two interfaces.
///
/// Application code should not construct or match on `Lifecycle`
/// directly in most cases. Interact through [`Store::send`] for
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
pub(crate) enum Lifecycle<Entity: EventSourced> {
    /// No events have been applied yet.
    Uninitialized,

    /// Normal operational state containing valid entity data.
    Live(Entity),

    /// Error state entered when event application fails.
    ///
    /// The entity becomes inert -- further events are ignored.
    /// `last_valid_state` preserves the state before failure
    /// for debugging and potential recovery.
    Failed {
        error: LifecycleError<Entity>,
        last_valid_state: Option<Box<Entity>>,
    },
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
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error,
)]
pub(crate) enum LifecycleError<Entity: EventSourced> {
    /// A transition event or command was applied to an entity
    /// that hasn't been initialized yet.
    #[error("operation on uninitialized state")]
    Uninitialized,

    /// An initialization event or command was applied to an
    /// entity that already exists.
    #[error("initialization on already-live state")]
    AlreadyInitialized,

    /// An event doesn't match the current state. Carries the
    /// actual state and event for debugging.
    #[error(
        "event '{event:?}' not applicable to state '{state:?}'"
    )]
    Mismatch {
        state: Box<Lifecycle<Entity>>,
        event: Entity::Event,
    },

    /// A domain-specific error during event application (e.g.,
    /// arithmetic overflow in Position). Wraps
    /// `Entity::Error`.
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
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    thiserror::Error,
)]
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
    Entity::Event:
        Clone + Debug + Serialize + DeserializeOwned + Send + Sync + PartialEq,
    Entity::Error:
        Clone + Serialize + DeserializeOwned + Send + Sync + PartialEq,
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
///
/// This enables using
/// `SqliteViewRepository<Lifecycle<E>, Lifecycle<E>>` to
/// maintain a view that mirrors the aggregate's current state,
/// useful for queries that need the full entity.
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
///
/// Without this, each `SqliteCqrs` framework would need its
/// own query instance. With it, a single `Arc<RebalancingTrigger>`
/// can be wired to position, mint, redemption, and USDC
/// frameworks simultaneously.
#[async_trait]
impl<QueryImpl, Entity> Query<Lifecycle<Entity>> for Arc<QueryImpl>
where
    QueryImpl: Query<Lifecycle<Entity>> + Send + Sync,
    Entity: EventSourced,
    Lifecycle<Entity>: Aggregate,
{
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<Entity>>],
    ) {
        QueryImpl::dispatch(self, aggregate_id, events).await;
    }
}

/// Convenience alias for a `GenericQuery` backed by SQLite that
/// materializes a `Lifecycle<Entity>` as its own view.
///
/// Used as the query type for entity views:
/// `type PositionQuery = SqliteQuery<Position>;`
pub(crate) type SqliteQuery<Entity> = GenericQuery<
    SqliteViewRepository<Lifecycle<Entity>, Lifecycle<Entity>>,
    Lifecycle<Entity>,
    Lifecycle<Entity>,
>;

/// Type-safe command dispatch for an event-sourced entity.
///
/// Wraps `SqliteCqrs<Lifecycle<Entity>>` and enforces that
/// commands are addressed to the correct entity type via
/// strongly-typed IDs. This prevents a class of bugs where
/// string aggregate IDs are mixed up between different entity
/// types.
///
/// # Usage
///
/// ```ignore
/// let position_store: Store<Position> = /* built by CqrsBuilder */;
///
/// // Typed ID -- can't accidentally pass an OffchainOrderId
/// let symbol = Symbol::new("AAPL").unwrap();
/// position_store.send(&symbol, PositionCommand::AcknowledgeFill { .. }).await?;
/// ```
///
/// Produced by [`CqrsBuilder::build()`] during conductor
/// startup. The builder handles CQRS framework construction,
/// query wiring, and schema reconciliation, returning a
/// ready-to-use `Store`.
pub(crate) struct Store<Entity: EventSourced> {
    inner: SqliteCqrs<Lifecycle<Entity>>,
}

impl<Entity: EventSourced> Store<Entity>
where
    Lifecycle<Entity>: Aggregate,
{
    /// Wrap an existing `SqliteCqrs` framework.
    ///
    /// Prefer using `CqrsBuilder::build()` which handles wiring
    /// and reconciliation. This constructor exists for cases
    /// where direct construction is needed (e.g., tests).
    pub(crate) fn new(inner: SqliteCqrs<Lifecycle<Entity>>) -> Self {
        Self { inner }
    }

    /// Send a command to the entity identified by `id`.
    ///
    /// The command is routed based on the entity's lifecycle
    /// state:
    /// - Uninitialized -> `Entity::initialize`
    /// - Live -> `Entity::transition`
    /// - Failed -> returns the stored error
    pub(crate) async fn send(
        &self,
        id: &Entity::Id,
        command: <Lifecycle<Entity> as Aggregate>::Command,
    ) -> Result<(), cqrs_es::AggregateError<LifecycleError<Entity>>>
    {
        self.inner.execute(&id.to_string(), command).await
    }
}
