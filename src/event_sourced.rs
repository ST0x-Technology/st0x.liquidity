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
use cqrs_es::{Aggregate, AggregateError};

/// Re-exported from cqrs-es so domain modules import from here,
/// not from cqrs-es directly.
pub(crate) use cqrs_es::DomainEvent;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlite_es::{SqliteCqrs, SqliteViewRepository};
use std::fmt::{Debug, Display};

use crate::lifecycle::{Lifecycle, LifecycleError};

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
///
/// [`Never`]: crate::lifecycle::Never
#[async_trait]
pub(crate) trait EventSourced:
    Clone + Debug + Send + Sync + Sized + Serialize + DeserializeOwned
{
    type Id: Display;
    type Event: DomainEvent + Eq;
    type Command: Send + Sync;
    type Error: DomainError;
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
    fn evolve(event: &Self::Event, state: &Self) -> Result<Option<Self>, Self::Error>;

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

/// Convenience alias for a `GenericQuery` backed by SQLite that
/// materializes a `Lifecycle<Entity>` as its own view.
///
/// Used as the query type for entity views:
/// `SqliteQuery<Position>`
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
/// let positions: Store<Position> = /* built by CqrsBuilder */;
///
/// // Typed ID -- can't accidentally pass an OffchainOrderId
/// let symbol = Symbol::new("AAPL").unwrap();
/// positions.send(&symbol, PositionCommand::AcknowledgeFill { .. }).await?;
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
    Lifecycle<Entity>: Aggregate<
            Command = Entity::Command,
            Event = Entity::Event,
            Error = LifecycleError<Entity>,
            Services = Entity::Services,
        >,
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
        command: Entity::Command,
    ) -> Result<(), SendError<Entity>> {
        self.inner.execute(&id.to_string(), command).await
    }
}

/// Error returned by [`Store::send`].
///
/// Wraps the cqrs-es `AggregateError` containing a
/// `LifecycleError` so that consumers don't import from cqrs-es
/// or lifecycle directly.
pub(crate) type SendError<Entity> = AggregateError<LifecycleError<Entity>>;

/// Bounds required for domain error types used with
/// [`EventSourced`].
///
/// [`LifecycleError`] stores the entity's error in its `Apply`
/// variant and derives `Clone`, `Serialize`, `Deserialize`,
/// `PartialEq`, and `Eq`. This trait captures those bounds in
/// one place so implementors see a single meaningful name
/// instead of a long bound list.
pub(crate) trait DomainError:
    std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync + PartialEq + Eq
{
}

impl<T> DomainError for T where
    T: std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync + PartialEq + Eq
{
}
