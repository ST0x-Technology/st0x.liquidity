//! Event reactor traits for responding to entity events.
//!
//! Two traits serve complementary roles:
//!
//! - [`ReactsTo<Entity>`] — per-entity event handler. This is what
//!   the wiring infrastructure ([`StoreBuilder`](crate::StoreBuilder),
//!   [`ReactorBridge`](crate::lifecycle::ReactorBridge)) uses to
//!   dispatch events to reactors. Single-entity processors like
//!   [`Projection`](crate::Projection) implement this directly.
//!
//! - [`Reactor`] — multi-entity reactor with a discriminated union
//!   event type. Reactors that handle events from multiple entities
//!   implement this trait once with an exhaustive `match`, then
//!   implement [`ReactsTo<Entity>`] per entity to bridge into the
//!   union type. The `Entities` associated type is a type-level
//!   list (built with [`deps!`](crate::deps)) for compile-time
//!   dependency tracking via [`Unwired`](crate::Unwired).

use async_trait::async_trait;
use std::sync::Arc;

use crate::EventSourced;

/// Per-entity event handler.
///
/// The foundational trait for reacting to events from a single
/// entity type. Used by the wiring infrastructure to bridge
/// reactors to cqrs-es queries.
///
/// Single-entity processors (like [`Projection`](crate::Projection))
/// implement this directly. Multi-entity reactors implement it
/// per entity, delegating to their [`Reactor::react`] method.
#[async_trait]
pub trait ReactsTo<Entity: EventSourced>: Send + Sync {
    async fn react(&self, id: &Entity::Id, event: &Entity::Event);
}

/// Multi-entity reactor with exhaustive event handling.
///
/// Each reactor defines its own `Event` enum covering all entities
/// it reacts to, with an exhaustive `match` in [`react`](Self::react).
/// Adding a new variant forces handling it — the compiler won't
/// let you forget.
///
/// The `Entities` associated type is a type-level list (built with
/// [`deps!`](crate::deps)) that mirrors the entity variants in the
/// event enum. The wiring infrastructure uses it for compile-time
/// dependency tracking via [`Unwired`](crate::Unwired).
///
/// # Implementing
///
/// 1. Define an event enum with a variant per entity
/// 2. Implement `Reactor` with exhaustive match
/// 3. Implement [`ReactsTo<Entity>`] per entity, wrapping into
///    the enum and calling [`Reactor::react`]
///
/// ```ignore
/// pub(crate) enum TriggerEvent {
///     Position(Symbol, PositionEvent),
///     Mint(IssuerRequestId, TokenizedEquityMintEvent),
/// }
///
/// #[async_trait]
/// impl Reactor for RebalancingTrigger {
///     type Entities = deps![Position, TokenizedEquityMint];
///     type Event = TriggerEvent;
///
///     async fn react(&self, event: TriggerEvent) {
///         match event {
///             TriggerEvent::Position(symbol, event) => { .. }
///             TriggerEvent::Mint(id, event) => { .. }
///         }
///     }
/// }
///
/// #[async_trait]
/// impl ReactsTo<Position> for RebalancingTrigger {
///     async fn react(&self, id: &Symbol, event: &PositionEvent) {
///         Reactor::react(self, TriggerEvent::Position(
///             id.clone(), event.clone(),
///         )).await;
///     }
/// }
/// ```
#[async_trait]
pub trait Reactor: Send + Sync {
    /// Type-level list of entities this reactor handles.
    type Entities;

    /// Discriminated union of all (id, event) pairs.
    type Event: Send;

    /// Handle a single event from any of the supported entities.
    async fn react(&self, event: Self::Event);
}

/// Enables sharing a single reactor's per-entity handler
/// across multiple CQRS frameworks via `Arc`.
#[async_trait]
impl<R, Entity> ReactsTo<Entity> for Arc<R>
where
    R: ReactsTo<Entity>,
    Entity: EventSourced,
{
    async fn react(&self, id: &Entity::Id, event: &Entity::Event) {
        R::react(self, id, event).await;
    }
}
