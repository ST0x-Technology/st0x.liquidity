//! Event reactor trait for multi-entity event handling.
//!
//! [`Reactor`] defines a multi-entity event handler whose event
//! type is computed from its dependency list. Reactors handle
//! events via the [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain, which
//! guarantees at compile time that every entity is handled.
//!
//! Dependency lists are declared via [`deps!`] in the
//! [`dependency`](crate::dependency) module.

use async_trait::async_trait;
use std::sync::Arc;

use crate::dependency::{Dependent, EntityList};

/// Event reactor with exhaustive compile-time checked handling.
///
/// The event type is computed from [`Dependent::Dependencies`]
/// -- no manual enum definition or `From` impls needed. Use the
/// [`.on()`](crate::OneOf::on) /
/// [`.exhaustive()`](crate::Fold::exhaustive) chain in the
/// `react` implementation to handle each entity.
///
/// Each `.on()` handler returns a future, which is boxed
/// internally for type erasure. Call `.exhaustive().await` to
/// run the matched handler.
///
/// ```ignore
/// deps!(RebalancingTrigger, [Position, TokenizedEquityMint]);
///
/// #[async_trait]
/// impl Reactor for RebalancingTrigger {
///     type Error = TriggerError;
///
///     async fn react(
///         &self,
///         event: <Self::Dependencies as EntityList>::Event,
///     ) -> Result<(), Self::Error> {
///         event
///             .on(|symbol, event| async move {
///                 self.on_position(symbol, event).await
///             })
///             .on(|id, event| async move {
///                 self.on_mint(id, event).await
///             })
///             .exhaustive()
///             .await;
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Reactor: Dependent {
    /// Error type for reactor failures.
    type Error: std::error::Error + Send + Sync;

    /// Handle a single event from any supported entity.
    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error>;
}

/// Enables sharing a reactor via `Arc`.
#[async_trait]
impl<R: Reactor> Reactor for Arc<R> {
    type Error = R::Error;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        R::react(self, event).await
    }
}
