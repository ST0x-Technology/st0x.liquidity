//! Order Collector: discovers and tracks Raindex orders.
//!
//! Subscribes to `AddOrderV3`, `RemoveOrderV3`, and `TakeOrderV3` events
//! on the orderbook contract. Maps each event to a `TrackedOrderCommand`
//! and dispatches it through the CQRS store.
//!
//! On startup, performs a historical backfill from the last processed
//! block (or `deployment_block` on first run) to catch up on events
//! emitted while the bot was offline.

mod block_cursor;
mod event_processor;

pub(crate) use block_cursor::BlockCursor;
pub(crate) use event_processor::EventProcessor;
