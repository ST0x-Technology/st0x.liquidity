//! Offchain trading venue: order placement, status tracking, and lifecycle
//! reconciliation against the brokerage.
//!
//! Submodules:
//! - [`order`] — the `OffchainOrder` aggregate and the per-job machinery that
//!   drives its lifecycle to terminal state.

pub(crate) mod order;
