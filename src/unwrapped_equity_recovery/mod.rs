//! Recovery for unwrapped equity tokens (tSTOCK) detected on the Base
//! wallet. See SPEC.md, "Unwrapped Equity Recovery" section.
//!
//! - [`aggregate`]: event-sourced aggregate that records each recovery
//!   action's lifecycle for audit. The apalis job that drives it (enqueued
//!   by the rebalancing reactor on `BaseWalletUnwrappedEquity` snapshots)
//!   lands upstack.

pub(crate) mod aggregate;
