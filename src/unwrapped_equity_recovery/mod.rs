//! Recovery for unwrapped equity tokens (tSTOCK) detected on the Base
//! wallet. See SPEC.md, "Unwrapped Equity Recovery" section.
//!
//! - [`aggregate`]: event-sourced aggregate that records each recovery
//!   action's lifecycle for audit.
//! - [`job`]: apalis job consumed by the recovery worker; enqueued by the
//!   rebalancing reactor whenever a `BaseWalletUnwrappedEquity` snapshot
//!   event reports a positive balance.

pub(crate) mod aggregate;
mod job;
