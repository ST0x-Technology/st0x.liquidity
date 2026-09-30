//! Recovery for wrapped equity tokens detected on a chain's bot wallet
//! outside the Raindex vault. See SPEC.md, "WrappedEquityRecovery
//! Aggregate" section.
//!
//! - [`aggregate`]: event-sourced aggregate that records each recovery
//!   action's lifecycle for audit.
//! - [`job`]: apalis job consumed by the recovery worker; enqueued by
//!   the rebalancing reactor whenever a `BaseWalletWrappedEquity`
//!   snapshot event reports a positive balance.

pub(crate) mod aggregate;
mod job;

pub(crate) use aggregate::{
    WrappedEquityRecovery, WrappedEquityRecoveryId, WrappedEquityRecoveryServices,
    open_recovery_ids,
};
pub(crate) use job::{
    WrappedEquityRecoveryCtx, WrappedEquityRecoveryJob, WrappedEquityRecoveryJobQueue,
};
