//! Recovery for unwrapped equity tokens (tSTOCK) detected on a chain's
//! bot wallet. See SPEC.md, "Unwrapped Equity Recovery" section.
//!
//! - [`aggregate`]: event-sourced aggregate that records each recovery
//!   action's lifecycle for audit.
//! - [`job`]: apalis job consumed by the recovery worker; enqueued by the
//!   rebalancing reactor whenever a chain's unwrapped wallet snapshot event
//!   (`BaseWalletUnwrappedEquity` on Base, `ChainWalletUnwrappedEquity`
//!   elsewhere) reports a positive balance.

pub(crate) mod aggregate;
mod job;

pub(crate) use aggregate::{
    UnwrappedEquityRecovery, UnwrappedEquityRecoveryId, UnwrappedEquityRecoveryServices,
    open_recovery_ids,
};
pub(crate) use job::{
    UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJob, UnwrappedEquityRecoveryJobQueue,
};
