//! Rebalancing orchestration.
//!
//! This module coordinates rebalancing operations between external services
//! (Alpaca tokenization, CCTP bridge, vault) and CQRS-ES aggregates.

pub(crate) mod equity;
mod spawn;
pub(crate) mod trigger;
pub(crate) mod usdc;

pub(crate) use spawn::{RebalancerServices, to_wrapped_equities};
#[cfg(test)]
pub(crate) use trigger::drain_pending_jobs;
pub(crate) use trigger::{
    EquityRebalancingCheck, EquityRebalancingCheckScheduler, RebalancingSchedulers,
    RebalancingService, RebalancingServiceConfig, UsdcRebalancingCheck,
    UsdcRebalancingCheckScheduler,
};
