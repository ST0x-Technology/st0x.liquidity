//! Rebalancing orchestration.
//!
//! This module coordinates rebalancing operations between external services
//! (Alpaca tokenization, CCTP bridge, vault) and CQRS-ES aggregates.

pub(crate) mod equity;
mod rebalancer;
mod spawn;
pub(crate) mod transfer;
pub(crate) mod trigger;
pub(crate) mod usdc;

pub(crate) use rebalancer::Rebalancer;
pub(crate) use spawn::{RebalancerServices, RebalancingCqrsFrameworks};
pub(crate) use trigger::RebalancingConfig;
#[cfg(any(test, feature = "test-support"))]
pub use trigger::UsdcRebalancing;
#[cfg(test)]
pub(crate) use trigger::drain_pending_jobs;
pub(crate) use trigger::{
    EquityRebalancingCheck, EquityRebalancingCheckScheduler, RebalancingSchedulers,
    RebalancingService, RebalancingServiceConfig, TriggeredOperation, UsdcRebalancingCheck,
    UsdcRebalancingCheckScheduler,
};
pub use trigger::{RebalancingCtx, RebalancingCtxError};
