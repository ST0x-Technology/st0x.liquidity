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
pub(crate) use trigger::{
    RebalancingConfig, RebalancingCtx, RebalancingCtxError, RebalancingSecrets, RebalancingTrigger,
    RebalancingTriggerConfig, TriggeredOperation,
};
