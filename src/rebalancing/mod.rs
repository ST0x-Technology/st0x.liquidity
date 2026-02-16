//! Rebalancing orchestration.
//!
//! This module coordinates rebalancing operations between external services
//! (Alpaca tokenization, CCTP bridge, vault) and CQRS-ES aggregates.

pub(crate) mod mint;
mod rebalancer;
pub(crate) mod redemption;
mod spawn;
pub(crate) mod transfer;
pub(crate) mod trigger;
pub(crate) mod usdc;

pub(crate) use rebalancer::Rebalancer;
pub(crate) use spawn::{RebalancerAddresses, RebalancingCqrsFrameworks, spawn_rebalancer};
pub(crate) use trigger::{
    RebalancingConfig, RebalancingCtx, RebalancingCtxError, RebalancingSecrets, RebalancingTrigger,
    RebalancingTriggerConfig, TriggeredOperation,
};
