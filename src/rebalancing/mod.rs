//! Rebalancing orchestration managers.
//!
//! This module provides managers that coordinate rebalancing
//! operations between external services (Alpaca tokenization, CCTP
//! bridge, vault) and CQRS-ES aggregates.
//!
//! # Manager Pattern
//!
//! Managers are stateless coordinators that:
//! 1. Receive triggers (programmatic calls)
//! 2. Call external services (Alpaca, CCTP, vault)
//! 3. Send commands to aggregates based on service results
//! 4. Handle polling for async operations
//! 5. Implement retry logic for transient failures
//!
//! All persistent state lives in aggregates via the event store. On restart,
//! managers can resume by querying aggregate state.

pub(crate) mod mint;
mod rebalancer;
pub(crate) mod redemption;
mod spawn;
mod trigger;
pub(crate) mod usdc;

pub(crate) use mint::manager::MintManager;
pub(crate) use rebalancer::Rebalancer;
pub(crate) use redemption::manager::RedemptionManager;
pub(crate) use spawn::{
    RebalancingCqrsFrameworks, RedemptionDependencies, build_rebalancing_queries, spawn_rebalancer,
};
pub(crate) use trigger::{
    RebalancingConfig, RebalancingCtx, RebalancingCtxError, RebalancingSecrets, RebalancingTrigger,
    RebalancingTriggerConfig, TriggeredOperation,
};
