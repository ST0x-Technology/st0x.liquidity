//! Configuration loading and runtime context assembly for the st0x bot.
//!
//! Restricted-visibility crate: only `st0x-server` (the bot binary) and
//! `st0x-cli` (the operator binary) may depend on it. Integration,
//! shared-metadata, and domain crates must remain config-agnostic.

mod alerts;
mod evm;
mod imbalance_threshold;
mod loader;
mod order_poller;
mod rebalancing;
mod telemetry;
mod threshold;
mod wallet;

pub use alerts::{AlertsAssemblyError, AlertsConfig, AlertsCtx, AlertsSecrets};
pub use evm::{EvmConfig, EvmCtx, EvmSecrets, IngestionCutoff};
pub use imbalance_threshold::{ImbalanceThreshold, InvalidImbalanceThreshold};
pub use loader::*;
pub use order_poller::OrderPollerCtx;
pub use rebalancing::{
    ALPACA_MINIMUM_WITHDRAWAL, RebalancingConfig, RebalancingCtx, RebalancingCtxError,
    UsdcRebalancing,
};
pub use telemetry::{
    ExtraLayer, FileLogGuard, TelemetryConfig, TelemetryCtx, TelemetryError, TelemetryGuard,
    mk_env_filter, setup_tracing,
};
pub use threshold::{ExecutionThreshold, InvalidThresholdError};
pub use wallet::{OnchainWalletCtx, WalletCtxError, build_wallet};
