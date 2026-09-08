//! Configuration loading and runtime context assembly for the st0x bot.
//!
//! Restricted-visibility crate: only `st0x-server` (the bot binary) and
//! `st0x-cli` (the operator binary) may depend on it. Integration,
//! shared-metadata, and domain crates must remain config-agnostic.

mod alerts;
mod assets;
mod bot_gas_valuation;
mod chain;
mod enablement;
mod imbalance_threshold;
mod loader;
mod orchestrator;
mod order_poller;
mod pricing;
mod rebalancing;
mod telemetry;
mod threshold;
mod wallet;

pub use alerts::{AlertsAssemblyError, AlertsConfig, AlertsCtx, GAS_MONITORED_CHAINS};
pub use assets::{
    CashHedgePolicy, ChainAssets, ChainCashAsset, ChainEquities, ChainEquityAsset,
    EquityHedgePolicy, HedgedEquities, HedgingAssets, OperationMode,
};
pub use bot_gas_valuation::BotGasValuationConfig;
pub use chain::{
    ChainConfig, ChainConfigError, ChainCtx, ChainRegistry, ChainRegistryError, ChainSecrets,
    IngestionCutoff, InventoryAdapter, InventoryAdapterVenue, InventoryAdapters, InventoryMode,
    InventoryModeTag, TradingChain, TradingConfig,
};
pub use enablement::{
    ChainCapability, ChainEnablementError, ChainLifecycle, MissingCapabilities,
    provided_capabilities, required_capabilities,
};
pub use imbalance_threshold::{ImbalanceThreshold, InvalidImbalanceThreshold};
pub use loader::*;
pub use orchestrator::{OrchestratorAddresses, OrchestratorConfig, OrchestratorError};
pub use order_poller::OrderPollerCtx;
pub use pricing::{PricingApiKey, PricingAuth, PricingConfig, PricingCtx, PricingCtxError};
pub use rebalancing::{
    ALPACA_MINIMUM_WITHDRAWAL, ALPACA_TO_BASE_MINIMUM_TRANSFER, RebalancingConfig, RebalancingCtx,
    RebalancingCtxError, UsdcRebalancing,
};
pub use telemetry::{
    ExtraLayer, FileLogGuard, FileLogging, TelemetryConfig, TelemetryCtx, TelemetryError,
    TelemetryGuard, mk_env_filter, setup_tracing,
};
pub use threshold::{ExecutionThreshold, InvalidThresholdError};
pub use wallet::{OnchainWalletCtx, SigningChains, WalletCtxError, build_wallet};
