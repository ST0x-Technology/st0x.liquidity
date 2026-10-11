//! The `liq_*` metric contract: the series external consumers (the capital
//! probe, the TVL report and the liquidity board) read from `/metrics`.
//!
//! These series do not go through the `metrics` recorder. The recorder never
//! forgets a label set, so a symbol, chain or day that leaves the bot would
//! keep its last value forever. Instead each [`LiqFamily`] is replaced as one
//! unit: a refresh swaps every sample of the family, so departed label sets
//! disappear on the next scrape.
//!
//! Every name is a gauge: each block has a `# HELP` line and a
//! `# TYPE <name> gauge` line. Untyped samples would be ingested twice by
//! Managed Prometheus. See SPEC.md "Prometheus metrics (`liq_*` contract)" and
//! `adrs/0026-liq-metrics-family-store-and-untyped-exposition.md`.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::num::ParseFloatError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use metrics_exporter_prometheus::formatting::{write_help_line, write_type_line};
use rain_math_float::{Float, FloatError};
use serde::Serialize;
use tracing::{debug, error, warn};

use st0x_float_serde::format_float;

use self::pnl::PnlWindowKey;

pub(crate) mod bands;
pub(crate) mod inventory;
pub(crate) mod log_counts;
pub(crate) mod orders;
pub(crate) mod performance;
pub(crate) mod pnl;
pub(crate) mod pnl_refresh;
pub(crate) mod prices;
pub(crate) mod refresh;
pub(crate) mod settings;

/// The process-wide store `/metrics` renders, like the recorder it sits next
/// to.
pub(crate) static LIQ_FAMILIES: LazyLock<LiqFamilies> = LazyLock::new(LiqFamilies::default);

/// One `liq_*` name. The variant list is the whole contract; a test pins it to
/// the published name list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LiqMetric {
    BotInfo,
    BotStartTimestampSeconds,
    SettingsInfo,
    SettingsEquityTarget,
    SettingsEquityDeviation,
    SettingsUsdcTarget,
    SettingsUsdcDeviation,
    SettingsCashReserved,
    SettingsExecutionThresholdUsd,
    SettingsOrderPollingSeconds,
    SettingsInventoryPollSeconds,
    SettingsDeploymentBlock,
    AssetCounterTrading,
    AssetExtendedHours,
    AssetRebalancing,
    UsdcCorridorTarget,
    UsdcCorridorDeviation,
    UsdcCorridorActive,
    EquityOnchainAvailable,
    EquityOffchainAvailable,
    EquityInflightTotal,
    EquityTotal,
    EquityUnwrapped,
    EquityWrapped,
    EquityRatio,
    EquityChainAvailable,
    EquityChainInflight,
    EquityChainShare,
    EquityChainVerdict,
    UsdcOnchainAvailable,
    UsdcOnchainInflight,
    UsdcOffchainAvailable,
    UsdcOffchainGross,
    UsdcOffchainInflight,
    UsdcAlpacaUsdc,
    UsdcAlpacaTotal,
    UsdcInflightTotal,
    UsdcInflightEthereumWallet,
    UsdcInflightBaseWallet,
    UsdcTotal,
    UsdcRatio,
    UsdcRebalanceable,
    UsdcChainAvailable,
    UsdcChainInflight,
    UsdcChainRatio,
    PositionLastPriceUsd,
    EquityExposureUsd,
    HedgeLatencyMs,
    HedgeLatencyMsSamples,
    OpenExposureFillCount,
    OpenExposureOldestTsSeconds,
    ReliabilityLogCount24h,
    LogTargetCount24h,
    FailureEventCount24h,
    JobQueue,
    BlockLagBlocks,
    BlockLagSampledTsSeconds,
    PollCycles24h,
    PollErrors24h,
    PollSkippedTicks24h,
    PollDurationMs,
    DependencyCalls24h,
    DependencyErrors24h,
    DependencyLatencyMs,
    RebalanceStageMs,
    AttestationLastMs,
    PnlSummaryUsd,
    PnlSummaryShares,
    PnlSummaryCount,
    PnlCostUsd,
    PnlRevenueUsd,
    PnlCostEntries,
    PnlCostMissingObservations,
    PnlCostCoverage,
    PnlCapitalAvgDeployedUsd,
    PnlCapitalAnnualizedReturnPct,
    PnlCapitalCoverageDays,
    PnlCapitalSampleDays,
    PnlSymbolUsd,
    PnlSymbolShares,
    PnlSymbolLots,
    PnlSymbolVolumeShares,
    PnlSampleTotalFills,
    PnlSampleSymbols,
    PnlSampleFirstTsSeconds,
    PnlSampleLastTsSeconds,
    PnlWarnings,
    PnlDayUsd,
    PnlDayCumUsd,
    PnlDayStreamUsd,
    PnlDayCumStreamUsd,
    PendingOrders,
    PendingOrdersTotal,
    PendingOrdersUncappedTotal,
    RaindexOrdersTotal,
    RaindexOrdersUnavailable,
    CollectorLastSuccessTsSeconds,
}

impl LiqMetric {
    /// Every variant, for the catalog tests. A new variant needs an entry
    /// here and its name in the catalog test.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 97] = [
        Self::BotInfo,
        Self::BotStartTimestampSeconds,
        Self::SettingsInfo,
        Self::SettingsEquityTarget,
        Self::SettingsEquityDeviation,
        Self::SettingsUsdcTarget,
        Self::SettingsUsdcDeviation,
        Self::SettingsCashReserved,
        Self::SettingsExecutionThresholdUsd,
        Self::SettingsOrderPollingSeconds,
        Self::SettingsInventoryPollSeconds,
        Self::SettingsDeploymentBlock,
        Self::AssetCounterTrading,
        Self::AssetExtendedHours,
        Self::AssetRebalancing,
        Self::UsdcCorridorTarget,
        Self::UsdcCorridorDeviation,
        Self::UsdcCorridorActive,
        Self::EquityOnchainAvailable,
        Self::EquityOffchainAvailable,
        Self::EquityInflightTotal,
        Self::EquityTotal,
        Self::EquityUnwrapped,
        Self::EquityWrapped,
        Self::EquityRatio,
        Self::EquityChainAvailable,
        Self::EquityChainInflight,
        Self::EquityChainShare,
        Self::EquityChainVerdict,
        Self::UsdcOnchainAvailable,
        Self::UsdcOnchainInflight,
        Self::UsdcOffchainAvailable,
        Self::UsdcOffchainGross,
        Self::UsdcOffchainInflight,
        Self::UsdcAlpacaUsdc,
        Self::UsdcAlpacaTotal,
        Self::UsdcInflightTotal,
        Self::UsdcInflightEthereumWallet,
        Self::UsdcInflightBaseWallet,
        Self::UsdcTotal,
        Self::UsdcRatio,
        Self::UsdcRebalanceable,
        Self::UsdcChainAvailable,
        Self::UsdcChainInflight,
        Self::UsdcChainRatio,
        Self::PositionLastPriceUsd,
        Self::EquityExposureUsd,
        Self::HedgeLatencyMs,
        Self::HedgeLatencyMsSamples,
        Self::OpenExposureFillCount,
        Self::OpenExposureOldestTsSeconds,
        Self::ReliabilityLogCount24h,
        Self::LogTargetCount24h,
        Self::FailureEventCount24h,
        Self::JobQueue,
        Self::BlockLagBlocks,
        Self::BlockLagSampledTsSeconds,
        Self::PollCycles24h,
        Self::PollErrors24h,
        Self::PollSkippedTicks24h,
        Self::PollDurationMs,
        Self::DependencyCalls24h,
        Self::DependencyErrors24h,
        Self::DependencyLatencyMs,
        Self::RebalanceStageMs,
        Self::AttestationLastMs,
        Self::PnlSummaryUsd,
        Self::PnlSummaryShares,
        Self::PnlSummaryCount,
        Self::PnlCostUsd,
        Self::PnlRevenueUsd,
        Self::PnlCostEntries,
        Self::PnlCostMissingObservations,
        Self::PnlCostCoverage,
        Self::PnlCapitalAvgDeployedUsd,
        Self::PnlCapitalAnnualizedReturnPct,
        Self::PnlCapitalCoverageDays,
        Self::PnlCapitalSampleDays,
        Self::PnlSymbolUsd,
        Self::PnlSymbolShares,
        Self::PnlSymbolLots,
        Self::PnlSymbolVolumeShares,
        Self::PnlSampleTotalFills,
        Self::PnlSampleSymbols,
        Self::PnlSampleFirstTsSeconds,
        Self::PnlSampleLastTsSeconds,
        Self::PnlWarnings,
        Self::PnlDayUsd,
        Self::PnlDayCumUsd,
        Self::PnlDayStreamUsd,
        Self::PnlDayCumStreamUsd,
        Self::PendingOrders,
        Self::PendingOrdersTotal,
        Self::PendingOrdersUncappedTotal,
        Self::RaindexOrdersTotal,
        Self::RaindexOrdersUnavailable,
        Self::CollectorLastSuccessTsSeconds,
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::BotInfo => "liq_bot_info",
            Self::BotStartTimestampSeconds => "liq_bot_start_timestamp_seconds",
            Self::SettingsInfo => "liq_settings_info",
            Self::SettingsEquityTarget => "liq_settings_equity_target",
            Self::SettingsEquityDeviation => "liq_settings_equity_deviation",
            Self::SettingsUsdcTarget => "liq_settings_usdc_target",
            Self::SettingsUsdcDeviation => "liq_settings_usdc_deviation",
            Self::SettingsCashReserved => "liq_settings_cash_reserved",
            Self::SettingsExecutionThresholdUsd => "liq_settings_execution_threshold_usd",
            Self::SettingsOrderPollingSeconds => "liq_settings_order_polling_seconds",
            Self::SettingsInventoryPollSeconds => "liq_settings_inventory_poll_seconds",
            Self::SettingsDeploymentBlock => "liq_settings_deployment_block",
            Self::AssetCounterTrading => "liq_asset_counter_trading",
            Self::AssetExtendedHours => "liq_asset_extended_hours",
            Self::AssetRebalancing => "liq_asset_rebalancing",
            Self::UsdcCorridorTarget => "liq_usdc_corridor_target",
            Self::UsdcCorridorDeviation => "liq_usdc_corridor_deviation",
            Self::UsdcCorridorActive => "liq_usdc_corridor_active",
            Self::EquityOnchainAvailable => "liq_equity_onchain_available",
            Self::EquityOffchainAvailable => "liq_equity_offchain_available",
            Self::EquityInflightTotal => "liq_equity_inflight_total",
            Self::EquityTotal => "liq_equity_total",
            Self::EquityUnwrapped => "liq_equity_unwrapped",
            Self::EquityWrapped => "liq_equity_wrapped",
            Self::EquityRatio => "liq_equity_ratio",
            Self::EquityChainAvailable => "liq_equity_chain_available",
            Self::EquityChainInflight => "liq_equity_chain_inflight",
            Self::EquityChainShare => "liq_equity_chain_share",
            Self::EquityChainVerdict => "liq_equity_chain_verdict",
            Self::UsdcOnchainAvailable => "liq_usdc_onchain_available",
            Self::UsdcOnchainInflight => "liq_usdc_onchain_inflight",
            Self::UsdcOffchainAvailable => "liq_usdc_offchain_available",
            Self::UsdcOffchainGross => "liq_usdc_offchain_gross",
            Self::UsdcOffchainInflight => "liq_usdc_offchain_inflight",
            Self::UsdcAlpacaUsdc => "liq_usdc_alpaca_usdc",
            Self::UsdcAlpacaTotal => "liq_usdc_alpaca_total",
            Self::UsdcInflightTotal => "liq_usdc_inflight_total",
            Self::UsdcInflightEthereumWallet => "liq_usdc_inflight_ethereum_wallet",
            Self::UsdcInflightBaseWallet => "liq_usdc_inflight_base_wallet",
            Self::UsdcTotal => "liq_usdc_total",
            Self::UsdcRatio => "liq_usdc_ratio",
            Self::UsdcRebalanceable => "liq_usdc_rebalanceable",
            Self::UsdcChainAvailable => "liq_usdc_chain_available",
            Self::UsdcChainInflight => "liq_usdc_chain_inflight",
            Self::UsdcChainRatio => "liq_usdc_chain_ratio",
            Self::PositionLastPriceUsd => "liq_position_last_price_usd",
            Self::EquityExposureUsd => "liq_equity_exposure_usd",
            Self::HedgeLatencyMs => "liq_hedge_latency_ms",
            Self::HedgeLatencyMsSamples => "liq_hedge_latency_ms_samples",
            Self::OpenExposureFillCount => "liq_open_exposure_fill_count",
            Self::OpenExposureOldestTsSeconds => "liq_open_exposure_oldest_ts_seconds",
            Self::ReliabilityLogCount24h => "liq_reliability_log_count_24h",
            Self::LogTargetCount24h => "liq_log_target_count_24h",
            Self::FailureEventCount24h => "liq_failure_event_count_24h",
            Self::JobQueue => "liq_job_queue",
            Self::BlockLagBlocks => "liq_block_lag_blocks",
            Self::BlockLagSampledTsSeconds => "liq_block_lag_sampled_ts_seconds",
            Self::PollCycles24h => "liq_poll_cycles_24h",
            Self::PollErrors24h => "liq_poll_errors_24h",
            Self::PollSkippedTicks24h => "liq_poll_skipped_ticks_24h",
            Self::PollDurationMs => "liq_poll_duration_ms",
            Self::DependencyCalls24h => "liq_dependency_calls_24h",
            Self::DependencyErrors24h => "liq_dependency_errors_24h",
            Self::DependencyLatencyMs => "liq_dependency_latency_ms",
            Self::RebalanceStageMs => "liq_rebalance_stage_ms",
            Self::AttestationLastMs => "liq_attestation_last_ms",
            Self::PnlSummaryUsd => "liq_pnl_summary_usd",
            Self::PnlSummaryShares => "liq_pnl_summary_shares",
            Self::PnlSummaryCount => "liq_pnl_summary_count",
            Self::PnlCostUsd => "liq_pnl_cost_usd",
            Self::PnlRevenueUsd => "liq_pnl_revenue_usd",
            Self::PnlCostEntries => "liq_pnl_cost_entries",
            Self::PnlCostMissingObservations => "liq_pnl_cost_missing_observations",
            Self::PnlCostCoverage => "liq_pnl_cost_coverage",
            Self::PnlCapitalAvgDeployedUsd => "liq_pnl_capital_avg_deployed_usd",
            Self::PnlCapitalAnnualizedReturnPct => "liq_pnl_capital_annualized_return_pct",
            Self::PnlCapitalCoverageDays => "liq_pnl_capital_coverage_days",
            Self::PnlCapitalSampleDays => "liq_pnl_capital_sample_days",
            Self::PnlSymbolUsd => "liq_pnl_symbol_usd",
            Self::PnlSymbolShares => "liq_pnl_symbol_shares",
            Self::PnlSymbolLots => "liq_pnl_symbol_lots",
            Self::PnlSymbolVolumeShares => "liq_pnl_symbol_volume_shares",
            Self::PnlSampleTotalFills => "liq_pnl_sample_total_fills",
            Self::PnlSampleSymbols => "liq_pnl_sample_symbols",
            Self::PnlSampleFirstTsSeconds => "liq_pnl_sample_first_ts_seconds",
            Self::PnlSampleLastTsSeconds => "liq_pnl_sample_last_ts_seconds",
            Self::PnlWarnings => "liq_pnl_warnings",
            Self::PnlDayUsd => "liq_pnl_day_usd",
            Self::PnlDayCumUsd => "liq_pnl_day_cum_usd",
            Self::PnlDayStreamUsd => "liq_pnl_day_stream_usd",
            Self::PnlDayCumStreamUsd => "liq_pnl_day_cum_stream_usd",
            Self::PendingOrders => "liq_pending_orders",
            Self::PendingOrdersTotal => "liq_pending_orders_total",
            Self::PendingOrdersUncappedTotal => "liq_pending_orders_uncapped_total",
            Self::RaindexOrdersTotal => "liq_raindex_orders_total",
            Self::RaindexOrdersUnavailable => "liq_raindex_orders_unavailable",
            Self::CollectorLastSuccessTsSeconds => "liq_collector_last_success_ts_seconds",
        }
    }

    pub(crate) const fn help(self) -> &'static str {
        match self {
            Self::BotInfo => "Always 1; git_commit is the first 12 characters of the running build",
            Self::BotStartTimestampSeconds => "Unix time the bot process started",
            Self::SettingsInfo => {
                "Always 1; the labels carry the bot's operational settings. trading_mode is \
                 always empty"
            }
            Self::SettingsEquityTarget => {
                "Primary chain default onchain equity share; absent with only per-symbol targets"
            }
            Self::SettingsEquityDeviation => "Equity rebalancing band half-width",
            Self::SettingsUsdcTarget => "Onchain USDC target share; absent without a USDC corridor",
            Self::SettingsUsdcDeviation => {
                "USDC rebalancing band half-width; absent without a USDC corridor"
            }
            Self::SettingsCashReserved => {
                "USD held back in the broker account; absent when not configured"
            }
            Self::SettingsExecutionThresholdUsd => {
                "Dollar hedge execution threshold; absent for a share-count threshold"
            }
            Self::SettingsOrderPollingSeconds => "Broker order polling interval in seconds",
            Self::SettingsInventoryPollSeconds => "Inventory polling interval in seconds",
            Self::SettingsDeploymentBlock => "Primary chain orderbook deployment block",
            Self::AssetCounterTrading => "1 when counter trading is enabled for the symbol, else 0",
            Self::AssetExtendedHours => {
                "1 when extended-hours counter trading is enabled, else 0; absent when counter \
                 trading is disabled"
            }
            Self::AssetRebalancing => "1 when the symbol starts new rebalancing operations, else 0",
            Self::UsdcCorridorTarget => {
                "Target onchain share of each configured USDC corridor's chain vault"
            }
            Self::UsdcCorridorDeviation => {
                "Rebalancing band half-width of each configured USDC corridor"
            }
            Self::UsdcCorridorActive => "1 while config lets the USDC trigger act on it, else 0",
            Self::EquityOnchainAvailable => "Primary chain vault shares available",
            Self::EquityOffchainAvailable => "Broker shares available",
            Self::EquityInflightTotal => "Primary chain vault plus broker shares in flight",
            Self::EquityTotal => {
                "Primary chain vault plus broker shares, available and in flight; excludes \
                 wallet tokens and other chains"
            }
            Self::EquityUnwrapped => "Unwrapped tokens held in the Base wallet",
            Self::EquityWrapped => "Wrapped tokens held in the Base wallet",
            Self::EquityRatio => {
                "Primary chain vault share of available shares; 0 when nothing is available"
            }
            Self::EquityChainAvailable => {
                "Wrapped vault shares on each hedged chain a snapshot read; never sum chains"
            }
            Self::EquityChainInflight => "In flight from each hedged chain vault; units in SPEC",
            Self::EquityChainShare => "Chain's share of the symbol in underlying shares",
            Self::EquityChainVerdict => "-1 below the chain's band, 0 within, 1 above",
            Self::UsdcOnchainAvailable => "Primary chain vault settlement stable available",
            Self::UsdcOnchainInflight => "Primary chain vault settlement stable in flight",
            Self::UsdcOffchainAvailable => "Broker cash available after the reserve",
            Self::UsdcOffchainGross => "Broker cash before the reserve; absent until read",
            Self::UsdcOffchainInflight => "Broker cash in flight",
            Self::UsdcAlpacaUsdc => "USDC held as a token in the broker account; absent until read",
            Self::UsdcAlpacaTotal => {
                "Broker cash before the reserve, or after it until the gross is read"
            }
            Self::UsdcInflightTotal => "Primary chain vault plus broker cash in flight",
            Self::UsdcInflightEthereumWallet => {
                "USDC held in the Ethereum wallet; absent until read"
            }
            Self::UsdcInflightBaseWallet => "USDC held in the Base wallet; absent until read",
            Self::UsdcTotal => "Primary chain vault cash plus broker total plus cash in flight",
            Self::UsdcRatio => {
                "Primary chain vault share of vault plus broker cash; 0 when both are 0"
            }
            Self::UsdcRebalanceable => {
                "Withdrawable broker cash above the reserve; absent until withdrawable is read"
            }
            Self::UsdcChainAvailable => {
                "Settlement stable available in each hedged chain vault a snapshot has read"
            }
            Self::UsdcChainInflight => {
                "Settlement stable in flight in each hedged chain vault a snapshot has read"
            }
            Self::UsdcChainRatio => {
                "Vault share of itself plus gross broker cash; absent until gross is read or both 0"
            }
            Self::PositionLastPriceUsd => {
                "Live wrapped-token mid price in USD of each symbol with a position"
            }
            Self::EquityExposureUsd => "Net position times its live price, in USD",
            Self::HedgeLatencyMs
            | Self::HedgeLatencyMsSamples
            | Self::OpenExposureFillCount
            | Self::OpenExposureOldestTsSeconds
            | Self::ReliabilityLogCount24h
            | Self::LogTargetCount24h
            | Self::FailureEventCount24h
            | Self::JobQueue
            | Self::BlockLagBlocks
            | Self::BlockLagSampledTsSeconds
            | Self::PollCycles24h
            | Self::PollErrors24h
            | Self::PollSkippedTicks24h
            | Self::PollDurationMs
            | Self::DependencyCalls24h
            | Self::DependencyErrors24h
            | Self::DependencyLatencyMs
            | Self::RebalanceStageMs
            | Self::AttestationLastMs => self.operations_help(),
            Self::PnlSummaryUsd => {
                "Window PnL in USD by stream; absent when the report's decimal does not parse"
            }
            Self::PnlSummaryShares => "Window share totals by kind",
            Self::PnlSummaryCount => "Window fill and lot counts by kind",
            Self::PnlCostUsd => "Window tracked costs in USD by category",
            Self::PnlRevenueUsd => "Window tracked revenue in USD by category",
            Self::PnlCostEntries => "Cost entries in the window",
            Self::PnlCostMissingObservations => "Cost observations missing from the window",
            Self::PnlCostCoverage => {
                "Always 1; one series per cost source with its coverage status"
            }
            Self::PnlCapitalAvgDeployedUsd => {
                "Average deployed capital in USD; absent when not computed"
            }
            Self::PnlCapitalAnnualizedReturnPct => {
                "Annualized return on capital in percent; absent when not computed"
            }
            Self::PnlCapitalCoverageDays => {
                "Days of the window the capital snapshots cover; absent when not computed"
            }
            Self::PnlCapitalSampleDays => "Snapshot days sampled for the capital figures",
            Self::PnlSymbolUsd => "Per-symbol window PnL in USD by column",
            Self::PnlSymbolShares => "Per-symbol window shares by kind",
            Self::PnlSymbolLots => "Per-symbol matched lots in the window",
            Self::PnlSymbolVolumeShares => "Per-symbol matched shares counted once per leg",
            Self::PnlSampleTotalFills => "Fills in the window",
            Self::PnlSampleSymbols => "Symbols with fills in the window",
            Self::PnlSampleFirstTsSeconds => {
                "Unix time of the window's first fill; absent without fills"
            }
            Self::PnlSampleLastTsSeconds => {
                "Unix time of the window's last fill; absent without fills"
            }
            Self::PnlWarnings => "Warnings the window's report carried",
            Self::PnlDayUsd => "Per-symbol PnL in USD of each of the window's last 90 day buckets",
            Self::PnlDayCumUsd => "Per-symbol running PnL in USD through each day bucket",
            Self::PnlDayStreamUsd => "PnL in USD of each day bucket by chart stream",
            Self::PnlDayCumStreamUsd => {
                "Running PnL in USD through each day bucket by chart stream"
            }
            Self::PendingOrders => {
                "Pending broker orders by status among the newest 100 rows that parse; absent at 0"
            }
            Self::PendingOrdersTotal => {
                "Pending broker orders among the newest 100 rows, without unparseable ones"
            }
            Self::PendingOrdersUncappedTotal => {
                "Every non-terminal broker order row, uncapped; absent when the count fails"
            }
            Self::RaindexOrdersTotal => {
                "Active Raindex orders the st0x REST API reports; absent while unavailable"
            }
            Self::RaindexOrdersUnavailable => {
                "1 with the reason while the Raindex orders are unavailable, else 0 with an empty reason"
            }
            Self::CollectorLastSuccessTsSeconds => {
                "Unix time each liq_ collector last published its family"
            }
        }
    }

    /// HELP text of the latency, reliability, infrastructure and rebalance
    /// names, which `help` routes here.
    const fn operations_help(self) -> &'static str {
        match self {
            Self::HedgeLatencyMs => {
                "Hedge pipeline stage latency over the last 24 hours, by stage and \
                 nearest-rank quantile (p50, p90, p95, p99, max)"
            }
            Self::HedgeLatencyMsSamples => "Samples behind each liq_hedge_latency_ms stage",
            Self::OpenExposureFillCount => {
                "Fills observed after the symbol's latest hedge placement"
            }
            Self::OpenExposureOldestTsSeconds => {
                "Block time of the oldest fill not yet covered by a hedge"
            }
            Self::ReliabilityLogCount24h => {
                "Error and warning log events over the last 24 hours, by level (error, warning); \
                 0 without file logging"
            }
            Self::LogTargetCount24h => {
                "Error and warning log events over the last 24 hours, by target and level (ERROR, \
                 WARN); only targets with events"
            }
            Self::FailureEventCount24h => {
                "Money-at-risk lifecycle failure events over the last 24 hours, by event type"
            }
            Self::JobQueue => "Jobs in each apalis queue now, by job type and state; not windowed",
            Self::BlockLagBlocks => "Latest sampled order-fill block lag of each hedged chain",
            Self::BlockLagSampledTsSeconds => {
                "Unix time of each hedged chain's latest block-lag sample"
            }
            Self::PollCycles24h => {
                "Order-fill poll cycles of each hedged chain over the last 24 hours"
            }
            Self::PollErrors24h => {
                "Failed order-fill poll cycles of each hedged chain over the last 24 hours"
            }
            Self::PollSkippedTicks24h => {
                "Order-fill poll ticks each hedged chain dropped over the last 24 hours"
            }
            Self::PollDurationMs => {
                "Order-fill poll cycle duration over the last 24 hours, by chain and quantile"
            }
            Self::DependencyCalls24h => {
                "External dependency calls over the last 24 hours, by dependency and operation"
            }
            Self::DependencyErrors24h => "Failed external dependency calls over the last 24 hours",
            Self::DependencyLatencyMs => {
                "External dependency call latency over the last 24 hours, by quantile"
            }
            Self::RebalanceStageMs => {
                "Completed rebalance stage duration over the last 30 days, by kind (usdc, \
                 equity), stage and quantile"
            }
            Self::AttestationLastMs => {
                "Duration of the latest CCTP attestation in the last 30 days"
            }
            // `help` routes only the names above here; the catalog test
            // fails on the empty text if one is routed without its own arm.
            _ => "",
        }
    }

    /// The label keys every sample of this metric carries, sorted.
    const fn label_keys(self) -> &'static [&'static str] {
        match self {
            Self::BotInfo => &["git_commit"],
            Self::SettingsInfo => &[
                "broker",
                "log_level",
                "orderbook",
                "server_port",
                "trading_mode",
                "turnkey_organization",
                "wallet_address",
                "wallet_kind",
            ],
            Self::AssetCounterTrading
            | Self::AssetExtendedHours
            | Self::AssetRebalancing
            | Self::EquityOnchainAvailable
            | Self::EquityOffchainAvailable
            | Self::EquityInflightTotal
            | Self::EquityTotal
            | Self::EquityUnwrapped
            | Self::EquityWrapped
            | Self::EquityRatio
            | Self::PositionLastPriceUsd
            | Self::EquityExposureUsd
            | Self::OpenExposureFillCount
            | Self::OpenExposureOldestTsSeconds => &["symbol"],
            Self::EquityChainAvailable
            | Self::EquityChainInflight
            | Self::EquityChainShare
            | Self::EquityChainVerdict => &["chain", "symbol"],
            Self::UsdcCorridorTarget
            | Self::UsdcCorridorDeviation
            | Self::UsdcCorridorActive
            | Self::UsdcChainAvailable
            | Self::UsdcChainInflight
            | Self::UsdcChainRatio
            | Self::BlockLagBlocks
            | Self::BlockLagSampledTsSeconds
            | Self::PollCycles24h
            | Self::PollErrors24h
            | Self::PollSkippedTicks24h => &["chain"],
            Self::HedgeLatencyMs => &["quantile", "stage"],
            Self::HedgeLatencyMsSamples => &["stage"],
            Self::FailureEventCount24h => &["event_type"],
            Self::JobQueue => &["job_type", "state"],
            Self::ReliabilityLogCount24h => &["level"],
            Self::LogTargetCount24h => &["level", "target"],
            Self::PollDurationMs => &["chain", "quantile"],
            Self::DependencyCalls24h | Self::DependencyErrors24h => &["dependency", "operation"],
            Self::DependencyLatencyMs => &["dependency", "operation", "quantile"],
            Self::RebalanceStageMs => &["kind", "quantile", "stage"],
            Self::AttestationLastMs => &["kind"],
            Self::PnlSummaryUsd => &["stream", "window"],
            Self::PnlSummaryShares | Self::PnlSummaryCount => &["kind", "window"],
            Self::PnlCostUsd | Self::PnlRevenueUsd => &["category", "window"],
            Self::PnlCostEntries
            | Self::PnlCostMissingObservations
            | Self::PnlCapitalAvgDeployedUsd
            | Self::PnlCapitalAnnualizedReturnPct
            | Self::PnlCapitalCoverageDays
            | Self::PnlCapitalSampleDays
            | Self::PnlSampleTotalFills
            | Self::PnlSampleSymbols
            | Self::PnlSampleFirstTsSeconds
            | Self::PnlSampleLastTsSeconds
            | Self::PnlWarnings => &["window"],
            Self::PnlCostCoverage => &["source", "status", "window"],
            Self::PnlSymbolUsd => &["col", "symbol", "window"],
            Self::PnlSymbolShares => &["kind", "symbol", "window"],
            Self::PnlSymbolLots | Self::PnlSymbolVolumeShares => &["symbol", "window"],
            Self::PnlDayUsd | Self::PnlDayCumUsd => &["day", "symbol", "window"],
            Self::PnlDayStreamUsd | Self::PnlDayCumStreamUsd => &["day", "stream", "window"],
            Self::PendingOrders => &["status"],
            Self::RaindexOrdersUnavailable => &["reason"],
            Self::CollectorLastSuccessTsSeconds => &["collector"],
            Self::BotStartTimestampSeconds
            | Self::SettingsEquityTarget
            | Self::SettingsEquityDeviation
            | Self::SettingsUsdcTarget
            | Self::SettingsUsdcDeviation
            | Self::SettingsCashReserved
            | Self::SettingsExecutionThresholdUsd
            | Self::SettingsOrderPollingSeconds
            | Self::SettingsInventoryPollSeconds
            | Self::SettingsDeploymentBlock
            | Self::UsdcOnchainAvailable
            | Self::UsdcOnchainInflight
            | Self::UsdcOffchainAvailable
            | Self::UsdcOffchainGross
            | Self::UsdcOffchainInflight
            | Self::UsdcAlpacaUsdc
            | Self::UsdcAlpacaTotal
            | Self::UsdcInflightTotal
            | Self::UsdcInflightEthereumWallet
            | Self::UsdcInflightBaseWallet
            | Self::UsdcTotal
            | Self::UsdcRatio
            | Self::UsdcRebalanceable
            | Self::PendingOrdersTotal
            | Self::PendingOrdersUncappedTotal
            | Self::RaindexOrdersTotal => &[],
        }
    }

    /// The one family allowed to publish this name. `None` means the store
    /// itself writes it during render, or the name carries a `window` label
    /// and each [`LiqFamily::Pnl`] family publishes its own window (see
    /// [`Self::per_pnl_window`]).
    const fn family(self) -> Option<LiqFamily> {
        match self {
            Self::BotInfo | Self::BotStartTimestampSeconds => Some(LiqFamily::Health),
            Self::SettingsInfo
            | Self::SettingsEquityTarget
            | Self::SettingsEquityDeviation
            | Self::SettingsUsdcTarget
            | Self::SettingsUsdcDeviation
            | Self::SettingsCashReserved
            | Self::SettingsExecutionThresholdUsd
            | Self::SettingsOrderPollingSeconds
            | Self::SettingsInventoryPollSeconds
            | Self::SettingsDeploymentBlock
            | Self::AssetCounterTrading
            | Self::AssetExtendedHours
            | Self::AssetRebalancing
            | Self::UsdcCorridorTarget
            | Self::UsdcCorridorDeviation
            | Self::UsdcCorridorActive => Some(LiqFamily::Settings),
            Self::EquityOnchainAvailable
            | Self::EquityOffchainAvailable
            | Self::EquityInflightTotal
            | Self::EquityTotal
            | Self::EquityUnwrapped
            | Self::EquityWrapped
            | Self::EquityRatio
            | Self::EquityChainAvailable
            | Self::EquityChainInflight
            | Self::UsdcOnchainAvailable
            | Self::UsdcOnchainInflight
            | Self::UsdcOffchainAvailable
            | Self::UsdcOffchainGross
            | Self::UsdcOffchainInflight
            | Self::UsdcAlpacaUsdc
            | Self::UsdcAlpacaTotal
            | Self::UsdcInflightTotal
            | Self::UsdcInflightEthereumWallet
            | Self::UsdcInflightBaseWallet
            | Self::UsdcTotal
            | Self::UsdcRatio
            | Self::UsdcRebalanceable
            | Self::UsdcChainAvailable
            | Self::UsdcChainInflight
            | Self::UsdcChainRatio => Some(LiqFamily::Inventory),
            Self::PositionLastPriceUsd | Self::EquityExposureUsd => Some(LiqFamily::Prices),
            Self::EquityChainShare | Self::EquityChainVerdict => Some(LiqFamily::EquityBands),
            Self::HedgeLatencyMs
            | Self::HedgeLatencyMsSamples
            | Self::OpenExposureFillCount
            | Self::OpenExposureOldestTsSeconds => Some(LiqFamily::Latencies),
            Self::ReliabilityLogCount24h | Self::LogTargetCount24h => Some(LiqFamily::Logs),
            Self::FailureEventCount24h | Self::JobQueue => Some(LiqFamily::Reliability),
            Self::BlockLagBlocks
            | Self::BlockLagSampledTsSeconds
            | Self::PollCycles24h
            | Self::PollErrors24h
            | Self::PollSkippedTicks24h
            | Self::PollDurationMs
            | Self::DependencyCalls24h
            | Self::DependencyErrors24h
            | Self::DependencyLatencyMs => Some(LiqFamily::Infra),
            Self::RebalanceStageMs | Self::AttestationLastMs => Some(LiqFamily::Rebalances),
            Self::PendingOrders | Self::PendingOrdersTotal | Self::PendingOrdersUncappedTotal => {
                Some(LiqFamily::PendingOrders)
            }
            Self::RaindexOrdersTotal | Self::RaindexOrdersUnavailable => Some(LiqFamily::Raindex),
            Self::PnlSummaryUsd
            | Self::PnlSummaryShares
            | Self::PnlSummaryCount
            | Self::PnlCostUsd
            | Self::PnlRevenueUsd
            | Self::PnlCostEntries
            | Self::PnlCostMissingObservations
            | Self::PnlCostCoverage
            | Self::PnlCapitalAvgDeployedUsd
            | Self::PnlCapitalAnnualizedReturnPct
            | Self::PnlCapitalCoverageDays
            | Self::PnlCapitalSampleDays
            | Self::PnlSymbolUsd
            | Self::PnlSymbolShares
            | Self::PnlSymbolLots
            | Self::PnlSymbolVolumeShares
            | Self::PnlSampleTotalFills
            | Self::PnlSampleSymbols
            | Self::PnlSampleFirstTsSeconds
            | Self::PnlSampleLastTsSeconds
            | Self::PnlWarnings
            | Self::PnlDayUsd
            | Self::PnlDayCumUsd
            | Self::PnlDayStreamUsd
            | Self::PnlDayCumStreamUsd
            | Self::CollectorLastSuccessTsSeconds => None,
        }
    }

    /// True for the names every [`LiqFamily::Pnl`] family publishes, each
    /// for its own `window` label value.
    fn per_pnl_window(self) -> bool {
        self.label_keys().contains(&"window")
    }
}

/// The unit of replacement. A refresh replaces all samples of one family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LiqFamily {
    Health,
    Settings,
    Inventory,
    Prices,
    /// The allocation planner's band verdict per chain, replaced each time
    /// the trigger plans a symbol or refreshes one whose transfer is in
    /// progress.
    EquityBands,
    Latencies,
    /// The error and warning counts, from the in-process counter. Apart from
    /// the reliability family, so a failing database loader does not keep
    /// them at their last values.
    Logs,
    Reliability,
    Infra,
    Rebalances,
    PendingOrders,
    Raindex,
    /// One PnL window. Each window is replaced on its own, so a window whose
    /// report failed keeps its last samples while the others refresh.
    Pnl(PnlWindowKey),
}

impl LiqFamily {
    /// The `collector` label of this family's freshness series.
    pub(crate) const fn collector(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Settings => "settings",
            Self::Inventory => "inventory",
            Self::Prices => "prices",
            Self::EquityBands => "equity_bands",
            Self::Latencies => "latencies",
            Self::Logs => "logs",
            Self::Reliability => "reliability",
            Self::Infra => "infra",
            Self::Rebalances => "rebalances",
            Self::PendingOrders => "pending_orders",
            Self::Raindex => "raindex",
            Self::Pnl(window) => window.collector(),
        }
    }

    /// Whether this family may publish `sample`: a name it owns, and for a
    /// PnL window only samples labelled with that window.
    fn owns(self, sample: &LiqSample) -> bool {
        match self {
            Self::Pnl(window) => {
                sample.metric.per_pnl_window()
                    && sample.labels.value("window") == Some(window.label())
            }
            Self::Health
            | Self::Settings
            | Self::Inventory
            | Self::Prices
            | Self::EquityBands
            | Self::Latencies
            | Self::Logs
            | Self::Reliability
            | Self::Infra
            | Self::Rebalances
            | Self::PendingOrders
            | Self::Raindex => sample.metric.family() == Some(self),
        }
    }
}

/// One `liq_*` sample. Construction checks the label keys against the metric
/// and rejects non-finite values, so a stored sample always renders.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LiqSample {
    metric: LiqMetric,
    labels: LabelSet,
    value: f64,
}

impl LiqSample {
    pub(crate) fn new(
        metric: LiqMetric,
        labels: Vec<(&'static str, String)>,
        value: f64,
    ) -> Result<Self, LiqSampleError> {
        if !value.is_finite() {
            return Err(LiqSampleError::NonFinite { metric, value });
        }

        let mut labels = labels;
        labels.sort_by_key(|(key, _)| *key);
        let keys: Vec<&'static str> = labels.iter().map(|(key, _)| *key).collect();
        if keys != metric.label_keys() {
            return Err(LiqSampleError::LabelKeys { metric, keys });
        }

        Ok(Self {
            metric,
            labels: LabelSet(labels),
            value,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LiqSampleError {
    #[error("{} value {value} is not finite", metric.name())]
    NonFinite { metric: LiqMetric, value: f64 },
    #[error("{} takes labels {:?}, got {keys:?}", metric.name(), metric.label_keys())]
    LabelKeys {
        metric: LiqMetric,
        keys: Vec<&'static str>,
    },
}

/// Label pairs sorted by key, like the exporter writes them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct LabelSet(Vec<(&'static str, String)>);

impl LabelSet {
    fn value(&self, key: &str) -> Option<&str> {
        let Self(labels) = self;
        labels
            .iter()
            .find(|(label, _)| *label == key)
            .map(|(_, value)| value.as_str())
    }
}

/// The family store `/metrics` renders after the recorder output.
#[derive(Default)]
pub(crate) struct LiqFamilies {
    families: Mutex<BTreeMap<LiqFamily, FamilyEntry>>,
    generations: AtomicU64,
}

/// Samples are shared, so a render copies the `Arc`s under the lock and
/// formats without it.
struct FamilyEntry {
    samples: Arc<[LiqSample]>,
    last_success: SystemTime,
    /// Set by [`LiqFamilies::replace_at_generation`], so a publisher that
    /// read its source before another cannot overwrite the newer samples.
    generation: Option<u64>,
}

impl LiqFamilies {
    /// Replaces every sample of `family` and records `at` as its last
    /// success. Samples the family may not publish are dropped and logged
    /// (see [`owned_unique_samples`]).
    pub(crate) fn replace(&self, family: LiqFamily, samples: Vec<LiqSample>, at: SystemTime) {
        let samples = owned_unique_samples(family, samples);

        self.families
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                family,
                FamilyEntry {
                    samples,
                    last_success: at,
                    generation: None,
                },
            );
    }

    /// Numbers a source read for [`Self::replace_at_generation`]. One
    /// sequence per store, so a publisher created later (a new bot session
    /// in the same process) never starts below a stored generation.
    pub(crate) fn next_generation(&self) -> u64 {
        self.generations.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The last number [`Self::next_generation`] gave out.
    pub(crate) fn current_generation(&self) -> u64 {
        self.generations.load(Ordering::SeqCst)
    }

    /// Like [`Self::replace`], for a family several publishers write: each
    /// read its source at `generation`, numbered in source order. A replace
    /// whose generation is older than the stored one is ignored and returns
    /// false, so a publisher that pauses after its read cannot put older
    /// values back.
    pub(crate) fn replace_at_generation(
        &self,
        family: LiqFamily,
        generation: u64,
        samples: Vec<LiqSample>,
        at: SystemTime,
    ) -> bool {
        let samples = owned_unique_samples(family, samples);
        let mut families = self.families.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(stored) = families.get(&family).and_then(|entry| entry.generation)
            && generation < stored
        {
            debug!(
                ?family,
                generation, stored, "Ignored a liq_ family read before the stored one"
            );
            return false;
        }

        families.insert(
            family,
            FamilyEntry {
                samples,
                last_success: at,
                generation: Some(generation),
            },
        );
        true
    }

    /// Appends the `liq_*` exposition to `body`, which already holds the
    /// recorder output. A `liq_` name the recorder already rendered is
    /// skipped and logged, so the body never holds two blocks for one name.
    pub(crate) fn render_into(&self, body: &mut String) {
        let snapshot = self.snapshot();
        render_snapshot(&snapshot, body);
    }

    fn snapshot(&self) -> Vec<(LiqFamily, Arc<[LiqSample]>, SystemTime)> {
        self.families
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(family, entry)| (*family, Arc::clone(&entry.samples), entry.last_success))
            .collect()
    }
}

/// The samples a family may publish, shared for rendering. A sample whose
/// metric belongs to another family, or that repeats an earlier
/// `(name, labels)`, is dropped and logged: each name has one writer, and the
/// text format allows one value per series.
fn owned_unique_samples(family: LiqFamily, samples: Vec<LiqSample>) -> Arc<[LiqSample]> {
    let mut seen = HashSet::new();

    samples
        .into_iter()
        .filter(|sample| {
            if !family.owns(sample) {
                error!(
                    metric = sample.metric.name(),
                    ?family,
                    "Dropped a liq_ sample published by a family that does not own its name"
                );
                return false;
            }

            if !seen.insert((sample.metric, sample.labels.clone())) {
                error!(
                    metric = sample.metric.name(),
                    labels = ?sample.labels,
                    "Dropped a duplicate liq_ sample; the first one is kept"
                );
                return false;
            }

            true
        })
        .collect()
}

/// Adds one sample, or logs and skips it. A value that does not convert is
/// left absent, as the exporter left a value it could not parse.
pub(crate) fn push_sample(
    samples: &mut Vec<LiqSample>,
    metric: LiqMetric,
    labels: Vec<(&'static str, String)>,
    value: Result<f64, LiqValueError>,
) {
    let value = match value {
        Ok(value) => value,
        Err(error) => {
            warn!(metric = metric.name(), ?labels, %error, "Skipped a liq_ sample");
            return;
        }
    };

    match LiqSample::new(metric, labels, value) {
        Ok(sample) => samples.push(sample),
        Err(error) => error!(metric = metric.name(), %error, "Built an invalid liq_ sample"),
    }
}

/// Removes a leading `wt` or `t` only when the next character is uppercase,
/// so `tAAPL` and `wtAAPL` both give `AAPL` while `tsla` stays as it is.
/// Every `symbol` label goes through this, matching the series the exporter
/// published.
pub(crate) fn strip_prefix(symbol: &str) -> &str {
    ["wt", "t"]
        .into_iter()
        .find_map(|prefix| {
            symbol
                .strip_prefix(prefix)
                .filter(|rest| rest.chars().next().is_some_and(char::is_uppercase))
        })
        .unwrap_or(symbol)
}

/// Converts an exact `Float` to the nearest `f64` for exposition. This is the
/// one lossy step, and it runs once, after all arithmetic.
pub(crate) fn float_value(value: Float) -> Result<f64, LiqValueError> {
    let converted: f64 = format_float(&value)?.parse()?;

    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(LiqValueError::NonFinite(converted))
    }
}

/// Converts an integer setting to `f64`, refusing values `f64` cannot hold
/// exactly.
pub(crate) fn integer_value(value: u64) -> Result<f64, LiqValueError> {
    const MAX_EXACT: u64 = 1 << f64::MANTISSA_DIGITS;

    if value > MAX_EXACT {
        return Err(LiqValueError::Inexact(value));
    }

    // Every integer up to 2^53 has an exact f64, so the parse is exact.
    Ok(value.to_string().parse()?)
}

/// Converts a signed integer to `f64`, refusing values `f64` cannot hold
/// exactly.
pub(crate) fn signed_integer_value(value: i64) -> Result<f64, LiqValueError> {
    let magnitude = integer_value(value.unsigned_abs())?;
    Ok(if value < 0 { -magnitude } else { magnitude })
}

/// Converts a count to `f64`, refusing values `f64` cannot hold exactly.
pub(crate) fn count_value(count: usize) -> Result<f64, LiqValueError> {
    integer_value(u64::try_from(count)?)
}

/// Unix seconds with microsecond precision, as the exporter computed them
/// from the RFC 3339 text: whole microseconds divided by one million.
pub(crate) fn timestamp_value(at: DateTime<Utc>) -> Result<f64, LiqValueError> {
    const MICROS_PER_SECOND: f64 = 1_000_000.0;

    Ok(signed_integer_value(at.timestamp_micros())? / MICROS_PER_SECOND)
}

/// The serde wire name of a unit enum variant, which is the label value the
/// exporter read from the bot's JSON.
pub(crate) fn wire_name<T: Serialize>(value: &T) -> Result<String, WireNameError> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(name) => Ok(name),
        _ => Err(WireNameError::NotAString),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WireNameError {
    #[error("failed to serialize the value")]
    Serialize(#[from] serde_json::Error),
    #[error("the value does not serialize to a string")]
    NotAString,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LiqValueError {
    #[error("count does not fit in u64")]
    Count(#[from] std::num::TryFromIntError),
    #[error("failed to format the Float value")]
    Format(#[from] FloatError),
    #[error("formatted Float value does not parse as f64")]
    Parse(#[from] ParseFloatError),
    #[error("value {0} is not finite")]
    NonFinite(f64),
    #[error("integer {0} has no exact f64 representation")]
    Inexact(u64),
    #[error("{value:?} is not a decimal")]
    Decimal {
        value: String,
        #[source]
        source: FloatError,
    },
    #[error("{value:?} is not an RFC 3339 timestamp")]
    Timestamp {
        value: String,
        #[source]
        source: chrono::ParseError,
    },
}

/// Every `liq_*` name is a gauge: snapshots, rolling windows that go down,
/// `*_total` names that mean a count right now, and precomputed quantiles that
/// are not real summaries.
const LIQ_TYPE: &str = "gauge";

fn render_snapshot(snapshot: &[(LiqFamily, Arc<[LiqSample]>, SystemTime)], body: &mut String) {
    let reserved = recorder_liq_names(body);

    let collector_samples: Vec<LiqSample> = snapshot
        .iter()
        .filter_map(|(family, _, last_success)| collector_sample(*family, *last_success))
        .collect();

    let mut by_name: BTreeMap<&'static str, Vec<&LiqSample>> = BTreeMap::new();
    for sample in snapshot
        .iter()
        .flat_map(|(_, samples, _)| samples.iter())
        .chain(&collector_samples)
    {
        by_name
            .entry(sample.metric.name())
            .or_default()
            .push(sample);
    }

    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }

    for (name, mut samples) in by_name {
        if reserved.contains(name) {
            error!(
                metric = name,
                "The recorder rendered a liq_ name; the family store skipped its own block"
            );
            continue;
        }

        samples.sort_by(|left, right| left.labels.cmp(&right.labels));
        let Some(first) = samples.first() else {
            continue;
        };
        write_help_line(body, name, first.metric.help());
        write_type_line(body, name, LIQ_TYPE);
        for sample in samples {
            write_sample(body, name, sample);
        }
    }
}

fn collector_sample(family: LiqFamily, last_success: SystemTime) -> Option<LiqSample> {
    let seconds = last_success
        .duration_since(UNIX_EPOCH)
        .inspect_err(|error| {
            warn!(?family, %error, "Collector success time is before the Unix epoch");
        })
        .ok()?
        .as_secs_f64();

    LiqSample::new(
        LiqMetric::CollectorLastSuccessTsSeconds,
        vec![("collector", family.collector().to_string())],
        seconds,
    )
    .inspect_err(|error| warn!(?family, %error, "Skipped a collector freshness sample"))
    .ok()
}

/// Names of `liq_` families already present in the recorder output, from
/// both sample lines and `# HELP`/`# TYPE` lines.
fn recorder_liq_names(body: &str) -> HashSet<String> {
    body.lines()
        .filter_map(|line| {
            let line = line
                .strip_prefix("# HELP ")
                .or_else(|| line.strip_prefix("# TYPE "))
                .unwrap_or(line);
            line.split(['{', ' ']).next()
        })
        .filter(|name| name.starts_with("liq_"))
        .map(str::to_string)
        .collect()
}

fn write_sample(body: &mut String, name: &str, sample: &LiqSample) {
    body.push_str(name);

    let LabelSet(labels) = &sample.labels;
    if !labels.is_empty() {
        body.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(key);
            body.push_str("=\"");
            escape_label_value(body, value);
            body.push('"');
        }
        body.push('}');
    }

    // Writing to a String cannot fail.
    let _ = writeln!(body, " {}", sample.value);
}

/// The exporter escaped every backslash and quote; newlines are escaped too
/// so a multi-line value cannot break the exposition.
fn escape_label_value(body: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => body.push_str("\\\\"),
            '"' => body.push_str("\\\""),
            '\n' => body.push_str("\\n"),
            other => body.push(other),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use proptest::prelude::*;
    use rain_math_float::Float;

    use super::*;

    /// One parsed series: name and sorted labels.
    pub(crate) type SeriesKey = (String, Vec<(String, String)>);

    /// Parses Prometheus text exposition into `(name, labels) -> value`.
    /// Comment lines are skipped. Panics on a malformed line, which is what a
    /// test wants.
    pub(crate) fn parse_exposition(text: &str) -> BTreeMap<SeriesKey, f64> {
        let mut series = BTreeMap::new();

        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let name_end = line.find(['{', ' ']).unwrap();
            let name = line[..name_end].to_string();
            let mut rest = &line[name_end..];
            let mut labels = Vec::new();

            if let Some(after_brace) = rest.strip_prefix('{') {
                let (parsed, remaining) = parse_labels(after_brace);
                labels = parsed;
                rest = remaining;
            }

            let value: f64 = rest.trim().parse().unwrap();
            labels.sort();
            let previous = series.insert((name, labels), value);
            assert_eq!(previous, None, "duplicate series in exposition: {line}");
        }

        series
    }

    fn parse_labels(input: &str) -> (Vec<(String, String)>, &str) {
        let mut labels = Vec::new();
        let mut rest = input;

        loop {
            if let Some(after) = rest.strip_prefix('}') {
                return (labels, after);
            }
            rest = rest.strip_prefix(',').unwrap_or(rest);

            let equals = rest.find('=').unwrap();
            let key = rest[..equals].to_string();
            let mut chars = rest[equals + 1..].strip_prefix('"').unwrap().char_indices();
            let mut value = String::new();
            let consumed = loop {
                let (index, character) = chars.next().unwrap();
                match character {
                    '"' => break index + 1,
                    '\\' => match chars.next().unwrap().1 {
                        'n' => value.push('\n'),
                        escaped => value.push(escaped),
                    },
                    other => value.push(other),
                }
            };
            labels.push((key, value));
            rest = &rest[equals + 2 + consumed..];
        }
    }

    pub(crate) fn series(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
        let mut labels: Vec<(String, String)> = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        labels.sort();
        (name.to_string(), labels)
    }

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn info(commit: &str) -> LiqSample {
        LiqSample::new(
            LiqMetric::BotInfo,
            vec![("git_commit", commit.to_string())],
            1.0,
        )
        .unwrap()
    }

    fn rebalancing(symbol: &str, value: f64) -> LiqSample {
        LiqSample::new(
            LiqMetric::AssetRebalancing,
            vec![("symbol", symbol.to_string())],
            value,
        )
        .unwrap()
    }

    fn render(families: &LiqFamilies) -> String {
        let mut body = String::new();
        families.render_into(&mut body);
        body
    }

    /// Adding or removing a name must be a visible change to this list. The
    /// collector strings are pinned the same way: the capital probe will
    /// read them by value.
    #[test]
    fn catalog_matches_the_published_names_and_collectors() {
        let names: Vec<&str> = LiqMetric::ALL.iter().map(|metric| metric.name()).collect();

        assert_eq!(
            names,
            [
                "liq_bot_info",
                "liq_bot_start_timestamp_seconds",
                "liq_settings_info",
                "liq_settings_equity_target",
                "liq_settings_equity_deviation",
                "liq_settings_usdc_target",
                "liq_settings_usdc_deviation",
                "liq_settings_cash_reserved",
                "liq_settings_execution_threshold_usd",
                "liq_settings_order_polling_seconds",
                "liq_settings_inventory_poll_seconds",
                "liq_settings_deployment_block",
                "liq_asset_counter_trading",
                "liq_asset_extended_hours",
                "liq_asset_rebalancing",
                "liq_usdc_corridor_target",
                "liq_usdc_corridor_deviation",
                "liq_usdc_corridor_active",
                "liq_equity_onchain_available",
                "liq_equity_offchain_available",
                "liq_equity_inflight_total",
                "liq_equity_total",
                "liq_equity_unwrapped",
                "liq_equity_wrapped",
                "liq_equity_ratio",
                "liq_equity_chain_available",
                "liq_equity_chain_inflight",
                "liq_equity_chain_share",
                "liq_equity_chain_verdict",
                "liq_usdc_onchain_available",
                "liq_usdc_onchain_inflight",
                "liq_usdc_offchain_available",
                "liq_usdc_offchain_gross",
                "liq_usdc_offchain_inflight",
                "liq_usdc_alpaca_usdc",
                "liq_usdc_alpaca_total",
                "liq_usdc_inflight_total",
                "liq_usdc_inflight_ethereum_wallet",
                "liq_usdc_inflight_base_wallet",
                "liq_usdc_total",
                "liq_usdc_ratio",
                "liq_usdc_rebalanceable",
                "liq_usdc_chain_available",
                "liq_usdc_chain_inflight",
                "liq_usdc_chain_ratio",
                "liq_position_last_price_usd",
                "liq_equity_exposure_usd",
                "liq_hedge_latency_ms",
                "liq_hedge_latency_ms_samples",
                "liq_open_exposure_fill_count",
                "liq_open_exposure_oldest_ts_seconds",
                "liq_reliability_log_count_24h",
                "liq_log_target_count_24h",
                "liq_failure_event_count_24h",
                "liq_job_queue",
                "liq_block_lag_blocks",
                "liq_block_lag_sampled_ts_seconds",
                "liq_poll_cycles_24h",
                "liq_poll_errors_24h",
                "liq_poll_skipped_ticks_24h",
                "liq_poll_duration_ms",
                "liq_dependency_calls_24h",
                "liq_dependency_errors_24h",
                "liq_dependency_latency_ms",
                "liq_rebalance_stage_ms",
                "liq_attestation_last_ms",
                "liq_pnl_summary_usd",
                "liq_pnl_summary_shares",
                "liq_pnl_summary_count",
                "liq_pnl_cost_usd",
                "liq_pnl_revenue_usd",
                "liq_pnl_cost_entries",
                "liq_pnl_cost_missing_observations",
                "liq_pnl_cost_coverage",
                "liq_pnl_capital_avg_deployed_usd",
                "liq_pnl_capital_annualized_return_pct",
                "liq_pnl_capital_coverage_days",
                "liq_pnl_capital_sample_days",
                "liq_pnl_symbol_usd",
                "liq_pnl_symbol_shares",
                "liq_pnl_symbol_lots",
                "liq_pnl_symbol_volume_shares",
                "liq_pnl_sample_total_fills",
                "liq_pnl_sample_symbols",
                "liq_pnl_sample_first_ts_seconds",
                "liq_pnl_sample_last_ts_seconds",
                "liq_pnl_warnings",
                "liq_pnl_day_usd",
                "liq_pnl_day_cum_usd",
                "liq_pnl_day_stream_usd",
                "liq_pnl_day_cum_stream_usd",
                "liq_pending_orders",
                "liq_pending_orders_total",
                "liq_pending_orders_uncapped_total",
                "liq_raindex_orders_total",
                "liq_raindex_orders_unavailable",
                "liq_collector_last_success_ts_seconds",
            ]
        );
        assert_eq!(
            [
                LiqFamily::Health,
                LiqFamily::Settings,
                LiqFamily::Inventory,
                LiqFamily::Prices,
                LiqFamily::EquityBands,
                LiqFamily::Latencies,
                LiqFamily::Logs,
                LiqFamily::Reliability,
                LiqFamily::Infra,
                LiqFamily::Rebalances,
                LiqFamily::PendingOrders,
                LiqFamily::Raindex,
            ]
            .map(LiqFamily::collector),
            [
                "health",
                "settings",
                "inventory",
                "prices",
                "equity_bands",
                "latencies",
                "logs",
                "reliability",
                "infra",
                "rebalances",
                "pending_orders",
                "raindex",
            ]
        );
        assert_eq!(
            [
                PnlWindowKey::OneDay,
                PnlWindowKey::OneWeek,
                PnlWindowKey::OneMonth,
                PnlWindowKey::YearToDate,
                PnlWindowKey::OneYear,
                PnlWindowKey::All,
            ]
            .map(|window| LiqFamily::Pnl(window).collector()),
            ["pnl_1d", "pnl_1w", "pnl_1m", "pnl_ytd", "pnl_1y", "pnl_all"]
        );
    }

    /// Every `liq_pnl_*` name is published per window and no other name is,
    /// so the PnL names are exactly the ones a window family owns.
    #[test]
    fn only_the_pnl_names_are_published_per_window() {
        let per_window: Vec<&str> = LiqMetric::ALL
            .into_iter()
            .filter(|metric| metric.per_pnl_window())
            .map(LiqMetric::name)
            .collect();
        let pnl: Vec<&str> = LiqMetric::ALL
            .into_iter()
            .map(LiqMetric::name)
            .filter(|name| name.starts_with("liq_pnl_"))
            .collect();

        assert_eq!(per_window.len(), 25);
        assert_eq!(per_window, pnl);
        for metric in LiqMetric::ALL {
            if metric.per_pnl_window() {
                assert_eq!(metric.family(), None, "{}", metric.name());
            }
        }
    }

    #[test]
    fn every_metric_has_a_valid_name_help_and_sorted_label_keys() {
        for metric in LiqMetric::ALL {
            let name = metric.name();
            assert!(name.starts_with("liq_"), "{name}");
            assert!(
                name.chars().all(|character| character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character == '_'),
                "{name}"
            );
            assert!(!metric.help().is_empty(), "{name}");
            assert!(!metric.help().contains(['\n', '\\']), "{name}");
            assert!(metric.label_keys().is_sorted(), "{name}");
        }
    }

    #[test]
    fn strip_prefix_removes_the_tokenized_prefix_only_before_an_uppercase_letter() {
        let cases = [
            ("tAAPL", "AAPL"),
            ("wtAAPL", "AAPL"),
            ("t", "t"),
            ("tsla", "tsla"),
            ("TSLA", "TSLA"),
            ("wt", "wt"),
            ("wtx", "wtx"),
            ("ttAAPL", "ttAAPL"),
        ];

        for (symbol, expected) in cases {
            assert_eq!(strip_prefix(symbol), expected, "strip_prefix({symbol})");
        }
    }

    #[test]
    fn float_value_converts_exact_decimals() {
        for (decimal, expected) in [("0.5", 0.5), ("1234.25", 1234.25), ("-3", -3.0)] {
            let float = Float::parse(decimal.to_string()).unwrap();
            assert_eq!(float_value(float).ok(), Some(expected), "{decimal}");
        }
    }

    #[test]
    fn integer_value_refuses_integers_beyond_exact_f64_range() {
        assert_eq!(integer_value(12_345_678).ok(), Some(12_345_678.0));
        assert_eq!(integer_value(1 << 53).ok(), Some(9_007_199_254_740_992.0));
        assert!(matches!(
            integer_value((1 << 53) + 1),
            Err(LiqValueError::Inexact(9_007_199_254_740_993))
        ));
    }

    #[test]
    fn signed_count_and_timestamp_values_are_exact() {
        assert_eq!(signed_integer_value(-12).ok(), Some(-12.0));
        assert_eq!(signed_integer_value(i64::MIN).ok(), None);
        assert_eq!(count_value(42).ok(), Some(42.0));

        let at = DateTime::parse_from_rfc3339("2026-03-01T12:30:15.250Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timestamp_value(at).ok(), Some(1_772_368_215.25));
    }

    #[test]
    fn wire_name_reads_the_serde_name() {
        assert_eq!(
            wire_name(&st0x_dto::ChainName::HyperEvm).unwrap(),
            "hyperevm"
        );
        assert_eq!(
            wire_name(&st0x_dto::FailureEventType::OffchainOrderFailed).unwrap(),
            "OffchainOrderEvent::Failed"
        );
        assert!(matches!(wire_name(&3), Err(WireNameError::NotAString)));
    }

    #[test]
    fn sample_rejects_non_finite_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = LiqSample::new(LiqMetric::SettingsEquityTarget, vec![], value).unwrap_err();
            assert!(matches!(
                error,
                LiqSampleError::NonFinite {
                    metric: LiqMetric::SettingsEquityTarget,
                    ..
                }
            ));
        }
    }

    #[test]
    fn sample_rejects_label_keys_the_metric_does_not_take() {
        let error = LiqSample::new(
            LiqMetric::AssetRebalancing,
            vec![("chain", "base".to_string())],
            1.0,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LiqSampleError::LabelKeys {
                metric: LiqMetric::AssetRebalancing,
                ref keys,
            } if keys == &["chain"]
        ));

        let error = LiqSample::new(LiqMetric::AssetRebalancing, vec![], 1.0).unwrap_err();
        assert!(matches!(error, LiqSampleError::LabelKeys { .. }));
    }

    #[test]
    fn empty_store_renders_nothing() {
        assert_eq!(render(&LiqFamilies::default()), "");
    }

    #[test]
    fn render_groups_by_name_sorts_labels_and_writes_help_and_gauge_type_once() {
        let families = LiqFamilies::default();
        families.replace(
            LiqFamily::Settings,
            vec![
                rebalancing("TSLA", 0.0),
                LiqSample::new(LiqMetric::SettingsEquityDeviation, vec![], 0.2).unwrap(),
                rebalancing("AAPL", 1.0),
            ],
            at(1_700_000_000),
        );
        families.replace(LiqFamily::Health, vec![info("abc")], at(1_700_000_005));

        assert_eq!(
            render(&families),
            "# HELP liq_asset_rebalancing 1 when the symbol starts new rebalancing operations, \
             else 0\n\
             # TYPE liq_asset_rebalancing gauge\n\
             liq_asset_rebalancing{symbol=\"AAPL\"} 1\n\
             liq_asset_rebalancing{symbol=\"TSLA\"} 0\n\
             # HELP liq_bot_info Always 1; git_commit is the first 12 characters of the running \
             build\n\
             # TYPE liq_bot_info gauge\n\
             liq_bot_info{git_commit=\"abc\"} 1\n\
             # HELP liq_collector_last_success_ts_seconds Unix time each liq_ collector last \
             published its family\n\
             # TYPE liq_collector_last_success_ts_seconds gauge\n\
             liq_collector_last_success_ts_seconds{collector=\"health\"} 1700000005\n\
             liq_collector_last_success_ts_seconds{collector=\"settings\"} 1700000000\n\
             # HELP liq_settings_equity_deviation Equity rebalancing band half-width\n\
             # TYPE liq_settings_equity_deviation gauge\n\
             liq_settings_equity_deviation 0.2\n"
        );
    }

    #[test]
    fn replace_drops_the_previous_samples_of_the_family() {
        let families = LiqFamilies::default();
        families.replace(
            LiqFamily::Settings,
            vec![rebalancing("AAPL", 1.0), rebalancing("TSLA", 1.0)],
            at(10),
        );
        families.replace(LiqFamily::Settings, vec![rebalancing("AAPL", 0.0)], at(20));

        assert_eq!(
            parse_exposition(&render(&families)),
            BTreeMap::from([
                (series("liq_asset_rebalancing", &[("symbol", "AAPL")]), 0.0),
                (
                    series(
                        "liq_collector_last_success_ts_seconds",
                        &[("collector", "settings")]
                    ),
                    20.0
                ),
            ])
        );
    }

    #[test]
    fn a_family_replaced_to_empty_keeps_only_its_freshness_series() {
        let families = LiqFamilies::default();
        families.replace(LiqFamily::Settings, vec![rebalancing("AAPL", 1.0)], at(10));
        families.replace(LiqFamily::Settings, vec![], at(30));

        assert_eq!(
            parse_exposition(&render(&families)),
            BTreeMap::from([(
                series(
                    "liq_collector_last_success_ts_seconds",
                    &[("collector", "settings")]
                ),
                30.0
            )])
        );
    }

    fn onchain(symbol: &str, value: f64) -> LiqSample {
        LiqSample::new(
            LiqMetric::EquityOnchainAvailable,
            vec![("symbol", symbol.to_string())],
            value,
        )
        .unwrap()
    }

    #[test]
    fn replace_at_generation_ignores_a_read_older_than_the_stored_one() {
        let families = LiqFamilies::default();
        let key = series("liq_equity_onchain_available", &[("symbol", "AAPL")]);
        let freshness = series(
            "liq_collector_last_success_ts_seconds",
            &[("collector", "inventory")],
        );

        assert!(families.replace_at_generation(
            LiqFamily::Inventory,
            5,
            vec![onchain("AAPL", 5.0)],
            at(50),
        ));
        assert!(!families.replace_at_generation(
            LiqFamily::Inventory,
            4,
            vec![onchain("AAPL", 4.0)],
            at(60),
        ));

        let rendered = parse_exposition(&render(&families));
        assert_eq!(rendered.get(&key), Some(&5.0));
        assert_eq!(rendered.get(&freshness), Some(&50.0));

        assert!(families.replace_at_generation(
            LiqFamily::Inventory,
            5,
            vec![onchain("AAPL", 5.5)],
            at(70),
        ));
        assert!(families.replace_at_generation(
            LiqFamily::Inventory,
            6,
            vec![onchain("AAPL", 6.0)],
            at(80),
        ));

        let rendered = parse_exposition(&render(&families));
        assert_eq!(rendered.get(&key), Some(&6.0));
        assert_eq!(rendered.get(&freshness), Some(&80.0));
    }

    #[test]
    fn replace_keeps_the_first_of_duplicate_series() {
        let families = LiqFamilies::default();
        families.replace(
            LiqFamily::Settings,
            vec![rebalancing("AAPL", 1.0), rebalancing("AAPL", 0.0)],
            at(10),
        );

        let rendered = parse_exposition(&render(&families));
        assert_eq!(
            rendered.get(&series("liq_asset_rebalancing", &[("symbol", "AAPL")])),
            Some(&1.0)
        );
    }

    #[test]
    fn replace_drops_samples_of_names_another_family_owns() {
        let families = LiqFamilies::default();
        families.replace(
            LiqFamily::Health,
            vec![info("abc"), rebalancing("AAPL", 1.0)],
            at(10),
        );

        let rendered = render(&families);
        assert!(rendered.contains("liq_bot_info{git_commit=\"abc\"} 1\n"));
        assert!(!rendered.contains("liq_asset_rebalancing"), "{rendered}");
    }

    #[test]
    fn render_skips_names_the_recorder_already_rendered() {
        let families = LiqFamilies::default();
        families.replace(LiqFamily::Health, vec![info("abc")], at(10));
        let recorder = "# TYPE liq_bot_info gauge\nliq_bot_info 3\nhedge_trades_total 1";
        let mut body = recorder.to_string();

        families.render_into(&mut body);

        assert_eq!(
            body,
            "# TYPE liq_bot_info gauge\nliq_bot_info 3\nhedge_trades_total 1\n\
             # HELP liq_collector_last_success_ts_seconds Unix time each liq_ collector last \
             published its family\n\
             # TYPE liq_collector_last_success_ts_seconds gauge\n\
             liq_collector_last_success_ts_seconds{collector=\"health\"} 10\n"
        );
    }

    #[test]
    fn render_escapes_backslash_quote_and_newline_in_label_values() {
        let families = LiqFamilies::default();
        families.replace(LiqFamily::Health, vec![info("a\\b\"c\nd")], at(10));

        let rendered = render(&families);
        assert!(
            rendered.contains("liq_bot_info{git_commit=\"a\\\\b\\\"c\\nd\"} 1\n"),
            "{rendered}"
        );
    }

    /// The render formats a snapshot of `Arc`s taken under the lock, so a
    /// replace that lands mid-render neither waits for it nor changes it.
    #[test]
    fn render_formats_a_snapshot_taken_before_a_concurrent_replace() {
        let families = LiqFamilies::default();
        families.replace(LiqFamily::Settings, vec![rebalancing("AAPL", 1.0)], at(10));

        let snapshot = families.snapshot();
        families.replace(LiqFamily::Settings, vec![rebalancing("AAPL", 0.0)], at(20));
        let mut before = String::new();
        render_snapshot(&snapshot, &mut before);

        let before = parse_exposition(&before);
        let after = parse_exposition(&render(&families));
        let key = series("liq_asset_rebalancing", &[("symbol", "AAPL")]);
        assert_eq!(before.get(&key), Some(&1.0));
        assert_eq!(after.get(&key), Some(&0.0));
    }

    #[test]
    fn a_large_family_renders_while_another_thread_replaces() {
        let families = Arc::new(LiqFamilies::default());
        let large: Vec<LiqSample> = (0..30_000)
            .map(|index| rebalancing(&format!("SYM{index}"), 1.0))
            .collect();
        families.replace(LiqFamily::Settings, large, at(10));

        let writer = {
            let families = Arc::clone(&families);
            std::thread::spawn(move || {
                for commit in 0..200 {
                    families.replace(LiqFamily::Health, vec![info(&commit.to_string())], at(20));
                }
            })
        };
        let rendered = parse_exposition(&render(&families));
        writer.join().unwrap();

        let rebalancing_series = rendered
            .keys()
            .filter(|(name, _)| name == "liq_asset_rebalancing")
            .count();
        assert_eq!(rebalancing_series, 30_000);
        let last = parse_exposition(&render(&families));
        assert_eq!(
            last.get(&series("liq_bot_info", &[("git_commit", "199")])),
            Some(&1.0)
        );
    }

    proptest! {
        #[test]
        fn any_label_value_round_trips_through_the_render(
            commit in any::<String>(),
            value in -1.0e15f64..1.0e15,
        ) {
            let families = LiqFamilies::default();
            families.replace(
                LiqFamily::Health,
                vec![LiqSample::new(LiqMetric::BotInfo, vec![("git_commit", commit.clone())], value)
                    .unwrap()],
                at(10),
            );

            let rendered = parse_exposition(&render(&families));
            prop_assert_eq!(
                rendered.get(&series("liq_bot_info", &[("git_commit", &commit)])),
                Some(&value)
            );
        }
    }
}
