//! Serializable DTOs for the backend PnL report endpoint.
use num_decimal::Num;
use serde::{Serialize, Serializer};
use st0x_finance::Symbol;

use super::costs::CostEntryInternal;
use super::parsing::fmt_decimal;
use super::state::{Direction, PnlBucket, Venue};

fn serialize_decimal<S>(value: &Num, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&fmt_decimal(value))
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with functions receive field values by reference"
)]
fn serialize_pnl_bucket<S>(value: &PnlBucket, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(value.as_str())
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with functions receive field values by reference"
)]
fn serialize_venue<S>(value: &Venue, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(value.as_str())
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with functions receive field values by reference"
)]
fn serialize_direction<S>(value: &Direction, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(value.as_str())
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlResponse {
    pub(crate) attribution_method: &'static str,
    pub(crate) as_of_rowid: i64,
    pub(crate) warnings: Vec<String>,
    pub(crate) available_range: PnlAvailableRange,
    pub(crate) sample_stats: PnlSampleStats,
    pub(crate) summary: PnlSummary,
    pub(crate) costs: PnlCostSummary,
    pub(crate) capital: PnlCapitalSummary,
    pub(crate) symbols: Vec<PnlSymbolSummary>,
    pub(crate) symbol_universe: Vec<Symbol>,
    pub(crate) entries: Vec<PnlEntry>,
    pub(crate) cost_entries: Vec<PnlCostEntry>,
    pub(crate) total: usize,
    pub(crate) has_more: bool,
    pub(crate) windows: Vec<PnlWindow>,
}

/// Capital and return-on-capital figures derived from persisted daily
/// portfolio snapshots. Every field is `None`/empty when capital could not be
/// computed for the query (see the accompanying `warnings` entries for why --
/// symbol-filtered queries, no snapshot coverage in range, missing/stale marks,
/// or zero average deployed capital).
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlCapitalSummary {
    pub(crate) average_deployed_capital_usd: Option<String>,
    pub(crate) annualized_return_pct: Option<String>,
    pub(crate) coverage_days: Option<i64>,
    pub(crate) sample_days: usize,
    pub(crate) first_snapshot_day: Option<String>,
    pub(crate) last_snapshot_day: Option<String>,
    pub(crate) excluded_days: Vec<PnlCapitalExcludedDay>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlCapitalExcludedDay {
    pub(crate) et_day: String,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlAvailableRange {
    pub(crate) first_at: Option<String>,
    pub(crate) last_at: Option<String>,
    pub(crate) first_date: Option<String>,
    pub(crate) last_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlSummary {
    pub(crate) counter_trade_pnl_usd: String,
    pub(crate) onchain_netting_pnl_usd: String,
    pub(crate) directional_inventory_baseline_pnl_usd: String,
    pub(crate) directional_imbalance_excess_pnl_usd: String,
    pub(crate) directional_exposure_pnl_usd: String,
    pub(crate) total_pnl_usd: String,
    pub(crate) gross_realized_pnl_usd: String,
    pub(crate) tracked_costs_usd: String,
    pub(crate) tracked_revenue_usd: String,
    pub(crate) net_realized_pnl_usd: String,
    pub(crate) realized_pnl_usd: String,
    pub(crate) matched_shares: String,
    pub(crate) onchain_notional_usd: String,
    pub(crate) offchain_notional_usd: String,
    pub(crate) inventory_drift_shares: String,
    pub(crate) inventory_drift_usd: String,
    pub(crate) open_long_shares: String,
    pub(crate) open_short_shares: String,
    pub(crate) unmatched_offchain_shares: String,
    pub(crate) unmatched_offchain_notional_usd: String,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) matched_lot_count: usize,
    pub(crate) open_lot_count: usize,
    pub(crate) unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlSymbolSummary {
    pub(crate) symbol: Symbol,
    pub(crate) counter_trade_pnl_usd: String,
    pub(crate) onchain_netting_pnl_usd: String,
    pub(crate) directional_inventory_baseline_pnl_usd: String,
    pub(crate) directional_imbalance_excess_pnl_usd: String,
    pub(crate) directional_exposure_pnl_usd: String,
    pub(crate) total_pnl_usd: String,
    pub(crate) gross_realized_pnl_usd: String,
    pub(crate) tracked_costs_usd: String,
    pub(crate) tracked_revenue_usd: String,
    pub(crate) net_realized_pnl_usd: String,
    pub(crate) realized_pnl_usd: String,
    pub(crate) matched_shares: String,
    pub(crate) inventory_drift_shares: String,
    pub(crate) inventory_drift_usd: String,
    pub(crate) open_long_shares: String,
    pub(crate) open_short_shares: String,
    pub(crate) unmatched_offchain_shares: String,
    pub(crate) matched_lot_count: usize,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlEntry {
    pub(crate) symbol: Symbol,
    #[serde(serialize_with = "serialize_pnl_bucket")]
    pub(crate) pnl_bucket: PnlBucket,
    pub(crate) matched_at: String,
    pub(crate) opened_at: String,
    pub(crate) closed_at: String,
    pub(crate) opening_fill_id: String,
    pub(crate) closing_fill_id: String,
    pub(crate) opening_rowid: i64,
    pub(crate) closing_rowid: i64,
    #[serde(serialize_with = "serialize_venue")]
    pub(crate) opening_venue: Venue,
    #[serde(serialize_with = "serialize_venue")]
    pub(crate) closing_venue: Venue,
    #[serde(serialize_with = "serialize_direction")]
    pub(crate) opening_direction: Direction,
    #[serde(serialize_with = "serialize_direction")]
    pub(crate) closing_direction: Direction,
    #[serde(serialize_with = "serialize_decimal")]
    pub(crate) opening_price_usd: Num,
    #[serde(serialize_with = "serialize_decimal")]
    pub(crate) closing_price_usd: Num,
    pub(crate) onchain_trade_id: String,
    pub(crate) offchain_order_id: String,
    pub(crate) onchain_direction: String,
    pub(crate) offchain_direction: String,
    #[serde(serialize_with = "serialize_decimal")]
    pub(crate) shares: Num,
    pub(crate) onchain_price_usdc: String,
    pub(crate) offchain_price_usd: String,
    #[serde(serialize_with = "serialize_decimal")]
    pub(crate) spread_usd: Num,
    #[serde(serialize_with = "serialize_decimal")]
    pub(crate) realized_pnl_usd: Num,
    pub(crate) elapsed_seconds: i64,
    pub(crate) counter_trade_threshold_seconds: i64,
    pub(crate) delayed_counter_trade: bool,
    pub(crate) attribution_method: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlSampleSymbolStats {
    pub(crate) symbol: Symbol,
    pub(crate) first_at: Option<String>,
    pub(crate) last_at: Option<String>,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) total_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlSampleStats {
    pub(crate) first_at: Option<String>,
    pub(crate) last_at: Option<String>,
    pub(crate) symbol_count: usize,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) total_fill_count: usize,
    pub(crate) symbols: Vec<PnlSampleSymbolStats>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlCostEntry {
    pub(crate) category: &'static str,
    pub(crate) accounting_bucket: &'static str,
    pub(crate) effect: &'static str,
    pub(crate) amount_usd: String,
    pub(crate) occurred_at: String,
    pub(crate) aggregate_type: String,
    pub(crate) aggregate_id: String,
    pub(crate) event_rowid: i64,
    pub(crate) symbol: Option<Symbol>,
    pub(crate) detail: String,
}

impl From<&CostEntryInternal> for PnlCostEntry {
    fn from(value: &CostEntryInternal) -> Self {
        Self {
            category: value.category.as_str(),
            accounting_bucket: value.accounting_bucket.as_str(),
            effect: value.effect.as_str(),
            amount_usd: fmt_decimal(&value.amount_usd),
            occurred_at: value.occurred_at.clone(),
            aggregate_type: value.aggregate_type.clone(),
            aggregate_id: value.aggregate_id.clone(),
            event_rowid: value.event_rowid,
            symbol: value.symbol.clone(),
            detail: value.detail.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlCostCoverage {
    pub(crate) source: &'static str,
    pub(crate) accounting_bucket: &'static str,
    pub(crate) effect: &'static str,
    pub(crate) status: &'static str,
    pub(crate) amount_usd: String,
    pub(crate) note: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlCostSummary {
    pub(crate) total_tracked_costs_usd: String,
    pub(crate) total_tracked_revenue_usd: String,
    pub(crate) counter_trade_costs_usd: String,
    pub(crate) onchain_netting_costs_usd: String,
    pub(crate) directional_exposure_costs_usd: String,
    pub(crate) generic_costs_usd: String,
    pub(crate) dividend_revenue_usd: String,
    pub(crate) offchain_execution_fees_usd: String,
    pub(crate) tokenization_fees_usd: String,
    pub(crate) cctp_fees_usd: String,
    pub(crate) conversion_slippage_usd: String,
    pub(crate) oracle_write_cost_usd: String,
    pub(crate) broker_fees_usd: String,
    pub(crate) regulatory_fees_usd: String,
    pub(crate) margin_interest_usd: String,
    pub(crate) bot_gas_usd: String,
    pub(crate) wallet_transfer_fees_usd: String,
    pub(crate) unclassified_costs_usd: String,
    pub(crate) cost_entry_count: usize,
    pub(crate) missing_cost_observation_count: usize,
    pub(crate) coverage: Vec<PnlCostCoverage>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlWindowSymbol {
    pub(crate) symbol: Symbol,
    pub(crate) counter_trade_pnl_usd: String,
    pub(crate) onchain_netting_pnl_usd: String,
    pub(crate) directional_inventory_baseline_pnl_usd: String,
    pub(crate) directional_imbalance_excess_pnl_usd: String,
    pub(crate) directional_exposure_pnl_usd: String,
    pub(crate) total_pnl_usd: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlWindow {
    pub(crate) window_id: String,
    pub(crate) start_at: String,
    pub(crate) end_at: String,
    pub(crate) label: String,
    pub(crate) is_weekend: bool,
    pub(crate) market_session: String,
    pub(crate) counter_trading_session: String,
    pub(crate) granularity: &'static str,
    pub(crate) symbols: Vec<PnlWindowSymbol>,
}
