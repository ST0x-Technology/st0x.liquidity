use serde::Serialize;

use super::costs::CostEntryInternal;
use super::parsing::fmt_decimal;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlResponse {
    pub(crate) attribution_method: &'static str,
    pub(crate) warnings: Vec<String>,
    pub(crate) sample_stats: PnlSampleStats,
    pub(crate) summary: PnlSummary,
    pub(crate) costs: PnlCostSummary,
    pub(crate) symbols: Vec<PnlSymbolSummary>,
    pub(crate) symbol_universe: Vec<String>,
    pub(crate) entries: Vec<PnlEntry>,
    pub(crate) cost_entries: Vec<PnlCostEntry>,
    pub(crate) total: usize,
    pub(crate) has_more: bool,
    pub(crate) windows: Vec<PnlWindow>,
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
    pub(crate) symbol: String,
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
    pub(crate) symbol: String,
    pub(crate) pnl_bucket: &'static str,
    pub(crate) matched_at: String,
    pub(crate) opened_at: String,
    pub(crate) closed_at: String,
    pub(crate) opening_fill_id: String,
    pub(crate) closing_fill_id: String,
    pub(crate) opening_rowid: i64,
    pub(crate) closing_rowid: i64,
    pub(crate) opening_venue: &'static str,
    pub(crate) closing_venue: &'static str,
    pub(crate) opening_direction: &'static str,
    pub(crate) closing_direction: &'static str,
    pub(crate) opening_price_usd: String,
    pub(crate) closing_price_usd: String,
    pub(crate) onchain_trade_id: String,
    pub(crate) offchain_order_id: String,
    pub(crate) onchain_direction: String,
    pub(crate) offchain_direction: String,
    pub(crate) shares: String,
    pub(crate) onchain_price_usdc: String,
    pub(crate) offchain_price_usd: String,
    pub(crate) spread_usd: String,
    pub(crate) realized_pnl_usd: String,
    pub(crate) elapsed_seconds: i64,
    pub(crate) counter_trade_threshold_seconds: i64,
    pub(crate) delayed_counter_trade: bool,
    pub(crate) attribution_method: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlSampleSymbolStats {
    pub(crate) symbol: String,
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
    pub(crate) symbol: Option<String>,
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
    pub(crate) symbol: String,
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
