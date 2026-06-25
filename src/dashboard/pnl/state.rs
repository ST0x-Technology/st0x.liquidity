use std::collections::{HashMap, HashSet};

use num_decimal::Num;
use serde_json::Value;

use super::response::{PnlSummary, PnlSymbolSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LotSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PnlBucket {
    CounterTrade,
    OnchainNetting,
    DirectionalExposure,
}

impl PnlBucket {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CounterTrade => "counter_trade",
            Self::OnchainNetting => "onchain_netting",
            Self::DirectionalExposure => "directional_exposure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Venue {
    Onchain,
    Offchain,
}

impl Venue {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Onchain => "onchain",
            Self::Offchain => "offchain",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Fill {
    pub(crate) rowid: i64,
    pub(crate) id: String,
    pub(crate) symbol: String,
    pub(crate) shares: Num,
    pub(crate) direction: Direction,
    pub(crate) price: Num,
    pub(crate) executed_at: String,
    pub(crate) venue: Venue,
}

#[derive(Debug, Clone)]
pub(crate) struct Lot {
    pub(crate) trade_id: String,
    pub(crate) side: LotSide,
    pub(crate) remaining_shares: Num,
    pub(crate) price: Num,
    pub(crate) opened_at: String,
    pub(crate) opened_rowid: i64,
    pub(crate) opened_venue: Venue,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SummaryAcc {
    pub(crate) counter_trade_pnl_usd: Num,
    pub(crate) onchain_netting_pnl_usd: Num,
    pub(crate) directional_inventory_baseline_pnl_usd: Num,
    pub(crate) directional_imbalance_excess_pnl_usd: Num,
    pub(crate) directional_exposure_pnl_usd: Num,
    pub(crate) realized_pnl_usd: Num,
    pub(crate) matched_shares: Num,
    pub(crate) onchain_notional_usd: Num,
    pub(crate) offchain_notional_usd: Num,
    pub(crate) open_long_shares: Num,
    pub(crate) open_short_shares: Num,
    pub(crate) open_long_notional_usd: Num,
    pub(crate) open_short_notional_usd: Num,
    pub(crate) unmatched_offchain_buy_shares: Num,
    pub(crate) unmatched_offchain_sell_shares: Num,
    pub(crate) unmatched_offchain_buy_notional_usd: Num,
    pub(crate) unmatched_offchain_sell_notional_usd: Num,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) matched_lot_count: usize,
    pub(crate) open_lot_count: usize,
    pub(crate) unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SymbolBook {
    pub(crate) long_lots: Vec<Lot>,
    pub(crate) short_lots: Vec<Lot>,
    pub(crate) seen_onchain_fill_ids: HashSet<String>,
    pub(crate) seen_offchain_placement_ids: HashSet<String>,
    pub(crate) seen_offchain_fill_ids: HashSet<String>,
    pub(crate) original_onchain_shares: HashMap<String, Num>,
    pub(crate) matched_onchain_shares: HashMap<String, Num>,
    pub(crate) summary: SummaryAcc,
}

#[derive(Debug, Clone)]
pub(crate) struct UnmatchedOffchainAllocation {
    pub(crate) symbol: String,
    pub(crate) fill_id: String,
    pub(crate) shares: Num,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionReplayDelta {
    pub(crate) symbol: String,
    pub(crate) replay_net: Num,
    pub(crate) position_net: Num,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionEventRow {
    pub(crate) rowid: i64,
    pub(crate) symbol: String,
    pub(crate) event_type: String,
    pub(crate) payload: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionViewRow {
    pub(crate) symbol: String,
    pub(crate) net_position: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CostEventRow {
    pub(crate) rowid: i64,
    pub(crate) aggregate_type: String,
    pub(crate) aggregate_id: String,
    pub(crate) event_type: String,
    pub(crate) payload: Value,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SampleStatsAcc {
    pub(crate) first_at: Option<String>,
    pub(crate) last_at: Option<String>,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
}

pub(crate) struct SummaryAndSymbols {
    pub(crate) summary: PnlSummary,
    pub(crate) symbols: Vec<PnlSymbolSummary>,
}
