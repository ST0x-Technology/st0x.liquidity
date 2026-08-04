//! Internal reporting state for the backend PnL replay ledger.
use rain_math_float::Float;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

use st0x_finance::Symbol;
use st0x_float_macro::float;

use super::response::{PnlSummary, PnlSymbolSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Buy,
    Sell,
}

impl Direction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
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
    Manual,
}

impl Venue {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Onchain => "onchain",
            Self::Offchain => "offchain",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Fill {
    pub(crate) rowid: i64,
    pub(crate) id: String,
    pub(crate) symbol: Symbol,
    pub(crate) shares: Float,
    pub(crate) direction: Direction,
    pub(crate) price: Float,
    pub(crate) executed_at: String,
    pub(crate) venue: Venue,
}

#[derive(Debug, Clone)]
pub(crate) struct Lot {
    pub(crate) trade_id: String,
    pub(crate) side: LotSide,
    pub(crate) remaining_shares: Float,
    pub(crate) price: Float,
    pub(crate) opened_at: String,
    pub(crate) opened_rowid: i64,
    pub(crate) opened_venue: Venue,
}

#[derive(Debug, Clone)]
pub(crate) struct SummaryAcc {
    pub(crate) counter_trade_pnl_usd: Float,
    pub(crate) onchain_netting_pnl_usd: Float,
    pub(crate) directional_inventory_baseline_pnl_usd: Float,
    pub(crate) directional_imbalance_excess_pnl_usd: Float,
    pub(crate) directional_exposure_pnl_usd: Float,
    pub(crate) realized_pnl_usd: Float,
    pub(crate) matched_shares: Float,
    pub(crate) onchain_notional_usd: Float,
    pub(crate) offchain_notional_usd: Float,
    pub(crate) open_long_shares: Float,
    pub(crate) open_short_shares: Float,
    pub(crate) open_long_notional_usd: Float,
    pub(crate) open_short_notional_usd: Float,
    pub(crate) unmatched_offchain_buy_shares: Float,
    pub(crate) unmatched_offchain_sell_shares: Float,
    pub(crate) unmatched_offchain_buy_notional_usd: Float,
    pub(crate) unmatched_offchain_sell_notional_usd: Float,
    pub(crate) onchain_fill_count: usize,
    pub(crate) offchain_fill_count: usize,
    pub(crate) matched_lot_count: usize,
    pub(crate) open_lot_count: usize,
    pub(crate) unmatched_offchain_fill_count: usize,
}

/// `Float` has no meaningful derived `Default`, and `Float::zero()` is
/// fallible because it evaluates in the EVM. `float!(0)` resolves the same
/// zero at compile time, so the accumulator stays infallibly constructible
/// and `entry().or_default()` keeps working across the replay.
impl Default for SummaryAcc {
    fn default() -> Self {
        Self {
            counter_trade_pnl_usd: float!(0),
            onchain_netting_pnl_usd: float!(0),
            directional_inventory_baseline_pnl_usd: float!(0),
            directional_imbalance_excess_pnl_usd: float!(0),
            directional_exposure_pnl_usd: float!(0),
            realized_pnl_usd: float!(0),
            matched_shares: float!(0),
            onchain_notional_usd: float!(0),
            offchain_notional_usd: float!(0),
            open_long_shares: float!(0),
            open_short_shares: float!(0),
            open_long_notional_usd: float!(0),
            open_short_notional_usd: float!(0),
            unmatched_offchain_buy_shares: float!(0),
            unmatched_offchain_sell_shares: float!(0),
            unmatched_offchain_buy_notional_usd: float!(0),
            unmatched_offchain_sell_notional_usd: float!(0),
            onchain_fill_count: 0,
            offchain_fill_count: 0,
            matched_lot_count: 0,
            open_lot_count: 0,
            unmatched_offchain_fill_count: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SymbolBook {
    pub(crate) long_lots: VecDeque<Lot>,
    pub(crate) short_lots: VecDeque<Lot>,
    pub(crate) seen_onchain_fill_ids: HashSet<String>,
    pub(crate) seen_offchain_placement_ids: HashSet<String>,
    pub(crate) seen_offchain_fill_ids: HashSet<String>,
    pub(crate) original_onchain_shares: HashMap<String, Float>,
    pub(crate) matched_onchain_shares: HashMap<String, Float>,
    pub(crate) last_price_usdc: Option<Float>,
    pub(crate) summary: SummaryAcc,
}

#[derive(Debug, Clone)]
pub(crate) struct UnmatchedOffchainAllocation {
    pub(crate) symbol: Symbol,
    pub(crate) fill_id: String,
    pub(crate) shares: Float,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionReplayDelta {
    pub(crate) symbol: Symbol,
    pub(crate) replay_net: Float,
    pub(crate) position_net: Float,
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

#[derive(Debug, Clone)]
pub(crate) struct BotGasCostRow {
    pub(crate) rowid: i64,
    pub(crate) chain: String,
    pub(crate) tx_hash: String,
    pub(crate) usd_cost: String,
    pub(crate) operation_category: String,
    pub(crate) symbol: Option<String>,
    pub(crate) occurred_at: String,
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
