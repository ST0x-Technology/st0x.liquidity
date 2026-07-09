//! Backend PnL report builder for the dashboard.

mod builder;
mod costs;
mod diagnostics;
mod parsing;
mod query;
mod replay;
mod response;
mod samples;
mod sessions;
mod source;
mod state;
mod windows;

#[cfg(test)]
mod tests;

pub(crate) use query::{PnlError, PnlQuery};
pub(crate) use response::PnlResponse;
pub(crate) use source::{build_pnl_report, validate_pnl_snapshot_rowid};

const ATTRIBUTION_METHOD: &str = "backend_position_fill_replay_fifo";
const COUNTER_TRADE_THRESHOLD_SECONDS: i64 = 300;
const SAFE_SYMBOL_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-";
const EPSILON: &str = "0.000001";

const ATTRIBUTION_WARNING: &str = "PnL source: realized gross replay from persisted backend fill events. Fills are ordered by \
     execution timestamp and replayed through per-symbol FIFO inventory lots for accounting and \
     attribution; explicit offchain_order_id -> onchain_trade_ids parentage is not currently \
     persisted.";
const BASELINE_WARNING: &str = "Displayed PnL is realized by lot close date from persisted fills. Baseline drift, percentage \
     return, and true period/NAV PnL require a persisted portfolio state vector, price vector, and \
     cash-flow events; those are not currently persisted, so baseline drift and percentage return \
     are not reported. Reconciliation diagnostics compare the replay against the current \
     position_view, not a historical position_view snapshot.";
const COST_WARNING: &str = "Tracked costs and revenues are built bottom-up by economic bucket. On-chain netting and raw \
     directional drift have no direct bot-paid execution cost. USD and USDC are treated as \
     equivalent reporting currency, so USD/USDC conversion basis is not modeled as PnL; only \
     explicit persisted fees are deducted. Persisted SQLite costs currently include tokenization \
     fees and CCTP fees. Alpaca account fees, fee credits, margin interest, dividends, and \
     capital-gain distributions are fetched live from Alpaca account activities by activity \
     creation-time window and then filtered locally by report date; asOfRowid snapshots persisted \
     SQLite events only, so exact broker-ledger point-in-time coverage requires a persisted Alpaca \
     activity ledger. Oracle write cost is zero for the current setup. Wallet \
     transfer fees and bot gas require additional ledger/receipt ingestion before they can be included.";
