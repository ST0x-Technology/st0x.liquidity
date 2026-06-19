//! Backend PnL report builder for the dashboard.

#![allow(clippy::module_name_repetitions, clippy::too_many_lines)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::str::FromStr;

use chrono::{DateTime, Datelike, Days, NaiveDate, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use num_decimal::Num;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;

use st0x_execution::AccountActivity;

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
     are not reported.";
const COST_WARNING: &str = "Tracked costs and revenues are built bottom-up by economic bucket. On-chain netting and raw \
     directional drift have no direct bot-paid execution cost. USD and USDC are treated as \
     equivalent reporting currency, so USD/USDC conversion basis is not modeled as PnL; only \
     explicit persisted fees are deducted. Persisted SQLite costs currently include tokenization \
     fees and CCTP fees. Alpaca account fees, margin interest, and dividends are fetched from \
     Alpaca account activities. Oracle write cost is zero for the current setup. Wallet transfer \
     fees and bot gas require additional ledger/receipt ingestion before they can be included.";

const FEE_ACTIVITY_TYPES: &[&str] = &["FEE", "PTC"];
const FEE_REBATE_ACTIVITY_TYPES: &[&str] = &["PTR"];
const INTEREST_ACTIVITY_TYPES: &[&str] = &["INT", "INTNRA", "INTTW"];
const DIVIDEND_ACTIVITY_TYPES: &[&str] = &[
    "DIV", "DIVCGL", "DIVCGS", "DIVFEE", "DIVFT", "DIVNRA", "DIVROC", "DIVTW", "DIVTXEX",
];

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlError {
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("failed to load PnL rows: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) symbol: Option<String>,
    pub(crate) from_date: Option<String>,
    pub(crate) to_date: Option<String>,
    pub(crate) market_session_filter: Option<PnlMarketSessionFilter>,
    pub(crate) counter_trading_filter: Option<PnlCounterTradingFilter>,
}

impl PnlQuery {
    pub(crate) fn normalized_limit(&self) -> usize {
        self.limit.unwrap_or(100).min(5_000)
    }

    fn normalized_offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    pub(crate) fn activity_after(&self) -> Result<Option<DateTime<Utc>>, PnlError> {
        self.from_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(parse_query_datetime)
            .transpose()
    }

    pub(crate) fn activity_until(&self) -> Result<Option<DateTime<Utc>>, PnlError> {
        let Some(value) = self
            .to_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
            return Ok(Some(parsed.with_timezone(&Utc)));
        }

        let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| PnlError::InvalidQuery(format!("invalid date: {value}")))?;
        let until = date
            .checked_add_days(Days::new(2))
            .ok_or_else(|| PnlError::InvalidQuery(format!("invalid date: {value}")))?;
        Ok(Some(DateTime::from_naive_utc_and_offset(
            until
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| PnlError::InvalidQuery(format!("invalid date: {value}")))?,
            Utc,
        )))
    }

    fn symbol_filter(&self, warnings: &mut Vec<String>) -> BTreeSet<String> {
        let Some(raw) = &self.symbol else {
            return BTreeSet::new();
        };

        let mut symbols = BTreeSet::new();
        let mut invalid = Vec::new();
        for symbol in raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if is_safe_symbol(symbol) {
                symbols.insert(symbol.to_owned());
            } else {
                invalid.push(symbol.to_owned());
            }
        }

        if !invalid.is_empty() {
            warnings.push(format!(
                "Skipped {} invalid symbol filters in backend PnL query: {}",
                invalid.len(),
                invalid.join(", ")
            ));
        }

        symbols
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PnlMarketSessionFilter {
    All,
    Pre,
    Rth,
    Post,
    Overnight,
    Weekend,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PnlCounterTradingFilter {
    All,
    CounterTradingActive,
    CounterTradingInactive,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlResponse {
    attribution_method: &'static str,
    warnings: Vec<String>,
    sample_stats: PnlSampleStats,
    summary: PnlSummary,
    costs: PnlCostSummary,
    symbols: Vec<PnlSymbolSummary>,
    symbol_universe: Vec<String>,
    entries: Vec<PnlEntry>,
    cost_entries: Vec<PnlCostEntry>,
    total: usize,
    has_more: bool,
    windows: Vec<PnlWindow>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlSummary {
    counter_trade_pnl_usd: String,
    onchain_netting_pnl_usd: String,
    directional_inventory_baseline_pnl_usd: String,
    directional_imbalance_excess_pnl_usd: String,
    directional_exposure_pnl_usd: String,
    total_pnl_usd: String,
    gross_realized_pnl_usd: String,
    tracked_costs_usd: String,
    tracked_revenue_usd: String,
    net_realized_pnl_usd: String,
    realized_pnl_usd: String,
    matched_shares: String,
    onchain_notional_usd: String,
    offchain_notional_usd: String,
    inventory_drift_shares: String,
    inventory_drift_usd: String,
    open_long_shares: String,
    open_short_shares: String,
    unmatched_offchain_shares: String,
    unmatched_offchain_notional_usd: String,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
    matched_lot_count: usize,
    open_lot_count: usize,
    unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlSymbolSummary {
    symbol: String,
    counter_trade_pnl_usd: String,
    onchain_netting_pnl_usd: String,
    directional_inventory_baseline_pnl_usd: String,
    directional_imbalance_excess_pnl_usd: String,
    directional_exposure_pnl_usd: String,
    total_pnl_usd: String,
    gross_realized_pnl_usd: String,
    tracked_costs_usd: String,
    tracked_revenue_usd: String,
    net_realized_pnl_usd: String,
    realized_pnl_usd: String,
    matched_shares: String,
    inventory_drift_shares: String,
    inventory_drift_usd: String,
    open_long_shares: String,
    open_short_shares: String,
    unmatched_offchain_shares: String,
    matched_lot_count: usize,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
    unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlEntry {
    symbol: String,
    pnl_bucket: &'static str,
    matched_at: String,
    opened_at: String,
    closed_at: String,
    opening_fill_id: String,
    closing_fill_id: String,
    opening_rowid: i64,
    closing_rowid: i64,
    opening_venue: &'static str,
    closing_venue: &'static str,
    opening_direction: &'static str,
    closing_direction: &'static str,
    opening_price_usd: String,
    closing_price_usd: String,
    onchain_trade_id: String,
    offchain_order_id: String,
    onchain_direction: String,
    offchain_direction: String,
    shares: String,
    onchain_price_usdc: String,
    offchain_price_usd: String,
    spread_usd: String,
    realized_pnl_usd: String,
    elapsed_seconds: i64,
    counter_trade_threshold_seconds: i64,
    delayed_counter_trade: bool,
    attribution_method: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlSampleSymbolStats {
    symbol: String,
    first_at: Option<String>,
    last_at: Option<String>,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
    total_fill_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlSampleStats {
    first_at: Option<String>,
    last_at: Option<String>,
    symbol_count: usize,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
    total_fill_count: usize,
    symbols: Vec<PnlSampleSymbolStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum CostCategory {
    OffchainExecutionFee,
    TokenizationFee,
    CctpFee,
    ConversionSlippage,
    OracleWrite,
    BrokerFee,
    RegulatoryFee,
    MarginInterest,
    BotGas,
    WalletTransferFee,
    DividendIncome,
    Unclassified,
}

impl CostCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::OffchainExecutionFee => "offchain_execution_fee",
            Self::TokenizationFee => "tokenization_fee",
            Self::CctpFee => "cctp_fee",
            Self::ConversionSlippage => "conversion_slippage",
            Self::OracleWrite => "oracle_write",
            Self::BrokerFee => "broker_fee",
            Self::RegulatoryFee => "regulatory_fee",
            Self::MarginInterest => "margin_interest",
            Self::BotGas => "bot_gas",
            Self::WalletTransferFee => "wallet_transfer_fee",
            Self::DividendIncome => "dividend_income",
            Self::Unclassified => "unclassified",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountingBucket {
    CounterTrade,
    OnchainNetting,
    DirectionalExposure,
    Generic,
    DividendRevenue,
}

impl AccountingBucket {
    fn as_str(self) -> &'static str {
        match self {
            Self::CounterTrade => "counter_trade",
            Self::OnchainNetting => "onchain_netting",
            Self::DirectionalExposure => "directional_exposure",
            Self::Generic => "generic",
            Self::DividendRevenue => "dividend_revenue",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountingEffect {
    Cost,
    Revenue,
    None,
}

impl AccountingEffect {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::Revenue => "revenue",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlCostEntry {
    category: &'static str,
    accounting_bucket: &'static str,
    effect: &'static str,
    amount_usd: String,
    occurred_at: String,
    aggregate_type: String,
    aggregate_id: String,
    event_rowid: i64,
    symbol: Option<String>,
    detail: String,
}

#[derive(Debug, Clone)]
struct CostEntryInternal {
    category: CostCategory,
    accounting_bucket: AccountingBucket,
    effect: AccountingEffect,
    amount_usd: Num,
    occurred_at: String,
    aggregate_type: String,
    aggregate_id: String,
    event_rowid: i64,
    symbol: Option<String>,
    detail: String,
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
struct PnlCostCoverage {
    source: &'static str,
    accounting_bucket: &'static str,
    effect: &'static str,
    status: &'static str,
    amount_usd: String,
    note: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlCostSummary {
    total_tracked_costs_usd: String,
    total_tracked_revenue_usd: String,
    counter_trade_costs_usd: String,
    onchain_netting_costs_usd: String,
    directional_exposure_costs_usd: String,
    generic_costs_usd: String,
    dividend_revenue_usd: String,
    offchain_execution_fees_usd: String,
    tokenization_fees_usd: String,
    cctp_fees_usd: String,
    conversion_slippage_usd: String,
    oracle_write_cost_usd: String,
    broker_fees_usd: String,
    regulatory_fees_usd: String,
    margin_interest_usd: String,
    bot_gas_usd: String,
    wallet_transfer_fees_usd: String,
    unclassified_costs_usd: String,
    cost_entry_count: usize,
    missing_cost_observation_count: usize,
    coverage: Vec<PnlCostCoverage>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlWindowSymbol {
    symbol: String,
    counter_trade_pnl_usd: String,
    onchain_netting_pnl_usd: String,
    directional_inventory_baseline_pnl_usd: String,
    directional_imbalance_excess_pnl_usd: String,
    directional_exposure_pnl_usd: String,
    total_pnl_usd: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PnlWindow {
    window_id: String,
    start_at: String,
    end_at: String,
    label: String,
    is_weekend: bool,
    market_session: String,
    counter_trading_session: String,
    granularity: &'static str,
    symbols: Vec<PnlWindowSymbol>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LotSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PnlBucket {
    CounterTrade,
    OnchainNetting,
    DirectionalExposure,
}

impl PnlBucket {
    fn as_str(self) -> &'static str {
        match self {
            Self::CounterTrade => "counter_trade",
            Self::OnchainNetting => "onchain_netting",
            Self::DirectionalExposure => "directional_exposure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Venue {
    Onchain,
    Offchain,
}

impl Venue {
    fn as_str(self) -> &'static str {
        match self {
            Self::Onchain => "onchain",
            Self::Offchain => "offchain",
        }
    }
}

#[derive(Debug, Clone)]
struct Fill {
    rowid: i64,
    id: String,
    symbol: String,
    shares: Num,
    direction: Direction,
    price: Num,
    executed_at: String,
    venue: Venue,
}

#[derive(Debug, Clone)]
struct Lot {
    trade_id: String,
    side: LotSide,
    remaining_shares: Num,
    price: Num,
    opened_at: String,
    opened_rowid: i64,
    opened_venue: Venue,
}

#[derive(Debug, Clone, Default)]
struct SummaryAcc {
    counter_trade_pnl_usd: Num,
    onchain_netting_pnl_usd: Num,
    directional_inventory_baseline_pnl_usd: Num,
    directional_imbalance_excess_pnl_usd: Num,
    directional_exposure_pnl_usd: Num,
    realized_pnl_usd: Num,
    matched_shares: Num,
    onchain_notional_usd: Num,
    offchain_notional_usd: Num,
    open_long_shares: Num,
    open_short_shares: Num,
    open_long_notional_usd: Num,
    open_short_notional_usd: Num,
    unmatched_offchain_buy_shares: Num,
    unmatched_offchain_sell_shares: Num,
    unmatched_offchain_buy_notional_usd: Num,
    unmatched_offchain_sell_notional_usd: Num,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
    matched_lot_count: usize,
    open_lot_count: usize,
    unmatched_offchain_fill_count: usize,
}

#[derive(Debug, Clone, Default)]
struct SymbolBook {
    long_lots: Vec<Lot>,
    short_lots: Vec<Lot>,
    seen_onchain_fill_ids: HashSet<String>,
    seen_offchain_placement_ids: HashSet<String>,
    seen_offchain_fill_ids: HashSet<String>,
    original_onchain_shares: HashMap<String, Num>,
    matched_onchain_shares: HashMap<String, Num>,
    summary: SummaryAcc,
}

#[derive(Debug, Clone)]
struct UnmatchedOffchainAllocation {
    symbol: String,
    fill_id: String,
    shares: Num,
}

#[derive(Debug, Clone)]
struct PositionReplayDelta {
    symbol: String,
    replay_net: Num,
    position_net: Num,
}

#[derive(Debug, Clone, Default)]
struct CostSummaryAcc {
    counter_trade_costs_usd: Num,
    onchain_netting_costs_usd: Num,
    directional_exposure_costs_usd: Num,
    generic_costs_usd: Num,
    generic_revenue_usd: Num,
    dividend_revenue_usd: Num,
    offchain_execution_fees_usd: Num,
    tokenization_fees_usd: Num,
    cctp_fees_usd: Num,
    conversion_slippage_usd: Num,
    oracle_write_cost_usd: Num,
    broker_fees_usd: Num,
    regulatory_fees_usd: Num,
    margin_interest_usd: Num,
    bot_gas_usd: Num,
    wallet_transfer_fees_usd: Num,
    unclassified_costs_usd: Num,
    missing_cost_observation_count: usize,
}

#[derive(Debug, Clone)]
struct PositionEventRow {
    rowid: i64,
    symbol: String,
    event_type: String,
    payload: Value,
}

#[derive(Debug, Clone)]
struct PositionViewRow {
    symbol: String,
    net_position: Option<String>,
}

#[derive(Debug, Clone)]
struct CostEventRow {
    rowid: i64,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Value,
}

#[derive(Debug, Clone, Default)]
struct SampleStatsAcc {
    first_at: Option<String>,
    last_at: Option<String>,
    onchain_fill_count: usize,
    offchain_fill_count: usize,
}

pub(crate) async fn build_pnl_report(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
) -> Result<PnlResponse, PnlError> {
    let mut warnings = vec![
        ATTRIBUTION_WARNING.to_owned(),
        BASELINE_WARNING.to_owned(),
        COST_WARNING.to_owned(),
    ];
    let symbols = query.symbol_filter(&mut warnings);

    let (event_rows, position_rows, cost_rows) = tokio::try_join!(
        load_position_events(pool, &symbols),
        load_position_view(pool),
        load_cost_events(pool),
    )?;

    Ok(build_pnl_response_from_rows(
        event_rows,
        position_rows,
        cost_rows,
        alpaca_activities,
        query,
        symbols,
        warnings,
    ))
}

async fn load_position_events(
    pool: &SqlitePool,
    symbols: &BTreeSet<String>,
) -> Result<Vec<PositionEventRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, String, String, String)>(
        "SELECT rowid, aggregate_id AS symbol, event_type, payload \
         FROM events \
         WHERE aggregate_type = 'Position' \
           AND event_type IN ( \
             'PositionEvent::OnChainOrderFilled', \
             'PositionEvent::OffChainOrderPlaced', \
             'PositionEvent::OffChainOrderFilled' \
           ) \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter(|(_, symbol, _, _)| symbols.is_empty() || symbols.contains(symbol))
        .map(|(rowid, symbol, event_type, payload)| PositionEventRow {
            rowid,
            symbol,
            event_type,
            payload: parse_payload_string(&payload),
        })
        .collect())
}

async fn load_position_view(pool: &SqlitePool) -> Result<Vec<PositionViewRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, net_position \
         FROM position_view \
         WHERE symbol IS NOT NULL \
         ORDER BY symbol ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, net_position)| PositionViewRow {
            symbol,
            net_position,
        })
        .collect())
}

async fn load_cost_events(pool: &SqlitePool) -> Result<Vec<CostEventRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, String, String, String, String)>(
        "SELECT rowid, aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE ( \
             aggregate_type = 'TokenizedEquityMint' \
             AND event_type IN ( \
               'TokenizedEquityMintEvent::MintRequested', \
               'TokenizedEquityMintEvent::TokensReceived', \
               'TokenizedEquityMintEvent::ProviderCompletionRecovered' \
             ) \
           ) \
           OR ( \
             aggregate_type = 'UsdcRebalance' \
             AND event_type IN ( \
               'UsdcRebalanceEvent::Bridged', \
               'UsdcRebalanceEvent::BridgingCompletionRecovered' \
             ) \
           ) \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(rowid, aggregate_type, aggregate_id, event_type, payload)| CostEventRow {
                rowid,
                aggregate_type,
                aggregate_id,
                event_type,
                payload: parse_payload_string(&payload),
            },
        )
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn build_pnl_response_from_rows(
    event_rows: Vec<PositionEventRow>,
    position_rows: Vec<PositionViewRow>,
    cost_rows: Vec<CostEventRow>,
    alpaca_activities: Vec<AccountActivity>,
    query: &PnlQuery,
    symbols: BTreeSet<String>,
    mut warnings: Vec<String>,
) -> PnlResponse {
    let (position_nets, position_symbols) = parse_position_view(&position_rows, &mut warnings);
    let sample_stats = build_sample_stats(&event_rows, query, &mut warnings);
    let mut books: HashMap<String, SymbolBook> = HashMap::new();
    let mut entries = Vec::new();
    let mut unmatched_offchain_allocations = Vec::new();
    let mut position_replay_deltas = Vec::new();

    for row in ordered_position_events(event_rows, &mut warnings) {
        if !is_safe_symbol(&row.symbol) {
            warnings.push(format!(
                "Skipped unsafe position event symbol in backend PnL response: {}",
                row.symbol
            ));
            continue;
        }

        let book = books.entry(row.symbol.clone()).or_default();
        match row.event_type.as_str() {
            "PositionEvent::OnChainOrderFilled" => {
                if let Some(fill) = parse_onchain_fill(&row, &mut warnings) {
                    apply_onchain_fill(book, fill, &mut entries, &mut warnings);
                }
            }
            "PositionEvent::OffChainOrderPlaced" => {
                apply_offchain_placement(book, &row, &mut warnings);
            }
            "PositionEvent::OffChainOrderFilled" => {
                if let Some(fill) = parse_offchain_fill(&row, &mut warnings) {
                    apply_offchain_fill(
                        book,
                        fill,
                        &mut entries,
                        &mut warnings,
                        &mut unmatched_offchain_allocations,
                    );
                }
            }
            _ => {}
        }
    }

    let mut full_total = SummaryAcc::default();
    let mut replay_symbols = Vec::new();
    let mut book_symbols: Vec<_> = books.keys().cloned().collect();
    book_symbols.sort();
    for symbol in book_symbols {
        if let Some(book) = books.get_mut(&symbol) {
            finalize_book(
                &symbol,
                book,
                &position_nets,
                &mut warnings,
                &mut position_replay_deltas,
            );
            add_summary(&mut full_total, &book.summary);
            replay_symbols.push(symbol_summary_to_dto(&symbol, &book.summary));
        }
    }
    append_replay_diagnostics(
        &mut warnings,
        &unmatched_offchain_allocations,
        &position_replay_deltas,
    );

    let mut filtered_entries: Vec<_> = entries
        .into_iter()
        .filter(|entry| matches_date_filter(entry, query))
        .collect();
    filtered_entries.sort_by(|left, right| {
        right
            .matched_at
            .cmp(&left.matched_at)
            .then_with(|| right.closing_rowid.cmp(&left.closing_rowid))
    });

    let total = filtered_entries.len();
    let start = query.normalized_offset().min(total);
    let end = (start + query.normalized_limit()).min(total);
    let page_entries = filtered_entries[start..end].to_vec();

    let mut cost_replay = build_cost_entries(&cost_rows, &mut warnings);
    let alpaca_entries = build_alpaca_activity_cost_entries(&alpaca_activities);
    if !alpaca_entries.is_empty() {
        warnings.push(format!(
            "Cost coverage note: {} Alpaca account activity rows were fetched from the broker API \
             and included as explicit cost/revenue ledger entries.",
            alpaca_entries.len()
        ));
    }
    cost_replay.entries.extend(alpaca_entries);

    let mut filtered_cost_entries: Vec<_> = cost_replay
        .entries
        .into_iter()
        .filter(|entry| matches_cost_symbol_filter(entry, &symbols))
        .filter(|entry| matches_cost_date_filter(entry, query))
        .collect();
    filtered_cost_entries.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| right.event_rowid.cmp(&left.event_rowid))
    });

    let cost_summary = summarize_cost_entries(
        &filtered_cost_entries,
        cost_replay.missing_cost_observation_count,
    );
    let filtered = summary_from_entries(&filtered_entries);
    let replay_summary = summary_to_dto(&full_total);
    let summary = with_costs(
        with_replay_exposure(filtered.summary, replay_summary),
        &cost_summary,
    );
    let symbols_with_exposure =
        merge_symbol_replay_exposure(filtered.symbols, replay_symbols.into_iter());
    let symbols_with_costs = with_direct_symbol_costs(
        reset_symbol_costs(symbols_with_exposure),
        &filtered_cost_entries,
    );

    let mut symbol_universe: BTreeSet<String> = position_symbols.into_iter().collect();
    symbol_universe.extend(books.keys().cloned());
    symbol_universe.extend(symbols_with_costs.iter().map(|row| row.symbol.clone()));
    let symbol_universe: Vec<_> = symbol_universe.into_iter().collect();

    let cost_entries = filtered_cost_entries
        .iter()
        .map(PnlCostEntry::from)
        .collect();

    PnlResponse {
        attribution_method: ATTRIBUTION_METHOD,
        warnings,
        sample_stats,
        summary,
        costs: cost_summary,
        symbols: symbols_with_costs,
        symbol_universe: symbol_universe.clone(),
        entries: page_entries,
        cost_entries,
        total,
        has_more: end < total,
        windows: build_windows(&filtered_entries, &symbol_universe),
    }
}

fn parse_payload_string(payload: &str) -> Value {
    serde_json::from_str(payload).unwrap_or(Value::Null)
}

fn parse_query_datetime(value: &str) -> Result<DateTime<Utc>, PnlError> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| PnlError::InvalidQuery(format!("invalid timestamp/date: {value}")))?;
    let datetime = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| PnlError::InvalidQuery(format!("invalid timestamp/date: {value}")))?;
    Ok(DateTime::from_naive_utc_and_offset(datetime, Utc))
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .ok()
}

fn is_safe_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .chars()
            .all(|character| SAFE_SYMBOL_CHARS.contains(character))
}

fn nested_record<'a>(payload: &'a Value, key: &str) -> Option<&'a Value> {
    payload.get(key).filter(|value| value.is_object())
}

fn text_field(payload: &Value, key: &str) -> Option<String> {
    payload.get(key).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        _ => None,
    })
}

fn number_text_field(payload: &Value, key: &str) -> Option<String> {
    payload.get(key).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn decimal_field(payload: &Value, key: &str) -> Option<Num> {
    number_text_field(payload, key).and_then(|value| Num::from_str(&value).ok())
}

fn direction_field(payload: &Value, key: &str) -> Option<Direction> {
    match text_field(payload, key)?.as_str() {
        "Buy" | "buy" => Some(Direction::Buy),
        "Sell" | "sell" => Some(Direction::Sell),
        _ => None,
    }
}

fn position_event_replay_timestamp(row: &PositionEventRow) -> Option<String> {
    match row.event_type.as_str() {
        "PositionEvent::OnChainOrderFilled" => nested_record(&row.payload, "OnChainOrderFilled")
            .and_then(|filled| text_field(filled, "block_timestamp")),
        "PositionEvent::OffChainOrderFilled" => nested_record(&row.payload, "OffChainOrderFilled")
            .and_then(|filled| text_field(filled, "broker_timestamp")),
        "PositionEvent::OffChainOrderPlaced" => nested_record(&row.payload, "OffChainOrderPlaced")
            .and_then(|placed| text_field(placed, "placed_at")),
        _ => None,
    }
}

fn ordered_position_events(
    rows: Vec<PositionEventRow>,
    warnings: &mut Vec<String>,
) -> Vec<PositionEventRow> {
    let mut sortable: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let timestamp = position_event_replay_timestamp(&row);
            let timestamp_ms = timestamp
                .as_deref()
                .and_then(parse_timestamp)
                .map(|parsed| parsed.timestamp_millis())
                .unwrap_or(i64::MAX);
            if timestamp.is_some() && timestamp_ms == i64::MAX {
                warnings.push(format!(
                    "PnL audit note: invalid replay timestamp for {} row {}; using event row order",
                    row.symbol, row.rowid
                ));
            }

            (timestamp_ms, row.rowid, row)
        })
        .collect();

    sortable.sort_by_key(|(timestamp_ms, rowid, _)| (*timestamp_ms, *rowid));
    sortable.into_iter().map(|(_, _, row)| row).collect()
}

fn date_key(iso: &str) -> &str {
    iso.get(..10).unwrap_or(iso)
}

fn market_session_for_iso(iso: &str) -> &'static str {
    let Some(parsed) = parse_timestamp(iso) else {
        return "overnight";
    };
    let now_et = parsed.with_timezone(&New_York);
    if matches!(now_et.weekday(), Weekday::Sat | Weekday::Sun) {
        return "weekend";
    }

    let minute_of_day = now_et.hour() * 60 + now_et.minute();
    let pre_start = 4 * 60;
    let rth_start = 9 * 60 + 30;
    let post_start = 16 * 60;
    let post_end = 20 * 60;

    if (pre_start..rth_start).contains(&minute_of_day) {
        "pre"
    } else if (rth_start..post_start).contains(&minute_of_day) {
        "rth"
    } else if (post_start..post_end).contains(&minute_of_day) {
        "post"
    } else {
        "overnight"
    }
}

fn counter_trading_session_for_iso(iso: &str) -> &'static str {
    if market_session_for_iso(iso) == "rth" {
        "counter_trading_active"
    } else {
        "counter_trading_inactive"
    }
}

fn matches_market_session_filter(iso: &str, filter: Option<PnlMarketSessionFilter>) -> bool {
    match filter.unwrap_or(PnlMarketSessionFilter::All) {
        PnlMarketSessionFilter::All => true,
        PnlMarketSessionFilter::Pre => market_session_for_iso(iso) == "pre",
        PnlMarketSessionFilter::Rth => market_session_for_iso(iso) == "rth",
        PnlMarketSessionFilter::Post => market_session_for_iso(iso) == "post",
        PnlMarketSessionFilter::Overnight => market_session_for_iso(iso) == "overnight",
        PnlMarketSessionFilter::Weekend => market_session_for_iso(iso) == "weekend",
    }
}

fn matches_counter_trading_filter(iso: &str, filter: Option<PnlCounterTradingFilter>) -> bool {
    match filter.unwrap_or(PnlCounterTradingFilter::All) {
        PnlCounterTradingFilter::All => true,
        PnlCounterTradingFilter::CounterTradingActive => {
            counter_trading_session_for_iso(iso) == "counter_trading_active"
        }
        PnlCounterTradingFilter::CounterTradingInactive => {
            counter_trading_session_for_iso(iso) == "counter_trading_inactive"
        }
    }
}

fn matches_trade_filters(iso: &str, query: &PnlQuery) -> bool {
    matches_market_session_filter(iso, query.market_session_filter)
        && matches_counter_trading_filter(iso, query.counter_trading_filter)
}

fn matches_date_bounds_for_iso(iso: &str, query: &PnlQuery) -> bool {
    let day = date_key(iso);
    if query
        .from_date
        .as_deref()
        .is_some_and(|from| day < date_key(from))
    {
        return false;
    }
    if query
        .to_date
        .as_deref()
        .is_some_and(|to| day > date_key(to))
    {
        return false;
    }
    true
}

fn matches_date_filter(entry: &PnlEntry, query: &PnlQuery) -> bool {
    matches_date_bounds_for_iso(&entry.closed_at, query)
        && matches_trade_filters(&entry.closed_at, query)
}

fn matches_cost_date_filter(entry: &CostEntryInternal, query: &PnlQuery) -> bool {
    matches_date_bounds_for_iso(&entry.occurred_at, query)
        && matches_trade_filters(&entry.occurred_at, query)
}

fn matches_cost_symbol_filter(entry: &CostEntryInternal, symbols: &BTreeSet<String>) -> bool {
    match &entry.symbol {
        Some(symbol) => symbols.is_empty() || symbols.contains(symbol),
        None => symbols.is_empty(),
    }
}

fn seconds_between(start_iso: &str, end_iso: &str) -> i64 {
    let start = parse_timestamp(start_iso);
    let end = parse_timestamp(end_iso);
    match (start, end) {
        (Some(start), Some(end)) => (end - start).num_seconds().max(0),
        _ => 0,
    }
}

fn fmt_decimal(value: &Num) -> String {
    let rounded = format!("{value:.9}");
    let trimmed = rounded.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn abs_decimal(value: &Num) -> Num {
    if value.is_negative() {
        -value.clone()
    } else {
        value.clone()
    }
}

fn min_decimal(left: &Num, right: &Num) -> Num {
    if left <= right {
        left.clone()
    } else {
        right.clone()
    }
}

fn nonzero(value: &Num) -> bool {
    !value.is_zero()
}

fn add_cost(
    summary: &mut CostSummaryAcc,
    category: CostCategory,
    accounting_bucket: AccountingBucket,
    effect: AccountingEffect,
    amount: &Num,
) {
    match (effect, accounting_bucket) {
        (AccountingEffect::Cost, AccountingBucket::CounterTrade) => {
            summary.counter_trade_costs_usd += amount;
        }
        (AccountingEffect::Cost, AccountingBucket::OnchainNetting) => {
            summary.onchain_netting_costs_usd += amount;
        }
        (AccountingEffect::Cost, AccountingBucket::DirectionalExposure) => {
            summary.directional_exposure_costs_usd += amount;
        }
        (AccountingEffect::Cost, _) => {
            summary.generic_costs_usd += amount;
        }
        (AccountingEffect::Revenue, AccountingBucket::DividendRevenue) => {
            summary.dividend_revenue_usd += amount;
        }
        (AccountingEffect::Revenue, _) => {
            summary.generic_revenue_usd += amount;
        }
        (AccountingEffect::None, _) => {}
    }

    match category {
        CostCategory::TokenizationFee => summary.tokenization_fees_usd += amount,
        CostCategory::CctpFee => summary.cctp_fees_usd += amount,
        CostCategory::ConversionSlippage => summary.conversion_slippage_usd += amount,
        CostCategory::OracleWrite => summary.oracle_write_cost_usd += amount,
        CostCategory::BrokerFee => summary.broker_fees_usd += amount,
        CostCategory::RegulatoryFee => summary.regulatory_fees_usd += amount,
        CostCategory::MarginInterest => summary.margin_interest_usd += amount,
        CostCategory::BotGas => summary.bot_gas_usd += amount,
        CostCategory::WalletTransferFee => summary.wallet_transfer_fees_usd += amount,
        CostCategory::OffchainExecutionFee => summary.offchain_execution_fees_usd += amount,
        CostCategory::DividendIncome => {}
        CostCategory::Unclassified => summary.unclassified_costs_usd += amount,
    }
}

fn total_tracked_costs(summary: &CostSummaryAcc) -> Num {
    &(&(&summary.counter_trade_costs_usd + &summary.onchain_netting_costs_usd)
        + &summary.directional_exposure_costs_usd)
        + &summary.generic_costs_usd
}

fn total_tracked_revenue(summary: &CostSummaryAcc) -> Num {
    &summary.dividend_revenue_usd + &summary.generic_revenue_usd
}

fn included_when_nonzero(value: &Num) -> &'static str {
    if value.is_zero() {
        "not_ingested"
    } else {
        "included"
    }
}

fn coverage(
    source: &'static str,
    bucket: AccountingBucket,
    effect: AccountingEffect,
    status: &'static str,
    amount: &Num,
    note: &'static str,
) -> PnlCostCoverage {
    PnlCostCoverage {
        source,
        accounting_bucket: bucket.as_str(),
        effect: effect.as_str(),
        status,
        amount_usd: fmt_decimal(amount),
        note,
    }
}

fn cost_summary_to_dto(summary: &CostSummaryAcc, cost_entry_count: usize) -> PnlCostSummary {
    let offchain_execution_fees =
        &summary.offchain_execution_fees_usd + &summary.regulatory_fees_usd;
    PnlCostSummary {
        total_tracked_costs_usd: fmt_decimal(&total_tracked_costs(summary)),
        total_tracked_revenue_usd: fmt_decimal(&total_tracked_revenue(summary)),
        counter_trade_costs_usd: fmt_decimal(&summary.counter_trade_costs_usd),
        onchain_netting_costs_usd: fmt_decimal(&summary.onchain_netting_costs_usd),
        directional_exposure_costs_usd: fmt_decimal(&summary.directional_exposure_costs_usd),
        generic_costs_usd: fmt_decimal(&summary.generic_costs_usd),
        dividend_revenue_usd: fmt_decimal(&summary.dividend_revenue_usd),
        offchain_execution_fees_usd: fmt_decimal(&offchain_execution_fees),
        tokenization_fees_usd: fmt_decimal(&summary.tokenization_fees_usd),
        cctp_fees_usd: fmt_decimal(&summary.cctp_fees_usd),
        conversion_slippage_usd: fmt_decimal(&summary.conversion_slippage_usd),
        oracle_write_cost_usd: fmt_decimal(&summary.oracle_write_cost_usd),
        broker_fees_usd: fmt_decimal(&summary.broker_fees_usd),
        regulatory_fees_usd: fmt_decimal(&summary.regulatory_fees_usd),
        margin_interest_usd: fmt_decimal(&summary.margin_interest_usd),
        bot_gas_usd: fmt_decimal(&summary.bot_gas_usd),
        wallet_transfer_fees_usd: fmt_decimal(&summary.wallet_transfer_fees_usd),
        unclassified_costs_usd: fmt_decimal(&summary.unclassified_costs_usd),
        cost_entry_count,
        missing_cost_observation_count: summary.missing_cost_observation_count,
        coverage: vec![
            coverage(
                "Alpaca fees",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                included_when_nonzero(&summary.broker_fees_usd),
                &summary.broker_fees_usd,
                "Read from Alpaca account activity fee rows. These rows are not subtype-classified for now and are not allocated to symbols unless Alpaca supplies a symbol.",
            ),
            coverage(
                "On-chain netting execution costs",
                AccountingBucket::OnchainNetting,
                AccountingEffect::None,
                "zero",
                &summary.onchain_netting_costs_usd,
                "Passive on-chain fills do not create bot-paid trade execution costs for the on-chain netting bucket.",
            ),
            coverage(
                "Directional drift direct costs",
                AccountingBucket::DirectionalExposure,
                AccountingEffect::None,
                "zero",
                &summary.directional_exposure_costs_usd,
                "Raw inventory drift is price movement on held exposure; it has no direct execution cost by itself.",
            ),
            coverage(
                "Tokenization fees",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                "included",
                &summary.tokenization_fees_usd,
                "Read from TokenizedEquityMint terminal events when Alpaca reports fees.",
            ),
            coverage(
                "CCTP fees",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                "included",
                &summary.cctp_fees_usd,
                "Read from UsdcRebalance bridge completion events as fee_collected.",
            ),
            coverage(
                "USD/USDC reporting basis",
                AccountingBucket::Generic,
                AccountingEffect::None,
                "zero",
                &summary.conversion_slippage_usd,
                "USD and USDC are treated as equivalent for reporting; conversion basis is not modeled as PnL. Only explicit persisted fees are deducted.",
            ),
            coverage(
                "Oracle writes",
                AccountingBucket::Generic,
                AccountingEffect::None,
                "zero",
                &summary.oracle_write_cost_usd,
                "Current setup does not pay oracle write cost through the bot.",
            ),
            coverage(
                "Dividend income",
                AccountingBucket::DividendRevenue,
                AccountingEffect::Revenue,
                included_when_nonzero(&summary.dividend_revenue_usd),
                &summary.dividend_revenue_usd,
                "Dividend-bearing stock revenue increases net PnL when Alpaca dividend activity rows are available.",
            ),
            coverage(
                "Margin interest",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                included_when_nonzero(&summary.margin_interest_usd),
                &summary.margin_interest_usd,
                "Included when Alpaca account activity interest rows are available; negative rows are costs and positive rows are credits.",
            ),
            coverage(
                "Bot gas",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                "not_ingested",
                &summary.bot_gas_usd,
                "Requires tx receipt ingestion, gas-payer classification, and ETH/USD valuation.",
            ),
            coverage(
                "Wallet transfer fees",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                "not_ingested",
                &summary.wallet_transfer_fees_usd,
                "Alpaca wallet fee fields are not currently persisted into the event stream.",
            ),
        ],
    }
}

fn summarize_cost_entries(
    entries: &[CostEntryInternal],
    missing_cost_observation_count: usize,
) -> PnlCostSummary {
    let mut summary = CostSummaryAcc {
        missing_cost_observation_count,
        ..CostSummaryAcc::default()
    };

    for entry in entries {
        add_cost(
            &mut summary,
            entry.category,
            entry.accounting_bucket,
            entry.effect,
            &entry.amount_usd,
        );
    }

    cost_summary_to_dto(&summary, entries.len())
}

fn with_costs(summary: PnlSummary, costs: &PnlCostSummary) -> PnlSummary {
    let gross = parse_decimal_lossy(&summary.total_pnl_usd);
    let tracked_costs = parse_decimal_lossy(&costs.total_tracked_costs_usd);
    let tracked_revenue = parse_decimal_lossy(&costs.total_tracked_revenue_usd);
    let net = &(&gross - &tracked_costs) + &tracked_revenue;

    PnlSummary {
        gross_realized_pnl_usd: fmt_decimal(&gross),
        tracked_costs_usd: fmt_decimal(&tracked_costs),
        tracked_revenue_usd: fmt_decimal(&tracked_revenue),
        net_realized_pnl_usd: fmt_decimal(&net),
        ..summary
    }
}

fn parse_decimal_lossy(value: &str) -> Num {
    Num::from_str(value).unwrap_or_default()
}

fn allocation_summary_text(allocations: &[UnmatchedOffchainAllocation]) -> Option<String> {
    if allocations.is_empty() {
        return None;
    }

    let mut by_symbol: HashMap<String, (HashSet<String>, Num)> = HashMap::new();
    for allocation in allocations {
        let (fill_ids, shares) = by_symbol.entry(allocation.symbol.clone()).or_default();
        fill_ids.insert(allocation.fill_id.clone());
        *shares += &allocation.shares;
    }

    let mut details: Vec<_> = by_symbol.into_iter().collect();
    details.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbol_details = details
        .into_iter()
        .map(|(symbol, (fill_ids, shares))| {
            format!(
                "{}: {} shares across {} fills",
                symbol,
                fmt_decimal(&shares),
                fill_ids.len()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    Some(format!(
        "Allocation note: {} offchain fills opened offchain-origin inventory outside the intended \
         onchain-to-offchain hedge flow ({}). Those shares are carried in the FIFO ledger so later \
         fills can close them.",
        allocations.len(),
        symbol_details
    ))
}

fn position_replay_delta_text(deltas: &[PositionReplayDelta]) -> Option<String> {
    if deltas.is_empty() {
        return None;
    }

    let mut sorted = deltas.to_vec();
    sorted.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    let details = sorted
        .iter()
        .map(|delta| {
            format!(
                "{}: replay {}, position_view {}",
                delta.symbol,
                fmt_decimal(&delta.replay_net),
                fmt_decimal(&delta.position_net)
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    Some(format!(
        "Reconciliation note: replayed open lots differ from position_view for {} symbols ({}). \
         This means the persisted Position fill events available to the dashboard do not fully \
         reconstruct the current projected position for those symbols.",
        deltas.len(),
        details
    ))
}

fn append_replay_diagnostics(
    warnings: &mut Vec<String>,
    unmatched_offchain_allocations: &[UnmatchedOffchainAllocation],
    position_replay_deltas: &[PositionReplayDelta],
) {
    if let Some(text) = allocation_summary_text(unmatched_offchain_allocations) {
        warnings.push(text);
    }

    if let Some(text) = position_replay_delta_text(position_replay_deltas) {
        warnings.push(text);
    }
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    }
}

fn lot_side_to_direction(side: LotSide) -> &'static str {
    match side {
        LotSide::Long => "buy",
        LotSide::Short => "sell",
    }
}

fn add_venue_notional(summary: &mut SummaryAcc, venue: Venue, notional: &Num) {
    match venue {
        Venue::Onchain => summary.onchain_notional_usd += notional,
        Venue::Offchain => summary.offchain_notional_usd += notional,
    }
}

fn add_realized_pnl(summary: &mut SummaryAcc, bucket: PnlBucket, value: &Num) {
    match bucket {
        PnlBucket::CounterTrade => {
            summary.counter_trade_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
        PnlBucket::OnchainNetting => {
            summary.onchain_netting_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
        PnlBucket::DirectionalExposure => {
            summary.directional_imbalance_excess_pnl_usd += value;
            summary.directional_exposure_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
    }
}

fn parse_onchain_fill(row: &PositionEventRow, warnings: &mut Vec<String>) -> Option<Fill> {
    let Some(filled) = nested_record(&row.payload, "OnChainOrderFilled") else {
        warnings.push(format!(
            "Skipped malformed position onchain fill {}: missing OnChainOrderFilled",
            row.symbol
        ));
        return None;
    };

    let amount = decimal_field(filled, "amount");
    let direction = direction_field(filled, "direction");
    let price = decimal_field(filled, "price_usdc");
    let executed_at = text_field(filled, "block_timestamp");
    let trade_id = nested_record(filled, "trade_id");
    let tx_hash = trade_id.and_then(|id| text_field(id, "tx_hash"));
    let log_index = trade_id.and_then(|id| number_text_field(id, "log_index"));

    let (
        Some(amount),
        Some(direction),
        Some(price),
        Some(executed_at),
        Some(tx_hash),
        Some(log_index),
    ) = (amount, direction, price, executed_at, tx_hash, log_index)
    else {
        warnings.push(format!(
            "Skipped malformed position onchain fill {}: incomplete payload",
            row.symbol
        ));
        return None;
    };

    Some(Fill {
        rowid: row.rowid,
        id: format!("{tx_hash}:{log_index}"),
        symbol: row.symbol.clone(),
        shares: amount,
        direction,
        price,
        executed_at,
        venue: Venue::Onchain,
    })
}

fn parse_offchain_fill(row: &PositionEventRow, warnings: &mut Vec<String>) -> Option<Fill> {
    let Some(filled) = nested_record(&row.payload, "OffChainOrderFilled") else {
        warnings.push(format!(
            "Skipped malformed position offchain fill {}: missing OffChainOrderFilled",
            row.symbol
        ));
        return None;
    };

    let order_id = text_field(filled, "offchain_order_id");
    let shares = decimal_field(filled, "shares_filled");
    let direction = direction_field(filled, "direction");
    let price = decimal_field(filled, "price");
    let executed_at = text_field(filled, "broker_timestamp");

    let (Some(order_id), Some(shares), Some(direction), Some(price), Some(executed_at)) =
        (order_id, shares, direction, price, executed_at)
    else {
        warnings.push(format!(
            "Skipped malformed position offchain fill {}: incomplete payload",
            row.symbol
        ));
        return None;
    };

    Some(Fill {
        rowid: row.rowid,
        id: order_id,
        symbol: row.symbol.clone(),
        shares,
        direction,
        price,
        executed_at,
        venue: Venue::Offchain,
    })
}

fn parse_offchain_placement_id(
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let placed = nested_record(&row.payload, "OffChainOrderPlaced");
    let order_id = placed.and_then(|value| text_field(value, "offchain_order_id"));
    if order_id.is_none() {
        warnings.push(format!(
            "Skipped malformed position offchain placement {}: incomplete payload",
            row.symbol
        ));
    }
    order_id
}

fn open_residual_lot(book: &mut SymbolBook, fill: &Fill, remaining: Num) {
    let side = if fill.direction == Direction::Buy {
        LotSide::Long
    } else {
        LotSide::Short
    };
    let lot = Lot {
        trade_id: fill.id.clone(),
        side,
        remaining_shares: remaining,
        price: fill.price.clone(),
        opened_at: fill.executed_at.clone(),
        opened_rowid: fill.rowid,
        opened_venue: fill.venue,
    };

    match side {
        LotSide::Long => book.long_lots.push(lot),
        LotSide::Short => book.short_lots.push(lot),
    }
}

fn apply_onchain_fill(
    book: &mut SymbolBook,
    fill: Fill,
    entries: &mut Vec<PnlEntry>,
    warnings: &mut Vec<String>,
) {
    if book.seen_onchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate onchain trade_id {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return;
    }

    book.seen_onchain_fill_ids.insert(fill.id.clone());
    book.summary.onchain_fill_count += 1;
    let source_lots = if fill.direction == Direction::Buy {
        &mut book.short_lots
    } else {
        &mut book.long_lots
    };
    let remaining = match_fill_against_lots(
        &fill,
        source_lots,
        &mut book.summary,
        &mut book.matched_onchain_shares,
        entries,
        PnlBucket::OnchainNetting,
    );
    if remaining.is_zero() {
        return;
    }

    let original = book
        .original_onchain_shares
        .entry(fill.id.clone())
        .or_default();
    *original += &remaining;
    open_residual_lot(book, &fill, remaining);
}

fn apply_offchain_placement(
    book: &mut SymbolBook,
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) {
    let Some(order_id) = parse_offchain_placement_id(row, warnings) else {
        return;
    };

    if book.seen_offchain_placement_ids.contains(&order_id) {
        warnings.push(format!(
            "PnL audit error: duplicate offchain placement {} for {} was skipped",
            order_id, row.symbol
        ));
        return;
    }

    book.seen_offchain_placement_ids.insert(order_id);
}

fn apply_offchain_fill(
    book: &mut SymbolBook,
    fill: Fill,
    entries: &mut Vec<PnlEntry>,
    warnings: &mut Vec<String>,
    unmatched_offchain_allocations: &mut Vec<UnmatchedOffchainAllocation>,
) {
    if book.seen_offchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate offchain fill {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return;
    }

    book.seen_offchain_fill_ids.insert(fill.id.clone());
    book.summary.offchain_fill_count += 1;
    let source_lots = if fill.direction == Direction::Buy {
        &mut book.short_lots
    } else {
        &mut book.long_lots
    };
    let remaining = match_fill_against_lots(
        &fill,
        source_lots,
        &mut book.summary,
        &mut book.matched_onchain_shares,
        entries,
        PnlBucket::CounterTrade,
    );

    if !remaining.is_zero() {
        unmatched_offchain_allocations.push(UnmatchedOffchainAllocation {
            symbol: fill.symbol.clone(),
            fill_id: fill.id.clone(),
            shares: remaining.clone(),
        });
        open_residual_lot(book, &fill, remaining);
    }
}

fn match_fill_against_lots(
    fill: &Fill,
    source_lots: &mut Vec<Lot>,
    summary: &mut SummaryAcc,
    matched_onchain_shares: &mut HashMap<String, Num>,
    entries: &mut Vec<PnlEntry>,
    bucket: PnlBucket,
) -> Num {
    let mut remaining = fill.shares.clone();

    while !remaining.is_zero() && !source_lots.is_empty() {
        let mut front_lot = source_lots.remove(0);
        let matched_shares = min_decimal(&remaining, &front_lot.remaining_shares);
        if matched_shares.is_zero() {
            continue;
        }

        let elapsed_seconds = seconds_between(&front_lot.opened_at, &fill.executed_at);
        let effective_bucket = if front_lot.opened_venue == Venue::Offchain
            || (bucket == PnlBucket::CounterTrade
                && elapsed_seconds > COUNTER_TRADE_THRESHOLD_SECONDS)
        {
            PnlBucket::DirectionalExposure
        } else {
            bucket
        };

        let spread = if front_lot.side == LotSide::Long {
            &fill.price - &front_lot.price
        } else {
            &front_lot.price - &fill.price
        };
        let realized_pnl = &matched_shares * &spread;
        let opening_notional = &matched_shares * &front_lot.price;
        let closing_notional = &matched_shares * &fill.price;

        front_lot.remaining_shares -= &matched_shares;
        if !front_lot.remaining_shares.is_zero() {
            source_lots.insert(0, front_lot.clone());
        }

        add_realized_pnl(summary, effective_bucket, &realized_pnl);
        summary.matched_shares += &matched_shares;
        add_venue_notional(summary, front_lot.opened_venue, &opening_notional);
        add_venue_notional(summary, fill.venue, &closing_notional);
        summary.matched_lot_count += 1;

        if front_lot.opened_venue == Venue::Onchain {
            let matched = matched_onchain_shares
                .entry(front_lot.trade_id.clone())
                .or_default();
            *matched += &matched_shares;
        }

        let opening_direction = lot_side_to_direction(front_lot.side);
        let closing_direction = direction_label(fill.direction);
        let opening_price_text = fmt_decimal(&front_lot.price);
        let closing_price_text = fmt_decimal(&fill.price);
        let onchain_direction = if front_lot.opened_venue == Venue::Onchain {
            opening_direction.to_owned()
        } else if fill.venue == Venue::Onchain {
            closing_direction.to_owned()
        } else {
            String::new()
        };
        let offchain_direction = if front_lot.opened_venue == Venue::Offchain {
            opening_direction.to_owned()
        } else if fill.venue == Venue::Offchain {
            closing_direction.to_owned()
        } else {
            String::new()
        };
        let onchain_trade_id = if front_lot.opened_venue == Venue::Onchain {
            front_lot.trade_id.clone()
        } else if fill.venue == Venue::Onchain {
            fill.id.clone()
        } else {
            String::new()
        };
        let offchain_order_id = if front_lot.opened_venue == Venue::Offchain {
            front_lot.trade_id.clone()
        } else if fill.venue == Venue::Offchain {
            fill.id.clone()
        } else {
            String::new()
        };
        let onchain_price_text = if front_lot.opened_venue == Venue::Onchain {
            opening_price_text.clone()
        } else if fill.venue == Venue::Onchain {
            closing_price_text.clone()
        } else {
            String::new()
        };
        let offchain_price_text = if front_lot.opened_venue == Venue::Offchain {
            opening_price_text.clone()
        } else if fill.venue == Venue::Offchain {
            closing_price_text.clone()
        } else {
            String::new()
        };

        entries.push(PnlEntry {
            symbol: fill.symbol.clone(),
            pnl_bucket: effective_bucket.as_str(),
            matched_at: fill.executed_at.clone(),
            opened_at: front_lot.opened_at.clone(),
            closed_at: fill.executed_at.clone(),
            opening_fill_id: front_lot.trade_id.clone(),
            closing_fill_id: fill.id.clone(),
            opening_rowid: front_lot.opened_rowid,
            closing_rowid: fill.rowid,
            opening_venue: front_lot.opened_venue.as_str(),
            closing_venue: fill.venue.as_str(),
            opening_direction,
            closing_direction,
            opening_price_usd: opening_price_text,
            closing_price_usd: closing_price_text,
            onchain_trade_id,
            offchain_order_id,
            onchain_direction,
            offchain_direction,
            shares: fmt_decimal(&matched_shares),
            onchain_price_usdc: onchain_price_text,
            offchain_price_usd: offchain_price_text,
            spread_usd: fmt_decimal(&spread),
            realized_pnl_usd: fmt_decimal(&realized_pnl),
            elapsed_seconds,
            counter_trade_threshold_seconds: COUNTER_TRADE_THRESHOLD_SECONDS,
            delayed_counter_trade: effective_bucket == PnlBucket::DirectionalExposure,
            attribution_method: ATTRIBUTION_METHOD,
        });

        remaining -= &matched_shares;
    }

    remaining
}

fn finalize_lots(summary: &mut SummaryAcc, lots: &[Lot]) {
    for lot in lots {
        let notional = &lot.remaining_shares * &lot.price;
        match lot.side {
            LotSide::Long => {
                summary.open_long_shares += &lot.remaining_shares;
                summary.open_long_notional_usd += &notional;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_buy_shares += &lot.remaining_shares;
                    summary.unmatched_offchain_buy_notional_usd += &notional;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
            LotSide::Short => {
                summary.open_short_shares += &lot.remaining_shares;
                summary.open_short_notional_usd += &notional;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_sell_shares += &lot.remaining_shares;
                    summary.unmatched_offchain_sell_notional_usd += &notional;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
        }
        summary.open_lot_count += 1;
    }
}

fn finalize_book(
    symbol: &str,
    book: &mut SymbolBook,
    position_nets: &HashMap<String, Num>,
    warnings: &mut Vec<String>,
    position_replay_deltas: &mut Vec<PositionReplayDelta>,
) {
    finalize_lots(&mut book.summary, &book.long_lots);
    finalize_lots(&mut book.summary, &book.short_lots);

    let epsilon = Num::from_str(EPSILON).unwrap_or_default();
    for (trade_id, matched_shares) in &book.matched_onchain_shares {
        if let Some(original_shares) = book.original_onchain_shares.get(trade_id) {
            let excess = matched_shares - original_shares;
            if excess > epsilon {
                warnings.push(format!(
                    "PnL audit error: onchain lot {} for {} matched {} shares above original {}",
                    trade_id,
                    symbol,
                    fmt_decimal(matched_shares),
                    fmt_decimal(original_shares)
                ));
            }
        }
    }

    if let Some(position_net) = position_nets.get(symbol) {
        let replay_net = &book.summary.open_long_shares - &book.summary.open_short_shares;
        let delta = &replay_net - position_net;
        if abs_decimal(&delta) > epsilon {
            position_replay_deltas.push(PositionReplayDelta {
                symbol: symbol.to_owned(),
                replay_net,
                position_net: position_net.clone(),
            });
        }
    }
}

fn add_summary(target: &mut SummaryAcc, source: &SummaryAcc) {
    target.counter_trade_pnl_usd += &source.counter_trade_pnl_usd;
    target.onchain_netting_pnl_usd += &source.onchain_netting_pnl_usd;
    target.directional_inventory_baseline_pnl_usd += &source.directional_inventory_baseline_pnl_usd;
    target.directional_imbalance_excess_pnl_usd += &source.directional_imbalance_excess_pnl_usd;
    target.directional_exposure_pnl_usd += &source.directional_exposure_pnl_usd;
    target.realized_pnl_usd += &source.realized_pnl_usd;
    target.matched_shares += &source.matched_shares;
    target.onchain_notional_usd += &source.onchain_notional_usd;
    target.offchain_notional_usd += &source.offchain_notional_usd;
    target.open_long_shares += &source.open_long_shares;
    target.open_short_shares += &source.open_short_shares;
    target.open_long_notional_usd += &source.open_long_notional_usd;
    target.open_short_notional_usd += &source.open_short_notional_usd;
    target.unmatched_offchain_buy_shares += &source.unmatched_offchain_buy_shares;
    target.unmatched_offchain_sell_shares += &source.unmatched_offchain_sell_shares;
    target.unmatched_offchain_buy_notional_usd += &source.unmatched_offchain_buy_notional_usd;
    target.unmatched_offchain_sell_notional_usd += &source.unmatched_offchain_sell_notional_usd;
    target.onchain_fill_count += source.onchain_fill_count;
    target.offchain_fill_count += source.offchain_fill_count;
    target.matched_lot_count += source.matched_lot_count;
    target.open_lot_count += source.open_lot_count;
    target.unmatched_offchain_fill_count += source.unmatched_offchain_fill_count;
}

fn summary_to_dto(summary: &SummaryAcc) -> PnlSummary {
    let directional_exposure_pnl = &summary.directional_inventory_baseline_pnl_usd
        + &summary.directional_imbalance_excess_pnl_usd;
    let total_pnl = &(&(&summary.counter_trade_pnl_usd + &summary.onchain_netting_pnl_usd)
        + &summary.directional_inventory_baseline_pnl_usd)
        + &summary.directional_imbalance_excess_pnl_usd;
    let inventory_drift_shares = &summary.open_long_shares - &summary.open_short_shares;
    let inventory_drift_usd = &summary.open_long_notional_usd - &summary.open_short_notional_usd;
    let unmatched_offchain_shares =
        &summary.unmatched_offchain_buy_shares + &summary.unmatched_offchain_sell_shares;
    let unmatched_offchain_notional = &summary.unmatched_offchain_buy_notional_usd
        + &summary.unmatched_offchain_sell_notional_usd;

    PnlSummary {
        counter_trade_pnl_usd: fmt_decimal(&summary.counter_trade_pnl_usd),
        onchain_netting_pnl_usd: fmt_decimal(&summary.onchain_netting_pnl_usd),
        directional_inventory_baseline_pnl_usd: fmt_decimal(
            &summary.directional_inventory_baseline_pnl_usd,
        ),
        directional_imbalance_excess_pnl_usd: fmt_decimal(
            &summary.directional_imbalance_excess_pnl_usd,
        ),
        directional_exposure_pnl_usd: fmt_decimal(&directional_exposure_pnl),
        total_pnl_usd: fmt_decimal(&total_pnl),
        gross_realized_pnl_usd: fmt_decimal(&total_pnl),
        tracked_costs_usd: "0".to_owned(),
        tracked_revenue_usd: "0".to_owned(),
        net_realized_pnl_usd: fmt_decimal(&total_pnl),
        realized_pnl_usd: fmt_decimal(&summary.realized_pnl_usd),
        matched_shares: fmt_decimal(&summary.matched_shares),
        onchain_notional_usd: fmt_decimal(&summary.onchain_notional_usd),
        offchain_notional_usd: fmt_decimal(&summary.offchain_notional_usd),
        inventory_drift_shares: fmt_decimal(&inventory_drift_shares),
        inventory_drift_usd: fmt_decimal(&inventory_drift_usd),
        open_long_shares: fmt_decimal(&summary.open_long_shares),
        open_short_shares: fmt_decimal(&summary.open_short_shares),
        unmatched_offchain_shares: fmt_decimal(&unmatched_offchain_shares),
        unmatched_offchain_notional_usd: fmt_decimal(&unmatched_offchain_notional),
        onchain_fill_count: summary.onchain_fill_count,
        offchain_fill_count: summary.offchain_fill_count,
        matched_lot_count: summary.matched_lot_count,
        open_lot_count: summary.open_lot_count,
        unmatched_offchain_fill_count: summary.unmatched_offchain_fill_count,
    }
}

fn symbol_summary_to_dto(symbol: &str, summary: &SummaryAcc) -> PnlSymbolSummary {
    let dto = summary_to_dto(summary);
    PnlSymbolSummary {
        symbol: symbol.to_owned(),
        counter_trade_pnl_usd: dto.counter_trade_pnl_usd,
        onchain_netting_pnl_usd: dto.onchain_netting_pnl_usd,
        directional_inventory_baseline_pnl_usd: dto.directional_inventory_baseline_pnl_usd,
        directional_imbalance_excess_pnl_usd: dto.directional_imbalance_excess_pnl_usd,
        directional_exposure_pnl_usd: dto.directional_exposure_pnl_usd,
        total_pnl_usd: dto.total_pnl_usd,
        gross_realized_pnl_usd: dto.gross_realized_pnl_usd,
        tracked_costs_usd: dto.tracked_costs_usd,
        tracked_revenue_usd: dto.tracked_revenue_usd,
        net_realized_pnl_usd: dto.net_realized_pnl_usd,
        realized_pnl_usd: dto.realized_pnl_usd,
        matched_shares: dto.matched_shares,
        inventory_drift_shares: dto.inventory_drift_shares,
        inventory_drift_usd: dto.inventory_drift_usd,
        open_long_shares: dto.open_long_shares,
        open_short_shares: dto.open_short_shares,
        unmatched_offchain_shares: dto.unmatched_offchain_shares,
        matched_lot_count: dto.matched_lot_count,
        onchain_fill_count: dto.onchain_fill_count,
        offchain_fill_count: dto.offchain_fill_count,
        unmatched_offchain_fill_count: dto.unmatched_offchain_fill_count,
    }
}

fn with_replay_exposure(filtered: PnlSummary, replay: PnlSummary) -> PnlSummary {
    PnlSummary {
        inventory_drift_shares: replay.inventory_drift_shares,
        inventory_drift_usd: replay.inventory_drift_usd,
        open_long_shares: replay.open_long_shares,
        open_short_shares: replay.open_short_shares,
        unmatched_offchain_shares: replay.unmatched_offchain_shares,
        unmatched_offchain_notional_usd: replay.unmatched_offchain_notional_usd,
        open_lot_count: replay.open_lot_count,
        unmatched_offchain_fill_count: replay.unmatched_offchain_fill_count,
        ..filtered
    }
}

fn with_symbol_replay_exposure(
    filtered: PnlSymbolSummary,
    replay: PnlSymbolSummary,
) -> PnlSymbolSummary {
    PnlSymbolSummary {
        inventory_drift_shares: replay.inventory_drift_shares,
        inventory_drift_usd: replay.inventory_drift_usd,
        open_long_shares: replay.open_long_shares,
        open_short_shares: replay.open_short_shares,
        unmatched_offchain_shares: replay.unmatched_offchain_shares,
        unmatched_offchain_fill_count: replay.unmatched_offchain_fill_count,
        ..filtered
    }
}

fn empty_symbol_summary(symbol: &str) -> PnlSymbolSummary {
    symbol_summary_to_dto(symbol, &SummaryAcc::default())
}

fn is_nonzero_text(value: &str) -> bool {
    Num::from_str(value).is_ok_and(|parsed| !parsed.is_zero())
}

fn has_replay_exposure(summary: &PnlSymbolSummary) -> bool {
    is_nonzero_text(&summary.inventory_drift_shares)
        || is_nonzero_text(&summary.inventory_drift_usd)
        || is_nonzero_text(&summary.open_long_shares)
        || is_nonzero_text(&summary.open_short_shares)
        || is_nonzero_text(&summary.unmatched_offchain_shares)
        || summary.unmatched_offchain_fill_count > 0
}

fn merge_symbol_replay_exposure(
    filtered_symbols: Vec<PnlSymbolSummary>,
    replay_symbols: impl Iterator<Item = PnlSymbolSummary>,
) -> Vec<PnlSymbolSummary> {
    let mut by_symbol: HashMap<String, PnlSymbolSummary> = filtered_symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();

    for replay in replay_symbols {
        let existing = by_symbol.remove(&replay.symbol);
        if existing.is_some() || has_replay_exposure(&replay) {
            by_symbol.insert(
                replay.symbol.clone(),
                with_symbol_replay_exposure(
                    existing.unwrap_or_else(|| empty_symbol_summary(&replay.symbol)),
                    replay,
                ),
            );
        }
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    rows
}

fn reset_symbol_costs(symbols: Vec<PnlSymbolSummary>) -> Vec<PnlSymbolSummary> {
    symbols
        .into_iter()
        .map(|row| PnlSymbolSummary {
            gross_realized_pnl_usd: row.total_pnl_usd.clone(),
            tracked_costs_usd: "0".to_owned(),
            tracked_revenue_usd: "0".to_owned(),
            net_realized_pnl_usd: row.total_pnl_usd.clone(),
            ..row
        })
        .collect()
}

fn with_direct_symbol_costs(
    symbols: Vec<PnlSymbolSummary>,
    cost_entries: &[CostEntryInternal],
) -> Vec<PnlSymbolSummary> {
    let mut costs_by_symbol: HashMap<String, Num> = HashMap::new();
    let mut revenue_by_symbol: HashMap<String, Num> = HashMap::new();
    for entry in cost_entries {
        let Some(symbol) = &entry.symbol else {
            continue;
        };
        if entry.effect == AccountingEffect::Revenue {
            *revenue_by_symbol.entry(symbol.clone()).or_default() += &entry.amount_usd;
        } else if entry.effect == AccountingEffect::Cost {
            *costs_by_symbol.entry(symbol.clone()).or_default() += &entry.amount_usd;
        }
    }

    if costs_by_symbol.is_empty() && revenue_by_symbol.is_empty() {
        return symbols;
    }

    let mut by_symbol: HashMap<String, PnlSymbolSummary> = symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();
    let mut affected_symbols: BTreeSet<String> = costs_by_symbol.keys().cloned().collect();
    affected_symbols.extend(revenue_by_symbol.keys().cloned());

    for symbol in affected_symbols {
        let existing = by_symbol
            .remove(&symbol)
            .unwrap_or_else(|| empty_symbol_summary(&symbol));
        let gross = parse_decimal_lossy(&existing.total_pnl_usd);
        let cost = costs_by_symbol.remove(&symbol).unwrap_or_default();
        let revenue = revenue_by_symbol.remove(&symbol).unwrap_or_default();
        let net = &(&gross - &cost) + &revenue;
        by_symbol.insert(
            symbol.clone(),
            PnlSymbolSummary {
                gross_realized_pnl_usd: fmt_decimal(&gross),
                tracked_costs_usd: fmt_decimal(&cost),
                tracked_revenue_usd: fmt_decimal(&revenue),
                net_realized_pnl_usd: fmt_decimal(&net),
                ..existing
            },
        );
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    rows
}

fn entry_bucket_to_stream(bucket: &str) -> Option<&'static str> {
    match bucket {
        "counter_trade" => Some("counterTradePnlUsd"),
        "onchain_netting" => Some("onchainNettingPnlUsd"),
        "directional_exposure" => Some("directionalImbalanceExcessPnlUsd"),
        _ => None,
    }
}

fn summary_from_entries(entries: &[PnlEntry]) -> SummaryAndSymbols {
    let mut total = SummaryAcc::default();
    let mut per_symbol: HashMap<String, SummaryAcc> = HashMap::new();

    for entry in entries {
        let summary = per_symbol.entry(entry.symbol.clone()).or_default();
        let shares = parse_decimal_lossy(&entry.shares);
        let opening_notional = &shares * &parse_decimal_lossy(&entry.opening_price_usd);
        let closing_notional = &shares * &parse_decimal_lossy(&entry.closing_price_usd);
        let pnl = parse_decimal_lossy(&entry.realized_pnl_usd);

        summary.matched_shares += &shares;
        if entry.opening_venue == "onchain" {
            add_venue_notional(summary, Venue::Onchain, &opening_notional);
        } else if entry.opening_venue == "offchain" {
            add_venue_notional(summary, Venue::Offchain, &opening_notional);
        }
        if entry.closing_venue == "onchain" {
            add_venue_notional(summary, Venue::Onchain, &closing_notional);
        } else if entry.closing_venue == "offchain" {
            add_venue_notional(summary, Venue::Offchain, &closing_notional);
        }
        summary.matched_lot_count += 1;

        match entry.pnl_bucket {
            "counter_trade" => {
                summary.counter_trade_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            "onchain_netting" => {
                summary.onchain_netting_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            "directional_exposure" => {
                summary.directional_imbalance_excess_pnl_usd += &pnl;
                summary.directional_exposure_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            _ => {}
        }
    }

    let mut symbols: Vec<_> = per_symbol.into_iter().collect();
    symbols.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbols = symbols
        .into_iter()
        .map(|(symbol, summary)| {
            add_summary(&mut total, &summary);
            symbol_summary_to_dto(&symbol, &summary)
        })
        .collect();

    SummaryAndSymbols {
        summary: summary_to_dto(&total),
        symbols,
    }
}

struct SummaryAndSymbols {
    summary: PnlSummary,
    symbols: Vec<PnlSymbolSummary>,
}

fn build_windows(entries: &[PnlEntry], symbols: &[String]) -> Vec<PnlWindow> {
    let mut by_date: HashMap<String, Vec<&PnlEntry>> = HashMap::new();
    for entry in entries {
        by_date
            .entry(date_key(&entry.closed_at).to_owned())
            .or_default()
            .push(entry);
    }

    let mut dates: Vec<_> = by_date.into_iter().collect();
    dates.sort_by(|(left, _), (right, _)| left.cmp(right));
    dates
        .into_iter()
        .map(|(date, day_entries)| {
            let market_sessions: BTreeSet<_> = day_entries
                .iter()
                .map(|entry| market_session_for_iso(&entry.closed_at))
                .collect();
            let counter_sessions: BTreeSet<_> = day_entries
                .iter()
                .map(|entry| counter_trading_session_for_iso(&entry.closed_at))
                .collect();
            let market_session = if market_sessions.len() == 1 {
                market_sessions
                    .iter()
                    .next()
                    .map_or("mixed".to_owned(), |session| (*session).to_owned())
            } else {
                "mixed".to_owned()
            };
            let counter_trading_session = if counter_sessions.len() == 1 {
                counter_sessions
                    .iter()
                    .next()
                    .map_or("mixed".to_owned(), |session| (*session).to_owned())
            } else {
                "mixed".to_owned()
            };

            let rows = symbols
                .iter()
                .map(|symbol| window_symbol_row(symbol, &day_entries))
                .collect();

            PnlWindow {
                window_id: date.clone(),
                start_at: format!("{date}T00:00:00.000Z"),
                end_at: format!("{date}T23:59:59.999Z"),
                label: date.clone(),
                is_weekend: market_session_for_iso(&format!("{date}T00:00:00.000Z")) == "weekend",
                market_session,
                counter_trading_session,
                granularity: "day",
                symbols: rows,
            }
        })
        .collect()
}

fn window_symbol_row(symbol: &str, entries: &[&PnlEntry]) -> PnlWindowSymbol {
    let mut counter_trade = Num::default();
    let mut onchain_netting = Num::default();
    let directional_baseline = Num::default();
    let mut directional_excess = Num::default();

    for entry in entries {
        if entry.symbol != symbol {
            continue;
        }
        let pnl = parse_decimal_lossy(&entry.realized_pnl_usd);
        match entry_bucket_to_stream(entry.pnl_bucket) {
            Some("counterTradePnlUsd") => counter_trade += &pnl,
            Some("onchainNettingPnlUsd") => onchain_netting += &pnl,
            Some("directionalImbalanceExcessPnlUsd") => directional_excess += &pnl,
            _ => {}
        }
    }

    let directional_exposure = &directional_baseline + &directional_excess;
    let total =
        &(&(&counter_trade + &onchain_netting) + &directional_baseline) + &directional_excess;
    PnlWindowSymbol {
        symbol: symbol.to_owned(),
        counter_trade_pnl_usd: fmt_decimal(&counter_trade),
        onchain_netting_pnl_usd: fmt_decimal(&onchain_netting),
        directional_inventory_baseline_pnl_usd: fmt_decimal(&directional_baseline),
        directional_imbalance_excess_pnl_usd: fmt_decimal(&directional_excess),
        directional_exposure_pnl_usd: fmt_decimal(&directional_exposure),
        total_pnl_usd: fmt_decimal(&total),
    }
}

fn cost_entry(
    row: &CostEventRow,
    category: CostCategory,
    accounting_bucket: AccountingBucket,
    effect: AccountingEffect,
    amount_usd: Num,
    occurred_at: String,
    detail: &str,
    symbol: Option<String>,
) -> CostEntryInternal {
    CostEntryInternal {
        category,
        accounting_bucket,
        effect,
        amount_usd,
        occurred_at,
        aggregate_type: row.aggregate_type.clone(),
        aggregate_id: row.aggregate_id.clone(),
        event_rowid: row.rowid,
        symbol,
        detail: detail.to_owned(),
    }
}

fn parse_tokenized_equity_mint_cost_event(
    row: &CostEventRow,
    symbols_by_mint_aggregate: &mut HashMap<String, String>,
    warnings: &mut Vec<String>,
) -> (Option<CostEntryInternal>, bool) {
    if row.event_type == "TokenizedEquityMintEvent::MintRequested" {
        let requested = nested_record(&row.payload, "MintRequested");
        let symbol = requested.and_then(|payload| text_field(payload, "symbol"));
        if let Some(symbol) = symbol {
            if is_safe_symbol(&symbol) {
                symbols_by_mint_aggregate.insert(row.aggregate_id.clone(), symbol);
            } else {
                warnings.push(format!(
                    "Skipped unsafe tokenization cost symbol in backend PnL response: {symbol}"
                ));
            }
        }
        return (None, false);
    }

    let terminal_key = match row.event_type.as_str() {
        "TokenizedEquityMintEvent::TokensReceived" => "TokensReceived",
        "TokenizedEquityMintEvent::ProviderCompletionRecovered" => "ProviderCompletionRecovered",
        _ => return (None, false),
    };

    let Some(terminal) = nested_record(&row.payload, terminal_key) else {
        warnings.push(format!(
            "Skipped malformed tokenization cost event row {}",
            row.rowid
        ));
        return (None, false);
    };

    let occurred_at = if terminal_key == "TokensReceived" {
        text_field(terminal, "received_at")
    } else {
        text_field(terminal, "recovered_at")
    };
    let fees = decimal_field(terminal, "fees");
    let Some(occurred_at) = occurred_at else {
        warnings.push(format!(
            "Skipped tokenization fee row {} without timestamp",
            row.rowid
        ));
        return (None, false);
    };

    let Some(fees) = fees else {
        return (None, true);
    };

    if fees.is_zero() {
        return (None, false);
    }

    let entry = cost_entry(
        row,
        CostCategory::TokenizationFee,
        AccountingBucket::Generic,
        AccountingEffect::Cost,
        fees,
        occurred_at,
        "Alpaca tokenization fee reported by tokenization provider",
        symbols_by_mint_aggregate.get(&row.aggregate_id).cloned(),
    );
    (Some(entry), false)
}

fn parse_usdc_rebalance_cost_event(
    row: &CostEventRow,
    warnings: &mut Vec<String>,
) -> Option<CostEntryInternal> {
    let bridge_key = match row.event_type.as_str() {
        "UsdcRebalanceEvent::Bridged" => "Bridged",
        "UsdcRebalanceEvent::BridgingCompletionRecovered" => "BridgingCompletionRecovered",
        _ => return None,
    };

    let bridged = nested_record(&row.payload, bridge_key);
    let fee_collected = bridged.and_then(|payload| decimal_field(payload, "fee_collected"));
    let occurred_at = bridged.and_then(|payload| {
        if bridge_key == "Bridged" {
            text_field(payload, "minted_at")
        } else {
            text_field(payload, "recovered_at")
        }
    });

    let (Some(fee_collected), Some(occurred_at)) = (fee_collected, occurred_at) else {
        warnings.push(format!(
            "Skipped malformed CCTP bridge fee row {}",
            row.rowid
        ));
        return None;
    };

    if fee_collected.is_zero() {
        return None;
    }

    Some(cost_entry(
        row,
        CostCategory::CctpFee,
        AccountingBucket::Generic,
        AccountingEffect::Cost,
        fee_collected,
        occurred_at,
        "CCTP fee_collected from bridge mint",
        None,
    ))
}

struct CostReplay {
    entries: Vec<CostEntryInternal>,
    missing_cost_observation_count: usize,
}

fn build_cost_entries(rows: &[CostEventRow], warnings: &mut Vec<String>) -> CostReplay {
    let mut entries = Vec::new();
    let mut symbols_by_mint_aggregate = HashMap::new();
    let mut missing_cost_observation_count = 0;
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|row| row.rowid);

    for row in sorted {
        if row.payload.is_null() {
            warnings.push(format!(
                "Skipped malformed cost event row {}: invalid payload",
                row.rowid
            ));
            continue;
        }

        match row.aggregate_type.as_str() {
            "TokenizedEquityMint" => {
                let (entry, missing) = parse_tokenized_equity_mint_cost_event(
                    &row,
                    &mut symbols_by_mint_aggregate,
                    warnings,
                );
                if missing {
                    missing_cost_observation_count += 1;
                }
                if let Some(entry) = entry {
                    entries.push(entry);
                }
            }
            "UsdcRebalance" => {
                if let Some(entry) = parse_usdc_rebalance_cost_event(&row, warnings) {
                    entries.push(entry);
                }
            }
            other => {
                warnings.push(format!(
                    "Skipped unsupported cost event row {} with aggregate_type {}",
                    row.rowid, other
                ));
            }
        }
    }

    if missing_cost_observation_count > 0 {
        warnings.push(format!(
            "Cost coverage note: {} tokenization completion events did not report a fee value; \
             those observations are treated as zero because no fee amount is persisted.",
            missing_cost_observation_count
        ));
    }

    CostReplay {
        entries,
        missing_cost_observation_count,
    }
}

fn parse_position_view(
    rows: &[PositionViewRow],
    warnings: &mut Vec<String>,
) -> (HashMap<String, Num>, Vec<String>) {
    let mut position_nets = HashMap::new();
    let mut symbols = BTreeSet::new();

    for row in rows {
        if !is_safe_symbol(&row.symbol) {
            warnings.push(format!(
                "Skipped unsafe position_view symbol in backend PnL response: {}",
                row.symbol
            ));
            continue;
        }

        symbols.insert(row.symbol.clone());
        if let Some(net_position) = &row.net_position {
            match Num::from_str(net_position) {
                Ok(value) => {
                    position_nets.insert(row.symbol.clone(), value);
                }
                Err(_) => {
                    warnings.push(format!(
                        "Skipped malformed position net for {}: {}",
                        row.symbol, net_position
                    ));
                }
            }
        }
    }

    (position_nets, symbols.into_iter().collect())
}

fn add_sample_fill(sample: &mut SampleStatsAcc, event_type: &str, timestamp: &str) {
    if event_type == "PositionEvent::OnChainOrderFilled" {
        sample.onchain_fill_count += 1;
    } else if event_type == "PositionEvent::OffChainOrderFilled" {
        sample.offchain_fill_count += 1;
    }

    if sample
        .first_at
        .as_ref()
        .is_none_or(|first| timestamp < first)
    {
        sample.first_at = Some(timestamp.to_owned());
    }
    if sample.last_at.as_ref().is_none_or(|last| timestamp > last) {
        sample.last_at = Some(timestamp.to_owned());
    }
}

fn build_sample_stats(
    rows: &[PositionEventRow],
    query: &PnlQuery,
    warnings: &mut Vec<String>,
) -> PnlSampleStats {
    let mut by_symbol: HashMap<String, SampleStatsAcc> = HashMap::new();
    for row in rows {
        if row.event_type != "PositionEvent::OnChainOrderFilled"
            && row.event_type != "PositionEvent::OffChainOrderFilled"
        {
            continue;
        }

        if !is_safe_symbol(&row.symbol) {
            warnings.push(format!(
                "Skipped unsafe sample stats symbol in backend PnL response: {}",
                row.symbol
            ));
            continue;
        }

        let Some(timestamp) = position_event_replay_timestamp(row) else {
            warnings.push(format!(
                "Skipped sample stats row {} for {}: missing fill timestamp",
                row.rowid, row.symbol
            ));
            continue;
        };
        if !matches_date_bounds_for_iso(&timestamp, query)
            || !matches_trade_filters(&timestamp, query)
        {
            continue;
        }

        let sample = by_symbol.entry(row.symbol.clone()).or_default();
        add_sample_fill(sample, &row.event_type, &timestamp);
    }

    let mut symbols: Vec<_> = by_symbol.into_iter().collect();
    symbols.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbols: Vec<_> = symbols
        .into_iter()
        .map(|(symbol, sample)| {
            let total_fill_count = sample.onchain_fill_count + sample.offchain_fill_count;
            PnlSampleSymbolStats {
                symbol,
                first_at: sample.first_at,
                last_at: sample.last_at,
                onchain_fill_count: sample.onchain_fill_count,
                offchain_fill_count: sample.offchain_fill_count,
                total_fill_count,
            }
        })
        .collect();

    let first_at = symbols.iter().filter_map(|row| row.first_at.clone()).min();
    let last_at = symbols.iter().filter_map(|row| row.last_at.clone()).max();
    PnlSampleStats {
        first_at,
        last_at,
        symbol_count: symbols.len(),
        onchain_fill_count: symbols.iter().map(|row| row.onchain_fill_count).sum(),
        offchain_fill_count: symbols.iter().map(|row| row.offchain_fill_count).sum(),
        total_fill_count: symbols.iter().map(|row| row.total_fill_count).sum(),
        symbols,
    }
}

fn activity_timestamp(activity: &AccountActivity) -> Option<String> {
    if let Some(transaction_time) = activity.transaction_time {
        return Some(transaction_time.to_rfc3339());
    }

    activity
        .date
        .map(|date| format!("{}T16:00:00.000Z", date.format("%Y-%m-%d")))
}

fn classify_activity(
    activity_type: &str,
    signed_amount: &Num,
) -> Option<(CostCategory, AccountingBucket, AccountingEffect)> {
    if FEE_ACTIVITY_TYPES.contains(&activity_type)
        || FEE_REBATE_ACTIVITY_TYPES.contains(&activity_type)
    {
        return Some((
            CostCategory::BrokerFee,
            AccountingBucket::Generic,
            if signed_amount.is_negative() {
                AccountingEffect::Cost
            } else {
                AccountingEffect::Revenue
            },
        ));
    }

    if INTEREST_ACTIVITY_TYPES.contains(&activity_type) {
        return Some((
            CostCategory::MarginInterest,
            AccountingBucket::Generic,
            if signed_amount.is_negative() {
                AccountingEffect::Cost
            } else {
                AccountingEffect::Revenue
            },
        ));
    }

    if DIVIDEND_ACTIVITY_TYPES.contains(&activity_type) {
        return Some((
            CostCategory::DividendIncome,
            if signed_amount.is_negative() {
                AccountingBucket::Generic
            } else {
                AccountingBucket::DividendRevenue
            },
            if signed_amount.is_negative() {
                AccountingEffect::Cost
            } else {
                AccountingEffect::Revenue
            },
        ));
    }

    None
}

fn activity_detail(activity: &AccountActivity) -> String {
    let mut parts = vec![format!(
        "Alpaca account activity {}",
        activity.activity_type
    )];
    if let Some(sub_type) = &activity.activity_sub_type {
        parts.push(format!("subtype {sub_type}"));
    }
    if let Some(qty) = &activity.qty {
        parts.push(format!("qty {qty}"));
    }
    if let Some(per_share_amount) = &activity.per_share_amount {
        parts.push(format!("per-share {per_share_amount}"));
    }
    if let Some(order_id) = activity.order_id {
        parts.push(format!("order {order_id}"));
    }
    if let Some(currency) = &activity.currency {
        parts.push(format!("currency {currency}"));
    }
    if let Some(description) = &activity.description {
        parts.push(description.clone());
    }
    parts.join("; ")
}

fn build_alpaca_activity_cost_entries(activities: &[AccountActivity]) -> Vec<CostEntryInternal> {
    let mut sorted = activities.to_vec();
    sorted.sort_by(|left, right| {
        activity_timestamp(left)
            .unwrap_or_default()
            .cmp(&activity_timestamp(right).unwrap_or_default())
            .then_with(|| left.id.cmp(&right.id))
    });

    sorted
        .iter()
        .enumerate()
        .filter_map(|(idx, activity)| {
            let occurred_at = activity_timestamp(activity)?;
            let signed_amount = activity
                .net_amount
                .as_deref()
                .and_then(|amount| Num::from_str(amount).ok())?;
            if signed_amount.is_zero() {
                return None;
            }
            let (category, accounting_bucket, effect) =
                classify_activity(&activity.activity_type, &signed_amount)?;

            Some(CostEntryInternal {
                category,
                accounting_bucket,
                effect,
                amount_usd: abs_decimal(&signed_amount),
                occurred_at,
                aggregate_type: "AlpacaAccountActivity".to_owned(),
                aggregate_id: activity.id.clone(),
                event_rowid: -1 - i64::try_from(idx).ok()?,
                symbol: activity.symbol.clone().filter(|symbol| !symbol.is_empty()),
                detail: activity_detail(activity),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(rowid: i64, symbol: &str, event_type: &str, payload: Value) -> PositionEventRow {
        PositionEventRow {
            rowid,
            symbol: symbol.to_owned(),
            event_type: event_type.to_owned(),
            payload,
        }
    }

    fn onchain_sell(rowid: i64, price: &str, timestamp: &str) -> PositionEventRow {
        event(
            rowid,
            "RKLB",
            "PositionEvent::OnChainOrderFilled",
            serde_json::json!({
                "OnChainOrderFilled": {
                    "amount": "1",
                    "direction": "Sell",
                    "price_usdc": price,
                    "block_timestamp": timestamp,
                    "trade_id": {
                        "tx_hash": format!("0x{rowid}"),
                        "log_index": 0
                    }
                }
            }),
        )
    }

    fn offchain_buy(rowid: i64, timestamp: &str, price: &str, shares: &str) -> PositionEventRow {
        event(
            rowid,
            "RKLB",
            "PositionEvent::OffChainOrderFilled",
            serde_json::json!({
                "OffChainOrderFilled": {
                    "offchain_order_id": format!("alpaca-{rowid}"),
                    "shares_filled": shares,
                    "direction": "Buy",
                    "price": price,
                    "broker_timestamp": timestamp
                }
            }),
        )
    }

    fn position_rows() -> Vec<PositionViewRow> {
        vec![PositionViewRow {
            symbol: "RKLB".to_owned(),
            net_position: Some("0".to_owned()),
        }]
    }

    fn query() -> PnlQuery {
        PnlQuery {
            limit: Some(100),
            offset: Some(0),
            from_date: Some("2026-05-15".to_owned()),
            to_date: Some("2026-05-15".to_owned()),
            ..PnlQuery::default()
        }
    }

    fn report(events: Vec<PositionEventRow>) -> PnlResponse {
        build_pnl_response_from_rows(
            events,
            position_rows(),
            Vec::new(),
            Vec::new(),
            &query(),
            BTreeSet::new(),
            vec![
                ATTRIBUTION_WARNING.to_owned(),
                BASELINE_WARNING.to_owned(),
                COST_WARNING.to_owned(),
            ],
        )
    }

    #[test]
    fn maps_prompt_counter_trades_into_counter_trade_pnl() {
        let report = report(vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ]);

        assert_eq!(report.summary.counter_trade_pnl_usd, "2");
        assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "0");
        assert_eq!(report.summary.total_pnl_usd, "2");
        assert_eq!(report.entries[0].pnl_bucket, "counter_trade");
        assert!(!report.entries[0].delayed_counter_trade);
    }

    #[test]
    fn carries_offchain_origin_inventory_until_later_fills_close_it() {
        let report = report(vec![
            offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1"),
            onchain_sell(2, "10", "2026-05-15T14:02:00Z"),
        ]);

        assert_eq!(report.summary.total_pnl_usd, "2");
        assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "2");
        assert_eq!(report.summary.open_long_shares, "0");
        assert_eq!(report.entries[0].opening_venue, "offchain");
        assert_eq!(report.entries[0].closing_venue, "onchain");
        assert_eq!(report.entries[0].pnl_bucket, "directional_exposure");
    }

    #[test]
    fn splits_offchain_overshoots_between_close_and_carried_inventory() {
        let report = report(vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "2"),
            onchain_sell(3, "11", "2026-05-15T14:02:00Z"),
        ]);

        assert_eq!(report.summary.total_pnl_usd, "5");
        assert_eq!(report.summary.counter_trade_pnl_usd, "2");
        assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "3");
        assert_eq!(report.summary.open_long_shares, "0");
        assert_eq!(report.summary.open_short_shares, "0");
        assert_eq!(report.entries.len(), 2);
    }

    #[test]
    fn reports_current_unmatched_offchain_origin_inventory() {
        let report = report(vec![offchain_buy(1, "2026-05-15T14:01:00Z", "8", "2")]);

        assert_eq!(report.summary.total_pnl_usd, "0");
        assert_eq!(report.summary.open_long_shares, "2");
        assert_eq!(report.summary.unmatched_offchain_shares, "2");
        assert_eq!(report.summary.unmatched_offchain_notional_usd, "16");
        assert_eq!(report.summary.unmatched_offchain_fill_count, 1);
    }

    #[test]
    fn filters_sample_stats_by_selected_date_range() {
        let report = report(vec![
            onchain_sell(1, "10", "2026-05-14T14:00:00Z"),
            onchain_sell(2, "11", "2026-05-15T14:00:00Z"),
            offchain_buy(3, "2026-05-16T14:00:00Z", "9", "1"),
        ]);

        assert_eq!(
            report.sample_stats.first_at.as_deref(),
            Some("2026-05-15T14:00:00Z")
        );
        assert_eq!(
            report.sample_stats.last_at.as_deref(),
            Some("2026-05-15T14:00:00Z")
        );
        assert_eq!(report.sample_stats.onchain_fill_count, 1);
        assert_eq!(report.sample_stats.offchain_fill_count, 0);
        assert_eq!(report.sample_stats.total_fill_count, 1);
    }
}
