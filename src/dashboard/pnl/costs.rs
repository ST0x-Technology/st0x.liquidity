//! Cost and revenue classification for backend PnL reports.
use num_decimal::Num;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use st0x_execution::alpaca_broker_api::AccountActivity;
use st0x_finance::Symbol;

use super::parsing::{
    abs_decimal, fmt_decimal, is_safe_symbol, nested_record, parse_internal_decimal,
    persisted_decimal_value, text_field,
};
use super::query::{PnlError, PnlFinancialFieldError};
use super::response::{PnlCostCoverage, PnlCostSummary, PnlSummary};
use super::state::CostEventRow;

const FEE_ACTIVITY_TYPES: &[&str] = &["FEE", "PTC"];
const INTEREST_ACTIVITY_TYPES: &[&str] = &["INT"];
// Activity codes are selected from Alpaca's documented account activity enum:
// https://docs.alpaca.markets/us/reference/getaccountactivitiesbytype
// `CIL` is cash in lieu and is reported with dividend/corporate-action income because it is a
// cash substitute for fractional corporate-action proceeds.
const DIVIDEND_ACTIVITY_TYPES: &[&str] = &[
    "DIV", "DIVCGL", "DIVCGS", "DIVNRA", "DIVROC", "DIVTXEX", "CGD", "CIL",
];
const CASH_CREDIT_ACTIVITY_TYPES: &[&str] = &["CSD"];

fn malformed_cost_payload(row: &CostEventRow, reason: &'static str) -> PnlError {
    PnlError::MalformedPayload {
        rowid: row.rowid,
        aggregate_type: "CostEvent",
        event_type: row.event_type.clone(),
        reason,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CostCategory {
    TokenizationFee,
    CctpFee,
    BotGas,
    BrokerFee,
    MarginInterest,
    DividendIncome,
    CashCredit,
}

impl CostCategory {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TokenizationFee => "tokenization_fee",
            Self::CctpFee => "cctp_fee",
            Self::BotGas => "bot_gas",
            Self::BrokerFee => "broker_fee",
            Self::MarginInterest => "margin_interest",
            Self::DividendIncome => "dividend_income",
            Self::CashCredit => "cash_credit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountingBucket {
    CounterTrade,
    OnchainNetting,
    DirectionalExposure,
    Generic,
    DividendRevenue,
}

impl AccountingBucket {
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) enum AccountingEffect {
    Cost,
    Revenue,
    None,
}

impl AccountingEffect {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::Revenue => "revenue",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CostEntryInternal {
    pub(crate) category: CostCategory,
    pub(crate) accounting_bucket: AccountingBucket,
    pub(crate) effect: AccountingEffect,
    pub(crate) amount_usd: Num,
    pub(crate) occurred_at: String,
    pub(crate) aggregate_type: String,
    pub(crate) aggregate_id: String,
    pub(crate) event_rowid: i64,
    pub(crate) symbol: Option<Symbol>,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CostSummaryAcc {
    pub(crate) counter_trade_costs_usd: Num,
    pub(crate) onchain_netting_costs_usd: Num,
    pub(crate) directional_exposure_costs_usd: Num,
    pub(crate) generic_costs_usd: Num,
    pub(crate) generic_revenue_usd: Num,
    pub(crate) dividend_revenue_usd: Num,
    pub(crate) offchain_execution_fees_usd: Num,
    pub(crate) tokenization_fees_usd: Num,
    pub(crate) cctp_fees_usd: Num,
    pub(crate) conversion_slippage_usd: Num,
    pub(crate) oracle_write_cost_usd: Num,
    pub(crate) broker_fees_usd: Num,
    pub(crate) regulatory_fees_usd: Num,
    pub(crate) margin_interest_usd: Num,
    pub(crate) bot_gas_usd: Num,
    pub(crate) wallet_transfer_fees_usd: Num,
    pub(crate) unclassified_costs_usd: Num,
    pub(crate) missing_cost_observation_count: usize,
    pub(crate) broker_fee_entry_count: usize,
    pub(crate) margin_interest_entry_count: usize,
    pub(crate) dividend_activity_entry_count: usize,
}

fn signed_category_amount(effect: AccountingEffect, amount: &Num) -> Num {
    match effect {
        AccountingEffect::Cost => -amount.clone(),
        AccountingEffect::Revenue => amount.clone(),
        AccountingEffect::None => Num::default(),
    }
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

    let signed_amount = signed_category_amount(effect, amount);
    match category {
        CostCategory::TokenizationFee => summary.tokenization_fees_usd += &signed_amount,
        CostCategory::CctpFee => summary.cctp_fees_usd += &signed_amount,
        CostCategory::BotGas => summary.bot_gas_usd += amount,
        CostCategory::BrokerFee => {
            summary.broker_fees_usd += &signed_amount;
            summary.broker_fee_entry_count += 1;
        }
        CostCategory::MarginInterest => {
            summary.margin_interest_usd += &signed_amount;
            summary.margin_interest_entry_count += 1;
        }
        CostCategory::DividendIncome => {
            summary.dividend_activity_entry_count += 1;
        }
        CostCategory::CashCredit => {}
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

fn included_when_observed(count: usize) -> &'static str {
    if count == 0 {
        "not_ingested"
    } else {
        "included"
    }
}

fn fmt_signed_category_amount(value: &Num) -> String {
    fmt_decimal(&abs_decimal(value))
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
        amount_usd: fmt_signed_category_amount(amount),
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
        offchain_execution_fees_usd: fmt_signed_category_amount(&offchain_execution_fees),
        tokenization_fees_usd: fmt_signed_category_amount(&summary.tokenization_fees_usd),
        cctp_fees_usd: fmt_signed_category_amount(&summary.cctp_fees_usd),
        conversion_slippage_usd: fmt_decimal(&summary.conversion_slippage_usd),
        oracle_write_cost_usd: fmt_decimal(&summary.oracle_write_cost_usd),
        broker_fees_usd: fmt_signed_category_amount(&summary.broker_fees_usd),
        regulatory_fees_usd: fmt_decimal(&summary.regulatory_fees_usd),
        margin_interest_usd: fmt_signed_category_amount(&summary.margin_interest_usd),
        bot_gas_usd: fmt_decimal(&summary.bot_gas_usd),
        wallet_transfer_fees_usd: fmt_decimal(&summary.wallet_transfer_fees_usd),
        unclassified_costs_usd: fmt_decimal(&summary.unclassified_costs_usd),
        cost_entry_count,
        missing_cost_observation_count: summary.missing_cost_observation_count,
        coverage: vec![
            coverage(
                "Alpaca fees",
                AccountingBucket::CounterTrade,
                AccountingEffect::Cost,
                included_when_observed(summary.broker_fee_entry_count),
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
                included_when_observed(summary.dividend_activity_entry_count),
                &summary.dividend_revenue_usd,
                "Dividend-bearing stock revenue increases net PnL when Alpaca dividend activity rows are available.",
            ),
            coverage(
                "Margin interest",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                included_when_observed(summary.margin_interest_entry_count),
                &summary.margin_interest_usd,
                "Included when Alpaca account activity interest rows are available; negative rows are costs and positive rows are credits.",
            ),
            coverage(
                "Bot gas",
                AccountingBucket::Generic,
                AccountingEffect::Cost,
                included_when_observed(usize::from(!summary.bot_gas_usd.is_zero())),
                &summary.bot_gas_usd,
                "Read from persisted bot-paid transaction receipts after gas-payer classification and ETH/USD valuation.",
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

pub(crate) fn summarize_cost_entries(
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

pub(crate) fn with_costs(
    summary: PnlSummary,
    costs: &PnlCostSummary,
) -> Result<PnlSummary, PnlError> {
    let gross = parse_internal_decimal("summary.totalPnlUsd", &summary.total_pnl_usd)?;
    let tracked_costs =
        parse_internal_decimal("costs.totalTrackedCostsUsd", &costs.total_tracked_costs_usd)?;
    let tracked_revenue = parse_internal_decimal(
        "costs.totalTrackedRevenueUsd",
        &costs.total_tracked_revenue_usd,
    )?;
    let net = &(&gross - &tracked_costs) + &tracked_revenue;

    Ok(PnlSummary {
        gross_realized_pnl_usd: fmt_decimal(&gross),
        tracked_costs_usd: fmt_decimal(&tracked_costs),
        tracked_revenue_usd: fmt_decimal(&tracked_revenue),
        net_realized_pnl_usd: fmt_decimal(&net),
        ..summary
    })
}

fn cost_entry(
    row: &CostEventRow,
    category: CostCategory,
    accounting_bucket: AccountingBucket,
    effect: AccountingEffect,
    amount_usd: Num,
    occurred_at: String,
    detail: &str,
    symbol: Option<Symbol>,
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
    symbols_by_mint_aggregate: &mut HashMap<String, Symbol>,
    warnings: &mut Vec<String>,
) -> Result<Option<CostEntryInternal>, PnlError> {
    if row.event_type == "TokenizedEquityMintEvent::MintRequested" {
        let requested = nested_record(&row.payload, "MintRequested");
        let symbol = requested.and_then(|payload| text_field(payload, "symbol"));
        if let Some(symbol) = symbol {
            if is_safe_symbol(&symbol) {
                if let Ok(symbol) = Symbol::new(symbol) {
                    symbols_by_mint_aggregate.insert(row.aggregate_id.clone(), symbol);
                }
            } else {
                warnings.push(format!(
                    "Skipped unsafe tokenization cost symbol in backend PnL response: {symbol}"
                ));
            }
        }
        return Ok(None);
    }

    let terminal_key = match row.event_type.as_str() {
        "TokenizedEquityMintEvent::TokensReceived" => "TokensReceived",
        "TokenizedEquityMintEvent::ProviderCompletionRecovered" => "ProviderCompletionRecovered",
        _ => return Ok(None),
    };

    let Some(terminal) = nested_record(&row.payload, terminal_key) else {
        return Err(malformed_cost_payload(
            row,
            "missing tokenization terminal payload",
        ));
    };

    let occurred_at = if terminal_key == "TokensReceived" {
        text_field(terminal, "received_at")
    } else {
        text_field(terminal, "recovered_at")
    };
    let fees = persisted_decimal_value(
        row.rowid,
        "TokenizedEquityMint",
        row.event_type.clone(),
        terminal,
        "fees",
    )?;
    let Some(occurred_at) = occurred_at else {
        return Err(malformed_cost_payload(
            row,
            "missing tokenization fee timestamp",
        ));
    };

    let Some(fees) = fees else {
        return Err(malformed_cost_payload(row, "missing tokenization fees"));
    };

    if fees.is_zero() {
        return Ok(None);
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
    Ok(Some(entry))
}

fn parse_usdc_rebalance_cost_event(
    row: &CostEventRow,
    _warnings: &mut Vec<String>,
) -> Result<Option<CostEntryInternal>, PnlError> {
    let bridge_key = match row.event_type.as_str() {
        "UsdcRebalanceEvent::Bridged" => "Bridged",
        "UsdcRebalanceEvent::BridgingCompletionRecovered" => "BridgingCompletionRecovered",
        _ => return Ok(None),
    };

    let bridged = nested_record(&row.payload, bridge_key);
    let Some(bridged) = bridged else {
        return Err(malformed_cost_payload(row, "missing CCTP bridge payload"));
    };
    let fee_collected = persisted_decimal_value(
        row.rowid,
        "UsdcRebalance",
        row.event_type.clone(),
        bridged,
        "fee_collected",
    )?;
    let occurred_at = {
        if bridge_key == "Bridged" {
            text_field(bridged, "minted_at")
        } else {
            text_field(bridged, "recovered_at")
        }
    };

    let (Some(fee_collected), Some(occurred_at)) = (fee_collected, occurred_at) else {
        return Err(malformed_cost_payload(
            row,
            "missing CCTP fee_collected or timestamp",
        ));
    };

    if fee_collected.is_zero() {
        return Ok(None);
    }

    Ok(Some(cost_entry(
        row,
        CostCategory::CctpFee,
        AccountingBucket::Generic,
        AccountingEffect::Cost,
        fee_collected,
        occurred_at,
        "CCTP fee_collected from bridge mint",
        None,
    )))
}

pub(crate) struct CostReplay {
    pub(crate) entries: Vec<CostEntryInternal>,
    pub(crate) missing_cost_observation_count: usize,
}

pub(crate) fn build_cost_entries(
    rows: &[CostEventRow],
    warnings: &mut Vec<String>,
) -> Result<CostReplay, PnlError> {
    let mut entries = Vec::new();
    let mut symbols_by_mint_aggregate = HashMap::new();
    let mut counted_tokenization_fee_aggregates = HashSet::new();
    let missing_cost_observation_count = 0;
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
                let entry = parse_tokenized_equity_mint_cost_event(
                    &row,
                    &mut symbols_by_mint_aggregate,
                    warnings,
                )?;
                if let Some(entry) = entry {
                    if counted_tokenization_fee_aggregates.insert(row.aggregate_id.clone()) {
                        entries.push(entry);
                    } else {
                        warnings.push(format!(
                            "Skipped duplicate tokenization fee for mint aggregate {}",
                            row.aggregate_id
                        ));
                    }
                }
            }
            "UsdcRebalance" => {
                if let Some(entry) = parse_usdc_rebalance_cost_event(&row, warnings)? {
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

    Ok(CostReplay {
        entries,
        missing_cost_observation_count,
    })
}

fn activity_timestamp(activity: &AccountActivity) -> Option<String> {
    if let Some(transaction_time) = activity.transaction_time {
        return Some(transaction_time.to_rfc3339());
    }

    if let Some(date) = activity.date {
        return Some(date.format("%Y-%m-%d").to_string());
    }

    activity
        .created_at
        .map(|created_at| created_at.to_rfc3339())
}

fn classify_activity(
    activity_type: &str,
    signed_amount: &Num,
) -> Option<(CostCategory, AccountingBucket, AccountingEffect)> {
    // Treat Alpaca `net_amount` as the signed broker-cash delta for the account activity row:
    // negative values decrease cash and positive values increase cash. The broker docs enumerate
    // activity codes but do not publish a status/sign matrix for every activity, so unknown
    // activity/status values fail the report instead of being silently skipped.
    if FEE_ACTIVITY_TYPES.contains(&activity_type) {
        return Some((
            CostCategory::BrokerFee,
            AccountingBucket::CounterTrade,
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

    if CASH_CREDIT_ACTIVITY_TYPES.contains(&activity_type) {
        return Some((
            CostCategory::CashCredit,
            AccountingBucket::Generic,
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

fn alpaca_activity_rowid(idx: usize) -> Result<i64, PnlError> {
    i64::try_from(idx)
        .map(|idx| -1 - idx)
        .map_err(|_| PnlError::MalformedPayload {
            rowid: i64::MIN,
            aggregate_type: "AlpacaAccountActivity",
            event_type: "index".to_owned(),
            reason: "activity index cannot be represented as i64",
        })
}

fn malformed_alpaca_activity(
    activity: &AccountActivity,
    rowid: i64,
    reason: &'static str,
) -> PnlError {
    PnlError::MalformedPayload {
        rowid,
        aggregate_type: "AlpacaAccountActivity",
        event_type: activity.activity_type.clone(),
        reason,
    }
}

fn parse_alpaca_net_amount(activity: &AccountActivity, rowid: i64) -> Result<Num, PnlError> {
    let Some(net_amount) = activity.net_amount.as_deref() else {
        return Err(malformed_alpaca_activity(
            activity,
            rowid,
            "missing Alpaca net_amount",
        ));
    };

    Num::from_str(net_amount).map_err(|error| PnlError::InvalidFinancialField {
        rowid,
        aggregate_type: "AlpacaAccountActivity",
        event_type: activity.activity_type.clone(),
        field: "net_amount",
        value: net_amount.to_owned(),
        source: PnlFinancialFieldError::InvalidDecimal(error),
    })
}

fn alpaca_activity_symbol(
    activity: &AccountActivity,
    rowid: i64,
) -> Result<Option<Symbol>, PnlError> {
    let Some(symbol) = activity
        .symbol
        .as_deref()
        .filter(|symbol| !symbol.is_empty())
    else {
        return Ok(None);
    };

    if !is_safe_symbol(symbol) {
        return Err(malformed_alpaca_activity(
            activity,
            rowid,
            "unsafe Alpaca account activity symbol",
        ));
    }

    Symbol::new(symbol.to_owned()).map(Some).map_err(|_| {
        malformed_alpaca_activity(activity, rowid, "invalid Alpaca account activity symbol")
    })
}

pub(crate) fn build_alpaca_activity_cost_entries(
    activities: &[AccountActivity],
    warnings: &mut Vec<String>,
) -> Result<Vec<CostEntryInternal>, PnlError> {
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
            let event_rowid = match alpaca_activity_rowid(idx) {
                Ok(rowid) => rowid,
                Err(error) => return Some(Err(error)),
            };
            let Some(occurred_at) = activity_timestamp(activity) else {
                return Some(Err(malformed_alpaca_activity(
                    activity,
                    event_rowid,
                    "missing Alpaca timestamp/date",
                )));
            };
            let signed_amount = match parse_alpaca_net_amount(activity, event_rowid) {
                Ok(amount) => amount,
                Err(error) => return Some(Err(error)),
            };
            if signed_amount.is_zero() {
                return None;
            }
            if let Some(currency) = activity.currency.as_deref()
                && !currency.eq_ignore_ascii_case("USD")
            {
                return Some(Err(malformed_alpaca_activity(
                    activity,
                    event_rowid,
                    "unsupported Alpaca account activity currency",
                )));
            }
            if let Some(status) = activity.status.as_deref() {
                // Alpaca's public docs do not enumerate activity statuses. These are the statuses
                // observed for immutable ledger rows: `executed`, corrected rows as `correct`, and
                // reversals as `canceled`. Unknown statuses fail the report instead of being
                // silently omitted.
                match status {
                    "executed" | "correct" => {}
                    "canceled" => {
                        warnings.push(format!(
                            "Skipped canceled Alpaca account activity {}",
                            activity.id
                        ));
                        return None;
                    }
                    _ => {
                        return Some(Err(malformed_alpaca_activity(
                            activity,
                            event_rowid,
                            "unsupported Alpaca account activity status",
                        )));
                    }
                }
            }
            let Some((category, accounting_bucket, effect)) =
                classify_activity(&activity.activity_type, &signed_amount)
            else {
                return Some(Err(malformed_alpaca_activity(
                    activity,
                    event_rowid,
                    "unsupported Alpaca account activity type",
                )));
            };

            Some(Ok(CostEntryInternal {
                category,
                accounting_bucket,
                effect,
                amount_usd: abs_decimal(&signed_amount),
                occurred_at,
                aggregate_type: "AlpacaAccountActivity".to_owned(),
                aggregate_id: activity.id.clone(),
                event_rowid,
                symbol: match alpaca_activity_symbol(activity, event_rowid) {
                    Ok(symbol) => symbol,
                    Err(error) => return Some(Err(error)),
                },
                detail: activity_detail(activity),
            }))
        })
        .collect()
}
