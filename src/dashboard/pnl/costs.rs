use std::collections::HashMap;
use std::str::FromStr;

use num_decimal::Num;
use st0x_execution::AccountActivity;

use super::parsing::{
    abs_decimal, decimal_field, fmt_decimal, is_safe_symbol, nested_record, parse_internal_decimal,
    text_field,
};
use super::response::{PnlCostCoverage, PnlCostSummary, PnlSummary};
use super::state::CostEventRow;

const FEE_ACTIVITY_TYPES: &[&str] = &["FEE", "PTC"];
const FEE_REBATE_ACTIVITY_TYPES: &[&str] = &["PTR"];
const INTEREST_ACTIVITY_TYPES: &[&str] = &["INT", "INTNRA", "INTTW"];
const DIVIDEND_ACTIVITY_TYPES: &[&str] = &[
    "DIV", "DIVCGL", "DIVCGS", "DIVFEE", "DIVFT", "DIVNRA", "DIVROC", "DIVTW", "DIVTXEX",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CostCategory {
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
    pub(crate) fn as_str(self) -> &'static str {
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
    pub(crate) symbol: Option<String>,
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
                AccountingBucket::CounterTrade,
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

pub(crate) fn with_costs(summary: PnlSummary, costs: &PnlCostSummary) -> PnlSummary {
    let gross = parse_internal_decimal(&summary.total_pnl_usd);
    let tracked_costs = parse_internal_decimal(&costs.total_tracked_costs_usd);
    let tracked_revenue = parse_internal_decimal(&costs.total_tracked_revenue_usd);
    let net = &(&gross - &tracked_costs) + &tracked_revenue;

    PnlSummary {
        gross_realized_pnl_usd: fmt_decimal(&gross),
        tracked_costs_usd: fmt_decimal(&tracked_costs),
        tracked_revenue_usd: fmt_decimal(&tracked_revenue),
        net_realized_pnl_usd: fmt_decimal(&net),
        ..summary
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

pub(crate) struct CostReplay {
    pub(crate) entries: Vec<CostEntryInternal>,
    pub(crate) missing_cost_observation_count: usize,
}

pub(crate) fn build_cost_entries(rows: &[CostEventRow], warnings: &mut Vec<String>) -> CostReplay {
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
            "Cost coverage note: {missing_cost_observation_count} tokenization completion events did not report a fee value; \
             those observations are treated as zero because no fee amount is persisted.",
        ));
    }

    CostReplay {
        entries,
        missing_cost_observation_count,
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

pub(crate) fn build_alpaca_activity_cost_entries(
    activities: &[AccountActivity],
    warnings: &mut Vec<String>,
) -> Vec<CostEntryInternal> {
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
            let Some(occurred_at) = activity_timestamp(activity) else {
                warnings.push(format!(
                    "Skipped malformed Alpaca account activity {}: missing timestamp/date",
                    activity.id
                ));
                return None;
            };
            let Some(net_amount) = activity.net_amount.as_deref() else {
                warnings.push(format!(
                    "Skipped malformed Alpaca account activity {}: missing net_amount",
                    activity.id
                ));
                return None;
            };
            let signed_amount = match Num::from_str(net_amount) {
                Ok(amount) => amount,
                Err(error) => {
                    warnings.push(format!(
                        "Skipped malformed Alpaca account activity {}: invalid net_amount {} ({error})",
                        activity.id, net_amount
                    ));
                    return None;
                }
            };
            if signed_amount.is_zero() {
                return None;
            }
            let (category, accounting_bucket, effect) =
                classify_activity(&activity.activity_type, &signed_amount)?;
            let event_rowid = match i64::try_from(idx) {
                Ok(idx) => -1 - idx,
                Err(error) => {
                    warnings.push(format!(
                        "Skipped malformed Alpaca account activity {}: activity index cannot be represented as i64 ({error})",
                        activity.id
                    ));
                    return None;
                }
            };

            Some(CostEntryInternal {
                category,
                accounting_bucket,
                effect,
                amount_usd: abs_decimal(&signed_amount),
                occurred_at,
                aggregate_type: "AlpacaAccountActivity".to_owned(),
                aggregate_id: activity.id.clone(),
                event_rowid,
                symbol: activity.symbol.clone().filter(|symbol| !symbol.is_empty()),
                detail: activity_detail(activity),
            })
        })
        .collect()
}
