use std::collections::BTreeSet;

use chrono::{TimeZone, Utc};
use serde_json::Value;
use st0x_execution::AccountActivity;

use super::builder::build_pnl_response_from_rows;
use super::parsing::{fmt_decimal, parse_payload_string, parse_timestamp};
use super::query::{PnlCounterTradingFilter, PnlMarketSessionFilter, PnlQuery};
use super::response::{PnlResponse, PnlSymbolSummary, PnlWindow, PnlWindowSymbol};
use super::state::{CostEventRow, Direction, PnlBucket, PositionEventRow, PositionViewRow, Venue};
use super::{ATTRIBUTION_WARNING, BASELINE_WARNING, COST_WARNING};

fn event(rowid: i64, symbol: &str, event_type: &str, payload: Value) -> PositionEventRow {
    PositionEventRow {
        rowid,
        symbol: symbol.to_owned(),
        event_type: event_type.to_owned(),
        payload,
    }
}

fn onchain_fill(
    rowid: i64,
    symbol: &str,
    direction: &str,
    price: &str,
    shares: &str,
    timestamp: &str,
) -> PositionEventRow {
    event(
        rowid,
        symbol,
        "PositionEvent::OnChainOrderFilled",
        serde_json::json!({
            "OnChainOrderFilled": {
                "amount": shares,
                "direction": direction,
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

fn onchain_sell(rowid: i64, price: &str, timestamp: &str) -> PositionEventRow {
    onchain_fill(rowid, "RKLB", "Sell", price, "1", timestamp)
}

fn onchain_buy(rowid: i64, price: &str, timestamp: &str) -> PositionEventRow {
    onchain_fill(rowid, "RKLB", "Buy", price, "1", timestamp)
}

fn offchain_fill(
    rowid: i64,
    symbol: &str,
    direction: &str,
    timestamp: &str,
    price: &str,
    shares: &str,
) -> PositionEventRow {
    event(
        rowid,
        symbol,
        "PositionEvent::OffChainOrderFilled",
        serde_json::json!({
            "OffChainOrderFilled": {
                "offchain_order_id": format!("alpaca-{rowid}"),
                "shares_filled": shares,
                "direction": direction,
                "price": price,
                "broker_timestamp": timestamp
            }
        }),
    )
}

fn offchain_buy(rowid: i64, timestamp: &str, price: &str, shares: &str) -> PositionEventRow {
    offchain_fill(rowid, "RKLB", "Buy", timestamp, price, shares)
}

fn offchain_sell(rowid: i64, timestamp: &str, price: &str, shares: &str) -> PositionEventRow {
    offchain_fill(rowid, "RKLB", "Sell", timestamp, price, shares)
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

#[test]
fn query_to_date_uses_exclusive_next_day_for_date_values() {
    let query = PnlQuery {
        to_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    };

    assert_eq!(
        query.activity_until().unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 16, 0, 0, 0).single()
    );
}

#[test]
fn query_to_timestamp_uses_exact_exclusive_bound() {
    let query = PnlQuery {
        to_date: Some("2026-05-15T14:30:00Z".to_owned()),
        ..PnlQuery::default()
    };

    assert_eq!(
        query.activity_until().unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 15, 14, 30, 0).single()
    );
}

#[test]
fn malformed_persisted_payloads_are_rejected() {
    assert!(parse_payload_string("{not-json").is_err());
}

fn position_row(symbol: &str, net_position: &str) -> PositionViewRow {
    PositionViewRow {
        symbol: symbol.to_owned(),
        net_position: Some(net_position.to_owned()),
    }
}

fn cost_event(
    rowid: i64,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: Value,
) -> CostEventRow {
    CostEventRow {
        rowid,
        aggregate_type: aggregate_type.to_owned(),
        aggregate_id: aggregate_id.to_owned(),
        event_type: event_type.to_owned(),
        payload,
    }
}

fn tokenized_mint_requested(rowid: i64, aggregate_id: &str, symbol: &str) -> CostEventRow {
    cost_event(
        rowid,
        "TokenizedEquityMint",
        aggregate_id,
        "TokenizedEquityMintEvent::MintRequested",
        serde_json::json!({
            "MintRequested": {
                "symbol": symbol
            }
        }),
    )
}

fn tokenized_tokens_received(
    rowid: i64,
    aggregate_id: &str,
    fees: &str,
    timestamp: &str,
) -> CostEventRow {
    cost_event(
        rowid,
        "TokenizedEquityMint",
        aggregate_id,
        "TokenizedEquityMintEvent::TokensReceived",
        serde_json::json!({
            "TokensReceived": {
                "received_at": timestamp,
                "fees": fees
            }
        }),
    )
}

fn usdc_bridged(rowid: i64, aggregate_id: &str, fee: &str, timestamp: &str) -> CostEventRow {
    cost_event(
        rowid,
        "UsdcRebalance",
        aggregate_id,
        "UsdcRebalanceEvent::Bridged",
        serde_json::json!({
            "Bridged": {
                "minted_at": timestamp,
                "fee_collected": fee
            }
        }),
    )
}

fn account_activity(
    id: &str,
    activity_type: &str,
    amount: &str,
    symbol: Option<&str>,
    timestamp: &str,
) -> AccountActivity {
    AccountActivity {
        id: id.to_owned(),
        activity_type: activity_type.to_owned(),
        activity_sub_type: None,
        date: None,
        created_at: None,
        net_amount: Some(amount.to_owned()),
        symbol: symbol.map(str::to_owned),
        qty: None,
        per_share_amount: None,
        price: None,
        side: None,
        order_id: None,
        transaction_time: parse_timestamp(timestamp),
        description: None,
        status: None,
        group_id: None,
        currency: Some("USD".to_owned()),
    }
}

fn report_with(
    events: Vec<PositionEventRow>,
    position_rows: Vec<PositionViewRow>,
    cost_rows: Vec<CostEventRow>,
    alpaca_activities: Vec<AccountActivity>,
    query: PnlQuery,
    symbols: BTreeSet<String>,
) -> PnlResponse {
    struct ReportInput {
        events: Vec<PositionEventRow>,
        position_rows: Vec<PositionViewRow>,
        cost_rows: Vec<CostEventRow>,
        alpaca_activities: Vec<AccountActivity>,
        query: PnlQuery,
        symbols: BTreeSet<String>,
    }

    let input = ReportInput {
        events,
        position_rows,
        cost_rows,
        alpaca_activities,
        query,
        symbols,
    };
    let ReportInput {
        events,
        position_rows,
        cost_rows,
        alpaca_activities,
        query,
        symbols,
    } = input;

    build_pnl_response_from_rows(
        events,
        &position_rows,
        &cost_rows,
        &alpaca_activities,
        &query,
        &symbols,
        vec![
            ATTRIBUTION_WARNING.to_owned(),
            BASELINE_WARNING.to_owned(),
            COST_WARNING.to_owned(),
        ],
    )
}

fn report(events: Vec<PositionEventRow>) -> PnlResponse {
    report_with(
        events,
        position_rows(),
        Vec::new(),
        Vec::new(),
        query(),
        BTreeSet::new(),
    )
}

fn symbol_summary<'a>(report: &'a PnlResponse, symbol: &str) -> &'a PnlSymbolSummary {
    report
        .symbols
        .iter()
        .find(|row| row.symbol == symbol)
        .expect("missing symbol summary")
}

fn window_symbol<'a>(window: &'a PnlWindow, symbol: &str) -> &'a PnlWindowSymbol {
    window
        .symbols
        .iter()
        .find(|row| row.symbol == symbol)
        .expect("missing window symbol")
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
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::CounterTrade);
    assert!(!report.entries[0].delayed_counter_trade);
}

#[test]
fn replays_fills_by_execution_timestamp_before_rowid() {
    let report = report(vec![
        offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1"),
        onchain_sell(2, "10", "2026-05-15T14:00:00Z"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(report.entries[0].opening_rowid, 2);
    assert_eq!(report.entries[0].closing_rowid, 1);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::CounterTrade);
}

#[test]
fn closes_long_inventory_with_counter_trade_sell() {
    let report = report(vec![
        onchain_buy(1, "8", "2026-05-15T14:00:00Z"),
        offchain_sell(2, "2026-05-15T14:01:00Z", "10", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].opening_direction, Direction::Buy);
    assert_eq!(report.entries[0].closing_direction, Direction::Sell);
}

#[test]
fn nets_onchain_fills_by_fifo_without_offchain_parentage() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        onchain_buy(2, "8", "2026-05-15T14:01:00Z"),
    ]);

    assert_eq!(report.summary.onchain_netting_pnl_usd, "2");
    assert_eq!(report.summary.counter_trade_pnl_usd, "0");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].opening_venue, Venue::Onchain);
    assert_eq!(report.entries[0].closing_venue, Venue::Onchain);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::OnchainNetting);
}

#[test]
fn delayed_counter_trade_is_bucketed_as_directional_exposure() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:10:01Z", "8", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "0");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "2");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::DirectionalExposure);
    assert!(report.entries[0].delayed_counter_trade);
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
    assert_eq!(report.entries[0].opening_venue, Venue::Offchain);
    assert_eq!(report.entries[0].closing_venue, Venue::Onchain);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::DirectionalExposure);
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
fn date_filter_uses_realized_close_date() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-14T20:00:00Z"),
        offchain_buy(2, "2026-05-15T14:00:00Z", "8", "1"),
    ]);

    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].opened_at, "2026-05-14T20:00:00Z");
    assert_eq!(report.entries[0].closed_at, "2026-05-15T14:00:00Z");
}

#[test]
fn date_filter_and_windows_use_new_york_trading_date() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-16T01:00:00Z"),
        offchain_buy(2, "2026-05-16T01:01:00Z", "9", "1"),
    ]);

    assert_eq!(report.total, 1);
    assert_eq!(report.entries[0].closing_rowid, 2);
    assert_eq!(report.windows[0].label, "2026-05-15");
}

#[test]
fn paginates_entries_without_changing_filtered_summary() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T15:00:00Z"),
            offchain_buy(4, "2026-05-15T15:01:00Z", "17", "1"),
        ],
        position_rows(),
        Vec::new(),
        Vec::new(),
        PnlQuery {
            limit: Some(1),
            offset: Some(0),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(report.total, 2);
    assert!(report.has_more);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.summary.total_pnl_usd, "5");
}

#[test]
fn counter_trading_filter_keeps_rth_closes_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T11:59:00Z"),
            offchain_buy(2, "2026-05-15T12:00:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T13:59:00Z"),
            offchain_buy(4, "2026-05-15T14:00:00Z", "17", "1"),
        ],
        position_rows(),
        Vec::new(),
        Vec::new(),
        PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingActive),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "3");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closed_at, "2026-05-15T14:00:00Z");
    assert_eq!(report.sample_stats.total_fill_count, 2);
}

#[test]
fn counter_trading_filter_keeps_inactive_closes_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        position_rows(),
        Vec::new(),
        Vec::new(),
        PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingInactive),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closing_rowid, 2);
    assert_eq!(
        report.windows[0].counter_trading_session,
        "counter_trading_inactive"
    );
}

#[test]
fn market_session_filter_is_independent_from_counter_trading_filter() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        position_rows(),
        Vec::new(),
        Vec::new(),
        PnlQuery {
            market_session_filter: Some(PnlMarketSessionFilter::Rth),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "3");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closing_rowid, 4);
    assert_eq!(report.windows[0].market_session, "rth");
    assert_eq!(
        report.windows[0].counter_trading_session,
        "counter_trading_active"
    );
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

#[test]
fn filters_sample_stats_by_selected_market_session() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        position_rows(),
        Vec::new(),
        Vec::new(),
        PnlQuery {
            market_session_filter: Some(PnlMarketSessionFilter::Pre),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T12:00:00Z")
    );
    assert_eq!(
        report.sample_stats.last_at.as_deref(),
        Some("2026-05-15T12:01:00Z")
    );
    assert_eq!(report.sample_stats.onchain_fill_count, 1);
    assert_eq!(report.sample_stats.offchain_fill_count, 1);
    assert_eq!(report.sample_stats.total_fill_count, 2);
}

#[test]
fn deducts_account_level_alpaca_fees_from_aggregate_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        position_rows(),
        Vec::new(),
        vec![account_activity(
            "fee-1",
            "FEE",
            "-0.25",
            None,
            "2026-05-15T14:02:00Z",
        )],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(report.summary.tracked_costs_usd, "0.25");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.75");
    assert_eq!(report.costs.counter_trade_costs_usd, "0.25");
    assert_eq!(report.costs.broker_fees_usd, "0.25");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "2");
}

#[test]
fn tracked_costs_follow_counter_trading_session_filter() {
    let active_report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        position_rows(),
        vec![
            tokenized_mint_requested(10, "mint-1", "RKLB"),
            tokenized_tokens_received(11, "mint-1", "0.25", "2026-05-15T12:02:00Z"),
        ],
        Vec::new(),
        PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingActive),
            ..query()
        },
        BTreeSet::new(),
    );
    let inactive_report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        position_rows(),
        vec![
            tokenized_mint_requested(10, "mint-1", "RKLB"),
            tokenized_tokens_received(11, "mint-1", "0.25", "2026-05-15T12:02:00Z"),
        ],
        Vec::new(),
        PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingInactive),
            ..query()
        },
        BTreeSet::new(),
    );

    assert_eq!(active_report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(active_report.summary.tracked_costs_usd, "0");
    assert_eq!(active_report.summary.net_realized_pnl_usd, "2");
    assert_eq!(active_report.cost_entries.len(), 0);

    assert_eq!(inactive_report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(inactive_report.summary.tracked_costs_usd, "0.25");
    assert_eq!(inactive_report.summary.net_realized_pnl_usd, "-0.25");
    assert_eq!(inactive_report.cost_entries.len(), 1);
}

#[test]
fn adds_symbol_dividends_to_aggregate_and_symbol_net_pnl() {
    let report = report_with(
        Vec::new(),
        vec![position_row("SGOV", "0")],
        Vec::new(),
        vec![account_activity(
            "div-1",
            "DIV",
            "1.25",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "1.25");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.25");
    assert_eq!(report.costs.dividend_revenue_usd, "1.25");
    assert_eq!(report.symbols[0].symbol, "SGOV");
    assert_eq!(report.symbols[0].tracked_revenue_usd, "1.25");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "1.25");
}

#[test]
fn records_negative_dividend_rows_as_account_costs() {
    let report = report_with(
        Vec::new(),
        vec![position_row("SGOV", "0")],
        Vec::new(),
        vec![account_activity(
            "div-tax-1",
            "DIVNRA",
            "-0.15",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.15");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.15");
    assert_eq!(report.costs.generic_costs_usd, "0.15");
    assert_eq!(report.costs.dividend_revenue_usd, "0");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0.15");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "-0.15");
}

#[test]
fn ignores_alpaca_activities_without_usable_amounts() {
    let mut missing_amount = account_activity(
        "missing-amount-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:02:00Z",
    );
    missing_amount.net_amount = None;

    let report = report_with(
        Vec::new(),
        position_rows(),
        Vec::new(),
        vec![
            missing_amount,
            account_activity("zero-fee-1", "FEE", "0", None, "2026-05-15T14:03:00Z"),
            account_activity("unknown-1", "UNKNOWN", "-0.5", None, "2026-05-15T14:04:00Z"),
        ],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "0");
    assert_eq!(report.cost_entries.len(), 0);
    assert!(report.warnings.iter().any(|warning| warning.contains(
        "Skipped malformed Alpaca account activity missing-amount-1: missing net_amount"
    )));
}

#[test]
fn records_fee_rebates_as_counter_trade_revenue() {
    let report = report_with(
        Vec::new(),
        position_rows(),
        Vec::new(),
        vec![account_activity(
            "rebate-1",
            "PTR",
            "0.12",
            None,
            "2026-05-15T14:02:00Z",
        )],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "0.12");
    assert_eq!(report.summary.net_realized_pnl_usd, "0.12");
    assert_eq!(report.costs.broker_fees_usd, "0.12");
    assert_eq!(report.costs.generic_costs_usd, "0");
}

#[test]
fn matches_legacy_frontend_sql_fixture_for_stable_report_fields() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:02:00Z"),
            onchain_buy(4, "17", "2026-05-15T14:03:00Z"),
            onchain_sell(5, "30", "2026-05-15T14:04:00Z"),
            offchain_buy(6, "2026-05-15T14:10:01Z", "25", "1"),
            offchain_buy(7, "2026-05-15T14:11:00Z", "12", "1"),
            onchain_sell(8, "15", "2026-05-15T14:12:00Z"),
            onchain_fill(9, "SPYM", "Buy", "80", "2", "2026-05-15T14:13:00Z"),
            offchain_fill(10, "SPYM", "Sell", "2026-05-15T14:14:00Z", "85", "1.5"),
        ],
        vec![position_row("RKLB", "0"), position_row("SPYM", "0.5")],
        vec![
            tokenized_mint_requested(20, "mint-rklb-1", "RKLB"),
            tokenized_tokens_received(21, "mint-rklb-1", "0.25", "2026-05-15T14:15:00Z"),
            usdc_bridged(22, "rebalance-1", "0.01", "2026-05-15T14:16:00Z"),
        ],
        Vec::new(),
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.counter_trade_pnl_usd, "9.5");
    assert_eq!(report.summary.onchain_netting_pnl_usd, "3");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(report.summary.directional_exposure_pnl_usd, "8");
    assert_eq!(report.summary.total_pnl_usd, "20.5");
    assert_eq!(report.summary.gross_realized_pnl_usd, "20.5");
    assert_eq!(report.summary.tracked_costs_usd, "0.26");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "20.24");
    assert_eq!(report.summary.realized_pnl_usd, "20.5");
    assert_eq!(report.summary.matched_shares, "5.5");
    assert_eq!(report.summary.onchain_notional_usd, "212");
    assert_eq!(report.summary.offchain_notional_usd, "172.5");
    assert_eq!(report.summary.inventory_drift_shares, "0.5");
    assert_eq!(report.summary.inventory_drift_usd, "40");
    assert_eq!(report.summary.open_long_shares, "0.5");
    assert_eq!(report.summary.open_short_shares, "0");
    assert_eq!(report.summary.unmatched_offchain_shares, "0");
    assert_eq!(report.summary.unmatched_offchain_notional_usd, "0");
    assert_eq!(report.summary.onchain_fill_count, 0);
    assert_eq!(report.summary.offchain_fill_count, 0);
    assert_eq!(report.summary.matched_lot_count, 5);
    assert_eq!(report.summary.open_lot_count, 1);
    assert_eq!(report.summary.unmatched_offchain_fill_count, 0);

    assert_eq!(report.costs.total_tracked_costs_usd, "0.26");
    assert_eq!(report.costs.total_tracked_revenue_usd, "0");
    assert_eq!(report.costs.generic_costs_usd, "0.26");
    assert_eq!(report.costs.tokenization_fees_usd, "0.25");
    assert_eq!(report.costs.cctp_fees_usd, "0.01");
    assert_eq!(report.costs.cost_entry_count, 2);
    assert_eq!(report.cost_entries.len(), 2);
    assert_eq!(report.cost_entries[0].category, "cctp_fee");
    assert_eq!(report.cost_entries[0].amount_usd, "0.01");
    assert_eq!(report.cost_entries[1].category, "tokenization_fee");
    assert_eq!(report.cost_entries[1].amount_usd, "0.25");
    assert_eq!(report.cost_entries[1].symbol.as_deref(), Some("RKLB"));

    assert_eq!(report.total, 5);
    assert!(!report.has_more);
    assert_eq!(report.entries.len(), 5);
    assert_eq!(
        report
            .entries
            .iter()
            .map(|entry| (
                entry.symbol.as_str(),
                entry.pnl_bucket.as_str(),
                entry.opening_rowid,
                entry.closing_rowid,
                fmt_decimal(&entry.shares),
                fmt_decimal(&entry.realized_pnl_usd),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "SPYM",
                "counter_trade",
                9,
                10,
                "1.5".to_owned(),
                "7.5".to_owned()
            ),
            (
                "RKLB",
                "directional_exposure",
                7,
                8,
                "1".to_owned(),
                "3".to_owned()
            ),
            (
                "RKLB",
                "directional_exposure",
                5,
                6,
                "1".to_owned(),
                "5".to_owned()
            ),
            (
                "RKLB",
                "onchain_netting",
                3,
                4,
                "1".to_owned(),
                "3".to_owned()
            ),
            (
                "RKLB",
                "counter_trade",
                1,
                2,
                "1".to_owned(),
                "2".to_owned()
            ),
        ]
    );

    let rklb = symbol_summary(&report, "RKLB");
    assert_eq!(rklb.counter_trade_pnl_usd, "2");
    assert_eq!(rklb.onchain_netting_pnl_usd, "3");
    assert_eq!(rklb.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(rklb.gross_realized_pnl_usd, "13");
    assert_eq!(rklb.tracked_costs_usd, "0.25");
    assert_eq!(rklb.net_realized_pnl_usd, "12.75");
    assert_eq!(rklb.inventory_drift_shares, "0");

    let spym = symbol_summary(&report, "SPYM");
    assert_eq!(spym.counter_trade_pnl_usd, "7.5");
    assert_eq!(spym.onchain_netting_pnl_usd, "0");
    assert_eq!(spym.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(spym.gross_realized_pnl_usd, "7.5");
    assert_eq!(spym.tracked_costs_usd, "0");
    assert_eq!(spym.net_realized_pnl_usd, "7.5");
    assert_eq!(spym.inventory_drift_shares, "0.5");
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);

    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T14:00:00Z")
    );
    assert_eq!(
        report.sample_stats.last_at.as_deref(),
        Some("2026-05-15T14:14:00Z")
    );
    assert_eq!(report.sample_stats.symbol_count, 2);
    assert_eq!(report.sample_stats.onchain_fill_count, 6);
    assert_eq!(report.sample_stats.offchain_fill_count, 4);
    assert_eq!(report.sample_stats.total_fill_count, 10);

    assert_eq!(report.windows.len(), 1);
    assert_eq!(report.windows[0].window_id, "2026-05-15");
    let window_rklb = window_symbol(&report.windows[0], "RKLB");
    assert_eq!(window_rklb.counter_trade_pnl_usd, "2");
    assert_eq!(window_rklb.onchain_netting_pnl_usd, "3");
    assert_eq!(window_rklb.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(window_rklb.total_pnl_usd, "13");

    let window_spym = window_symbol(&report.windows[0], "SPYM");
    assert_eq!(window_spym.counter_trade_pnl_usd, "7.5");
    assert_eq!(window_spym.onchain_netting_pnl_usd, "0");
    assert_eq!(window_spym.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(window_spym.total_pnl_usd, "7.5");
}

#[test]
fn records_margin_interest_as_generic_account_cost() {
    let report = report_with(
        Vec::new(),
        position_rows(),
        Vec::new(),
        vec![account_activity(
            "interest-1",
            "INT",
            "-0.50",
            None,
            "2026-05-15T14:02:00Z",
        )],
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.5");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.5");
    assert_eq!(report.costs.generic_costs_usd, "0.5");
    assert_eq!(report.costs.margin_interest_usd, "0.5");
}

#[test]
fn includes_tokenization_and_cctp_cost_events() {
    let report = report_with(
        Vec::new(),
        position_rows(),
        vec![
            tokenized_mint_requested(1, "mint-1", "RKLB"),
            tokenized_tokens_received(2, "mint-1", "0.40", "2026-05-15T14:02:00Z"),
            usdc_bridged(3, "rebalance-1", "0.10", "2026-05-15T14:03:00Z"),
        ],
        Vec::new(),
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.5");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.5");
    assert_eq!(report.costs.tokenization_fees_usd, "0.4");
    assert_eq!(report.costs.cctp_fees_usd, "0.1");
    assert_eq!(report.symbols[0].symbol, "RKLB");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0.4");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "-0.4");
}

#[test]
fn keeps_symbol_universe_separate_from_filtered_pnl_rows() {
    let mut symbols = BTreeSet::new();
    symbols.insert("RKLB".to_owned());

    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        vec![position_row("RKLB", "0"), position_row("SPYM", "0")],
        Vec::new(),
        Vec::new(),
        query(),
        symbols,
    );

    assert_eq!(
        report
            .symbols
            .iter()
            .map(|row| row.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["RKLB"]
    );
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);
}

#[test]
fn symbol_filter_excludes_unallocated_account_level_costs() {
    let mut symbols = BTreeSet::new();
    symbols.insert("RKLB".to_owned());

    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        position_rows(),
        Vec::new(),
        vec![account_activity(
            "fee-1",
            "FEE",
            "-0.25",
            None,
            "2026-05-15T14:02:00Z",
        )],
        query(),
        symbols,
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "2");
    assert_eq!(report.cost_entries.len(), 0);
}

#[test]
fn drops_unsafe_symbols_from_replay_rows_and_position_view() {
    let report = report_with(
        vec![
            PositionEventRow {
                symbol: "RKLB'); DROP TABLE events; --".to_owned(),
                ..onchain_sell(1, "10", "2026-05-15T14:00:00Z")
            },
            onchain_sell(2, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(3, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        vec![
            position_row("RKLB", "0"),
            position_row("SPYM", "0"),
            position_row("BAD';--", "0"),
        ],
        Vec::new(),
        Vec::new(),
        query(),
        BTreeSet::new(),
    );

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe position_view symbol") })
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe sample stats symbol") })
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe position event symbol") })
    );
}

#[test]
fn invalid_symbol_filter_warns_and_drops_input() {
    let mut warnings = Vec::new();
    let symbols = PnlQuery {
        symbol: Some("RKLB,RKLB'); DROP TABLE events; --".to_owned()),
        ..PnlQuery::default()
    }
    .symbol_filter(&mut warnings);

    assert_eq!(symbols.into_iter().collect::<Vec<_>>(), vec!["RKLB"]);
    assert!(
        warnings
            .iter()
            .any(|warning| { warning.contains("Skipped 1 invalid symbol filters") })
    );
}

#[test]
fn reports_position_view_reconciliation_delta() {
    let report = report(vec![onchain_sell(1, "10", "2026-05-15T14:00:00Z")]);

    assert_eq!(report.summary.open_short_shares, "1");
    assert_eq!(report.summary.inventory_drift_shares, "-1");
    assert!(report.warnings.iter().any(|warning| {
        warning.contains("Reconciliation note")
            && warning.contains("RKLB: replay -1, position_view 0")
    }));
}

#[test]
fn summarizes_offchain_origin_diagnostics_without_raw_per_fill_warnings() {
    let report = report(vec![offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1")]);

    assert!(report.warnings.iter().any(|warning| {
        warning.contains("Allocation note: 1 offchain fills opened offchain-origin inventory")
    }));
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Reconciliation note") })
    );
    assert!(
        !report
            .warnings
            .iter()
            .any(|warning| warning.contains("no open opposite-side"))
    );
    assert!(
        !report
            .warnings
            .iter()
            .any(|warning| warning.contains("PnL audit warning"))
    );
    assert_eq!(report.summary.total_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "1");
}
