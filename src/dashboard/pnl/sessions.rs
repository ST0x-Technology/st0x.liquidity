use std::collections::BTreeSet;

use chrono::{Datelike, Timelike, Weekday};
use chrono_tz::America::New_York;

use super::costs::CostEntryInternal;
use super::parsing::parse_timestamp;
use super::query::{PnlCounterTradingFilter, PnlMarketSessionFilter, PnlQuery};
use super::response::PnlEntry;

pub(crate) fn date_key(iso: &str) -> &str {
    iso.get(..10).unwrap_or(iso)
}

pub(crate) fn market_session_for_iso(iso: &str) -> &'static str {
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

pub(crate) fn counter_trading_session_for_iso(iso: &str) -> &'static str {
    if market_session_for_iso(iso) == "rth" {
        "counter_trading_active"
    } else {
        "counter_trading_inactive"
    }
}

pub(crate) fn matches_market_session_filter(
    iso: &str,
    filter: Option<PnlMarketSessionFilter>,
) -> bool {
    match filter.unwrap_or(PnlMarketSessionFilter::All) {
        PnlMarketSessionFilter::All => true,
        PnlMarketSessionFilter::Pre => market_session_for_iso(iso) == "pre",
        PnlMarketSessionFilter::Rth => market_session_for_iso(iso) == "rth",
        PnlMarketSessionFilter::Post => market_session_for_iso(iso) == "post",
        PnlMarketSessionFilter::Overnight => market_session_for_iso(iso) == "overnight",
        PnlMarketSessionFilter::Weekend => market_session_for_iso(iso) == "weekend",
    }
}

pub(crate) fn matches_counter_trading_filter(
    iso: &str,
    filter: Option<PnlCounterTradingFilter>,
) -> bool {
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

pub(crate) fn matches_trade_filters(iso: &str, query: &PnlQuery) -> bool {
    matches_market_session_filter(iso, query.market_session_filter)
        && matches_counter_trading_filter(iso, query.counter_trading_filter)
}

pub(crate) fn matches_date_bounds_for_iso(iso: &str, query: &PnlQuery) -> bool {
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

pub(crate) fn matches_date_filter(entry: &PnlEntry, query: &PnlQuery) -> bool {
    matches_date_bounds_for_iso(&entry.closed_at, query)
        && matches_trade_filters(&entry.closed_at, query)
}

pub(crate) fn matches_cost_date_filter(entry: &CostEntryInternal, query: &PnlQuery) -> bool {
    matches_date_bounds_for_iso(&entry.occurred_at, query)
        && matches_trade_filters(&entry.occurred_at, query)
}

pub(crate) fn matches_cost_symbol_filter(
    entry: &CostEntryInternal,
    symbols: &BTreeSet<String>,
) -> bool {
    match &entry.symbol {
        Some(symbol) => symbols.is_empty() || symbols.contains(symbol),
        None => symbols.is_empty(),
    }
}

pub(crate) fn seconds_between(start_iso: &str, end_iso: &str) -> i64 {
    let start = parse_timestamp(start_iso);
    let end = parse_timestamp(end_iso);
    match (start, end) {
        (Some(start), Some(end)) => (end - start).num_seconds().max(0),
        _ => 0,
    }
}
