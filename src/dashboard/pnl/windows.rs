use std::collections::{BTreeSet, HashMap};

use chrono::{NaiveDate, SecondsFormat, TimeZone, Utc};
use chrono_tz::America::New_York;
use num_decimal::Num;
use st0x_finance::Symbol;
use tracing::warn;

use super::parsing::fmt_decimal;
use super::response::{PnlEntry, PnlWindow, PnlWindowSymbol};
use super::sessions::{counter_trading_session_for_iso, date_key, market_session_for_iso};
use super::state::PnlBucket;

fn et_day_boundary(date: &str, is_end: bool) -> String {
    let fallback = if is_end {
        format!("{date}T23:59:59.999Z")
    } else {
        format!("{date}T00:00:00.000Z")
    };

    let Some(day) = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok() else {
        warn!(%date, is_end, "Failed to parse PnL window ET boundary date");
        return fallback;
    };
    let local_time = if is_end {
        day.and_hms_milli_opt(23, 59, 59, 999)
    } else {
        day.and_hms_milli_opt(0, 0, 0, 0)
    };
    let Some(local_time) = local_time else {
        warn!(%date, is_end, "Failed to construct PnL window ET boundary time");
        return fallback;
    };
    New_York
        .from_local_datetime(&local_time)
        .single()
        .map_or_else(
            || {
                warn!(%date, is_end, "Failed to resolve PnL window ET boundary");
                fallback
            },
            |local| {
                local
                    .with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::Millis, true)
            },
        )
}

pub(crate) fn build_windows(entries: &[PnlEntry], symbols: &[Symbol]) -> Vec<PnlWindow> {
    let mut by_date: HashMap<String, Vec<&PnlEntry>> = HashMap::new();
    for entry in entries {
        by_date
            .entry(date_key(&entry.closed_at))
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
                    .map_or_else(|| "mixed".to_owned(), |session| session.as_str().to_owned())
            } else {
                "mixed".to_owned()
            };
            let counter_trading_session = if counter_sessions.len() == 1 {
                counter_sessions
                    .iter()
                    .next()
                    .map_or_else(|| "mixed".to_owned(), |session| (*session).to_owned())
            } else {
                "mixed".to_owned()
            };
            let is_weekend = market_session == "weekend";
            let mut entries_by_symbol: HashMap<Symbol, Vec<&PnlEntry>> = HashMap::new();
            for entry in &day_entries {
                entries_by_symbol
                    .entry(entry.symbol.clone())
                    .or_default()
                    .push(entry);
            }

            let rows = symbols
                .iter()
                .map(|symbol| {
                    window_symbol_row(
                        symbol,
                        entries_by_symbol.get(symbol).map_or(&[][..], Vec::as_slice),
                    )
                })
                .collect();

            PnlWindow {
                window_id: date.clone(),
                start_at: et_day_boundary(&date, false),
                end_at: et_day_boundary(&date, true),
                label: date,
                is_weekend,
                market_session,
                counter_trading_session,
                granularity: "day",
                symbols: rows,
            }
        })
        .collect()
}

fn window_symbol_row(symbol: &Symbol, entries: &[&PnlEntry]) -> PnlWindowSymbol {
    let mut counter_trade = Num::default();
    let mut onchain_netting = Num::default();
    let directional_baseline = Num::default();
    let mut directional_excess = Num::default();

    for entry in entries {
        if entry.symbol != *symbol {
            continue;
        }
        let pnl = entry.realized_pnl_usd.clone();
        match entry.pnl_bucket {
            PnlBucket::CounterTrade => counter_trade += &pnl,
            PnlBucket::OnchainNetting => onchain_netting += &pnl,
            PnlBucket::DirectionalExposure => directional_excess += &pnl,
        }
    }

    let directional_exposure = &directional_baseline + &directional_excess;
    let total =
        &(&(&counter_trade + &onchain_netting) + &directional_baseline) + &directional_excess;
    PnlWindowSymbol {
        symbol: symbol.clone(),
        counter_trade_pnl_usd: fmt_decimal(&counter_trade),
        onchain_netting_pnl_usd: fmt_decimal(&onchain_netting),
        directional_inventory_baseline_pnl_usd: fmt_decimal(&directional_baseline),
        directional_imbalance_excess_pnl_usd: fmt_decimal(&directional_excess),
        directional_exposure_pnl_usd: fmt_decimal(&directional_exposure),
        total_pnl_usd: fmt_decimal(&total),
    }
}
