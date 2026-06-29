use std::collections::{BTreeSet, HashMap};

use st0x_execution::AccountActivity;

use super::ATTRIBUTION_METHOD;
use super::costs::{
    build_alpaca_activity_cost_entries, build_cost_entries, summarize_cost_entries, with_costs,
};
use super::diagnostics::append_replay_diagnostics;
use super::parsing::{is_safe_symbol, ordered_position_events};
use super::query::PnlQuery;
use super::replay::{
    add_summary, apply_offchain_fill, apply_offchain_placement, apply_onchain_fill, finalize_book,
    merge_symbol_replay_exposure, parse_offchain_fill, parse_onchain_fill, reset_symbol_costs,
    summary_from_entries, summary_to_dto, symbol_summary_to_dto, with_direct_symbol_costs,
    with_replay_exposure,
};
use super::response::{PnlCostEntry, PnlResponse};
use super::samples::{build_sample_stats, parse_position_view};
use super::sessions::{matches_cost_date_filter, matches_cost_symbol_filter, matches_date_filter};
use super::state::{CostEventRow, PositionEventRow, PositionViewRow, SummaryAcc, SymbolBook};
use super::windows::build_windows;

pub(crate) fn build_pnl_response_from_rows(
    event_rows: Vec<PositionEventRow>,
    position_rows: &[PositionViewRow],
    cost_rows: &[CostEventRow],
    alpaca_activities: &[AccountActivity],
    query: &PnlQuery,
    symbols: &BTreeSet<String>,
    mut warnings: Vec<String>,
) -> PnlResponse {
    let event_rows = if symbols.is_empty() {
        event_rows
    } else {
        event_rows
            .into_iter()
            .filter(|row| symbols.contains(&row.symbol))
            .collect()
    };
    let (position_nets, position_symbols) = parse_position_view(position_rows, &mut warnings);
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
                    apply_onchain_fill(book, &fill, &mut entries, &mut warnings);
                }
            }
            "PositionEvent::OffChainOrderPlaced" => {
                apply_offchain_placement(book, &row, &mut warnings);
            }
            "PositionEvent::OffChainOrderFilled" => {
                if let Some(fill) = parse_offchain_fill(&row, &mut warnings) {
                    apply_offchain_fill(
                        book,
                        &fill,
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

    let mut cost_replay = build_cost_entries(cost_rows, &mut warnings);
    let alpaca_entries = build_alpaca_activity_cost_entries(alpaca_activities, &mut warnings);
    cost_replay.entries.extend(alpaca_entries);

    let mut filtered_cost_entries: Vec<_> = cost_replay
        .entries
        .into_iter()
        .filter(|entry| matches_cost_symbol_filter(entry, symbols))
        .filter(|entry| matches_cost_date_filter(entry, query))
        .collect();
    let filtered_alpaca_entry_count = filtered_cost_entries
        .iter()
        .filter(|entry| entry.aggregate_type == "AlpacaAccountActivity")
        .count();
    if filtered_alpaca_entry_count > 0 {
        warnings.push(format!(
            "Cost coverage note: {filtered_alpaca_entry_count} Alpaca account activity rows were fetched from the broker API \
             and included as explicit cost/revenue ledger entries."
        ));
    }
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
