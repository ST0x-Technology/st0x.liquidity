//! Replay diagnostics for PnL report warnings.
use num_decimal::Num;
use st0x_finance::Symbol;
use std::collections::{HashMap, HashSet};

use super::parsing::fmt_decimal;
use super::state::{PositionReplayDelta, UnmatchedOffchainAllocation};

fn allocation_summary_text(allocations: &[UnmatchedOffchainAllocation]) -> Option<String> {
    if allocations.is_empty() {
        return None;
    }

    let mut by_symbol: HashMap<Symbol, (HashSet<String>, Num)> = HashMap::new();
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

pub(crate) fn append_replay_diagnostics(
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
