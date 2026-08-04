//! Replay diagnostics for PnL report warnings.
use rain_math_float::Float;
use std::collections::{HashMap, HashSet};

use st0x_finance::Symbol;
use st0x_float_macro::float;

use super::parsing::fmt_decimal;
use super::query::PnlError;
use super::state::{PositionReplayDelta, UnmatchedOffchainAllocation};

const ALLOCATION_FORMATTING_WARNING: &str =
    "Allocation note unavailable: failed to format diagnostic values";
const RECONCILIATION_FORMATTING_WARNING: &str =
    "Reconciliation note unavailable: failed to format diagnostic values";

fn diagnostic_decimal(
    result: Result<String, PnlError>,
    formatting_warning: &'static str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    result.map_or_else(
        |_| {
            if !warnings.iter().any(|warning| warning == formatting_warning) {
                warnings.push(formatting_warning.to_owned());
            }
            None
        },
        Some,
    )
}

fn allocation_summary_text(
    allocations: &[UnmatchedOffchainAllocation],
    warnings: &mut Vec<String>,
) -> Result<Option<String>, PnlError> {
    if allocations.is_empty() {
        return Ok(None);
    }

    let mut by_symbol: HashMap<Symbol, (HashSet<String>, Float)> = HashMap::new();
    for allocation in allocations {
        let (fill_ids, shares) = by_symbol
            .entry(allocation.symbol.clone())
            .or_insert_with(|| (HashSet::new(), float!(0)));
        fill_ids.insert(allocation.fill_id.clone());
        *shares = (*shares + allocation.shares)?;
    }

    let mut details: Vec<_> = by_symbol.into_iter().collect();
    details.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbol_details = details
        .into_iter()
        .map(|(symbol, (fill_ids, shares))| {
            diagnostic_decimal(fmt_decimal(shares), ALLOCATION_FORMATTING_WARNING, warnings).map(
                |shares| {
                    format!(
                        "{}: {} shares across {} fills",
                        symbol,
                        shares,
                        fill_ids.len()
                    )
                },
            )
        })
        .collect::<Option<Vec<_>>>();
    let Some(symbol_details) = symbol_details else {
        return Ok(None);
    };
    let symbol_details = symbol_details.join("; ");

    Ok(Some(format!(
        "Allocation note: {} offchain fills opened offchain-origin inventory outside the intended \
         onchain-to-offchain hedge flow ({}). Those shares are carried in the FIFO ledger so later \
         fills can close them.",
        allocations.len(),
        symbol_details
    )))
}

fn position_replay_delta_text(
    deltas: &[PositionReplayDelta],
    warnings: &mut Vec<String>,
) -> Option<String> {
    if deltas.is_empty() {
        return None;
    }

    let mut sorted = deltas.to_vec();
    sorted.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    let details = sorted
        .iter()
        .map(|delta| {
            let replay = diagnostic_decimal(
                fmt_decimal(delta.replay_net),
                RECONCILIATION_FORMATTING_WARNING,
                warnings,
            )?;
            let position = diagnostic_decimal(
                fmt_decimal(delta.position_net),
                RECONCILIATION_FORMATTING_WARNING,
                warnings,
            )?;

            Some(format!(
                "{}: replay {}, position_view {}",
                delta.symbol, replay, position
            ))
        })
        .collect::<Option<Vec<_>>>();
    let details = details?;
    let details = details.join("; ");

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
) -> Result<(), PnlError> {
    if let Some(text) = allocation_summary_text(unmatched_offchain_allocations, warnings)? {
        warnings.push(text);
    }

    if let Some(text) = position_replay_delta_text(position_replay_deltas, warnings) {
        warnings.push(text);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_formatting_failure_adds_an_advisory_warning() {
        let mut warnings = Vec::new();
        let error = PnlError::InvalidDate {
            field: "test",
            value: "invalid".to_owned(),
        };

        let text = diagnostic_decimal(Err(error), ALLOCATION_FORMATTING_WARNING, &mut warnings);

        assert_eq!(text, None);
        assert_eq!(warnings, [ALLOCATION_FORMATTING_WARNING]);
    }
}
