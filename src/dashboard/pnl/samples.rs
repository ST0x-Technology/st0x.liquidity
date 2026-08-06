use rain_math_float::Float;
use std::collections::{BTreeSet, HashMap};

use st0x_finance::Symbol;

use super::parsing::is_safe_symbol;
use super::query::{PnlError, PnlFinancialFieldError, PnlQuery};
use super::response::{PnlAvailableRange, PnlSampleStats, PnlSampleSymbolStats};
use super::sessions::{date_key, matches_date_bounds_for_iso, matches_trade_filters};
use super::state::{PositionLedgerRow, PositionViewRow, SampleStatsAcc, Venue};

pub(crate) fn parse_position_view(
    rows: &[PositionViewRow],
    warnings: &mut Vec<String>,
) -> Result<(HashMap<Symbol, Float>, Vec<Symbol>), PnlError> {
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

        let Ok(symbol) = Symbol::new(row.symbol.clone()) else {
            continue;
        };

        symbols.insert(symbol.clone());
        if let Some(net_position) = &row.net_position {
            match Float::parse(net_position.clone()) {
                Ok(value) => {
                    position_nets.insert(symbol, value);
                }
                Err(error) => {
                    return Err(PnlError::InvalidFinancialField {
                        rowid: 0,
                        aggregate_type: "PositionView",
                        event_type: "position_view".to_owned(),
                        field: "net_position",
                        value: net_position.clone(),
                        source: PnlFinancialFieldError::InvalidDecimal(Box::new(error)),
                    });
                }
            }
        }
    }

    Ok((position_nets, symbols.into_iter().collect()))
}

/// The fill venue and execution timestamp of a ledger row, or `None` for the
/// non-fill kinds (placements, manual adjustments) that sample stats and the
/// available range ignore.
fn fill_timestamp(row: &PositionLedgerRow) -> Option<(Venue, &str)> {
    match row {
        PositionLedgerRow::OnchainFill(fill) => Some((Venue::Onchain, fill.executed_at.as_str())),
        PositionLedgerRow::OffchainFill(fill) => Some((Venue::Offchain, fill.executed_at.as_str())),
        PositionLedgerRow::OffchainPlacement(_) | PositionLedgerRow::ManualAdjustment(_) => None,
    }
}

fn add_sample_fill(sample: &mut SampleStatsAcc, venue: Venue, timestamp: &str) {
    match venue {
        Venue::Onchain => sample.onchain_fill_count += 1,
        Venue::Offchain => sample.offchain_fill_count += 1,
        // `fill_timestamp` yields fills only, and manual adjustments are not
        // fills; a manual venue can only mean a future caller bug, and it
        // must not count into either fill bucket.
        Venue::Manual => {}
    }

    if sample
        .first_at
        .as_deref()
        .is_none_or(|first| timestamp < first)
    {
        sample.first_at = Some(timestamp.to_owned());
    }
    if sample
        .last_at
        .as_deref()
        .is_none_or(|last| timestamp > last)
    {
        sample.last_at = Some(timestamp.to_owned());
    }
}

pub(crate) fn build_sample_stats(
    rows: &[PositionLedgerRow],
    query: &PnlQuery,
    warnings: &mut Vec<String>,
) -> PnlSampleStats {
    let mut by_symbol: HashMap<Symbol, SampleStatsAcc> = HashMap::new();
    for row in rows {
        let Some((venue, timestamp)) = fill_timestamp(row) else {
            continue;
        };

        if !is_safe_symbol(row.symbol()) {
            warnings.push(format!(
                "Skipped unsafe sample stats symbol in backend PnL response: {}",
                row.symbol()
            ));
            continue;
        }

        if !matches_date_bounds_for_iso(timestamp, query)
            || !matches_trade_filters(timestamp, query)
        {
            continue;
        }

        let Ok(symbol) = Symbol::new(row.symbol().to_owned()) else {
            warnings.push(format!(
                "Skipped invalid sample stats symbol in backend PnL response: {}",
                row.symbol()
            ));
            continue;
        };
        let sample = by_symbol.entry(symbol).or_default();
        add_sample_fill(sample, venue, timestamp);
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

pub(crate) fn build_available_range(
    rows: &[PositionLedgerRow],
    warnings: &mut Vec<String>,
) -> PnlAvailableRange {
    let mut first_at: Option<String> = None;
    let mut last_at: Option<String> = None;

    for row in rows {
        let Some((_, timestamp)) = fill_timestamp(row) else {
            continue;
        };

        if !is_safe_symbol(row.symbol()) {
            warnings.push(format!(
                "Skipped unsafe available range symbol in backend PnL response: {}",
                row.symbol()
            ));
            continue;
        }

        if first_at
            .as_deref()
            .is_none_or(|current| timestamp < current)
        {
            first_at = Some(timestamp.to_owned());
        }
        if last_at.as_deref().is_none_or(|current| timestamp > current) {
            last_at = Some(timestamp.to_owned());
        }
    }

    PnlAvailableRange {
        first_date: first_at.as_deref().map(date_key),
        last_date: last_at.as_deref().map(date_key),
        first_at,
        last_at,
    }
}
