use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use num_decimal::Num;

use super::parsing::{is_safe_symbol, position_event_replay_timestamp};
use super::query::{PnlError, PnlFinancialFieldError, PnlQuery};
use super::response::{PnlSampleStats, PnlSampleSymbolStats};
use super::sessions::{matches_date_bounds_for_iso, matches_trade_filters};
use super::state::{PositionEventRow, PositionViewRow, SampleStatsAcc};

pub(crate) fn parse_position_view(
    rows: &[PositionViewRow],
    warnings: &mut Vec<String>,
) -> Result<(HashMap<String, Num>, Vec<String>), PnlError> {
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
                Err(error) => {
                    return Err(PnlError::InvalidFinancialField {
                        rowid: 0,
                        aggregate_type: "PositionView",
                        event_type: "position_view".to_owned(),
                        field: "net_position",
                        value: net_position.clone(),
                        source: PnlFinancialFieldError::InvalidDecimal(error),
                    });
                }
            }
        }
    }

    Ok((position_nets, symbols.into_iter().collect()))
}

fn add_sample_fill(sample: &mut SampleStatsAcc, event_type: &str, timestamp: &str) {
    if event_type == "PositionEvent::OnChainOrderFilled" {
        sample.onchain_fill_count += 1;
    } else if event_type == "PositionEvent::OffChainOrderFilled" {
        sample.offchain_fill_count += 1;
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
