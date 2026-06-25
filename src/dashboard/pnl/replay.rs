use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use num_decimal::Num;

use super::costs::{AccountingEffect, CostEntryInternal};
use super::parsing::{
    abs_decimal, decimal_field, direction_field, fmt_decimal, min_decimal, nested_record,
    number_text_field, parse_decimal_lossy, text_field,
};
use super::response::{PnlEntry, PnlSummary, PnlSymbolSummary};
use super::sessions::seconds_between;
use super::state::{
    Direction, Fill, Lot, LotSide, PnlBucket, PositionEventRow, PositionReplayDelta, SummaryAcc,
    SummaryAndSymbols, SymbolBook, UnmatchedOffchainAllocation, Venue,
};
use super::{ATTRIBUTION_METHOD, COUNTER_TRADE_THRESHOLD_SECONDS, EPSILON};

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    }
}

fn lot_side_to_direction(side: LotSide) -> &'static str {
    match side {
        LotSide::Long => "buy",
        LotSide::Short => "sell",
    }
}

fn add_venue_notional(summary: &mut SummaryAcc, venue: Venue, notional: &Num) {
    match venue {
        Venue::Onchain => summary.onchain_notional_usd += notional,
        Venue::Offchain => summary.offchain_notional_usd += notional,
    }
}

fn add_realized_pnl(summary: &mut SummaryAcc, bucket: PnlBucket, value: &Num) {
    match bucket {
        PnlBucket::CounterTrade => {
            summary.counter_trade_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
        PnlBucket::OnchainNetting => {
            summary.onchain_netting_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
        PnlBucket::DirectionalExposure => {
            summary.directional_imbalance_excess_pnl_usd += value;
            summary.directional_exposure_pnl_usd += value;
            summary.realized_pnl_usd += value;
        }
    }
}

pub(crate) fn parse_onchain_fill(
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) -> Option<Fill> {
    let Some(filled) = nested_record(&row.payload, "OnChainOrderFilled") else {
        warnings.push(format!(
            "Skipped malformed position onchain fill {}: missing OnChainOrderFilled",
            row.symbol
        ));
        return None;
    };

    let amount = decimal_field(filled, "amount");
    let direction = direction_field(filled, "direction");
    let price = decimal_field(filled, "price_usdc");
    let executed_at = text_field(filled, "block_timestamp");
    let trade_id = nested_record(filled, "trade_id");
    let tx_hash = trade_id.and_then(|id| text_field(id, "tx_hash"));
    let log_index = trade_id.and_then(|id| number_text_field(id, "log_index"));

    let (
        Some(amount),
        Some(direction),
        Some(price),
        Some(executed_at),
        Some(tx_hash),
        Some(log_index),
    ) = (amount, direction, price, executed_at, tx_hash, log_index)
    else {
        warnings.push(format!(
            "Skipped malformed position onchain fill {}: incomplete payload",
            row.symbol
        ));
        return None;
    };

    Some(Fill {
        rowid: row.rowid,
        id: format!("{tx_hash}:{log_index}"),
        symbol: row.symbol.clone(),
        shares: amount,
        direction,
        price,
        executed_at,
        venue: Venue::Onchain,
    })
}

pub(crate) fn parse_offchain_fill(
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) -> Option<Fill> {
    let Some(filled) = nested_record(&row.payload, "OffChainOrderFilled") else {
        warnings.push(format!(
            "Skipped malformed position offchain fill {}: missing OffChainOrderFilled",
            row.symbol
        ));
        return None;
    };

    let order_id = text_field(filled, "offchain_order_id");
    let shares = decimal_field(filled, "shares_filled");
    let direction = direction_field(filled, "direction");
    let price = decimal_field(filled, "price");
    let executed_at = text_field(filled, "broker_timestamp");

    let (Some(order_id), Some(shares), Some(direction), Some(price), Some(executed_at)) =
        (order_id, shares, direction, price, executed_at)
    else {
        warnings.push(format!(
            "Skipped malformed position offchain fill {}: incomplete payload",
            row.symbol
        ));
        return None;
    };

    Some(Fill {
        rowid: row.rowid,
        id: order_id,
        symbol: row.symbol.clone(),
        shares,
        direction,
        price,
        executed_at,
        venue: Venue::Offchain,
    })
}

fn parse_offchain_placement_id(
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let placed = nested_record(&row.payload, "OffChainOrderPlaced");
    let order_id = placed.and_then(|value| text_field(value, "offchain_order_id"));
    if order_id.is_none() {
        warnings.push(format!(
            "Skipped malformed position offchain placement {}: incomplete payload",
            row.symbol
        ));
    }
    order_id
}

fn open_residual_lot(book: &mut SymbolBook, fill: &Fill, remaining: Num) {
    let side = if fill.direction == Direction::Buy {
        LotSide::Long
    } else {
        LotSide::Short
    };
    let lot = Lot {
        trade_id: fill.id.clone(),
        side,
        remaining_shares: remaining,
        price: fill.price.clone(),
        opened_at: fill.executed_at.clone(),
        opened_rowid: fill.rowid,
        opened_venue: fill.venue,
    };

    match side {
        LotSide::Long => book.long_lots.push(lot),
        LotSide::Short => book.short_lots.push(lot),
    }
}

pub(crate) fn apply_onchain_fill(
    book: &mut SymbolBook,
    fill: Fill,
    entries: &mut Vec<PnlEntry>,
    warnings: &mut Vec<String>,
) {
    if book.seen_onchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate onchain trade_id {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return;
    }

    book.seen_onchain_fill_ids.insert(fill.id.clone());
    book.summary.onchain_fill_count += 1;
    let source_lots = if fill.direction == Direction::Buy {
        &mut book.short_lots
    } else {
        &mut book.long_lots
    };
    let remaining = match_fill_against_lots(
        &fill,
        source_lots,
        &mut book.summary,
        &mut book.matched_onchain_shares,
        entries,
        PnlBucket::OnchainNetting,
    );
    if remaining.is_zero() {
        return;
    }

    let original = book
        .original_onchain_shares
        .entry(fill.id.clone())
        .or_default();
    *original += &remaining;
    open_residual_lot(book, &fill, remaining);
}

pub(crate) fn apply_offchain_placement(
    book: &mut SymbolBook,
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) {
    let Some(order_id) = parse_offchain_placement_id(row, warnings) else {
        return;
    };

    if book.seen_offchain_placement_ids.contains(&order_id) {
        warnings.push(format!(
            "PnL audit error: duplicate offchain placement {} for {} was skipped",
            order_id, row.symbol
        ));
        return;
    }

    book.seen_offchain_placement_ids.insert(order_id);
}

pub(crate) fn apply_offchain_fill(
    book: &mut SymbolBook,
    fill: Fill,
    entries: &mut Vec<PnlEntry>,
    warnings: &mut Vec<String>,
    unmatched_offchain_allocations: &mut Vec<UnmatchedOffchainAllocation>,
) {
    if book.seen_offchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate offchain fill {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return;
    }

    book.seen_offchain_fill_ids.insert(fill.id.clone());
    book.summary.offchain_fill_count += 1;
    let source_lots = if fill.direction == Direction::Buy {
        &mut book.short_lots
    } else {
        &mut book.long_lots
    };
    let remaining = match_fill_against_lots(
        &fill,
        source_lots,
        &mut book.summary,
        &mut book.matched_onchain_shares,
        entries,
        PnlBucket::CounterTrade,
    );

    if !remaining.is_zero() {
        unmatched_offchain_allocations.push(UnmatchedOffchainAllocation {
            symbol: fill.symbol.clone(),
            fill_id: fill.id.clone(),
            shares: remaining.clone(),
        });
        open_residual_lot(book, &fill, remaining);
    }
}

fn match_fill_against_lots(
    fill: &Fill,
    source_lots: &mut Vec<Lot>,
    summary: &mut SummaryAcc,
    matched_onchain_shares: &mut HashMap<String, Num>,
    entries: &mut Vec<PnlEntry>,
    bucket: PnlBucket,
) -> Num {
    let mut remaining = fill.shares.clone();

    while !remaining.is_zero() && !source_lots.is_empty() {
        let mut front_lot = source_lots.remove(0);
        let matched_shares = min_decimal(&remaining, &front_lot.remaining_shares);
        if matched_shares.is_zero() {
            continue;
        }

        let elapsed_seconds = seconds_between(&front_lot.opened_at, &fill.executed_at);
        let effective_bucket = if front_lot.opened_venue == Venue::Offchain
            || (bucket == PnlBucket::CounterTrade
                && elapsed_seconds > COUNTER_TRADE_THRESHOLD_SECONDS)
        {
            PnlBucket::DirectionalExposure
        } else {
            bucket
        };

        let spread = if front_lot.side == LotSide::Long {
            &fill.price - &front_lot.price
        } else {
            &front_lot.price - &fill.price
        };
        let realized_pnl = &matched_shares * &spread;
        let opening_notional = &matched_shares * &front_lot.price;
        let closing_notional = &matched_shares * &fill.price;

        front_lot.remaining_shares -= &matched_shares;
        if !front_lot.remaining_shares.is_zero() {
            source_lots.insert(0, front_lot.clone());
        }

        add_realized_pnl(summary, effective_bucket, &realized_pnl);
        summary.matched_shares += &matched_shares;
        add_venue_notional(summary, front_lot.opened_venue, &opening_notional);
        add_venue_notional(summary, fill.venue, &closing_notional);
        summary.matched_lot_count += 1;

        if front_lot.opened_venue == Venue::Onchain {
            let matched = matched_onchain_shares
                .entry(front_lot.trade_id.clone())
                .or_default();
            *matched += &matched_shares;
        }

        let opening_direction = lot_side_to_direction(front_lot.side);
        let closing_direction = direction_label(fill.direction);
        let opening_price_text = fmt_decimal(&front_lot.price);
        let closing_price_text = fmt_decimal(&fill.price);
        let onchain_direction = if front_lot.opened_venue == Venue::Onchain {
            opening_direction.to_owned()
        } else if fill.venue == Venue::Onchain {
            closing_direction.to_owned()
        } else {
            String::new()
        };
        let offchain_direction = if front_lot.opened_venue == Venue::Offchain {
            opening_direction.to_owned()
        } else if fill.venue == Venue::Offchain {
            closing_direction.to_owned()
        } else {
            String::new()
        };
        let onchain_trade_id = if front_lot.opened_venue == Venue::Onchain {
            front_lot.trade_id.clone()
        } else if fill.venue == Venue::Onchain {
            fill.id.clone()
        } else {
            String::new()
        };
        let offchain_order_id = if front_lot.opened_venue == Venue::Offchain {
            front_lot.trade_id.clone()
        } else if fill.venue == Venue::Offchain {
            fill.id.clone()
        } else {
            String::new()
        };
        let onchain_price_text = if front_lot.opened_venue == Venue::Onchain {
            opening_price_text.clone()
        } else if fill.venue == Venue::Onchain {
            closing_price_text.clone()
        } else {
            String::new()
        };
        let offchain_price_text = if front_lot.opened_venue == Venue::Offchain {
            opening_price_text.clone()
        } else if fill.venue == Venue::Offchain {
            closing_price_text.clone()
        } else {
            String::new()
        };

        entries.push(PnlEntry {
            symbol: fill.symbol.clone(),
            pnl_bucket: effective_bucket.as_str(),
            matched_at: fill.executed_at.clone(),
            opened_at: front_lot.opened_at.clone(),
            closed_at: fill.executed_at.clone(),
            opening_fill_id: front_lot.trade_id.clone(),
            closing_fill_id: fill.id.clone(),
            opening_rowid: front_lot.opened_rowid,
            closing_rowid: fill.rowid,
            opening_venue: front_lot.opened_venue.as_str(),
            closing_venue: fill.venue.as_str(),
            opening_direction,
            closing_direction,
            opening_price_usd: opening_price_text,
            closing_price_usd: closing_price_text,
            onchain_trade_id,
            offchain_order_id,
            onchain_direction,
            offchain_direction,
            shares: fmt_decimal(&matched_shares),
            onchain_price_usdc: onchain_price_text,
            offchain_price_usd: offchain_price_text,
            spread_usd: fmt_decimal(&spread),
            realized_pnl_usd: fmt_decimal(&realized_pnl),
            elapsed_seconds,
            counter_trade_threshold_seconds: COUNTER_TRADE_THRESHOLD_SECONDS,
            delayed_counter_trade: effective_bucket == PnlBucket::DirectionalExposure,
            attribution_method: ATTRIBUTION_METHOD,
        });

        remaining -= &matched_shares;
    }

    remaining
}

fn finalize_lots(summary: &mut SummaryAcc, lots: &[Lot]) {
    for lot in lots {
        let notional = &lot.remaining_shares * &lot.price;
        match lot.side {
            LotSide::Long => {
                summary.open_long_shares += &lot.remaining_shares;
                summary.open_long_notional_usd += &notional;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_buy_shares += &lot.remaining_shares;
                    summary.unmatched_offchain_buy_notional_usd += &notional;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
            LotSide::Short => {
                summary.open_short_shares += &lot.remaining_shares;
                summary.open_short_notional_usd += &notional;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_sell_shares += &lot.remaining_shares;
                    summary.unmatched_offchain_sell_notional_usd += &notional;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
        }
        summary.open_lot_count += 1;
    }
}

pub(crate) fn finalize_book(
    symbol: &str,
    book: &mut SymbolBook,
    position_nets: &HashMap<String, Num>,
    warnings: &mut Vec<String>,
    position_replay_deltas: &mut Vec<PositionReplayDelta>,
) {
    finalize_lots(&mut book.summary, &book.long_lots);
    finalize_lots(&mut book.summary, &book.short_lots);

    let epsilon = Num::from_str(EPSILON).unwrap_or_default();
    for (trade_id, matched_shares) in &book.matched_onchain_shares {
        if let Some(original_shares) = book.original_onchain_shares.get(trade_id) {
            let excess = matched_shares - original_shares;
            if excess > epsilon {
                warnings.push(format!(
                    "PnL audit error: onchain lot {} for {} matched {} shares above original {}",
                    trade_id,
                    symbol,
                    fmt_decimal(matched_shares),
                    fmt_decimal(original_shares)
                ));
            }
        }
    }

    if let Some(position_net) = position_nets.get(symbol) {
        let replay_net = &book.summary.open_long_shares - &book.summary.open_short_shares;
        let delta = &replay_net - position_net;
        if abs_decimal(&delta) > epsilon {
            position_replay_deltas.push(PositionReplayDelta {
                symbol: symbol.to_owned(),
                replay_net,
                position_net: position_net.clone(),
            });
        }
    }
}

pub(crate) fn add_summary(target: &mut SummaryAcc, source: &SummaryAcc) {
    target.counter_trade_pnl_usd += &source.counter_trade_pnl_usd;
    target.onchain_netting_pnl_usd += &source.onchain_netting_pnl_usd;
    target.directional_inventory_baseline_pnl_usd += &source.directional_inventory_baseline_pnl_usd;
    target.directional_imbalance_excess_pnl_usd += &source.directional_imbalance_excess_pnl_usd;
    target.directional_exposure_pnl_usd += &source.directional_exposure_pnl_usd;
    target.realized_pnl_usd += &source.realized_pnl_usd;
    target.matched_shares += &source.matched_shares;
    target.onchain_notional_usd += &source.onchain_notional_usd;
    target.offchain_notional_usd += &source.offchain_notional_usd;
    target.open_long_shares += &source.open_long_shares;
    target.open_short_shares += &source.open_short_shares;
    target.open_long_notional_usd += &source.open_long_notional_usd;
    target.open_short_notional_usd += &source.open_short_notional_usd;
    target.unmatched_offchain_buy_shares += &source.unmatched_offchain_buy_shares;
    target.unmatched_offchain_sell_shares += &source.unmatched_offchain_sell_shares;
    target.unmatched_offchain_buy_notional_usd += &source.unmatched_offchain_buy_notional_usd;
    target.unmatched_offchain_sell_notional_usd += &source.unmatched_offchain_sell_notional_usd;
    target.onchain_fill_count += source.onchain_fill_count;
    target.offchain_fill_count += source.offchain_fill_count;
    target.matched_lot_count += source.matched_lot_count;
    target.open_lot_count += source.open_lot_count;
    target.unmatched_offchain_fill_count += source.unmatched_offchain_fill_count;
}

pub(crate) fn summary_to_dto(summary: &SummaryAcc) -> PnlSummary {
    let directional_exposure_pnl = &summary.directional_inventory_baseline_pnl_usd
        + &summary.directional_imbalance_excess_pnl_usd;
    let total_pnl = &(&(&summary.counter_trade_pnl_usd + &summary.onchain_netting_pnl_usd)
        + &summary.directional_inventory_baseline_pnl_usd)
        + &summary.directional_imbalance_excess_pnl_usd;
    let inventory_drift_shares = &summary.open_long_shares - &summary.open_short_shares;
    let inventory_drift_usd = &summary.open_long_notional_usd - &summary.open_short_notional_usd;
    let unmatched_offchain_shares =
        &summary.unmatched_offchain_buy_shares + &summary.unmatched_offchain_sell_shares;
    let unmatched_offchain_notional = &summary.unmatched_offchain_buy_notional_usd
        + &summary.unmatched_offchain_sell_notional_usd;

    PnlSummary {
        counter_trade_pnl_usd: fmt_decimal(&summary.counter_trade_pnl_usd),
        onchain_netting_pnl_usd: fmt_decimal(&summary.onchain_netting_pnl_usd),
        directional_inventory_baseline_pnl_usd: fmt_decimal(
            &summary.directional_inventory_baseline_pnl_usd,
        ),
        directional_imbalance_excess_pnl_usd: fmt_decimal(
            &summary.directional_imbalance_excess_pnl_usd,
        ),
        directional_exposure_pnl_usd: fmt_decimal(&directional_exposure_pnl),
        total_pnl_usd: fmt_decimal(&total_pnl),
        gross_realized_pnl_usd: fmt_decimal(&total_pnl),
        tracked_costs_usd: "0".to_owned(),
        tracked_revenue_usd: "0".to_owned(),
        net_realized_pnl_usd: fmt_decimal(&total_pnl),
        realized_pnl_usd: fmt_decimal(&summary.realized_pnl_usd),
        matched_shares: fmt_decimal(&summary.matched_shares),
        onchain_notional_usd: fmt_decimal(&summary.onchain_notional_usd),
        offchain_notional_usd: fmt_decimal(&summary.offchain_notional_usd),
        inventory_drift_shares: fmt_decimal(&inventory_drift_shares),
        inventory_drift_usd: fmt_decimal(&inventory_drift_usd),
        open_long_shares: fmt_decimal(&summary.open_long_shares),
        open_short_shares: fmt_decimal(&summary.open_short_shares),
        unmatched_offchain_shares: fmt_decimal(&unmatched_offchain_shares),
        unmatched_offchain_notional_usd: fmt_decimal(&unmatched_offchain_notional),
        onchain_fill_count: summary.onchain_fill_count,
        offchain_fill_count: summary.offchain_fill_count,
        matched_lot_count: summary.matched_lot_count,
        open_lot_count: summary.open_lot_count,
        unmatched_offchain_fill_count: summary.unmatched_offchain_fill_count,
    }
}

pub(crate) fn symbol_summary_to_dto(symbol: &str, summary: &SummaryAcc) -> PnlSymbolSummary {
    let dto = summary_to_dto(summary);
    PnlSymbolSummary {
        symbol: symbol.to_owned(),
        counter_trade_pnl_usd: dto.counter_trade_pnl_usd,
        onchain_netting_pnl_usd: dto.onchain_netting_pnl_usd,
        directional_inventory_baseline_pnl_usd: dto.directional_inventory_baseline_pnl_usd,
        directional_imbalance_excess_pnl_usd: dto.directional_imbalance_excess_pnl_usd,
        directional_exposure_pnl_usd: dto.directional_exposure_pnl_usd,
        total_pnl_usd: dto.total_pnl_usd,
        gross_realized_pnl_usd: dto.gross_realized_pnl_usd,
        tracked_costs_usd: dto.tracked_costs_usd,
        tracked_revenue_usd: dto.tracked_revenue_usd,
        net_realized_pnl_usd: dto.net_realized_pnl_usd,
        realized_pnl_usd: dto.realized_pnl_usd,
        matched_shares: dto.matched_shares,
        inventory_drift_shares: dto.inventory_drift_shares,
        inventory_drift_usd: dto.inventory_drift_usd,
        open_long_shares: dto.open_long_shares,
        open_short_shares: dto.open_short_shares,
        unmatched_offchain_shares: dto.unmatched_offchain_shares,
        matched_lot_count: dto.matched_lot_count,
        onchain_fill_count: dto.onchain_fill_count,
        offchain_fill_count: dto.offchain_fill_count,
        unmatched_offchain_fill_count: dto.unmatched_offchain_fill_count,
    }
}

pub(crate) fn with_replay_exposure(filtered: PnlSummary, replay: PnlSummary) -> PnlSummary {
    PnlSummary {
        inventory_drift_shares: replay.inventory_drift_shares,
        inventory_drift_usd: replay.inventory_drift_usd,
        open_long_shares: replay.open_long_shares,
        open_short_shares: replay.open_short_shares,
        unmatched_offchain_shares: replay.unmatched_offchain_shares,
        unmatched_offchain_notional_usd: replay.unmatched_offchain_notional_usd,
        open_lot_count: replay.open_lot_count,
        unmatched_offchain_fill_count: replay.unmatched_offchain_fill_count,
        ..filtered
    }
}

fn with_symbol_replay_exposure(
    filtered: PnlSymbolSummary,
    replay: PnlSymbolSummary,
) -> PnlSymbolSummary {
    PnlSymbolSummary {
        inventory_drift_shares: replay.inventory_drift_shares,
        inventory_drift_usd: replay.inventory_drift_usd,
        open_long_shares: replay.open_long_shares,
        open_short_shares: replay.open_short_shares,
        unmatched_offchain_shares: replay.unmatched_offchain_shares,
        unmatched_offchain_fill_count: replay.unmatched_offchain_fill_count,
        ..filtered
    }
}

fn empty_symbol_summary(symbol: &str) -> PnlSymbolSummary {
    symbol_summary_to_dto(symbol, &SummaryAcc::default())
}

fn is_nonzero_text(value: &str) -> bool {
    Num::from_str(value).is_ok_and(|parsed| !parsed.is_zero())
}

fn has_replay_exposure(summary: &PnlSymbolSummary) -> bool {
    is_nonzero_text(&summary.inventory_drift_shares)
        || is_nonzero_text(&summary.inventory_drift_usd)
        || is_nonzero_text(&summary.open_long_shares)
        || is_nonzero_text(&summary.open_short_shares)
        || is_nonzero_text(&summary.unmatched_offchain_shares)
        || summary.unmatched_offchain_fill_count > 0
}

pub(crate) fn merge_symbol_replay_exposure(
    filtered_symbols: Vec<PnlSymbolSummary>,
    replay_symbols: impl Iterator<Item = PnlSymbolSummary>,
) -> Vec<PnlSymbolSummary> {
    let mut by_symbol: HashMap<String, PnlSymbolSummary> = filtered_symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();

    for replay in replay_symbols {
        let existing = by_symbol.remove(&replay.symbol);
        if existing.is_some() || has_replay_exposure(&replay) {
            by_symbol.insert(
                replay.symbol.clone(),
                with_symbol_replay_exposure(
                    existing.unwrap_or_else(|| empty_symbol_summary(&replay.symbol)),
                    replay,
                ),
            );
        }
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    rows
}

pub(crate) fn reset_symbol_costs(symbols: Vec<PnlSymbolSummary>) -> Vec<PnlSymbolSummary> {
    symbols
        .into_iter()
        .map(|row| PnlSymbolSummary {
            gross_realized_pnl_usd: row.total_pnl_usd.clone(),
            tracked_costs_usd: "0".to_owned(),
            tracked_revenue_usd: "0".to_owned(),
            net_realized_pnl_usd: row.total_pnl_usd.clone(),
            ..row
        })
        .collect()
}

pub(crate) fn with_direct_symbol_costs(
    symbols: Vec<PnlSymbolSummary>,
    cost_entries: &[CostEntryInternal],
) -> Vec<PnlSymbolSummary> {
    let mut costs_by_symbol: HashMap<String, Num> = HashMap::new();
    let mut revenue_by_symbol: HashMap<String, Num> = HashMap::new();
    for entry in cost_entries {
        let Some(symbol) = &entry.symbol else {
            continue;
        };
        if entry.effect == AccountingEffect::Revenue {
            *revenue_by_symbol.entry(symbol.clone()).or_default() += &entry.amount_usd;
        } else if entry.effect == AccountingEffect::Cost {
            *costs_by_symbol.entry(symbol.clone()).or_default() += &entry.amount_usd;
        }
    }

    if costs_by_symbol.is_empty() && revenue_by_symbol.is_empty() {
        return symbols;
    }

    let mut by_symbol: HashMap<String, PnlSymbolSummary> = symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();
    let mut affected_symbols: BTreeSet<String> = costs_by_symbol.keys().cloned().collect();
    affected_symbols.extend(revenue_by_symbol.keys().cloned());

    for symbol in affected_symbols {
        let existing = by_symbol
            .remove(&symbol)
            .unwrap_or_else(|| empty_symbol_summary(&symbol));
        let gross = parse_decimal_lossy(&existing.total_pnl_usd);
        let cost = costs_by_symbol.remove(&symbol).unwrap_or_default();
        let revenue = revenue_by_symbol.remove(&symbol).unwrap_or_default();
        let net = &(&gross - &cost) + &revenue;
        by_symbol.insert(
            symbol.clone(),
            PnlSymbolSummary {
                gross_realized_pnl_usd: fmt_decimal(&gross),
                tracked_costs_usd: fmt_decimal(&cost),
                tracked_revenue_usd: fmt_decimal(&revenue),
                net_realized_pnl_usd: fmt_decimal(&net),
                ..existing
            },
        );
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    rows
}

pub(crate) fn summary_from_entries(entries: &[PnlEntry]) -> SummaryAndSymbols {
    let mut total = SummaryAcc::default();
    let mut per_symbol: HashMap<String, SummaryAcc> = HashMap::new();

    for entry in entries {
        let summary = per_symbol.entry(entry.symbol.clone()).or_default();
        let shares = parse_decimal_lossy(&entry.shares);
        let opening_notional = &shares * &parse_decimal_lossy(&entry.opening_price_usd);
        let closing_notional = &shares * &parse_decimal_lossy(&entry.closing_price_usd);
        let pnl = parse_decimal_lossy(&entry.realized_pnl_usd);

        summary.matched_shares += &shares;
        if entry.opening_venue == "onchain" {
            add_venue_notional(summary, Venue::Onchain, &opening_notional);
        } else if entry.opening_venue == "offchain" {
            add_venue_notional(summary, Venue::Offchain, &opening_notional);
        }
        if entry.closing_venue == "onchain" {
            add_venue_notional(summary, Venue::Onchain, &closing_notional);
        } else if entry.closing_venue == "offchain" {
            add_venue_notional(summary, Venue::Offchain, &closing_notional);
        }
        summary.matched_lot_count += 1;

        match entry.pnl_bucket {
            "counter_trade" => {
                summary.counter_trade_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            "onchain_netting" => {
                summary.onchain_netting_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            "directional_exposure" => {
                summary.directional_imbalance_excess_pnl_usd += &pnl;
                summary.directional_exposure_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            _ => {}
        }
    }

    let mut symbols: Vec<_> = per_symbol.into_iter().collect();
    symbols.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbols = symbols
        .into_iter()
        .map(|(symbol, summary)| {
            add_summary(&mut total, &summary);
            symbol_summary_to_dto(&symbol, &summary)
        })
        .collect();

    SummaryAndSymbols {
        summary: summary_to_dto(&total),
        symbols,
    }
}
