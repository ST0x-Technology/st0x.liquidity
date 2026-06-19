use std::collections::{BTreeSet, HashMap, VecDeque};
use std::str::FromStr;

use num_decimal::Num;
use st0x_finance::Symbol;

use super::costs::{AccountingEffect, CostEntryInternal};
use super::parsing::{
    abs_decimal, direction_field, fmt_decimal, min_decimal, nested_record, number_text_field,
    optional_persisted_decimal_field, parse_internal_decimal, persisted_decimal_field, text_field,
};
use super::query::PnlError;
use super::response::{PnlEntry, PnlSummary, PnlSymbolSummary};
use super::sessions::seconds_between;
use super::state::{
    Direction, Fill, Lot, LotSide, PnlBucket, PositionEventRow, PositionReplayDelta, SummaryAcc,
    SummaryAndSymbols, SymbolBook, UnmatchedOffchainAllocation, Venue,
};
use super::{ATTRIBUTION_METHOD, COUNTER_TRADE_THRESHOLD_SECONDS, EPSILON};

fn lot_side_to_direction(side: LotSide) -> Direction {
    match side {
        LotSide::Long => Direction::Buy,
        LotSide::Short => Direction::Sell,
    }
}

fn add_venue_notional(summary: &mut SummaryAcc, venue: Venue, notional: &Num) {
    match venue {
        Venue::Onchain => summary.onchain_notional_usd += notional,
        Venue::Offchain => summary.offchain_notional_usd += notional,
        Venue::Manual => {}
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

fn malformed_position_payload(row: &PositionEventRow, reason: &'static str) -> PnlError {
    PnlError::MalformedPayload {
        rowid: row.rowid,
        aggregate_type: "Position",
        event_type: row.event_type.clone(),
        reason,
    }
}

fn ensure_positive_fill_decimal(
    row: &PositionEventRow,
    value: &Num,
    reason: &'static str,
) -> Result<(), PnlError> {
    if value.is_zero() || value.is_negative() {
        return Err(malformed_position_payload(row, reason));
    }

    Ok(())
}

pub(crate) fn parse_onchain_fill(
    row: &PositionEventRow,
    _warnings: &mut Vec<String>,
) -> Result<Option<Fill>, PnlError> {
    let Some(filled) = nested_record(&row.payload, "OnChainOrderFilled") else {
        return Err(malformed_position_payload(
            row,
            "missing OnChainOrderFilled",
        ));
    };

    let amount = persisted_decimal_field(row, filled, "amount")?;
    let direction = direction_field(filled, "direction");
    let price = persisted_decimal_field(row, filled, "price_usdc")?;
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
        return Err(malformed_position_payload(
            row,
            "incomplete OnChainOrderFilled payload",
        ));
    };

    ensure_positive_fill_decimal(row, &amount, "non-positive OnChainOrderFilled amount")?;
    ensure_positive_fill_decimal(row, &price, "non-positive OnChainOrderFilled price_usdc")?;

    Ok(Some(Fill {
        rowid: row.rowid,
        id: format!("{tx_hash}:{log_index}"),
        symbol: Symbol::new(row.symbol.clone())
            .map_err(|_| malformed_position_payload(row, "invalid OnChainOrderFilled symbol"))?,
        shares: amount,
        direction,
        price,
        executed_at,
        venue: Venue::Onchain,
    }))
}

pub(crate) fn parse_offchain_fill(
    row: &PositionEventRow,
    _warnings: &mut Vec<String>,
) -> Result<Option<Fill>, PnlError> {
    let Some(filled) = nested_record(&row.payload, "OffChainOrderFilled") else {
        return Err(malformed_position_payload(
            row,
            "missing OffChainOrderFilled",
        ));
    };

    let order_id = text_field(filled, "offchain_order_id");
    let shares = persisted_decimal_field(row, filled, "shares_filled")?;
    let direction = direction_field(filled, "direction");
    let price = persisted_decimal_field(row, filled, "price")?;
    let executed_at = text_field(filled, "broker_timestamp");

    let (Some(order_id), Some(shares), Some(direction), Some(price), Some(executed_at)) =
        (order_id, shares, direction, price, executed_at)
    else {
        return Err(malformed_position_payload(
            row,
            "incomplete OffChainOrderFilled payload",
        ));
    };

    ensure_positive_fill_decimal(
        row,
        &shares,
        "non-positive OffChainOrderFilled shares_filled",
    )?;
    ensure_positive_fill_decimal(row, &price, "non-positive OffChainOrderFilled price")?;

    Ok(Some(Fill {
        rowid: row.rowid,
        id: order_id,
        symbol: Symbol::new(row.symbol.clone())
            .map_err(|_| malformed_position_payload(row, "invalid OffChainOrderFilled symbol"))?,
        shares,
        direction,
        price,
        executed_at,
        venue: Venue::Offchain,
    }))
}

pub(crate) fn apply_manual_position_adjustment(
    book: &mut SymbolBook,
    row: &PositionEventRow,
    warnings: &mut Vec<String>,
) -> Result<(), PnlError> {
    let Some(adjusted) = nested_record(&row.payload, "ManualPositionAdjusted") else {
        warnings.push(format!(
            "Skipped malformed manual position adjustment {}: missing ManualPositionAdjusted",
            row.symbol
        ));
        return Ok(());
    };

    let target_net = persisted_decimal_field(row, adjusted, "target_net")?;
    let adjusted_at = text_field(adjusted, "adjusted_at");
    let (Some(target_net), Some(adjusted_at)) = (target_net, adjusted_at) else {
        warnings.push(format!(
            "Skipped malformed manual position adjustment {}: incomplete payload",
            row.symbol
        ));
        return Ok(());
    };

    book.long_lots.clear();
    book.short_lots.clear();
    book.original_onchain_shares.clear();
    book.matched_onchain_shares.clear();

    if target_net.is_zero() {
        return Ok(());
    }

    let event_price = optional_persisted_decimal_field(row, adjusted, "price_usdc")?;
    let price = event_price.clone().or_else(|| book.last_price_usdc.clone());
    let Some(price) = price else {
        return Err(malformed_position_payload(
            row,
            "nonzero ManualPositionAdjusted missing price_usdc and no prior replay price",
        ));
    };
    if let Some(event_price) = event_price {
        book.last_price_usdc = Some(event_price);
    }

    let side = if target_net.is_negative() {
        LotSide::Short
    } else {
        LotSide::Long
    };
    let lot = Lot {
        trade_id: format!("manual-position-adjustment:{}", row.rowid),
        side,
        remaining_shares: abs_decimal(&target_net),
        price,
        opened_at: adjusted_at,
        opened_rowid: row.rowid,
        opened_venue: Venue::Manual,
    };

    match side {
        LotSide::Long => book.long_lots.push_back(lot),
        LotSide::Short => book.short_lots.push_back(lot),
    }

    Ok(())
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
        LotSide::Long => book.long_lots.push_back(lot),
        LotSide::Short => book.short_lots.push_back(lot),
    }
}

pub(crate) fn apply_onchain_fill(
    book: &mut SymbolBook,
    fill: &Fill,
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
    book.last_price_usdc = Some(fill.price.clone());
    book.summary.onchain_fill_count += 1;
    let source_lots = if fill.direction == Direction::Buy {
        &mut book.short_lots
    } else {
        &mut book.long_lots
    };
    let remaining = match_fill_against_lots(
        fill,
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
    open_residual_lot(book, fill, remaining);
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
    fill: &Fill,
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
        fill,
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
        open_residual_lot(book, fill, remaining);
    }
}

fn match_fill_against_lots(
    fill: &Fill,
    source_lots: &mut VecDeque<Lot>,
    summary: &mut SummaryAcc,
    matched_onchain_shares: &mut HashMap<String, Num>,
    entries: &mut Vec<PnlEntry>,
    bucket: PnlBucket,
) -> Num {
    let mut remaining = fill.shares.clone();

    while !remaining.is_zero() {
        let Some(mut front_lot) = source_lots.pop_front() else {
            break;
        };
        let matched_shares = min_decimal(&remaining, &front_lot.remaining_shares);
        if matched_shares.is_zero() {
            continue;
        }

        let elapsed_seconds = seconds_between(&front_lot.opened_at, &fill.executed_at);
        let delayed_counter_trade = bucket == PnlBucket::CounterTrade
            && front_lot.opened_venue != Venue::Offchain
            && elapsed_seconds > COUNTER_TRADE_THRESHOLD_SECONDS;
        let effective_bucket = if front_lot.opened_venue == Venue::Offchain
            || front_lot.opened_venue == Venue::Manual
            || delayed_counter_trade
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
            source_lots.push_front(front_lot.clone());
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
        let closing_direction = fill.direction;
        let opening_price_text = fmt_decimal(&front_lot.price);
        let closing_price_text = fmt_decimal(&fill.price);
        let onchain_direction = text_for_venue(
            Venue::Onchain,
            &front_lot,
            fill,
            opening_direction.as_str(),
            closing_direction.as_str(),
        );
        let offchain_direction = text_for_venue(
            Venue::Offchain,
            &front_lot,
            fill,
            opening_direction.as_str(),
            closing_direction.as_str(),
        );
        let onchain_trade_id = text_for_venue(
            Venue::Onchain,
            &front_lot,
            fill,
            &front_lot.trade_id,
            &fill.id,
        );
        let offchain_order_id = text_for_venue(
            Venue::Offchain,
            &front_lot,
            fill,
            &front_lot.trade_id,
            &fill.id,
        );
        let onchain_price_text = text_for_venue(
            Venue::Onchain,
            &front_lot,
            fill,
            &opening_price_text,
            &closing_price_text,
        );
        let offchain_price_text = text_for_venue(
            Venue::Offchain,
            &front_lot,
            fill,
            &opening_price_text,
            &closing_price_text,
        );

        entries.push(PnlEntry {
            symbol: fill.symbol.clone(),
            pnl_bucket: effective_bucket,
            matched_at: fill.executed_at.clone(),
            opened_at: front_lot.opened_at.clone(),
            closed_at: fill.executed_at.clone(),
            opening_fill_id: front_lot.trade_id.clone(),
            closing_fill_id: fill.id.clone(),
            opening_rowid: front_lot.opened_rowid,
            closing_rowid: fill.rowid,
            opening_venue: front_lot.opened_venue,
            closing_venue: fill.venue,
            opening_direction,
            closing_direction,
            opening_price_usd: front_lot.price.clone(),
            closing_price_usd: fill.price.clone(),
            onchain_trade_id,
            offchain_order_id,
            onchain_direction,
            offchain_direction,
            shares: matched_shares.clone(),
            onchain_price_usdc: onchain_price_text,
            offchain_price_usd: offchain_price_text,
            spread_usd: spread,
            realized_pnl_usd: realized_pnl,
            elapsed_seconds,
            counter_trade_threshold_seconds: COUNTER_TRADE_THRESHOLD_SECONDS,
            delayed_counter_trade,
            attribution_method: ATTRIBUTION_METHOD,
        });

        remaining -= &matched_shares;
    }

    remaining
}

fn text_for_venue(
    venue: Venue,
    front_lot: &Lot,
    fill: &Fill,
    opening_value: &str,
    closing_value: &str,
) -> String {
    if front_lot.opened_venue == venue {
        opening_value.to_owned()
    } else if fill.venue == venue {
        closing_value.to_owned()
    } else {
        String::new()
    }
}

fn finalize_lots(summary: &mut SummaryAcc, lots: &VecDeque<Lot>) {
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
    symbol: &Symbol,
    book: &mut SymbolBook,
    position_nets: &HashMap<Symbol, Num>,
    warnings: &mut Vec<String>,
    position_replay_deltas: &mut Vec<PositionReplayDelta>,
) {
    finalize_lots(&mut book.summary, &book.long_lots);
    finalize_lots(&mut book.summary, &book.short_lots);

    let epsilon = match Num::from_str(EPSILON) {
        Ok(value) => value,
        Err(error) => panic!("EPSILON must be a valid decimal: {error}"),
    };
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

pub(crate) fn symbol_summary_to_dto(symbol: &Symbol, summary: &SummaryAcc) -> PnlSymbolSummary {
    let dto = summary_to_dto(summary);
    PnlSymbolSummary {
        symbol: symbol.clone(),
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

fn empty_symbol_summary(symbol: &Symbol) -> PnlSymbolSummary {
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
    let mut by_symbol: HashMap<Symbol, PnlSymbolSummary> = filtered_symbols
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
) -> Result<Vec<PnlSymbolSummary>, PnlError> {
    let mut costs_by_symbol: HashMap<Symbol, Num> = HashMap::new();
    let mut revenue_by_symbol: HashMap<Symbol, Num> = HashMap::new();
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
        return Ok(symbols);
    }

    let mut by_symbol: HashMap<Symbol, PnlSymbolSummary> = symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();
    let mut affected_symbols: BTreeSet<Symbol> = costs_by_symbol.keys().cloned().collect();
    affected_symbols.extend(revenue_by_symbol.keys().cloned());

    for symbol in affected_symbols {
        let existing = by_symbol
            .remove(&symbol)
            .unwrap_or_else(|| empty_symbol_summary(&symbol));
        let gross = parse_internal_decimal("symbol.totalPnlUsd", &existing.total_pnl_usd)?;
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
    Ok(rows)
}

pub(crate) fn summary_from_entries(entries: &[PnlEntry]) -> SummaryAndSymbols {
    let mut total = SummaryAcc::default();
    let mut per_symbol: HashMap<Symbol, SummaryAcc> = HashMap::new();

    for entry in entries {
        let summary = per_symbol.entry(entry.symbol.clone()).or_default();
        let shares = entry.shares.clone();
        let opening_notional = &shares * &entry.opening_price_usd;
        let closing_notional = &shares * &entry.closing_price_usd;
        let pnl = entry.realized_pnl_usd.clone();

        summary.matched_shares += &shares;
        if entry.opening_venue == Venue::Onchain {
            add_venue_notional(summary, Venue::Onchain, &opening_notional);
        } else if entry.opening_venue == Venue::Offchain {
            add_venue_notional(summary, Venue::Offchain, &opening_notional);
        }
        if entry.closing_venue == Venue::Onchain {
            add_venue_notional(summary, Venue::Onchain, &closing_notional);
        } else if entry.closing_venue == Venue::Offchain {
            add_venue_notional(summary, Venue::Offchain, &closing_notional);
        }
        summary.matched_lot_count += 1;

        match entry.pnl_bucket {
            PnlBucket::CounterTrade => {
                summary.counter_trade_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            PnlBucket::OnchainNetting => {
                summary.onchain_netting_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
            PnlBucket::DirectionalExposure => {
                summary.directional_imbalance_excess_pnl_usd += &pnl;
                summary.directional_exposure_pnl_usd += &pnl;
                summary.realized_pnl_usd += &pnl;
            }
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
