use rain_math_float::Float;
use std::collections::{HashMap, VecDeque};

use st0x_finance::Symbol;
use st0x_float_macro::float;

use super::costs::{AccountingEffect, CostEntryInternal};
use super::parsing::{fmt_decimal, parse_internal_decimal, parse_ledger_decimal};
use super::query::PnlError;
use super::response::{PnlEntry, PnlSummary, PnlSymbolSummary};
use super::sessions::seconds_between;
use super::state::{
    Direction, Fill, Lot, LotSide, ManualAdjustmentRow, OffchainFillRow, OffchainPlacementRow,
    OnchainFillRow, PnlBucket, PositionReplayDelta, SummaryAcc, SummaryAndSymbols, SymbolBook,
    UnmatchedOffchainAllocation, Venue,
};
use super::{ATTRIBUTION_METHOD, COUNTER_TRADE_THRESHOLD_SECONDS, EPSILON};

fn lot_side_to_direction(side: LotSide) -> Direction {
    match side {
        LotSide::Long => Direction::Buy,
        LotSide::Short => Direction::Sell,
    }
}

// `#[track_caller]`: called from both the FIFO replay path
// (`match_fill_against_lots`) and the window/total aggregation path
// (`summary_from_entries`). Without it, a `FloatError` converted by the `?`
// below always reports this function's own line, so a failure in one stage
// is indistinguishable from a failure in the other; with it, the location
// captured by `PnlError::Arithmetic` (see query.rs's `ArithmeticFailure`)
// forwards to whichever call site invoked this helper.
#[track_caller]
fn add_venue_notional(
    summary: &mut SummaryAcc,
    venue: Venue,
    notional: Float,
) -> Result<(), PnlError> {
    match venue {
        Venue::Onchain => summary.onchain_notional_usd = (summary.onchain_notional_usd + notional)?,
        Venue::Offchain => {
            summary.offchain_notional_usd = (summary.offchain_notional_usd + notional)?;
        }
        Venue::Manual => {}
    }

    Ok(())
}

// Same multi-caller reasoning as `add_venue_notional` above: shared by the
// FIFO replay path and the window/total aggregation path.
#[track_caller]
fn add_realized_pnl(
    summary: &mut SummaryAcc,
    bucket: PnlBucket,
    value: Float,
) -> Result<(), PnlError> {
    match bucket {
        PnlBucket::CounterTrade => {
            summary.counter_trade_pnl_usd = (summary.counter_trade_pnl_usd + value)?;
        }
        PnlBucket::OnchainNetting => {
            summary.onchain_netting_pnl_usd = (summary.onchain_netting_pnl_usd + value)?;
        }
        PnlBucket::DirectionalExposure => {
            summary.directional_imbalance_excess_pnl_usd =
                (summary.directional_imbalance_excess_pnl_usd + value)?;
            summary.directional_exposure_pnl_usd = (summary.directional_exposure_pnl_usd + value)?;
        }
    }
    summary.realized_pnl_usd = (summary.realized_pnl_usd + value)?;

    Ok(())
}

fn corrupt_ledger_row(table: &'static str, rowid: i64, reason: &'static str) -> PnlError {
    PnlError::InvalidLedgerRow {
        table,
        rowid,
        reason,
    }
}

fn ensure_positive_fill_decimal(
    table: &'static str,
    rowid: i64,
    value: &Float,
    reason: &'static str,
) -> Result<(), PnlError> {
    if value.is_zero()? || value.lt(float!(0))? {
        return Err(corrupt_ledger_row(table, rowid, reason));
    }

    Ok(())
}

pub(crate) fn parse_onchain_fill(row: &OnchainFillRow) -> Result<Fill, PnlError> {
    const TABLE: &str = "pnl_onchain_fill";
    let shares = parse_ledger_decimal(TABLE, row.event_rowid, "shares", &row.shares)?;
    let price = parse_ledger_decimal(TABLE, row.event_rowid, "price_usd", &row.price_usd)?;
    ensure_positive_fill_decimal(
        TABLE,
        row.event_rowid,
        &shares,
        "non-positive onchain fill shares",
    )?;
    ensure_positive_fill_decimal(
        TABLE,
        row.event_rowid,
        &price,
        "non-positive onchain fill price",
    )?;

    Ok(Fill {
        rowid: row.event_rowid,
        id: format!("{}:{}", row.tx_hash, row.log_index),
        symbol: Symbol::new(row.symbol.clone())
            .map_err(|_| corrupt_ledger_row(TABLE, row.event_rowid, "invalid symbol"))?,
        shares,
        direction: row.direction,
        price,
        executed_at: row.executed_at.clone(),
        venue: Venue::Onchain,
    })
}

pub(crate) fn parse_offchain_fill(row: &OffchainFillRow) -> Result<Fill, PnlError> {
    const TABLE: &str = "pnl_offchain_fill";
    let shares = parse_ledger_decimal(TABLE, row.event_rowid, "shares", &row.shares)?;
    let price = parse_ledger_decimal(TABLE, row.event_rowid, "price_usd", &row.price_usd)?;
    ensure_positive_fill_decimal(
        TABLE,
        row.event_rowid,
        &shares,
        "non-positive offchain fill shares",
    )?;
    ensure_positive_fill_decimal(
        TABLE,
        row.event_rowid,
        &price,
        "non-positive offchain fill price",
    )?;

    Ok(Fill {
        rowid: row.event_rowid,
        id: row.offchain_order_id.clone(),
        symbol: Symbol::new(row.symbol.clone())
            .map_err(|_| corrupt_ledger_row(TABLE, row.event_rowid, "invalid symbol"))?,
        shares,
        direction: row.direction,
        price,
        executed_at: row.executed_at.clone(),
        venue: Venue::Offchain,
    })
}

pub(crate) fn apply_manual_position_adjustment(
    book: &mut SymbolBook,
    row: &ManualAdjustmentRow,
) -> Result<(), PnlError> {
    const TABLE: &str = "pnl_manual_adjustment";
    let target_net = parse_ledger_decimal(TABLE, row.event_rowid, "target_net", &row.target_net)?;

    book.long_lots.clear();
    book.short_lots.clear();
    book.original_onchain_shares.clear();
    book.matched_onchain_shares.clear();

    if target_net.is_zero()? {
        return Ok(());
    }

    let event_price = row
        .price_usd
        .as_deref()
        .map(|price| parse_ledger_decimal(TABLE, row.event_rowid, "price_usd", price))
        .transpose()?;
    let price = event_price.or(book.last_price_usdc);
    let Some(price) = price else {
        return Err(corrupt_ledger_row(
            TABLE,
            row.event_rowid,
            "nonzero manual adjustment missing price_usd and no prior replay price",
        ));
    };
    if let Some(event_price) = event_price {
        book.last_price_usdc = Some(event_price);
    }

    let side = if target_net.lt(float!(0))? {
        LotSide::Short
    } else {
        LotSide::Long
    };
    let lot = Lot {
        trade_id: format!("manual-position-adjustment:{}", row.event_rowid),
        side,
        remaining_shares: target_net.abs()?,
        price,
        opened_at: row.adjusted_at.clone(),
        opened_rowid: row.event_rowid,
        opened_venue: Venue::Manual,
    };

    match side {
        LotSide::Long => book.long_lots.push_back(lot),
        LotSide::Short => book.short_lots.push_back(lot),
    }

    Ok(())
}

fn open_residual_lot(book: &mut SymbolBook, fill: &Fill, remaining: Float) {
    let side = if fill.direction == Direction::Buy {
        LotSide::Long
    } else {
        LotSide::Short
    };
    let lot = Lot {
        trade_id: fill.id.clone(),
        side,
        remaining_shares: remaining,
        price: fill.price,
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
) -> Result<(), PnlError> {
    if book.seen_onchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate onchain trade_id {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return Ok(());
    }

    book.seen_onchain_fill_ids.insert(fill.id.clone());
    book.last_price_usdc = Some(fill.price);
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
    )?;
    if remaining.is_zero()? {
        return Ok(());
    }

    let original = book
        .original_onchain_shares
        .entry(fill.id.clone())
        .or_insert(float!(0));
    *original = (*original + remaining)?;
    open_residual_lot(book, fill, remaining);

    Ok(())
}

pub(crate) fn apply_offchain_placement(
    book: &mut SymbolBook,
    row: &OffchainPlacementRow,
    warnings: &mut Vec<String>,
) {
    if book
        .seen_offchain_placement_ids
        .contains(&row.offchain_order_id)
    {
        warnings.push(format!(
            "PnL audit error: duplicate offchain placement {} for {} was skipped",
            row.offchain_order_id, row.symbol
        ));
        return;
    }

    book.seen_offchain_placement_ids
        .insert(row.offchain_order_id.clone());
}

pub(crate) fn apply_offchain_fill(
    book: &mut SymbolBook,
    fill: &Fill,
    entries: &mut Vec<PnlEntry>,
    warnings: &mut Vec<String>,
    unmatched_offchain_allocations: &mut Vec<UnmatchedOffchainAllocation>,
) -> Result<(), PnlError> {
    if book.seen_offchain_fill_ids.contains(&fill.id) {
        warnings.push(format!(
            "PnL audit error: duplicate offchain fill {} for {} was skipped",
            fill.id, fill.symbol
        ));
        return Ok(());
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
    )?;

    if !remaining.is_zero()? {
        unmatched_offchain_allocations.push(UnmatchedOffchainAllocation {
            symbol: fill.symbol.clone(),
            fill_id: fill.id.clone(),
            shares: remaining,
        });
        open_residual_lot(book, fill, remaining);
    }

    Ok(())
}

fn match_fill_against_lots(
    fill: &Fill,
    source_lots: &mut VecDeque<Lot>,
    summary: &mut SummaryAcc,
    matched_onchain_shares: &mut HashMap<String, Float>,
    entries: &mut Vec<PnlEntry>,
    bucket: PnlBucket,
) -> Result<Float, PnlError> {
    let mut remaining = fill.shares;

    while !remaining.is_zero()? {
        let Some(mut front_lot) = source_lots.pop_front() else {
            break;
        };
        let matched_shares = remaining.min(front_lot.remaining_shares)?;
        if matched_shares.is_zero()? {
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
            (fill.price - front_lot.price)?
        } else {
            (front_lot.price - fill.price)?
        };
        let realized_pnl = (matched_shares * spread)?;
        let opening_notional = (matched_shares * front_lot.price)?;
        let closing_notional = (matched_shares * fill.price)?;

        front_lot.remaining_shares = (front_lot.remaining_shares - matched_shares)?;
        if !front_lot.remaining_shares.is_zero()? {
            source_lots.push_front(front_lot.clone());
        }

        add_realized_pnl(summary, effective_bucket, realized_pnl)?;
        summary.matched_shares = (summary.matched_shares + matched_shares)?;
        add_venue_notional(summary, front_lot.opened_venue, opening_notional)?;
        add_venue_notional(summary, fill.venue, closing_notional)?;
        summary.matched_lot_count += 1;

        if front_lot.opened_venue == Venue::Onchain {
            let matched = matched_onchain_shares
                .entry(front_lot.trade_id.clone())
                .or_insert(float!(0));
            *matched = (*matched + matched_shares)?;
        }

        let opening_direction = lot_side_to_direction(front_lot.side);
        let closing_direction = fill.direction;
        let opening_price_text = fmt_decimal(front_lot.price)?;
        let closing_price_text = fmt_decimal(fill.price)?;
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
            opening_price_usd: front_lot.price,
            closing_price_usd: fill.price,
            onchain_trade_id,
            offchain_order_id,
            onchain_direction,
            offchain_direction,
            shares: matched_shares,
            onchain_price_usdc: onchain_price_text,
            offchain_price_usd: offchain_price_text,
            spread_usd: spread,
            realized_pnl_usd: realized_pnl,
            elapsed_seconds,
            counter_trade_threshold_seconds: COUNTER_TRADE_THRESHOLD_SECONDS,
            delayed_counter_trade,
            attribution_method: ATTRIBUTION_METHOD,
        });

        remaining = (remaining - matched_shares)?;
    }

    Ok(remaining)
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

fn finalize_lots(summary: &mut SummaryAcc, lots: &VecDeque<Lot>) -> Result<(), PnlError> {
    for lot in lots {
        let notional = (lot.remaining_shares * lot.price)?;
        match lot.side {
            LotSide::Long => {
                summary.open_long_shares = (summary.open_long_shares + lot.remaining_shares)?;
                summary.open_long_notional_usd = (summary.open_long_notional_usd + notional)?;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_buy_shares =
                        (summary.unmatched_offchain_buy_shares + lot.remaining_shares)?;
                    summary.unmatched_offchain_buy_notional_usd =
                        (summary.unmatched_offchain_buy_notional_usd + notional)?;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
            LotSide::Short => {
                summary.open_short_shares = (summary.open_short_shares + lot.remaining_shares)?;
                summary.open_short_notional_usd = (summary.open_short_notional_usd + notional)?;
                if lot.opened_venue == Venue::Offchain {
                    summary.unmatched_offchain_sell_shares =
                        (summary.unmatched_offchain_sell_shares + lot.remaining_shares)?;
                    summary.unmatched_offchain_sell_notional_usd =
                        (summary.unmatched_offchain_sell_notional_usd + notional)?;
                    summary.unmatched_offchain_fill_count += 1;
                }
            }
        }
        summary.open_lot_count += 1;
    }

    Ok(())
}

pub(crate) fn finalize_book(
    symbol: &Symbol,
    book: &mut SymbolBook,
    position_nets: &HashMap<Symbol, Float>,
    warnings: &mut Vec<String>,
    position_replay_deltas: &mut Vec<PositionReplayDelta>,
) -> Result<(), PnlError> {
    finalize_lots(&mut book.summary, &book.long_lots)?;
    finalize_lots(&mut book.summary, &book.short_lots)?;

    for (trade_id, matched_shares) in &book.matched_onchain_shares {
        if let Some(original_shares) = book.original_onchain_shares.get(trade_id) {
            let excess = (*matched_shares - *original_shares)?;
            if excess.gt(EPSILON)? {
                warnings.push(format!(
                    "PnL audit error: onchain lot {} for {} matched {} shares above original {}",
                    trade_id,
                    symbol,
                    fmt_decimal(*matched_shares)?,
                    fmt_decimal(*original_shares)?
                ));
            }
        }
    }

    if let Some(position_net) = position_nets.get(symbol) {
        let replay_net = (book.summary.open_long_shares - book.summary.open_short_shares)?;
        let delta = (replay_net - *position_net)?;
        if delta.abs()?.gt(EPSILON)? {
            position_replay_deltas.push(PositionReplayDelta {
                symbol: symbol.to_owned(),
                replay_net,
                position_net: *position_net,
            });
        }
    }

    Ok(())
}

// Same multi-caller reasoning as `add_venue_notional` above: shared by the
// grand-total accumulation in `builder.rs` and the per-symbol aggregation in
// `summary_from_entries` below.
#[track_caller]
pub(crate) fn add_summary(target: &mut SummaryAcc, source: &SummaryAcc) -> Result<(), PnlError> {
    target.counter_trade_pnl_usd = (target.counter_trade_pnl_usd + source.counter_trade_pnl_usd)?;
    target.onchain_netting_pnl_usd =
        (target.onchain_netting_pnl_usd + source.onchain_netting_pnl_usd)?;
    target.directional_inventory_baseline_pnl_usd = (target
        .directional_inventory_baseline_pnl_usd
        + source.directional_inventory_baseline_pnl_usd)?;
    target.directional_imbalance_excess_pnl_usd = (target.directional_imbalance_excess_pnl_usd
        + source.directional_imbalance_excess_pnl_usd)?;
    target.directional_exposure_pnl_usd =
        (target.directional_exposure_pnl_usd + source.directional_exposure_pnl_usd)?;
    target.realized_pnl_usd = (target.realized_pnl_usd + source.realized_pnl_usd)?;
    target.matched_shares = (target.matched_shares + source.matched_shares)?;
    target.onchain_notional_usd = (target.onchain_notional_usd + source.onchain_notional_usd)?;
    target.offchain_notional_usd = (target.offchain_notional_usd + source.offchain_notional_usd)?;
    target.open_long_shares = (target.open_long_shares + source.open_long_shares)?;
    target.open_short_shares = (target.open_short_shares + source.open_short_shares)?;
    target.open_long_notional_usd =
        (target.open_long_notional_usd + source.open_long_notional_usd)?;
    target.open_short_notional_usd =
        (target.open_short_notional_usd + source.open_short_notional_usd)?;
    target.unmatched_offchain_buy_shares =
        (target.unmatched_offchain_buy_shares + source.unmatched_offchain_buy_shares)?;
    target.unmatched_offchain_sell_shares =
        (target.unmatched_offchain_sell_shares + source.unmatched_offchain_sell_shares)?;
    target.unmatched_offchain_buy_notional_usd =
        (target.unmatched_offchain_buy_notional_usd + source.unmatched_offchain_buy_notional_usd)?;
    target.unmatched_offchain_sell_notional_usd = (target.unmatched_offchain_sell_notional_usd
        + source.unmatched_offchain_sell_notional_usd)?;
    target.onchain_fill_count += source.onchain_fill_count;
    target.offchain_fill_count += source.offchain_fill_count;
    target.matched_lot_count += source.matched_lot_count;
    target.open_lot_count += source.open_lot_count;
    target.unmatched_offchain_fill_count += source.unmatched_offchain_fill_count;

    Ok(())
}

pub(crate) fn summary_to_dto(summary: &SummaryAcc) -> Result<PnlSummary, PnlError> {
    let directional_exposure_pnl = (summary.directional_inventory_baseline_pnl_usd
        + summary.directional_imbalance_excess_pnl_usd)?;
    let total_pnl = (((summary.counter_trade_pnl_usd + summary.onchain_netting_pnl_usd)?
        + summary.directional_inventory_baseline_pnl_usd)?
        + summary.directional_imbalance_excess_pnl_usd)?;
    let inventory_drift_shares = (summary.open_long_shares - summary.open_short_shares)?;
    let inventory_drift_usd = (summary.open_long_notional_usd - summary.open_short_notional_usd)?;
    let unmatched_offchain_shares =
        (summary.unmatched_offchain_buy_shares + summary.unmatched_offchain_sell_shares)?;
    let unmatched_offchain_notional = (summary.unmatched_offchain_buy_notional_usd
        + summary.unmatched_offchain_sell_notional_usd)?;

    Ok(PnlSummary {
        counter_trade_pnl_usd: fmt_decimal(summary.counter_trade_pnl_usd)?,
        onchain_netting_pnl_usd: fmt_decimal(summary.onchain_netting_pnl_usd)?,
        directional_inventory_baseline_pnl_usd: fmt_decimal(
            summary.directional_inventory_baseline_pnl_usd,
        )?,
        directional_imbalance_excess_pnl_usd: fmt_decimal(
            summary.directional_imbalance_excess_pnl_usd,
        )?,
        directional_exposure_pnl_usd: fmt_decimal(directional_exposure_pnl)?,
        total_pnl_usd: fmt_decimal(total_pnl)?,
        gross_realized_pnl_usd: fmt_decimal(total_pnl)?,
        tracked_costs_usd: "0".to_owned(),
        tracked_revenue_usd: "0".to_owned(),
        net_realized_pnl_usd: fmt_decimal(total_pnl)?,
        realized_pnl_usd: fmt_decimal(summary.realized_pnl_usd)?,
        matched_shares: fmt_decimal(summary.matched_shares)?,
        onchain_notional_usd: fmt_decimal(summary.onchain_notional_usd)?,
        offchain_notional_usd: fmt_decimal(summary.offchain_notional_usd)?,
        inventory_drift_shares: fmt_decimal(inventory_drift_shares)?,
        inventory_drift_usd: fmt_decimal(inventory_drift_usd)?,
        open_long_shares: fmt_decimal(summary.open_long_shares)?,
        open_short_shares: fmt_decimal(summary.open_short_shares)?,
        unmatched_offchain_shares: fmt_decimal(unmatched_offchain_shares)?,
        unmatched_offchain_notional_usd: fmt_decimal(unmatched_offchain_notional)?,
        onchain_fill_count: summary.onchain_fill_count,
        offchain_fill_count: summary.offchain_fill_count,
        matched_lot_count: summary.matched_lot_count,
        open_lot_count: summary.open_lot_count,
        unmatched_offchain_fill_count: summary.unmatched_offchain_fill_count,
    })
}

pub(crate) fn symbol_summary_to_dto(
    symbol: &Symbol,
    summary: &SummaryAcc,
) -> Result<PnlSymbolSummary, PnlError> {
    let dto = summary_to_dto(summary)?;
    Ok(PnlSymbolSummary {
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
    })
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

fn empty_symbol_summary(symbol: &Symbol) -> Result<PnlSymbolSummary, PnlError> {
    symbol_summary_to_dto(symbol, &SummaryAcc::default())
}

fn has_replay_exposure(summary: &SummaryAcc) -> Result<bool, PnlError> {
    let inventory_drift_shares = (summary.open_long_shares - summary.open_short_shares)?;
    let inventory_drift_usd = (summary.open_long_notional_usd - summary.open_short_notional_usd)?;
    let unmatched_offchain_shares =
        (summary.unmatched_offchain_buy_shares + summary.unmatched_offchain_sell_shares)?;

    Ok(!inventory_drift_shares.is_zero()?
        || !inventory_drift_usd.is_zero()?
        || !summary.open_long_shares.is_zero()?
        || !summary.open_short_shares.is_zero()?
        || !unmatched_offchain_shares.is_zero()?
        || summary.unmatched_offchain_fill_count > 0)
}

pub(crate) fn merge_symbol_replay_exposure(
    filtered_symbols: Vec<PnlSymbolSummary>,
    replay_symbols: impl Iterator<Item = (Symbol, SummaryAcc)>,
) -> Result<Vec<PnlSymbolSummary>, PnlError> {
    let mut by_symbol: HashMap<Symbol, PnlSymbolSummary> = filtered_symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();

    for (symbol, replay) in replay_symbols {
        let existing = by_symbol.remove(&symbol);
        if existing.is_some() || has_replay_exposure(&replay)? {
            let base = match existing {
                Some(existing) => existing,
                None => empty_symbol_summary(&symbol)?,
            };
            let replay = symbol_summary_to_dto(&symbol, &replay)?;
            by_symbol.insert(symbol, with_symbol_replay_exposure(base, replay));
        }
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    Ok(rows)
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
    let mut amounts_by_symbol: HashMap<Symbol, (Float, Float)> = HashMap::new();
    for entry in cost_entries {
        let Some(symbol) = &entry.symbol else {
            continue;
        };
        match entry.effect {
            AccountingEffect::Revenue => {
                let amounts = amounts_by_symbol
                    .entry(symbol.clone())
                    .or_insert((float!(0), float!(0)));
                amounts.1 = (amounts.1 + entry.amount_usd.inner())?;
            }
            AccountingEffect::Cost => {
                let amounts = amounts_by_symbol
                    .entry(symbol.clone())
                    .or_insert((float!(0), float!(0)));
                amounts.0 = (amounts.0 + entry.amount_usd.inner())?;
            }
            AccountingEffect::None => {}
        }
    }

    if amounts_by_symbol.is_empty() {
        return Ok(symbols);
    }

    let mut by_symbol: HashMap<Symbol, PnlSymbolSummary> = symbols
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();
    for (symbol, (cost, revenue)) in amounts_by_symbol {
        let existing = match by_symbol.remove(&symbol) {
            Some(existing) => existing,
            None => empty_symbol_summary(&symbol)?,
        };
        let gross = parse_internal_decimal("symbol.totalPnlUsd", &existing.total_pnl_usd)?;
        let net = ((gross - cost)? + revenue)?;
        by_symbol.insert(
            symbol.clone(),
            PnlSymbolSummary {
                gross_realized_pnl_usd: fmt_decimal(gross)?,
                tracked_costs_usd: fmt_decimal(cost)?,
                tracked_revenue_usd: fmt_decimal(revenue)?,
                net_realized_pnl_usd: fmt_decimal(net)?,
                ..existing
            },
        );
    }

    let mut rows: Vec<_> = by_symbol.into_values().collect();
    rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    Ok(rows)
}

pub(crate) fn summary_from_entries(entries: &[PnlEntry]) -> Result<SummaryAndSymbols, PnlError> {
    let mut total = SummaryAcc::default();
    let mut per_symbol: HashMap<Symbol, SummaryAcc> = HashMap::new();

    for entry in entries {
        let summary = per_symbol.entry(entry.symbol.clone()).or_default();
        let shares = entry.shares;
        let opening_notional = (shares * entry.opening_price_usd)?;
        let closing_notional = (shares * entry.closing_price_usd)?;
        let pnl = entry.realized_pnl_usd;

        summary.matched_shares = (summary.matched_shares + shares)?;
        if entry.opening_venue == Venue::Onchain {
            add_venue_notional(summary, Venue::Onchain, opening_notional)?;
        } else if entry.opening_venue == Venue::Offchain {
            add_venue_notional(summary, Venue::Offchain, opening_notional)?;
        }
        if entry.closing_venue == Venue::Onchain {
            add_venue_notional(summary, Venue::Onchain, closing_notional)?;
        } else if entry.closing_venue == Venue::Offchain {
            add_venue_notional(summary, Venue::Offchain, closing_notional)?;
        }
        summary.matched_lot_count += 1;

        add_realized_pnl(summary, entry.pnl_bucket, pnl)?;
    }

    let mut symbols: Vec<_> = per_symbol.into_iter().collect();
    symbols.sort_by(|(left, _), (right, _)| left.cmp(right));
    let symbols = symbols
        .into_iter()
        .map(|(symbol, summary)| {
            add_summary(&mut total, &summary)?;
            symbol_summary_to_dto(&symbol, &summary)
        })
        .collect::<Result<Vec<_>, PnlError>>()?;

    Ok(SummaryAndSymbols {
        summary: summary_to_dto(&total)?,
        symbols,
    })
}
