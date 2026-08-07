//! Ledger and broker-backed source loading for backend PnL reports.
//!
//! Replay inputs come from the typed, append-only `pnl_*` ledger tables
//! maintained by [`super::ledger::PnlLedger`] (ADR 0018) -- never from the
//! `events` table. Freshness is guaranteed by running the ledger's
//! `catch_up()` before resolving the `asOfRowid` watermark, outside the
//! replay admission permit.
use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use rain_math_float::Float;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task;

use st0x_execution::alpaca_broker_api::AccountActivity;
use st0x_float_serde::format_float;

use crate::portfolio_snapshot::{
    CAPTURE_BUFFER, EtDayRange, capital_summary, evaluate_portfolio_days, load_portfolio_day_rows,
};

use super::builder::build_pnl_response_from_rows;
use super::ledger::{
    CCTP_FEE_SOURCE, DIRECTION_BUY_TEXT, DIRECTION_SELL_TEXT, LedgerHead, TOKENIZATION_FEE_SOURCE,
};
use super::query::{PnlError, PnlQuery};
use super::response::{PnlCapitalSummary, PnlResponse};
use super::state::{
    BotGasCostRow, CostLedgerRow, CostSource, Direction, ManualAdjustmentRow, OffchainFillRow,
    OffchainPlacementRow, OnchainFillRow, PositionLedgerRow, PositionViewRow,
};
use super::{
    ATTRIBUTION_WARNING, BASELINE_WARNING, CAPITAL_AVAILABLE_NOTE, CAPITAL_UNAVAILABLE_NOTE,
    COST_WARNING, SYMBOL_FILTERED_CAPITAL_WARNING,
};

pub(crate) const MAX_CONCURRENT_PNL_REPORTS: usize = 2;

#[derive(Clone)]
pub(crate) struct PnlReportAdmission(Arc<Semaphore>);

impl PnlReportAdmission {
    fn new() -> Self {
        Self(Arc::new(Semaphore::new(MAX_CONCURRENT_PNL_REPORTS)))
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        self.0.clone().try_acquire_owned()
    }
}

pub(crate) fn pnl_report_admission() -> PnlReportAdmission {
    PnlReportAdmission::new()
}

pub(crate) fn acquire_pnl_report_permit(
    admission: &PnlReportAdmission,
) -> Result<OwnedSemaphorePermit, PnlError> {
    Ok(admission.try_acquire()?)
}

pub(super) async fn run_pnl_replay_with_permit<T, F>(
    permit: OwnedSemaphorePermit,
    replay: F,
) -> Result<(T, OwnedSemaphorePermit), PnlError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, PnlError> + Send + 'static,
{
    let (result, permit) = task::spawn_blocking(move || (replay(), permit)).await?;

    Ok((result?, permit))
}

#[cfg(test)]
pub(super) async fn run_pnl_replay<T, F>(replay: F) -> Result<T, PnlError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, PnlError> + Send + 'static,
{
    let admission = pnl_report_admission();
    let permit = acquire_pnl_report_permit(&admission)?;
    run_pnl_replay_with_permit(permit, replay)
        .await
        .map(|(result, _permit)| result)
}

#[cfg(test)]
pub(crate) async fn build_pnl_report(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
    now: DateTime<Utc>,
) -> Result<PnlResponse, PnlError> {
    let ledger = super::ledger::PnlLedger::new(pool.clone());
    let head = ledger.catch_up().await?;
    let admission = pnl_report_admission();
    let permit = acquire_pnl_report_permit(&admission)?;
    build_pnl_report_with_permit(pool, query, alpaca_activities, now, permit, head).await
}

/// `head` is the event-log head returned by the ledger's `catch_up()`, which
/// the caller MUST have run before acquiring the replay permit: freshness is
/// async I/O and must not burn a blocking-replay slot, and the resolved
/// `asOfRowid` watermark is only meaningful once the ledger contains
/// everything at or below it.
pub(crate) async fn build_pnl_report_with_permit(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
    now: DateTime<Utc>,
    permit: OwnedSemaphorePermit,
    head: LedgerHead,
) -> Result<PnlResponse, PnlError> {
    let mut warnings = vec![
        ATTRIBUTION_WARNING.to_owned(),
        BASELINE_WARNING.to_owned(),
        COST_WARNING.to_owned(),
    ];
    let symbols = query.symbol_filter(&mut warnings)?;
    let resolved_rowid = resolve_as_of_rowid(query, head)?;
    let effective_query = PnlQuery {
        as_of_rowid: Some(resolved_rowid.resolved),
        ..query.clone()
    };

    let event_rows = load_position_rows(pool, &symbols, resolved_rowid.resolved).await?;
    let position_rows = load_position_view(pool).await?;
    let cost_rows = load_cost_rows(pool, resolved_rowid.resolved).await?;
    let bot_gas_rows = load_bot_gas_rows(pool, resolved_rowid.resolved).await?;

    let replay_symbols = symbols.clone();
    let ((mut response, daily_net_realized_pnl_usd), permit) =
        run_pnl_replay_with_permit(permit, move || {
            build_pnl_response_from_rows(
                event_rows,
                &position_rows,
                &cost_rows,
                &bot_gas_rows,
                &alpaca_activities,
                &effective_query,
                &replay_symbols,
                warnings,
            )
        })
        .await?;

    apply_capital_summary(
        pool,
        query,
        &resolved_rowid,
        &symbols,
        &daily_net_realized_pnl_usd,
        &mut response,
        now,
        permit,
    )
    .await?;

    Ok(response)
}

/// Populates `response.capital` and its accompanying warnings. Symbol-filtered
/// queries omit capital entirely because a symbol-scoped slice of
/// whole-portfolio capital is not a meaningful denominator. `symbols` is the
/// same parsed filter set the PnL body itself was scoped by
/// (`query.symbol_filter`), not the raw `query.symbol` string, so an
/// empty/whitespace-only `symbol=` param (which `symbol_filter` treats as no
/// filter at all) does not suppress capital while the PnL stays
/// whole-portfolio. Capital is never watermarked to `as_of_rowid` -- it always
/// reflects the live `portfolio_snapshot` table, so a non-current
/// `as_of_rowid` gets an explicit caveat rather than a different figure.
async fn apply_capital_summary(
    pool: &SqlitePool,
    query: &PnlQuery,
    resolved_rowid: &ResolvedRowid,
    symbols: &BTreeSet<String>,
    daily_net_realized_pnl_usd: &BTreeMap<NaiveDate, Float>,
    response: &mut PnlResponse,
    now: DateTime<Utc>,
    permit: OwnedSemaphorePermit,
) -> Result<(), PnlError> {
    if resolved_rowid.resolved != resolved_rowid.max {
        response.warnings.push(format!(
            "Capital and return-on-capital figures reflect the current portfolio snapshot \
             table, not a historical view as of rowid {}: daily snapshots are not watermarked \
             to event rowids.",
            resolved_rowid.resolved
        ));
        // A past as_of_rowid asks for a historical view the snapshot table
        // cannot provide. Leave response.capital at its default (both fields
        // None) rather than silently substituting the live snapshot's current
        // capital for a requested historical one.
        return Ok(());
    }

    if !symbols.is_empty() {
        response
            .warnings
            .push(SYMBOL_FILTERED_CAPITAL_WARNING.to_owned());
        response.warnings.push(CAPITAL_UNAVAILABLE_NOTE.to_owned());
        return Ok(());
    }

    let et_day_range = complete_capital_range(
        pool,
        query.et_day_range()?,
        daily_net_realized_pnl_usd,
        latest_capture_day(now)?,
    )
    .await?;
    let day_rows = load_portfolio_day_rows(pool, et_day_range).await?;
    let daily_net_realized_pnl_usd = daily_net_realized_pnl_usd.clone();
    let (((capital_summary, capital_warnings), capital_note), _permit) =
        run_pnl_replay_with_permit(permit, move || {
            let days = evaluate_portfolio_days(day_rows)?;
            let capital = capital_summary(&days, &daily_net_realized_pnl_usd)?;
            let capital_note = if capital.average_deployed_capital_usd.is_some() {
                CAPITAL_AVAILABLE_NOTE
            } else {
                CAPITAL_UNAVAILABLE_NOTE
            };
            let capital_response = PnlCapitalSummary {
                average_deployed_capital_usd: capital
                    .average_deployed_capital_usd
                    .as_ref()
                    .map(format_float)
                    .transpose()?,
                annualized_return_pct: capital
                    .annualized_return_pct
                    .as_ref()
                    .map(format_float)
                    .transpose()?,
                coverage_days: capital.coverage_days,
                sample_days: capital.sample_days,
                first_snapshot_day: capital.first_snapshot_day.map(|day| day.to_string()),
                last_snapshot_day: capital.last_snapshot_day.map(|day| day.to_string()),
                excluded_days: capital
                    .excluded_days
                    .into_iter()
                    .map(|day| super::response::PnlCapitalExcludedDay {
                        et_day: day.et_day.to_string(),
                        kind: day.reason.kind(),
                        reason: day.reason.describe(),
                    })
                    .collect(),
            };

            Ok(((capital_response, capital.warnings), capital_note))
        })
        .await?;

    response.warnings.extend(capital_warnings);
    response.warnings.push(capital_note.to_owned());
    response.capital = capital_summary;

    Ok(())
}

pub(crate) fn latest_capture_day(now: DateTime<Utc>) -> Result<NaiveDate, PnlError> {
    let now_et = now.with_timezone(&New_York);
    let day = now_et.date_naive();
    if now_et.time() < NaiveTime::MIN + CAPTURE_BUFFER {
        day.checked_sub_days(Days::new(1))
            .ok_or_else(|| PnlError::InvalidDate {
                field: "reportThrough",
                value: day.to_string(),
            })
    } else {
        Ok(day)
    }
}

async fn complete_capital_range(
    pool: &SqlitePool,
    mut range: EtDayRange,
    daily_net_realized_pnl_usd: &BTreeMap<NaiveDate, Float>,
    report_through: NaiveDate,
) -> Result<EtDayRange, PnlError> {
    if range.from.is_none() {
        let first_snapshot: Option<String> =
            sqlx::query_scalar("SELECT MIN(et_day) FROM portfolio_snapshot")
                .fetch_one(pool)
                .await?;
        let first_snapshot = first_snapshot
            .map(|day| {
                NaiveDate::parse_from_str(&day, "%Y-%m-%d").map_err(|_| PnlError::InvalidDate {
                    field: "portfolioSnapshotDay",
                    value: day,
                })
            })
            .transpose()?;
        let first_pnl = daily_net_realized_pnl_usd.keys().next().copied();
        range.from = first_snapshot.into_iter().chain(first_pnl).min();
    }
    if range.to.is_none() {
        range.to = Some(report_through);
    }

    Ok(range)
}

/// Validates a user-supplied `asOfRowid` against the ledger head returned by
/// `catch_up()`.
pub(crate) fn validate_pnl_snapshot_rowid(
    LedgerHead(head): LedgerHead,
    query: &PnlQuery,
) -> Result<(), PnlError> {
    query
        .as_of_rowid
        .map_or(Ok(()), |as_of_rowid| check_as_of_rowid(as_of_rowid, head))
}

/// The effective `as_of_rowid` alongside the current head, so callers can
/// tell whether the resolved value is the live head (needed for the capital
/// as-of-rowid caveat). A named pair rather than a bare `(i64, i64)`
/// prevents the two rowids from being transposed at a call site.
struct ResolvedRowid {
    resolved: i64,
    max: i64,
}

/// Resolves the effective `as_of_rowid` against the caught-up ledger head.
fn resolve_as_of_rowid(
    query: &PnlQuery,
    LedgerHead(head): LedgerHead,
) -> Result<ResolvedRowid, PnlError> {
    if let Some(as_of_rowid) = query.as_of_rowid {
        check_as_of_rowid(as_of_rowid, head)?;
        return Ok(ResolvedRowid {
            resolved: as_of_rowid,
            max: head,
        });
    }

    Ok(ResolvedRowid {
        resolved: head,
        max: head,
    })
}

fn check_as_of_rowid(as_of_rowid: i64, max_rowid: i64) -> Result<(), PnlError> {
    if as_of_rowid < 0 || as_of_rowid > max_rowid {
        return Err(PnlError::InvalidSnapshotRowid { value: as_of_rowid });
    }

    Ok(())
}

fn push_symbol_filter(query: &mut QueryBuilder<Sqlite>, symbols: &BTreeSet<String>) {
    if symbols.is_empty() {
        return;
    }

    query.push(" AND symbol IN (");
    let mut separated = query.separated(", ");
    for symbol in symbols {
        separated.push_bind(symbol.clone());
    }
    separated.push_unseparated(")");
}

fn ledger_direction(
    table: &'static str,
    rowid: i64,
    direction: &str,
) -> Result<Direction, PnlError> {
    match direction {
        DIRECTION_BUY_TEXT => Ok(Direction::Buy),
        DIRECTION_SELL_TEXT => Ok(Direction::Sell),
        _ => Err(PnlError::InvalidLedgerRow {
            table,
            rowid,
            reason: "unknown direction",
        }),
    }
}

/// Loads all four position row kinds from the ledger at or below the
/// watermark, merged in global rowid order -- the same stream shape the raw
/// events query produced, already typed.
async fn load_position_rows(
    pool: &SqlitePool,
    symbols: &BTreeSet<String>,
    as_of_rowid: i64,
) -> Result<Vec<PositionLedgerRow>, PnlError> {
    let mut rows = Vec::new();

    let mut onchain = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, tx_hash, log_index, shares, direction, price_usd, \
         executed_at FROM pnl_onchain_fill WHERE event_rowid <= ",
    );
    onchain.push_bind(as_of_rowid);
    push_symbol_filter(&mut onchain, symbols);
    for (event_rowid, symbol, tx_hash, log_index, shares, direction, price_usd, executed_at) in
        onchain
            .build_query_as::<(i64, String, String, i64, String, String, String, String)>()
            .fetch_all(pool)
            .await?
    {
        rows.push(PositionLedgerRow::OnchainFill(OnchainFillRow {
            event_rowid,
            symbol,
            tx_hash,
            log_index,
            shares,
            direction: ledger_direction("pnl_onchain_fill", event_rowid, &direction)?,
            price_usd,
            executed_at,
        }));
    }

    let mut offchain = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, offchain_order_id, shares, direction, price_usd, \
         executed_at FROM pnl_offchain_fill WHERE event_rowid <= ",
    );
    offchain.push_bind(as_of_rowid);
    push_symbol_filter(&mut offchain, symbols);
    for (event_rowid, symbol, offchain_order_id, shares, direction, price_usd, executed_at) in
        offchain
            .build_query_as::<(i64, String, String, String, String, String, String)>()
            .fetch_all(pool)
            .await?
    {
        rows.push(PositionLedgerRow::OffchainFill(OffchainFillRow {
            event_rowid,
            symbol,
            offchain_order_id,
            shares,
            direction: ledger_direction("pnl_offchain_fill", event_rowid, &direction)?,
            price_usd,
            executed_at,
        }));
    }

    let mut placements = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, offchain_order_id, placed_at \
         FROM pnl_offchain_placement WHERE event_rowid <= ",
    );
    placements.push_bind(as_of_rowid);
    push_symbol_filter(&mut placements, symbols);
    for (event_rowid, symbol, offchain_order_id, placed_at) in placements
        .build_query_as::<(i64, String, String, String)>()
        .fetch_all(pool)
        .await?
    {
        rows.push(PositionLedgerRow::OffchainPlacement(OffchainPlacementRow {
            event_rowid,
            symbol,
            offchain_order_id,
            placed_at,
        }));
    }

    let mut adjustments = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, target_net, price_usd, adjusted_at \
         FROM pnl_manual_adjustment WHERE event_rowid <= ",
    );
    adjustments.push_bind(as_of_rowid);
    push_symbol_filter(&mut adjustments, symbols);
    for (event_rowid, symbol, target_net, price_usd, adjusted_at) in adjustments
        .build_query_as::<(i64, String, String, Option<String>, String)>()
        .fetch_all(pool)
        .await?
    {
        rows.push(PositionLedgerRow::ManualAdjustment(ManualAdjustmentRow {
            event_rowid,
            symbol,
            target_net,
            price_usd,
            adjusted_at,
        }));
    }

    rows.sort_by_key(PositionLedgerRow::event_rowid);
    Ok(rows)
}

async fn load_position_view(pool: &SqlitePool) -> Result<Vec<PositionViewRow>, PnlError> {
    let rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, net_position \
         FROM position_view \
         WHERE symbol IS NOT NULL \
         ORDER BY symbol ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, net_position)| PositionViewRow {
            symbol,
            net_position,
        })
        .collect())
}

async fn load_cost_rows(
    pool: &SqlitePool,
    as_of_rowid: i64,
) -> Result<Vec<CostLedgerRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String, String, Option<String>, Option<String>, String)>(
        "SELECT event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at \
         FROM pnl_cost_entry \
         WHERE event_rowid <= ? \
         ORDER BY event_rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(
            |(event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at)| {
                let source = match source.as_str() {
                    TOKENIZATION_FEE_SOURCE => CostSource::TokenizationFee,
                    CCTP_FEE_SOURCE => CostSource::CctpFee,
                    _ => {
                        return Err(PnlError::InvalidLedgerRow {
                            table: "pnl_cost_entry",
                            rowid: event_rowid,
                            reason: "unknown cost source",
                        });
                    }
                };

                Ok(CostLedgerRow {
                    event_rowid,
                    source,
                    aggregate_id,
                    symbol,
                    amount_usd,
                    occurred_at,
                })
            },
        )
        .collect()
}

async fn load_bot_gas_rows(
    pool: &SqlitePool,
    as_of_rowid: i64,
) -> Result<Vec<BotGasCostRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String, String, String, String, Option<String>, String)>(
        "SELECT event_rowid, chain, tx_hash, usd_cost, operation_category, symbol, occurred_at \
         FROM pnl_bot_gas_cost \
         WHERE event_rowid <= ? \
         ORDER BY event_rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(rowid, chain, tx_hash, usd_cost, operation_category, symbol, occurred_at)| {
                BotGasCostRow {
                    rowid,
                    chain,
                    tx_hash,
                    usd_cost,
                    operation_category,
                    symbol,
                    occurred_at,
                }
            },
        )
        .collect())
}
