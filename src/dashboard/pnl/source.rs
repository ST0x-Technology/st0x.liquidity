//! SQLite and broker-backed source loading for backend PnL reports.
use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use rain_math_float::Float;
use sqlx::{QueryBuilder, Sqlite, SqlitePool, Transaction};
use std::collections::{BTreeMap, BTreeSet};

use st0x_execution::alpaca_broker_api::AccountActivity;
use st0x_float_serde::format_float;

use crate::portfolio_snapshot::{EtDayRange, capital_summary, load_portfolio_days};

use super::builder::build_pnl_response_from_rows;
use super::parsing::{fmt_decimal, parse_payload_string};
use super::query::{PnlError, PnlQuery};
use super::response::{PnlCapitalSummary, PnlResponse};
use super::state::{CostEventRow, PositionEventRow, PositionViewRow};
use super::{
    ATTRIBUTION_WARNING, BASELINE_WARNING, CAPITAL_AVAILABLE_NOTE, CAPITAL_UNAVAILABLE_NOTE,
    COST_WARNING, SYMBOL_FILTERED_CAPITAL_WARNING,
};

pub(crate) async fn build_pnl_report(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
) -> Result<PnlResponse, PnlError> {
    let mut warnings = vec![
        ATTRIBUTION_WARNING.to_owned(),
        BASELINE_WARNING.to_owned(),
        COST_WARNING.to_owned(),
    ];
    let symbols = query.symbol_filter(&mut warnings)?;
    let mut tx = pool.begin().await?;
    let resolved_rowid = effective_as_of_rowid(&mut tx, query).await?;
    let effective_query = PnlQuery {
        as_of_rowid: Some(resolved_rowid.resolved),
        ..query.clone()
    };

    let event_rows = load_position_events(&mut tx, &symbols, resolved_rowid.resolved).await?;
    let position_rows = load_position_view(&mut tx).await?;
    let cost_rows = load_cost_events(&mut tx, resolved_rowid.resolved).await?;
    tx.commit().await?;

    let (mut response, daily_net_realized_pnl_usd) = build_pnl_response_from_rows(
        event_rows,
        &position_rows,
        &cost_rows,
        &alpaca_activities,
        &effective_query,
        &symbols,
        warnings,
    )?;

    apply_capital_summary(
        pool,
        query,
        &resolved_rowid,
        &symbols,
        &daily_net_realized_pnl_usd,
        &mut response,
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
    daily_net_realized_pnl_usd: &BTreeMap<String, num_decimal::Num>,
    response: &mut PnlResponse,
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
        latest_capture_day(Utc::now())?,
    )
    .await?;
    let days = load_portfolio_days(pool, et_day_range).await?;
    let daily_net_realized_pnl_usd = daily_net_realized_pnl_usd
        .iter()
        .map(|(day, pnl)| Ok((day.clone(), Float::parse(fmt_decimal(pnl))?)))
        .collect::<Result<BTreeMap<_, _>, PnlError>>()?;
    let capital = capital_summary(&days, &daily_net_realized_pnl_usd)?;

    response.warnings.extend(capital.warnings);
    response.warnings.push(
        if capital.average_deployed_capital_usd.is_some() {
            CAPITAL_AVAILABLE_NOTE
        } else {
            CAPITAL_UNAVAILABLE_NOTE
        }
        .to_owned(),
    );

    response.capital = PnlCapitalSummary {
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
                reason: day.reason,
            })
            .collect(),
    };

    Ok(())
}

pub(crate) fn latest_capture_day(now: DateTime<Utc>) -> Result<NaiveDate, PnlError> {
    let now_et = now.with_timezone(&New_York);
    let day = now_et.date_naive();
    if now_et.time() < NaiveTime::MIN + chrono::Duration::minutes(5) {
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
    daily_net_realized_pnl_usd: &BTreeMap<String, num_decimal::Num>,
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
        let first_pnl = daily_net_realized_pnl_usd
            .keys()
            .map(|day| {
                NaiveDate::parse_from_str(day, "%Y-%m-%d").map_err(|_| PnlError::InvalidDate {
                    field: "dailyPnlDay",
                    value: day.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min();
        range.from = first_snapshot.into_iter().chain(first_pnl).min();
    }
    if range.to.is_none() {
        range.to = Some(report_through);
    }

    Ok(range)
}

pub(crate) async fn validate_pnl_snapshot_rowid(
    pool: &SqlitePool,
    query: &PnlQuery,
) -> Result<(), PnlError> {
    let Some(as_of_rowid) = query.as_of_rowid else {
        return Ok(());
    };

    let mut tx = pool.begin().await?;
    let max_rowid = max_event_rowid(&mut tx).await?;
    tx.commit().await?;

    check_as_of_rowid(as_of_rowid, max_rowid)
}

/// The effective `as_of_rowid` alongside the current max event rowid, so
/// callers can tell whether the resolved value is the live head (needed for
/// the capital as-of-rowid caveat). A named pair rather than a bare
/// `(i64, i64)` prevents the two rowids from being transposed at a call site.
struct ResolvedRowid {
    resolved: i64,
    max: i64,
}

/// Resolves the effective `as_of_rowid`.
async fn effective_as_of_rowid(
    tx: &mut Transaction<'_, Sqlite>,
    query: &PnlQuery,
) -> Result<ResolvedRowid, PnlError> {
    let max_rowid = max_event_rowid(tx).await?;

    if let Some(as_of_rowid) = query.as_of_rowid {
        check_as_of_rowid(as_of_rowid, max_rowid)?;
        return Ok(ResolvedRowid {
            resolved: as_of_rowid,
            max: max_rowid,
        });
    }

    Ok(ResolvedRowid {
        resolved: max_rowid,
        max: max_rowid,
    })
}

fn check_as_of_rowid(as_of_rowid: i64, max_rowid: i64) -> Result<(), PnlError> {
    if as_of_rowid < 0 || as_of_rowid > max_rowid {
        return Err(PnlError::InvalidSnapshotRowid { value: as_of_rowid });
    }

    Ok(())
}

async fn max_event_rowid(tx: &mut Transaction<'_, Sqlite>) -> Result<i64, PnlError> {
    let (max_rowid,) = sqlx::query_as::<_, (Option<i64>,)>("SELECT MAX(rowid) FROM events")
        .fetch_one(&mut **tx)
        .await?;

    Ok(max_rowid.unwrap_or(0))
}

async fn load_position_events(
    tx: &mut Transaction<'_, Sqlite>,
    symbols: &BTreeSet<String>,
    as_of_rowid: i64,
) -> Result<Vec<PositionEventRow>, PnlError> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT rowid, aggregate_id AS symbol, event_type, payload \
         FROM events \
         WHERE aggregate_type = 'Position' \
           AND event_type IN ( \
             'PositionEvent::OnChainOrderFilled', \
             'PositionEvent::OffChainOrderPlaced', \
             'PositionEvent::OffChainOrderFilled', \
             'PositionEvent::ManualPositionAdjusted' \
           ) \
           AND rowid <= ",
    );
    query.push_bind(as_of_rowid);
    if !symbols.is_empty() {
        query.push(" AND aggregate_id IN (");
        let mut separated = query.separated(", ");
        for symbol in symbols {
            separated.push_bind(symbol);
        }
        separated.push_unseparated(")");
    }
    query.push(" ORDER BY rowid ASC");

    let rows = query
        .build_query_as::<(i64, String, String, String)>()
        .fetch_all(&mut **tx)
        .await?;

    let mut events = Vec::with_capacity(rows.len());
    for (rowid, symbol, event_type, payload) in rows {
        let payload =
            parse_payload_string(&payload).map_err(|source| PnlError::InvalidPayload {
                rowid,
                aggregate_type: "Position".to_owned(),
                event_type: event_type.clone(),
                source,
            })?;
        events.push(PositionEventRow {
            rowid,
            symbol,
            event_type,
            payload,
        });
    }

    Ok(events)
}

async fn load_position_view(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<Vec<PositionViewRow>, PnlError> {
    let rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, net_position \
         FROM position_view \
         WHERE symbol IS NOT NULL \
         ORDER BY symbol ASC",
    )
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, net_position)| PositionViewRow {
            symbol,
            net_position,
        })
        .collect())
}

async fn load_cost_events(
    tx: &mut Transaction<'_, Sqlite>,
    as_of_rowid: i64,
) -> Result<Vec<CostEventRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String, String, String, String)>(
        "SELECT rowid, aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE rowid <= ? \
           AND ( \
             ( \
               aggregate_type = 'TokenizedEquityMint' \
               AND event_type IN ( \
                 'TokenizedEquityMintEvent::MintRequested', \
                 'TokenizedEquityMintEvent::TokensReceived', \
                 'TokenizedEquityMintEvent::ProviderCompletionRecovered' \
               ) \
             ) \
             OR ( \
               aggregate_type = 'UsdcRebalance' \
               AND event_type IN ( \
                 'UsdcRebalanceEvent::Bridged', \
                 'UsdcRebalanceEvent::BridgingCompletionRecovered' \
               ) \
             ) \
           ) \
         ORDER BY rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(&mut **tx)
    .await?;

    let mut events = Vec::with_capacity(rows.len());
    for (rowid, aggregate_type, aggregate_id, event_type, payload) in rows {
        let payload =
            parse_payload_string(&payload).map_err(|source| PnlError::InvalidPayload {
                rowid,
                aggregate_type: aggregate_type.clone(),
                event_type: event_type.clone(),
                source,
            })?;
        events.push(CostEventRow {
            rowid,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
        });
    }

    Ok(events)
}
