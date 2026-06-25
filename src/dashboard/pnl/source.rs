use std::collections::BTreeSet;

use sqlx::SqlitePool;
use st0x_execution::AccountActivity;

use super::builder::build_pnl_response_from_rows;
use super::parsing::parse_payload_string;
use super::query::{PnlError, PnlQuery};
use super::response::PnlResponse;
use super::state::{CostEventRow, PositionEventRow, PositionViewRow};
use super::{ATTRIBUTION_WARNING, BASELINE_WARNING, COST_WARNING};

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
    let symbols = query.symbol_filter(&mut warnings);

    let (event_rows, position_rows, cost_rows) = tokio::try_join!(
        load_position_events(pool, &symbols),
        load_position_view(pool),
        load_cost_events(pool),
    )?;

    Ok(build_pnl_response_from_rows(
        event_rows,
        position_rows,
        cost_rows,
        alpaca_activities,
        query,
        symbols,
        warnings,
    ))
}

async fn load_position_events(
    pool: &SqlitePool,
    symbols: &BTreeSet<String>,
) -> Result<Vec<PositionEventRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, String, String, String)>(
        "SELECT rowid, aggregate_id AS symbol, event_type, payload \
         FROM events \
         WHERE aggregate_type = 'Position' \
           AND event_type IN ( \
             'PositionEvent::OnChainOrderFilled', \
             'PositionEvent::OffChainOrderPlaced', \
             'PositionEvent::OffChainOrderFilled' \
           ) \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter(|(_, symbol, _, _)| symbols.is_empty() || symbols.contains(symbol))
        .map(|(rowid, symbol, event_type, payload)| PositionEventRow {
            rowid,
            symbol,
            event_type,
            payload: parse_payload_string(&payload),
        })
        .collect())
}

async fn load_position_view(pool: &SqlitePool) -> Result<Vec<PositionViewRow>, sqlx::Error> {
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

async fn load_cost_events(pool: &SqlitePool) -> Result<Vec<CostEventRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, String, String, String, String)>(
        "SELECT rowid, aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE ( \
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
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(rowid, aggregate_type, aggregate_id, event_type, payload)| CostEventRow {
                rowid,
                aggregate_type,
                aggregate_id,
                event_type,
                payload: parse_payload_string(&payload),
            },
        )
        .collect())
}
