use std::collections::BTreeSet;

use sqlx::{QueryBuilder, Sqlite, SqlitePool};
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
    let symbols = query.symbol_filter(&mut warnings)?;

    let (event_rows, position_rows, cost_rows) = tokio::try_join!(
        load_position_events(pool, &symbols),
        load_position_view(pool),
        load_cost_events(pool),
    )?;

    build_pnl_response_from_rows(
        event_rows,
        &position_rows,
        &cost_rows,
        &alpaca_activities,
        query,
        &symbols,
        warnings,
    )
}

async fn load_position_events(
    pool: &SqlitePool,
    symbols: &BTreeSet<String>,
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
           )",
    );
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
        .fetch_all(pool)
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

async fn load_cost_events(pool: &SqlitePool) -> Result<Vec<CostEventRow>, PnlError> {
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
