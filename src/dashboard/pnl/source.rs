//! SQLite and broker-backed source loading for backend PnL reports.
use sqlx::{QueryBuilder, Sqlite, SqlitePool, Transaction};
use std::collections::BTreeSet;

use st0x_execution::alpaca_broker_api::AccountActivity;

use crate::bot_gas::BotGasReceiptCostEvent;

use super::builder::build_pnl_response_from_rows;
use super::parsing::parse_payload_string;
use super::query::{PnlError, PnlQuery};
use super::response::PnlResponse;
use super::state::{BotGasCostRow, CostEventRow, PositionEventRow, PositionViewRow};
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
    let mut tx = pool.begin().await?;
    let as_of_rowid = effective_as_of_rowid(&mut tx, query).await?;
    let effective_query = PnlQuery {
        as_of_rowid: Some(as_of_rowid),
        ..query.clone()
    };

    let event_rows = load_position_events(&mut tx, &symbols, as_of_rowid).await?;
    let position_rows = load_position_view(&mut tx).await?;
    let cost_rows = load_cost_events(&mut tx, as_of_rowid).await?;
    let bot_gas_rows = load_bot_gas_costs(&mut tx, as_of_rowid).await?;
    tx.commit().await?;

    build_pnl_response_from_rows(
        event_rows,
        &position_rows,
        &cost_rows,
        &bot_gas_rows,
        &alpaca_activities,
        &effective_query,
        &symbols,
        warnings,
    )
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

async fn effective_as_of_rowid(
    tx: &mut Transaction<'_, Sqlite>,
    query: &PnlQuery,
) -> Result<i64, PnlError> {
    if let Some(as_of_rowid) = query.as_of_rowid {
        let max_rowid = max_event_rowid(tx).await?;
        check_as_of_rowid(as_of_rowid, max_rowid)?;
        return Ok(as_of_rowid);
    }

    max_event_rowid(tx).await
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

async fn load_bot_gas_costs(
    tx: &mut Transaction<'_, Sqlite>,
    as_of_rowid: i64,
) -> Result<Vec<BotGasCostRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String)>(
        "SELECT rowid, payload \
         FROM events \
         WHERE aggregate_type = 'BotGasReceiptCost' \
           AND event_type = 'BotGasReceiptCostEvent::Recorded' \
           AND rowid <= ? \
         ORDER BY rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(&mut **tx)
    .await?;

    let mut costs = Vec::with_capacity(rows.len());
    for (rowid, payload) in rows {
        let event = parse_payload_string(&payload)
            .and_then(serde_json::from_value::<BotGasReceiptCostEvent>)
            .map_err(|source| PnlError::InvalidPayload {
                rowid,
                aggregate_type: "BotGasReceiptCost".to_owned(),
                event_type: "BotGasReceiptCostEvent::Recorded".to_owned(),
                source,
            })?;
        let BotGasReceiptCostEvent::Recorded { cost } = event;
        cost.validate()
            .map_err(|source| PnlError::InvalidBotGasReceiptCost { rowid, source })?;
        costs.push(BotGasCostRow {
            rowid,
            chain: cost.chain.to_string(),
            tx_hash: cost.tx_hash.to_string(),
            usd_cost: cost.usd_cost.to_string(),
            operation_category: cost.operation_category.to_string(),
            symbol: cost.symbol.map(|symbol| symbol.to_string()),
            occurred_at: cost.occurred_at.to_rfc3339(),
        });
    }

    Ok(costs)
}
