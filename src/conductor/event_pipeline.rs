//! Event queue draining and live blockchain event receiving.

use std::time::Duration;

use alloy::rpc::types::Log;
use alloy::sol_types;
use apalis_sql::sqlite::SqliteStorage;
use futures_util::{Stream, StreamExt};
use sqlx::SqlitePool;
use tracing::{error, info, trace};

use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::onchain::EvmCtx;
use crate::onchain::event_processor::{ProcessOnchainEventCtx, ProcessOnchainEventJob};
use crate::onchain::trade::TradeEvent;

/// Creates a `SqliteStorage` for `ProcessOnchainEventJob`, using the
/// `ProcessOnchainEventCtx` to bind the provider type parameter `P`.
pub(super) fn create_event_storage<P, E>(
    pool: &SqlitePool,
    _ctx: &ProcessOnchainEventCtx<P, E>,
) -> SqliteStorage<ProcessOnchainEventJob<P, E>>
where
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    SqliteStorage::new(pool.clone())
}

/// Drains unprocessed events from the legacy `event_queue` table into
/// apalis SqliteStorage so they are processed by the Monitor worker.
pub(super) async fn drain_event_queue_to_storage<P, E>(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
    storage: &mut SqliteStorage<ProcessOnchainEventJob<P, E>>,
) where
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    log_unprocessed_count(pool).await;

    while drain_next_event(pool, evm_ctx, storage).await {}

    info!("Event queue drain complete");
}

async fn log_unprocessed_count(pool: &SqlitePool) {
    match crate::queue::count_unprocessed(pool).await {
        Ok(0) => info!("No unprocessed events in event_queue"),
        Ok(n) => info!("Draining {n} unprocessed events from event_queue to apalis storage"),
        Err(e) => error!("Failed to count unprocessed events: {e}"),
    }
}

/// Drains a single event from event_queue to apalis storage.
/// Returns `true` to continue draining, `false` to stop.
async fn drain_next_event<P, E>(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
    storage: &mut SqliteStorage<ProcessOnchainEventJob<P, E>>,
) -> bool
where
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    match try_drain_next_event(pool, evm_ctx, storage).await {
        Ok(has_more) => has_more,
        Err(e) => {
            error!("Error draining event: {e}");
            false
        }
    }
}

async fn try_drain_next_event<P, E>(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
    storage: &mut SqliteStorage<ProcessOnchainEventJob<P, E>>,
) -> anyhow::Result<bool>
where
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    let Some(queued_event) = crate::queue::get_next_unprocessed_event(pool).await? else {
        return Ok(false);
    };

    let log = reconstruct_log_from_queued_event(evm_ctx, &queued_event);
    let job = ProcessOnchainEventJob::new(queued_event.event, log);
    apalis::prelude::Storage::push(storage, job).await?;
    mark_queued_event_processed(pool, queued_event.id).await?;

    Ok(true)
}

/// Marks a queued event as processed within a transaction.
async fn mark_queued_event_processed(
    pool: &SqlitePool,
    event_id: Option<i64>,
) -> anyhow::Result<()> {
    let Some(id) = event_id else {
        return Ok(());
    };

    let mut tx = pool.begin().await?;
    crate::queue::mark_event_processed(&mut tx, id).await?;
    tx.commit().await?;
    Ok(())
}

/// Pushes any buffered live events (received during backfill) to storage.
pub(super) async fn enqueue_live_events_to_storage<S1, S2, P, E>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    storage: &mut SqliteStorage<ProcessOnchainEventJob<P, E>>,
) where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    // Try to read any events that may have been buffered by the stream.
    // Use a zero-duration timeout to avoid blocking.
    loop {
        let event_result = tokio::select! {
            biased;
            Some(result) = clear_stream.next() => {
                Some(result.map(|(event, log)| (TradeEvent::ClearV3(Box::new(event)), log)))
            }
            Some(result) = take_stream.next() => {
                Some(result.map(|(event, log)| (TradeEvent::TakeOrderV3(Box::new(event)), log)))
            }
            () = tokio::time::sleep(Duration::from_millis(0)) => None,
        };

        let Some(Ok((event, log))) = event_result else {
            break;
        };

        let job = ProcessOnchainEventJob::new(event, log);
        if let Err(e) = apalis::prelude::Storage::push(storage, job).await {
            error!("Failed to push buffered event to storage: {e}");
        }
    }
}

/// Receives live blockchain events from WS streams and pushes them
/// directly to apalis SqliteStorage for processing by the Monitor.
pub(super) async fn receive_blockchain_events_to_storage<S1, S2, P, E>(
    mut clear_stream: S1,
    mut take_stream: S2,
    mut storage: SqliteStorage<ProcessOnchainEventJob<P, E>>,
) where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    loop {
        let event_result = tokio::select! {
            Some(result) = clear_stream.next() => {
                result.map(|(event, log)| (TradeEvent::ClearV3(Box::new(event)), log))
            }
            Some(result) = take_stream.next() => {
                result.map(|(event, log)| (TradeEvent::TakeOrderV3(Box::new(event)), log))
            }
            else => {
                error!(
                    "All event streams ended, \
                     shutting down event receiver"
                );
                break;
            }
        };

        enqueue_event_result(event_result, &mut storage).await;
    }
}

async fn enqueue_event_result<P, E>(
    result: Result<(TradeEvent, Log), sol_types::Error>,
    storage: &mut SqliteStorage<ProcessOnchainEventJob<P, E>>,
) where
    P: Send + Sync + Unpin + 'static,
    E: Send + Sync + Unpin + 'static,
{
    let (event, log) = match result {
        Ok(pair) => pair,
        Err(e) => {
            error!("Error in event stream: {e}");
            return;
        }
    };

    trace!(
        "Received blockchain event: tx_hash={:?}, \
         log_index={:?}, block_number={:?}",
        log.transaction_hash, log.log_index, log.block_number
    );

    let job = ProcessOnchainEventJob::new(event, log);
    if let Err(e) = apalis::prelude::Storage::push(storage, job).await {
        error!("Failed to push live event to storage: {e}");
    }
}

pub(super) fn reconstruct_log_from_queued_event(
    evm_ctx: &EvmCtx,
    queued_event: &crate::queue::QueuedEvent,
) -> Log {
    use alloy::primitives::IntoLogData;

    let log_data = match &queued_event.event {
        TradeEvent::ClearV3(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TradeEvent::TakeOrderV3(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = queued_event
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: evm_ctx.orderbook,
            data: log_data,
        },
        block_hash: None,
        block_number: Some(queued_event.block_number),
        block_timestamp,
        transaction_hash: Some(queued_event.tx_hash),
        transaction_index: None,
        log_index: Some(queued_event.log_index),
        removed: false,
    }
}
