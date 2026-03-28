//! Order taker bot for tokenized equities.
//!
//! Monitors Raindex orders placed by other users and takes profitable
//! opportunities by acquiring tokens via Alpaca's tokenization API
//! and executing `takeOrders4` on the orderbook contract.

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::{B256, Bytes};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use futures_util::StreamExt;
use sqlx::SqlitePool;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use st0x_event_sorcery::{Projection, StoreBuilder};
use st0x_shared::bindings::{
    IOrderBookV6::{AddOrderV3, IOrderBookV6Instance, RemoveOrderV3, TakeOrderV3},
    MetaV1_2,
};

use crate::order_collector::{BlockCursor, ClassificationOutcome, EventProcessor};
use crate::tracked_order::{OrderFilter, TrackedOrder};

#[allow(dead_code)]
mod approval;
mod classification;
pub mod config;
mod order_collector;
mod tracked_order;

#[cfg(test)]
mod integration_tests;

pub use config::{Ctx, Env, LogLevel};

/// Initializes tracing with the given log level and sensible defaults.
pub fn setup_tracing(log_level: &LogLevel) {
    let level: tracing::Level = log_level.into();

    let filter = EnvFilter::new(format!(
        "st0x_taker={level},st0x_shared={level},st0x_execution={level}"
    ));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .ok();
}

/// Main entry point for the taker bot.
#[allow(clippy::cognitive_complexity)]
pub async fn launch(ctx: Ctx) -> anyhow::Result<()> {
    let pool = ctx.get_sqlite_pool().await?;
    sqlx::migrate!().run(&pool).await?;
    info!("taker bot started, database migrated");

    let (store, projection) = StoreBuilder::<TrackedOrder>::new(pool.clone())
        .build(())
        .await?;

    let filter = build_order_filter(&ctx);
    let bot_address = ctx.evm.wallet_address()?;
    let processor = EventProcessor::new(store, filter, bot_address);

    let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
    let provider = ProviderBuilder::new().connect_ws(ws).await?;
    let orderbook = IOrderBookV6Instance::new(ctx.evm.orderbook, &provider);

    let mut add_stream = orderbook.AddOrderV3_filter().watch().await?.into_stream();
    let mut remove_stream = orderbook
        .RemoveOrderV3_filter()
        .watch()
        .await?
        .into_stream();
    let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

    // MetaV1_2 subscription via raw filter (not part of IOrderBookV6 interface)
    let meta_filter = Filter::new()
        .address(ctx.evm.orderbook)
        .event_signature(MetaV1_2::SIGNATURE_HASH);
    let meta_sub = provider.subscribe_logs(&meta_filter).await?;
    let mut meta_stream = meta_sub.into_stream();

    info!("WebSocket subscriptions established (including MetaV1_2)");

    run_backfill(&provider, &processor, &pool, &ctx).await?;

    info!(
        "Entering live event loop. Active orders: {}",
        count_active_orders(&projection).await
    );

    // Metadata cache: maps order hash (subject) -> meta bytes.
    // MetaV1_2 is emitted in the same transaction as AddOrderV3.
    // In the live stream, MetaV1_2 may arrive before or after AddOrderV3,
    // so we cache metadata and look it up when processing AddOrderV3.
    let mut meta_cache: HashMap<B256, Bytes> = HashMap::new();

    let cursor = BlockCursor::new(&pool);

    run_event_loop(
        &processor,
        &cursor,
        &mut add_stream,
        &mut remove_stream,
        &mut take_stream,
        &mut meta_stream,
        &mut meta_cache,
    )
    .await?;

    Ok(())
}

/// Runs the live event loop, dispatching events and updating the cursor.
///
/// TODO: The current design subscribes to WS events before backfill
/// (see `launch`), which creates a window where events mined between
/// subscription start and the backfill snapshot appear in both the
/// backfill and the live stream. `Discover` is idempotent so
/// duplicates are safe, but once real fill accounting replaces the
/// `U256::MAX` placeholder, `RecordFill` duplicates will corrupt
/// state. Fix by snapshotting the block first, backfilling to it,
/// then subscribing from snapshot+1.
#[allow(clippy::cognitive_complexity)]
async fn run_event_loop(
    processor: &EventProcessor,
    cursor: &BlockCursor<'_>,
    add_stream: &mut (impl StreamExt<Item = Result<(AddOrderV3, Log), alloy::sol_types::Error>> + Unpin),
    remove_stream: &mut (
             impl StreamExt<Item = Result<(RemoveOrderV3, Log), alloy::sol_types::Error>> + Unpin
         ),
    take_stream: &mut (
             impl StreamExt<Item = Result<(TakeOrderV3, Log), alloy::sol_types::Error>> + Unpin
         ),
    meta_stream: &mut (impl StreamExt<Item = Log> + Unpin),
    meta_cache: &mut HashMap<B256, Bytes>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            Some(log) = meta_stream.next() => {
                if let Ok(decoded) = log.log_decode::<MetaV1_2>() {
                    let event = decoded.data();

                    match processor
                        .classify_order(event.subject, &event.meta)
                        .await
                    {
                        Ok(
                            ClassificationOutcome::Classified
                            | ClassificationOutcome::AlreadyClassified,
                        ) => {}

                        Ok(ClassificationOutcome::OrderNotFound) => {
                            // AddOrderV3 hasn't arrived yet — cache for later use.
                            meta_cache.insert(event.subject, event.meta.clone());
                            debug!(
                                subject = %event.subject,
                                meta_len = event.meta.len(),
                                "Cached MetaV1_2 metadata (order not yet discovered)"
                            );
                        }

                        Err(error) => {
                            warn!(
                                subject = %event.subject,
                                "Late classification failed: {error}"
                            );
                        }
                    }
                }
            }

            Some(result) = add_stream.next() => match result {
                Ok((event, log)) => {
                    let meta = meta_cache.remove(&event.orderHash);
                    if processor
                        .process_add_order(&event, &log, meta.as_ref())
                        .await
                        .is_ok()
                    {
                        advance_cursor(cursor, &log).await;
                    } else {
                        error!("Failed to process AddOrderV3, cursor not advanced");
                    }
                }
                Err(error) => error!("Error decoding AddOrderV3: {error}"),
            },

            Some(result) = remove_stream.next() => match result {
                Ok((event, log)) => {
                    if processor.process_remove_order(&event).await.is_ok() {
                        advance_cursor(cursor, &log).await;
                    } else {
                        error!("Failed to process RemoveOrderV3, cursor not advanced");
                    }
                }
                Err(error) => error!("Error decoding RemoveOrderV3: {error}"),
            },

            Some(result) = take_stream.next() => match result {
                Ok((event, log)) => {
                    if processor.process_take_order(&event).await.is_ok() {
                        advance_cursor(cursor, &log).await;
                    } else {
                        error!("Failed to process TakeOrderV3, cursor not advanced");
                    }
                }
                Err(error) => error!("Error decoding TakeOrderV3: {error}"),
            },

            else => {
                error!("All WebSocket event streams closed");
                return Err(anyhow::anyhow!("All WebSocket event streams closed"));
            }
        }
    }
}

/// Advances the block cursor from a log's block number.
async fn advance_cursor(cursor: &BlockCursor<'_>, log: &Log) {
    if let Some(block) = log.block_number
        && let Err(error) = cursor.update(block).await
    {
        warn!("Failed to update block cursor: {error}");
    }
}

/// Performs historical backfill if needed.
#[allow(clippy::cognitive_complexity)]
async fn run_backfill<P: Provider + Clone>(
    provider: &P,
    processor: &EventProcessor,
    pool: &SqlitePool,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let cursor = BlockCursor::new(pool);
    let current_block = provider.get_block_number().await?;
    let start_block = cursor
        .last_block()
        .await?
        .map_or(ctx.evm.deployment_block, |last| last + 1);

    if start_block > current_block {
        info!("Already caught up to block {current_block}, skipping backfill");
        return Ok(());
    }

    info!(
        "Backfilling events from block {} to {}",
        start_block, current_block
    );

    let total_blocks = current_block - start_block + 1;
    let mut processed_events = 0u64;
    let mut batch_start = start_block;

    while batch_start <= current_block {
        let batch_end = (batch_start + BACKFILL_BATCH_SIZE - 1).min(current_block);

        let (add_logs, remove_logs, take_logs, meta_logs) =
            fetch_batch_logs(provider, ctx.evm.orderbook, batch_start, batch_end).await?;

        processed_events +=
            process_backfill_logs(processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
                .await?;

        cursor.update(batch_end).await?;

        debug!(
            "Backfill batch {}-{} complete ({} events)",
            batch_start,
            batch_end,
            add_logs.len() + remove_logs.len() + take_logs.len()
        );

        batch_start = batch_end + 1;
    }

    info!(
        "Backfill complete: {} events processed across {} blocks",
        processed_events, total_blocks
    );

    Ok(())
}

/// Batch size for historical backfill log queries.
const BACKFILL_BATCH_SIZE: u64 = 2000;

/// Fetches logs for all event types in a block range.
pub(crate) async fn fetch_batch_logs<P: Provider + Clone>(
    provider: &P,
    orderbook: alloy::primitives::Address,
    batch_start: u64,
    batch_end: u64,
) -> Result<(Vec<Log>, Vec<Log>, Vec<Log>, Vec<Log>), alloy::transports::TransportError> {
    let add_filter = Filter::new()
        .address(orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(AddOrderV3::SIGNATURE_HASH);

    let remove_filter = Filter::new()
        .address(orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(RemoveOrderV3::SIGNATURE_HASH);

    let take_filter = Filter::new()
        .address(orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(TakeOrderV3::SIGNATURE_HASH);

    let meta_filter = Filter::new()
        .address(orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(MetaV1_2::SIGNATURE_HASH);

    let (add_logs, remove_logs, take_logs, meta_logs) = tokio::try_join!(
        provider.get_logs(&add_filter),
        provider.get_logs(&remove_filter),
        provider.get_logs(&take_filter),
        provider.get_logs(&meta_filter),
    )?;

    Ok((add_logs, remove_logs, take_logs, meta_logs))
}

/// Ordering key for sorting logs by their position in the chain.
fn log_sort_key(log: &Log) -> (u64, u64, u64) {
    (
        log.block_number.unwrap_or(0),
        log.transaction_index.unwrap_or(0),
        log.log_index.unwrap_or(0),
    )
}

/// Processes backfill logs through the event processor.
///
/// Merges all three event-type log vectors and sorts by chain
/// position `(block_number, transaction_index, log_index)` to
/// preserve the canonical ordering. Without this, an add+take+remove
/// in the same batch could replay as add+remove+take, causing the
/// fill to hit a terminal aggregate and be lost.
///
/// MetaV1_2 logs are processed first to build the metadata cache,
/// then event logs are processed with their corresponding metadata.
#[allow(clippy::cognitive_complexity)]
pub(crate) async fn process_backfill_logs(
    processor: &EventProcessor,
    add_logs: &[Log],
    remove_logs: &[Log],
    take_logs: &[Log],
    meta_logs: &[Log],
) -> anyhow::Result<u64> {
    let mut all_logs: Vec<&Log> =
        Vec::with_capacity(add_logs.len() + remove_logs.len() + take_logs.len());
    all_logs.extend(add_logs);
    all_logs.extend(remove_logs);
    all_logs.extend(take_logs);
    all_logs.sort_by_key(|log| log_sort_key(log));

    let mut count = 0u64;

    // Build metadata cache from MetaV1_2 logs first
    let mut meta_cache: HashMap<B256, Bytes> = HashMap::new();

    for log in meta_logs {
        if let Ok(decoded) = log.log_decode::<MetaV1_2>() {
            let event = decoded.data();
            meta_cache.insert(event.subject, event.meta.clone());
        }
    }

    for log in all_logs {
        // Try each event type in turn — exactly one decode will succeed
        // per log since each was fetched by its specific event signature.
        if let Ok(decoded) = log.log_decode::<AddOrderV3>() {
            let event = decoded.data();
            let meta = meta_cache.get(&event.orderHash);

            processor.process_add_order(event, log, meta).await?;
            count += 1;
        } else if let Ok(decoded) = log.log_decode::<RemoveOrderV3>() {
            processor.process_remove_order(decoded.data()).await?;
            count += 1;
        } else if let Ok(decoded) = log.log_decode::<TakeOrderV3>() {
            processor.process_take_order(decoded.data()).await?;
            count += 1;
        }
    }

    Ok(count)
}

/// Builds the order filter from configuration.
fn build_order_filter(ctx: &Ctx) -> OrderFilter {
    let equity_tokens = ctx
        .equities
        .iter()
        .map(|(symbol, addresses)| (symbol.clone(), addresses.wrapped));

    OrderFilter::new(ctx.evm.excluded_owner, ctx.evm.usdc_address, equity_tokens)
}

/// Counts active orders in the projection for startup logging.
async fn count_active_orders(projection: &Arc<Projection<TrackedOrder>>) -> usize {
    match projection.load_all().await {
        Ok(orders) => orders
            .iter()
            .filter(|(_, order)| matches!(order, TrackedOrder::Active { .. }))
            .count(),
        Err(error) => {
            warn!("Failed to count active orders: {error}");
            0
        }
    }
}
