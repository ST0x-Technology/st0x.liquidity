//! Startup synchronization: cutoff block detection, event buffering,
//! and rebalancing setup.

use std::sync::Arc;
use std::time::Duration;

use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types;
use futures_util::{Stream, StreamExt};
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info};

use st0x_dto::ServerMessage;

use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::config::Ctx;
use crate::inventory::InventoryView;
use crate::onchain::trade::TradeEvent;
use crate::rebalancing::{
    RebalancingCqrsFrameworks, RebalancingCtx, RebalancingTrigger, RebalancingTriggerConfig,
    build_rebalancing_queries, spawn_rebalancer,
};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

pub(crate) async fn get_cutoff_block<S1, S2, P>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    provider: &P,
    pool: &SqlitePool,
) -> anyhow::Result<u64>
where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
    P: Provider + Clone,
{
    info!("Starting WebSocket subscriptions and waiting for first event...");

    let first_event_result =
        wait_for_first_event_with_timeout(clear_stream, take_stream, Duration::from_secs(5)).await;

    let Some((mut event_buffer, block_number)) = first_event_result else {
        let current_block = provider.get_block_number().await?;
        info!(
            "No subscription events within timeout, \
             using current block {current_block} as cutoff"
        );
        return Ok(current_block);
    };

    buffer_live_events(clear_stream, take_stream, &mut event_buffer, block_number).await;

    crate::queue::enqueue_buffer(pool, event_buffer).await;

    Ok(block_number)
}

pub(super) async fn wait_for_first_event_with_timeout<S1, S2>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    timeout: Duration,
) -> Option<(Vec<(TradeEvent, Log)>, u64)>
where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
{
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    let mut events = Vec::new();

    loop {
        tokio::select! {
            Some(result) = clear_stream.next() => {
                if let Some(first_event) = handle_startup_clear_event(result, &mut events) {
                    return Some(first_event);
                }
            }
            Some(result) = take_stream.next() => {
                if let Some(first_event) = handle_startup_take_event(result, &mut events) {
                    return Some(first_event);
                }
            }
            () = &mut deadline => return None,
        }
    }
}

fn handle_startup_clear_event(
    result: Result<(ClearV3, Log), sol_types::Error>,
    events: &mut Vec<(TradeEvent, Log)>,
) -> Option<(Vec<(TradeEvent, Log)>, u64)> {
    match result {
        Ok((event, log)) => {
            let Some(block_number) = log.block_number else {
                error!("ClearV3 event missing block number");
                return None;
            };
            events.push((TradeEvent::ClearV3(Box::new(event)), log));
            Some((std::mem::take(events), block_number))
        }
        Err(e) => {
            error!("Error in clear event stream during startup: {e}");
            None
        }
    }
}

fn handle_startup_take_event(
    result: Result<(TakeOrderV3, Log), sol_types::Error>,
    events: &mut Vec<(TradeEvent, Log)>,
) -> Option<(Vec<(TradeEvent, Log)>, u64)> {
    match result {
        Ok((event, log)) => {
            let Some(block_number) = log.block_number else {
                error!("TakeOrderV3 event missing block number");
                return None;
            };
            events.push((TradeEvent::TakeOrderV3(Box::new(event)), log));
            Some((std::mem::take(events), block_number))
        }
        Err(e) => {
            error!("Error in take event stream during startup: {e}");
            None
        }
    }
}

pub(super) async fn buffer_live_events<S1, S2>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    event_buffer: &mut Vec<(TradeEvent, Log)>,
    cutoff_block: u64,
) where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin,
{
    loop {
        tokio::select! {
            Some(result) = clear_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::ClearV3(Box::new(event)), log));
                }
                Err(e) => error!("Error in clear event stream during backfill: {e}"),
                _ => {}
            },
            Some(result) = take_stream.next() => match result {
                Ok((event, log)) if log.block_number.unwrap_or(0) >= cutoff_block => {
                    event_buffer.push((TradeEvent::TakeOrderV3(Box::new(event)), log));
                }
                Err(e) => error!("Error in take event stream during backfill: {e}"),
                _ => {}
            },
            else => break,
        }
    }
}

pub(super) async fn spawn_rebalancing_infrastructure<P: Provider + Clone + Send + 'static>(
    rebalancing_ctx: &RebalancingCtx,
    pool: &SqlitePool,
    ctx: &Ctx,
    inventory: &Arc<RwLock<InventoryView>>,
    event_sender: broadcast::Sender<ServerMessage>,
    provider: &P,
) -> anyhow::Result<JoinHandle<()>> {
    info!("Initializing rebalancing infrastructure");

    let signer = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key)?;
    let market_maker_wallet = signer.address();

    const OPERATION_CHANNEL_CAPACITY: usize = 100;
    let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

    let trigger = Arc::new(RebalancingTrigger::new(
        RebalancingTriggerConfig {
            equity_threshold: rebalancing_ctx.equity_threshold,
            usdc_threshold: rebalancing_ctx.usdc_threshold,
        },
        pool.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        inventory.clone(),
        operation_sender,
    ));

    let event_broadcast = Some(event_sender);

    let frameworks = RebalancingCqrsFrameworks {
        mint: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<TokenizedEquityMint>(
                trigger.clone(),
                event_broadcast.clone(),
            ),
            (),
        )),
        redemption: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<crate::equity_redemption::EquityRedemption>(
                trigger.clone(),
                event_broadcast.clone(),
            ),
            (),
        )),
        usdc: Arc::new(sqlite_cqrs(
            pool.clone(),
            build_rebalancing_queries::<UsdcRebalance>(trigger, event_broadcast),
            (),
        )),
    };

    Ok(spawn_rebalancer(
        rebalancing_ctx,
        provider.clone(),
        ctx.evm.orderbook,
        market_maker_wallet,
        operation_receiver,
        frameworks,
    )
    .await?)
}
