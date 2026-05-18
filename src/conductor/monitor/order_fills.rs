//! Supervised order fill monitor that subscribes to onchain events
//! and pushes them into apalis storage for processing.
//!
//! On every (re)connect -- both initial startup and supervisor restarts
//! after a WebSocket disconnect -- the monitor:
//! 1. Subscribes to fresh `ClearV3` / `TakeOrderV3` streams.
//! 2. Determines a cutoff block from the provider's latest tip minus
//!    `required_confirmations`.
//! 3. Enqueues a `BackfillRange` apalis job covering
//!    `(checkpoint+1, cutoff)` to recover events emitted while the
//!    previous subscription was unavailable. The cutoff never exceeds
//!    the confirmation boundary, so backfill never returns logs from
//!    blocks that could still reorg (under the assumed reorg depth).
//! 4. Enters the live listen loop, which buffers events in-memory and
//!    only flushes (persists, advances checkpoint) once an event is
//!    `required_confirmations` blocks behind the highest observed tip.
//!    `removed: true` logs are observed explicitly -- dropped from the
//!    buffer if present, or surfaced as a loud error if they target an
//!    already-flushed event (which the confirmation gate should make
//!    impossible under that assumption).
//!
//! Note: `required_confirmations` is a naive reorg-protection heuristic,
//! not real chain finality. A deep reorg exceeding the configured depth
//! will still corrupt state; the design surfaces that case loudly
//! rather than masking it.
//!
//! The supervisor's restart policy provides automatic reconnection;
//! this design keeps the disconnect-window data loss issue from
//! recurring on every restart.

use alloy::providers::Provider;
use alloy::providers::ProviderBuilder;
use alloy::providers::WsConnect;
use alloy::sol_types;
use futures_util::{Stream, StreamExt};
use sqlx::SqlitePool;
use std::collections::BTreeMap;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{debug, error, info, trace, warn};

use st0x_config::EvmCtx;

use crate::bindings::IOrderBookV6::{ClearV3, IOrderBookV6Instance, TakeOrderV3};
use crate::onchain::backfill::{
    BackfillJobQueue, BackfillRange, backfill_start_block, save_backfill_checkpoint,
};
use crate::onchain::trade::RaindexTradeEvent;
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{AccountForDexTrade, DexTradeAccountingJobQueue};

/// Monitors DEX WebSocket streams and pushes
/// [`AccountForDexTrade`] jobs into apalis storage.
///
/// Implements [`SupervisedTask`] so the supervisor restarts it on
/// WebSocket disconnection or errors. On every invocation -- first
/// run and every restart -- the monitor opens a fresh WS subscription,
/// determines a cutoff block, enqueues a [`BackfillRange`] job for
/// the gap since the last checkpoint, and then enters the live
/// listen loop.
#[derive(Clone)]
pub(crate) struct OrderFillMonitor {
    evm_ctx: EvmCtx,
    job_queue: DexTradeAccountingJobQueue,
    backfill_queue: BackfillJobQueue,
    pool: SqlitePool,
}

impl OrderFillMonitor {
    pub(crate) fn new(
        evm_ctx: EvmCtx,
        job_queue: DexTradeAccountingJobQueue,
        backfill_queue: BackfillJobQueue,
        pool: SqlitePool,
    ) -> Self {
        Self {
            evm_ctx,
            job_queue,
            backfill_queue,
            pool,
        }
    }
}

impl SupervisedTask for OrderFillMonitor {
    async fn run(&mut self) -> TaskResult {
        self.connect_and_listen().await
    }
}

#[derive(Debug, thiserror::Error)]
enum OrderFillMonitorError {
    #[error("{0} event stream closed unexpectedly")]
    StreamClosed(&'static str),
    #[error(
        "received `removed: true` for log past the confirmation depth \
         (tx_hash={tx_hash:?}, log_index={log_index}, block={block_number}): \
         a deep reorg crossed `required_confirmations` blocks; review chain stability"
    )]
    RemovedPastConfirmations {
        tx_hash: alloy::primitives::TxHash,
        log_index: u64,
        block_number: u64,
    },
}

impl OrderFillMonitor {
    async fn connect_and_listen(&mut self) -> TaskResult {
        // Production WS URLs (Infura, Alchemy) embed API keys in the
        // path or query, so log only scheme/host/port.
        info!(
            ws_host = %self.evm_ctx.ws_rpc_url.host_str().unwrap_or("<unknown>"),
            ws_scheme = %self.evm_ctx.ws_rpc_url.scheme(),
            "Connecting to DEX WebSocket"
        );

        let ws = WsConnect::new(self.evm_ctx.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let orderbook = IOrderBookV6Instance::new(self.evm_ctx.orderbook, &provider);

        let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        self.prepare_and_listen(&provider, &mut clear_stream, &mut take_stream)
            .await
    }

    /// Enqueues a `BackfillRange` job for the gap between the last
    /// checkpoint and the confirmation boundary, then enters the live
    /// listen loop. Backfill is enqueued before any awaitable work
    /// that could be preempted so that the job is durably persisted
    /// even if the supervisor or runtime aborts the task mid-startup.
    /// Split out so unit tests can drive the post-subscribe path with
    /// mocked streams.
    async fn prepare_and_listen<P, ClearEvents, TakeOrderEvents>(
        &mut self,
        provider: &P,
        clear_stream: &mut ClearEvents,
        take_stream: &mut TakeOrderEvents,
    ) -> TaskResult
    where
        P: Provider + Clone,
        ClearEvents:
            Stream<Item = Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
        TakeOrderEvents:
            Stream<Item = Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    {
        // Cutoff is the latest confirmed block at (re)connect time:
        // events at or before this block are caught by backfill (which
        // is guaranteed not to ingest logs above the confirmation
        // boundary), events after it are caught by the live stream's
        // buffer (which defers persistence until the same confirmation
        // depth). The two ranges may overlap at the boundary;
        // downstream dedupes by (tx_hash, log_index).
        let cutoff_block = latest_confirmed_block(provider, self.evm_ctx.required_confirmations)
            .await?
            .unwrap_or(0);
        let from_block = backfill_start_block(&self.pool, &self.evm_ctx).await?;

        if from_block <= cutoff_block {
            info!(
                from_block,
                cutoff_block,
                required_confirmations = self.evm_ctx.required_confirmations,
                "Enqueuing backfill range for missed events on (re)connect"
            );

            self.backfill_queue
                .push(BackfillRange {
                    from_block,
                    to_block: cutoff_block,
                })
                .await?;
        } else {
            debug!(
                from_block,
                cutoff_block, "Already caught up on (re)connect; skipping backfill enqueue"
            );
        }

        self.listen(clear_stream, take_stream).await
    }
}

/// Latest block past the configured confirmation depth:
/// `latest_tip - required_confirmations`. Returns `None` when the chain has
/// fewer blocks than the requested confirmation depth (no block is
/// safe to ingest yet). This is a naive reorg-protection heuristic,
/// not real chain finality.
pub(crate) async fn latest_confirmed_block<P: Provider>(
    provider: &P,
    required_confirmations: u64,
) -> Result<Option<u64>, alloy::transports::RpcError<alloy::transports::TransportErrorKind>> {
    let tip = provider.get_block_number().await?;
    Ok(tip.checked_sub(required_confirmations))
}

/// Tracks the highest block observed on each WS subscription so the
/// checkpoint only advances past blocks that both streams have
/// crossed *and* that are behind the confirmation boundary. `listen()`
/// merges two unordered subscriptions; advancing solely from the
/// latest event on one stream would risk skipping straggling events
/// on the other stream when a future backfill resumes from
/// `checkpoint+1`.
#[derive(Default)]
struct StreamWatermarks {
    clear_high: Option<u64>,
    take_high: Option<u64>,
}

impl StreamWatermarks {
    fn observe(&mut self, event: &RaindexTradeEvent, block: u64) {
        let slot = match event {
            RaindexTradeEvent::ClearV3(_) => &mut self.clear_high,
            RaindexTradeEvent::TakeOrderV3(_) => &mut self.take_high,
        };
        *slot = Some(slot.map_or(block, |existing| existing.max(block)));
    }

    /// The highest block both streams have provably crossed (returns
    /// `None` until at least one event has been observed on each
    /// stream). Caller intersects this with the confirmation boundary.
    fn observed_on_both(&self) -> Option<u64> {
        self.clear_high
            .zip(self.take_high)
            .map(|(clear, take)| clear.min(take))
    }

    /// The highest block observed on *any* stream so far. Used as the
    /// "tip" estimate for the buffer-flush confirmation check, since live
    /// events implicitly tell us the chain head has at least reached
    /// their block number.
    fn highest_observed(&self) -> Option<u64> {
        match (self.clear_high, self.take_high) {
            (Some(clear), Some(take)) => Some(clear.max(take)),
            (Some(only), None) | (None, Some(only)) => Some(only),
            (None, None) => None,
        }
    }
}

/// In-memory buffer of events emitted by the live WS subscription
/// that have not yet reached `required_confirmations` blocks behind the
/// tip.
///
/// Ordering by `(block_number, log_index)` lets us cheaply drain the
/// prefix of entries whose block is at or below the confirmation
/// boundary on every iteration. The buffer is local to the running
/// monitor task; on supervisor restart it's dropped, but reconnect-time
/// backfill (which also caps at the confirmation boundary) recovers
/// any unflushed events whose blocks have since cleared the
/// confirmation depth.
#[derive(Default)]
struct LiveEventBuffer {
    entries: BTreeMap<(u64, u64), EmittedOnChain<RaindexTradeEvent>>,
}

impl LiveEventBuffer {
    fn insert(&mut self, event: EmittedOnChain<RaindexTradeEvent>) {
        self.entries
            .insert((event.block_number, event.log_index), event);
    }

    /// Remove a buffered entry if present. Returns whether removal
    /// happened, so the caller can distinguish "reverted before
    /// confirmation depth reached" from "reverted after confirmation
    /// depth reached" (the latter is an error our design should
    /// prevent under the assumed reorg depth).
    fn remove(&mut self, block_number: u64, log_index: u64) -> bool {
        self.entries.remove(&(block_number, log_index)).is_some()
    }

    /// Drain all entries with `block_number <= boundary`. Returned
    /// entries are ordered by `(block_number, log_index)`.
    fn drain_through(&mut self, boundary: u64) -> Vec<EmittedOnChain<RaindexTradeEvent>> {
        let keep = self.entries.split_off(&(boundary + 1, 0));
        let drained = std::mem::replace(&mut self.entries, keep);
        drained.into_values().collect()
    }
}

impl OrderFillMonitor {
    async fn listen<ClearEvents, TakeOrderEvents>(
        &mut self,
        clear_stream: &mut ClearEvents,
        take_stream: &mut TakeOrderEvents,
    ) -> TaskResult
    where
        ClearEvents:
            Stream<Item = Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
        TakeOrderEvents:
            Stream<Item = Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    {
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();
        loop {
            let (event, log) = tokio::select! {
                item = clear_stream.next() => {
                    let Some(result) = item else {
                        error!("ClearV3 event stream closed unexpectedly");
                        return Err(OrderFillMonitorError::StreamClosed("ClearV3").into());
                    };
                    let (event, log) = result?;
                    (RaindexTradeEvent::ClearV3(Box::new(event)), log)
                }

                item = take_stream.next() => {
                    let Some(result) = item else {
                        error!("TakeOrderV3 event stream closed unexpectedly");
                        return Err(OrderFillMonitorError::StreamClosed("TakeOrderV3").into());
                    };
                    let (event, log) = result?;
                    (RaindexTradeEvent::TakeOrderV3(Box::new(event)), log)
                }
            };

            self.handle_log(event, &log, &mut watermarks, &mut buffer)
                .await?;
        }
    }

    /// Handles one log delivered by the live WS subscription:
    /// observes the watermark, branches on `log.removed`, buffers
    /// otherwise, and flushes the buffer up to the current
    /// confirmation boundary derived from the highest observed tip.
    async fn handle_log(
        &mut self,
        event: RaindexTradeEvent,
        log: &alloy::rpc::types::Log,
        watermarks: &mut StreamWatermarks,
        buffer: &mut LiveEventBuffer,
    ) -> TaskResult {
        let trade_event = match EmittedOnChain::<RaindexTradeEvent>::from_log(event, log) {
            Ok(trade_event) => trade_event,
            Err(err) => {
                warn!(target: "hedge", %err, "Failed to extract block inclusion metadata from log");
                return Ok(());
            }
        };

        if log.removed {
            return self.handle_removed(&trade_event, buffer);
        }

        watermarks.observe(&trade_event.event, trade_event.block_number);

        trace!(
            target: "hedge",
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            block_number = trade_event.block_number,
            "Buffering trade event pending confirmation depth"
        );

        buffer.insert(trade_event);

        self.flush_buffer(buffer, watermarks).await
    }

    /// Handles a removed log. If the event is still buffered (i.e.,
    /// reverted before reaching the configured confirmation depth),
    /// drop it and log info -- the system never persisted it, so no
    /// follow-up is needed.
    ///
    /// If the event is not in the buffer, it was already flushed and
    /// downstream jobs have run against it. Our confirmation gate
    /// should make this impossible under the assumed reorg depth; emit
    /// a loud error tagged with the identifying metadata so the
    /// deep-reorg case is visible to operators and is surfaced as an
    /// error rather than silently consumed.
    fn handle_removed(
        &self,
        trade_event: &EmittedOnChain<RaindexTradeEvent>,
        buffer: &mut LiveEventBuffer,
    ) -> TaskResult {
        if buffer.remove(trade_event.block_number, trade_event.log_index) {
            info!(
                target: "hedge",
                tx_hash = ?trade_event.tx_hash,
                log_index = trade_event.log_index,
                block_number = trade_event.block_number,
                "Discarded reverted trade event before persistence (confirmation buffer absorbed reorg)"
            );
            return Ok(());
        }

        error!(
            target: "hedge",
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            block_number = trade_event.block_number,
            confirmations = self.evm_ctx.required_confirmations,
            "Received `removed: true` for an already-confirmed trade event -- \
             deep reorg crossed the confirmation depth"
        );
        Err(OrderFillMonitorError::RemovedPastConfirmations {
            tx_hash: trade_event.tx_hash,
            log_index: trade_event.log_index,
            block_number: trade_event.block_number,
        }
        .into())
    }

    /// Flush any buffered events whose blocks are at or below the
    /// confirmation boundary `highest_observed - confirmations`. Each
    /// flushed event is pushed to the trade-accounting queue, and the
    /// backfill checkpoint advances to the highest block that is both
    /// past the confirmation depth *and* has been observed on both
    /// subscriptions.
    async fn flush_buffer(
        &mut self,
        buffer: &mut LiveEventBuffer,
        watermarks: &StreamWatermarks,
    ) -> TaskResult {
        let Some(tip) = watermarks.highest_observed() else {
            trace!(
                target: "hedge",
                "Skipping buffer flush: no highest_observed watermark yet"
            );
            return Ok(());
        };
        let Some(confirmation_boundary) = tip.checked_sub(self.evm_ctx.required_confirmations)
        else {
            trace!(
                target: "hedge",
                tip,
                required_confirmations = self.evm_ctx.required_confirmations,
                "Skipping buffer flush: tip too small for required_confirmations"
            );
            return Ok(());
        };

        let ready = buffer.drain_through(confirmation_boundary);
        if ready.is_empty() {
            return Ok(());
        }

        for trade_event in ready {
            trace!(
                target: "hedge",
                tx_hash = ?trade_event.tx_hash,
                log_index = trade_event.log_index,
                block_number = trade_event.block_number,
                "Flushing confirmed trade event to accounting queue"
            );
            self.job_queue
                .push(AccountForDexTrade { trade: trade_event })
                .await?;
        }

        // Apalis storage now durably owns flushed events. Advance the
        // checkpoint to the highest block that is both past the
        // confirmation depth and crossed by both streams -- the
        // monotonic guard intersected with the confirmation boundary.
        let Some(both_streams) = watermarks.observed_on_both() else {
            trace!(
                target: "hedge",
                clear_high = ?watermarks.clear_high,
                take_high = ?watermarks.take_high,
                confirmation_boundary,
                "Deferring checkpoint advance: only one stream observed so far"
            );
            return Ok(());
        };
        let advance_to = both_streams.min(confirmation_boundary);
        if let Some(checkpoint) = advance_to.checked_sub(1) {
            save_backfill_checkpoint(&self.pool, &self.evm_ctx, checkpoint).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address};
    use futures_util::stream;
    use sqlx::SqlitePool;

    use super::*;
    use crate::bindings::IOrderBookV6::{ClearConfigV2, TakeOrderConfigV4};
    use crate::conductor::setup_apalis_tables;
    use crate::test_utils::{create_log, get_test_order, setup_test_db};

    async fn job_count(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn create_test_monitor_with_pool() -> (OrderFillMonitor, SqlitePool, EvmCtx) {
        create_test_monitor_with_confirmations(0).await
    }

    async fn create_test_monitor_with_confirmations(
        required_confirmations: u64,
    ) -> (OrderFillMonitor, SqlitePool, EvmCtx) {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();
        let job_queue = DexTradeAccountingJobQueue::new(&pool);
        let backfill_queue = BackfillJobQueue::new(&pool);

        let evm_ctx = EvmCtx {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: Address::ZERO,
            deployment_block: 1,
            required_confirmations,
        };

        let monitor =
            OrderFillMonitor::new(evm_ctx.clone(), job_queue, backfill_queue, pool.clone());

        (monitor, pool, evm_ctx)
    }

    fn test_clear_event() -> ClearV3 {
        ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: alloy::primitives::B256::ZERO,
                bobBountyVaultId: alloy::primitives::B256::ZERO,
            },
        }
    }

    fn test_take_event() -> TakeOrderV3 {
        TakeOrderV3 {
            sender: address!("0x2222222222222222222222222222222222222222"),
            config: TakeOrderConfigV4::default(),
            input: alloy::primitives::B256::ZERO,
            output: alloy::primitives::B256::ZERO,
        }
    }

    fn log_at_block(block_number: u64) -> alloy::rpc::types::Log {
        log_at_block_with_index(block_number, 0, false)
    }

    fn log_at_block_with_index(
        block_number: u64,
        log_index: u64,
        removed: bool,
    ) -> alloy::rpc::types::Log {
        alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: alloy::primitives::LogData::empty(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(alloy::primitives::B256::ZERO),
            transaction_index: None,
            log_index: Some(log_index),
            removed,
        }
    }

    #[tokio::test]
    async fn listen_enqueues_clear_event() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;
        let log = create_log(0);

        let mut clear_stream = stream::iter(vec![Ok((test_clear_event(), log))]);
        let mut take_stream =
            stream::pending::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let _result = monitor.listen(&mut clear_stream, &mut take_stream).await;

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn listen_enqueues_take_event() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;
        let log = create_log(1);

        let mut clear_stream =
            stream::pending::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream = stream::iter(vec![Ok((test_take_event(), log))]);

        let _result = monitor.listen(&mut clear_stream, &mut take_stream).await;

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn listen_returns_error_when_both_streams_empty() {
        let (mut monitor, _pool, _evm_ctx) = create_test_monitor_with_pool().await;

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let result = monitor.listen(&mut clear_stream, &mut take_stream).await;

        let error = result
            .unwrap_err()
            .downcast::<OrderFillMonitorError>()
            .expect("expected OrderFillMonitorError");
        assert!(matches!(*error, OrderFillMonitorError::StreamClosed(_)));
    }

    #[tokio::test]
    async fn listen_returns_error_when_clear_stream_closes() {
        let (mut monitor, _pool, _evm_ctx) = create_test_monitor_with_pool().await;

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream = stream::pending();

        let result = monitor.listen(&mut clear_stream, &mut take_stream).await;

        let error = result
            .unwrap_err()
            .downcast::<OrderFillMonitorError>()
            .expect("expected OrderFillMonitorError");
        assert!(matches!(
            *error,
            OrderFillMonitorError::StreamClosed("ClearV3")
        ));
    }

    #[tokio::test]
    async fn listen_returns_error_when_take_stream_closes() {
        let (mut monitor, _pool, _evm_ctx) = create_test_monitor_with_pool().await;

        let mut clear_stream = stream::pending();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let result = monitor.listen(&mut clear_stream, &mut take_stream).await;

        let error = result
            .unwrap_err()
            .downcast::<OrderFillMonitorError>()
            .expect("expected OrderFillMonitorError");
        assert!(matches!(
            *error,
            OrderFillMonitorError::StreamClosed("TakeOrderV3")
        ));
    }

    #[tokio::test]
    async fn handle_log_persists_to_storage() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;
        let log = create_log(42);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(event, &log, &mut watermarks, &mut buffer)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn handle_log_survives_invalid_log() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;

        let invalid_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: alloy::primitives::LogData::empty(),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };

        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(event, &invalid_log, &mut watermarks, &mut buffer)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0, "Invalid log should not persist");
    }

    #[tokio::test]
    async fn listen_processes_events_from_both_streams() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;

        let clear_log = create_log(0);
        let take_log = create_log(1);

        let mut clear_stream =
            stream::iter(vec![Ok((test_clear_event(), clear_log))]).chain(stream::pending());
        let mut take_stream =
            stream::iter(vec![Ok((test_take_event(), take_log))]).chain(stream::pending());

        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            monitor.listen(&mut clear_stream, &mut take_stream),
        )
        .await;

        assert_eq!(job_count(&pool).await, 2);
    }

    #[tokio::test]
    async fn handle_log_does_not_advance_until_both_streams_observed() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let log = log_at_block(100);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(event, &log, &mut watermarks, &mut buffer)
            .await
            .unwrap();

        let checkpoint = crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
            .await
            .unwrap();

        assert_eq!(
            checkpoint, None,
            "checkpoint must not advance until both streams have crossed a block"
        );
    }

    #[tokio::test]
    async fn handle_log_advances_to_min_of_both_streams() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        let clear_log = log_at_block(200);
        let take_log = log_at_block(150);

        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &clear_log,
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        // Only clear stream observed: no advance.
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            None
        );

        monitor
            .handle_log(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &take_log,
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        // Both observed (clear=200, take=150). With required_confirmations=0,
        // confirmation_boundary = 200, both_streams = 150,
        // advance_to = 150, checkpoint = 149.
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(149),
            "checkpoint must advance to min(both_streams, confirmation_boundary) - 1"
        );
    }

    #[tokio::test]
    async fn handle_log_checkpoint_is_monotonic() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        // Establish initial frontier at block 200 on both streams.
        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block(200),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();
        monitor
            .handle_log(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &log_at_block(200),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        // An out-of-order earlier event must not rewind the checkpoint.
        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block(50),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        let checkpoint = crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
            .await
            .unwrap();

        assert_eq!(
            checkpoint,
            Some(199),
            "checkpoint must never rewind when an out-of-order event is observed"
        );
    }

    #[tokio::test]
    async fn prepare_and_listen_caps_cutoff_at_confirmation_boundary() {
        // Backfill cutoff must be `latest - required_confirmations`,
        // not the raw tip. With required_confirmations=3 and
        // latest=105, cutoff=102.
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_confirmations(3).await;

        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&serde_json::Value::from(105_u64));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let _ = monitor
            .prepare_and_listen(&provider, &mut clear_stream, &mut take_stream)
            .await;

        let job_count = job_count(&pool).await;
        assert_eq!(
            job_count, 1,
            "exactly one job (the BackfillRange) must be enqueued on reconnect"
        );

        let job_payload = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT job FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let job: BackfillRange = serde_json::from_slice(&job_payload).unwrap();
        assert_eq!(job.from_block, 100, "backfill must resume after checkpoint");
        assert_eq!(
            job.to_block, 102,
            "backfill cutoff must equal latest tip minus required_confirmations"
        );
    }

    #[tokio::test]
    async fn prepare_and_listen_enqueues_backfill_for_disconnect_window() {
        // RAI-198 repro: when the monitor (re)connects, it must
        // enqueue a BackfillRange covering (checkpoint+1, cutoff)
        // so events emitted during the WS disconnect window are
        // recovered. Pre-fix: no BackfillRange is enqueued and
        // those events are silently dropped.
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;

        // Simulate prior progress: live monitor processed events up
        // through block 99 before the disconnect.
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        // Mock provider returns block 105 from `eth_blockNumber`,
        // which (with required_confirmations=0) the monitor uses verbatim as
        // the cutoff.
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&serde_json::Value::from(105_u64));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        // prepare_and_listen returns Err(StreamsClosed) once it falls
        // through to the listen loop with empty streams; we don't
        // care about that -- we only care that the backfill job was
        // enqueued before the listen loop starts.
        let _ = monitor
            .prepare_and_listen(&provider, &mut clear_stream, &mut take_stream)
            .await;

        let job_count = job_count(&pool).await;
        assert_eq!(
            job_count, 1,
            "exactly one job (the BackfillRange) must be enqueued on reconnect"
        );

        let job_payload = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT job FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let job: BackfillRange = serde_json::from_slice(&job_payload).unwrap();
        assert_eq!(job.from_block, 100, "backfill must resume after checkpoint");
        assert_eq!(job.to_block, 105, "backfill must extend up to the cutoff");
    }

    #[tokio::test]
    async fn handle_log_does_not_advance_on_invalid_log() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;

        let invalid_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: alloy::primitives::LogData::empty(),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };

        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(event, &invalid_log, &mut watermarks, &mut buffer)
            .await
            .unwrap();

        let checkpoint = crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
            .await
            .unwrap();

        assert_eq!(
            checkpoint, None,
            "checkpoint must not advance when the log lacks block metadata"
        );
    }

    #[tokio::test]
    async fn handle_log_buffers_event_before_confirmations() {
        // With required_confirmations=3, an event at block N must not be
        // flushed until the tip advances to N+3. The first event at
        // block 100 is the only signal we have; tip = 100,
        // confirmation_boundary = 97. The event sits in the buffer.
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_confirmations(3).await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        let log = log_at_block(100);
        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log,
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        assert_eq!(
            job_count(&pool).await,
            0,
            "event at block 100 must not be flushed until tip reaches 103"
        );
        assert_eq!(
            buffer.entries.len(),
            1,
            "event must remain buffered pending confirmation depth"
        );
    }

    #[tokio::test]
    async fn handle_log_flushes_when_confirmation_boundary_advances() {
        // First event at block 100 (buffered). Second event at block
        // 103 advances the tip to 103, so confirmation_boundary
        // becomes 100. The block-100 event flushes.
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_confirmations(3).await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block(100),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();
        monitor
            .handle_log(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &log_at_block(103),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        assert_eq!(
            job_count(&pool).await,
            1,
            "block-100 event must flush once confirmation_boundary=100 is reached"
        );
        assert_eq!(
            buffer.entries.len(),
            1,
            "block-103 event must remain buffered (boundary is 100)"
        );
    }

    #[tokio::test]
    async fn handle_log_removed_pre_confirmation_drops_from_buffer() {
        // A `removed: true` log for a still-buffered event must drop
        // it cleanly without erroring -- the confirmation buffer
        // absorbed the reorg.
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_confirmations(3).await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block_with_index(100, 0, false),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();
        assert_eq!(buffer.entries.len(), 1, "event must be buffered");

        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block_with_index(100, 0, true),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();

        assert_eq!(
            buffer.entries.len(),
            0,
            "removed log must drop the buffered entry"
        );
        assert_eq!(
            job_count(&pool).await,
            0,
            "removed event must never be flushed to the job queue"
        );
    }

    #[tokio::test]
    async fn handle_log_removed_does_not_advance_watermark() {
        // A removed log represents reverted chain state, so its block
        // number must not contribute to the tip estimate. Otherwise an
        // out-of-order `removed: true` could prematurely advance the
        // confirmation boundary and flush still-unconfirmed entries.
        let (mut monitor, _pool, _evm_ctx) = create_test_monitor_with_confirmations(3).await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        // Seed the buffer with an event at block 100 so handle_removed
        // takes the buffered-drop branch instead of erroring.
        monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block_with_index(100, 0, false),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap();
        assert_eq!(watermarks.highest_observed(), Some(100));

        // A removed log at a far-future block must not advance the
        // watermark -- the reverted chain doesn't prove forward
        // progress.
        monitor
            .handle_log(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &log_at_block_with_index(1000, 0, true),
                &mut watermarks,
                &mut buffer,
            )
            .await
            .unwrap_err();

        assert_eq!(
            watermarks.highest_observed(),
            Some(100),
            "removed log must not advance the watermark"
        );
    }

    #[tokio::test]
    async fn handle_log_removed_past_confirmations_errors() {
        // If a `removed: true` log arrives for an event that is no
        // longer in the buffer (i.e., already flushed), the
        // confirmation gate failed -- a deep reorg crossed our depth.
        // Must surface as a loud error rather than be silently
        // consumed.
        let (mut monitor, _pool, _evm_ctx) = create_test_monitor_with_confirmations(3).await;
        let mut watermarks = StreamWatermarks::default();
        let mut buffer = LiveEventBuffer::default();

        // No prior insert -- simulate that the event was already
        // flushed and the buffer no longer holds it.
        let result = monitor
            .handle_log(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block_with_index(100, 7, true),
                &mut watermarks,
                &mut buffer,
            )
            .await;

        let error = result
            .unwrap_err()
            .downcast::<OrderFillMonitorError>()
            .expect("expected OrderFillMonitorError");
        assert!(
            matches!(
                *error,
                OrderFillMonitorError::RemovedPastConfirmations {
                    log_index: 7,
                    block_number: 100,
                    ..
                }
            ),
            "expected RemovedPastConfirmations variant, got {error:?}"
        );
    }

    #[tokio::test]
    async fn latest_confirmed_block_subtracts_confirmations() {
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&serde_json::Value::from(105_u64));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let confirmed = latest_confirmed_block(&provider, 3).await.unwrap();
        assert_eq!(confirmed, Some(102));
    }

    #[tokio::test]
    async fn latest_confirmed_block_returns_none_when_chain_too_shallow() {
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&serde_json::Value::from(2_u64));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let confirmed = latest_confirmed_block(&provider, 3).await.unwrap();
        assert_eq!(confirmed, None);
    }
}
