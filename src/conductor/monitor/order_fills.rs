//! Supervised order fill monitor that subscribes to onchain events
//! and pushes them into apalis storage for processing.
//!
//! On every (re)connect -- both initial startup and supervisor restarts
//! after a WebSocket disconnect -- the monitor:
//! 1. Subscribes to fresh `ClearV3` / `TakeOrderV3` streams.
//! 2. Determines a cutoff block from the provider's current head.
//! 3. Enqueues a `BackfillRange` apalis job covering
//!    `(checkpoint+1, cutoff)` to recover events emitted while the
//!    previous subscription was unavailable.
//! 4. Enters the live listen loop, advancing the backfill checkpoint
//!    after every event is durably enqueued.
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
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{debug, error, info, trace, warn};

use crate::bindings::IOrderBookV6::{ClearV3, IOrderBookV6Instance, TakeOrderV3};
use crate::onchain::EvmCtx;
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

    /// Enqueues a `BackfillRange` job for the gap since the last
    /// checkpoint, then enters the live listen loop. Backfill is
    /// enqueued before any awaitable work that could be preempted so
    /// that the job is durably persisted even if the supervisor or
    /// runtime aborts the task mid-startup. Split out so unit tests
    /// can drive the post-subscribe path with mocked streams.
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
        // Cutoff is the provider's current head at the moment of
        // (re)connect: events at or before this block are caught by
        // backfill, events after it are caught by the live stream.
        // The two ranges may overlap at the boundary; OnChainTrade
        // deduplicates by (tx_hash, log_index).
        let cutoff_block = provider.get_block_number().await?;
        let from_block = backfill_start_block(&self.pool, &self.evm_ctx).await?;

        if from_block <= cutoff_block {
            info!(
                from_block,
                cutoff_block, "Enqueuing backfill range for missed events on (re)connect"
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

/// Tracks the highest block observed on each WS subscription so the
/// reconnect-time backfill checkpoint only advances past blocks that
/// both streams have crossed. `listen()` merges two unordered
/// subscriptions; advancing solely from the latest event would risk
/// skipping straggling events on the other stream when a future
/// backfill resumes from `checkpoint+1`.
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

    /// The highest block both streams have provably crossed. Returns
    /// `None` until at least one event has been observed on each stream.
    fn safe_advance_to(&self) -> Option<u64> {
        self.clear_high
            .zip(self.take_high)
            .and_then(|(clear, take)| clear.min(take).checked_sub(1))
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

            self.enqueue_trade_event(event, &log, &mut watermarks)
                .await?;
        }
    }

    async fn enqueue_trade_event(
        &mut self,
        event: RaindexTradeEvent,
        log: &alloy::rpc::types::Log,
        watermarks: &mut StreamWatermarks,
    ) -> TaskResult {
        let trade_event = match EmittedOnChain::<RaindexTradeEvent>::from_log(event, log) {
            Ok(trade_event) => trade_event,
            Err(err) => {
                warn!(target: "hedge", %err, "Failed to extract block inclusion metadata from log");
                return Ok(());
            }
        };

        let block_number = trade_event.block_number;

        trace!(
            target: "hedge",
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            "Enqueuing trade accounting job"
        );

        watermarks.observe(&trade_event.event, block_number);

        self.job_queue
            .push(AccountForDexTrade { trade: trade_event })
            .await?;

        // Apalis storage now durably owns this event. Only advance the
        // checkpoint past blocks that both streams have crossed --
        // otherwise a straggling event on the slower stream could be
        // skipped when a future backfill resumes from `checkpoint+1`.
        // The checkpoint is monotonic via `MAX(...)` upsert, so calling
        // `save_backfill_checkpoint` with a stale value is harmless.
        if let Some(advance_to) = watermarks.safe_advance_to() {
            save_backfill_checkpoint(&self.pool, &self.evm_ctx, advance_to).await?;
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
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();
        let job_queue = DexTradeAccountingJobQueue::new(&pool);
        let backfill_queue = BackfillJobQueue::new(&pool);

        let evm_ctx = EvmCtx {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: Address::ZERO,
            deployment_block: 1,
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
    async fn enqueue_trade_event_persists_to_storage() {
        let (mut monitor, pool, _evm_ctx) = create_test_monitor_with_pool().await;
        let log = create_log(42);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();

        monitor
            .enqueue_trade_event(event, &log, &mut watermarks)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn enqueue_trade_event_survives_invalid_log() {
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

        monitor
            .enqueue_trade_event(event, &invalid_log, &mut watermarks)
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

    fn log_at_block(block_number: u64) -> alloy::rpc::types::Log {
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
            log_index: Some(0),
            removed: false,
        }
    }

    #[tokio::test]
    async fn enqueue_trade_event_does_not_advance_until_both_streams_observed() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let log = log_at_block(100);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let mut watermarks = StreamWatermarks::default();

        monitor
            .enqueue_trade_event(event, &log, &mut watermarks)
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
    async fn enqueue_trade_event_advances_to_min_of_both_streams() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let mut watermarks = StreamWatermarks::default();

        let clear_log = log_at_block(200);
        let take_log = log_at_block(150);

        monitor
            .enqueue_trade_event(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &clear_log,
                &mut watermarks,
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
            .enqueue_trade_event(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &take_log,
                &mut watermarks,
            )
            .await
            .unwrap();

        // Both observed: advance to min(200, 150) - 1 = 149.
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(149),
            "checkpoint must advance to min(clear_high, take_high) - 1"
        );
    }

    #[tokio::test]
    async fn enqueue_trade_event_checkpoint_is_monotonic() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        let mut watermarks = StreamWatermarks::default();

        // Establish initial frontier at block 200 on both streams.
        monitor
            .enqueue_trade_event(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block(200),
                &mut watermarks,
            )
            .await
            .unwrap();
        monitor
            .enqueue_trade_event(
                RaindexTradeEvent::TakeOrderV3(Box::new(test_take_event())),
                &log_at_block(200),
                &mut watermarks,
            )
            .await
            .unwrap();

        // An out-of-order earlier event must not rewind the checkpoint.
        monitor
            .enqueue_trade_event(
                RaindexTradeEvent::ClearV3(Box::new(test_clear_event())),
                &log_at_block(50),
                &mut watermarks,
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
        // which the monitor will use as the cutoff.
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
    async fn enqueue_trade_event_does_not_advance_on_invalid_log() {
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

        monitor
            .enqueue_trade_event(event, &invalid_log, &mut watermarks)
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
}
