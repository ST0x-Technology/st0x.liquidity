//! Supervised order fill monitor that subscribes to onchain events
//! and pushes them into apalis storage for processing.
//!
//! On every (re)connect — both initial startup and supervisor restarts
//! after a WebSocket disconnect — the monitor:
//! 1. Subscribes to fresh `ClearV3` / `TakeOrderV3` streams.
//! 2. Determines a cutoff block via `get_cutoff_block`.
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
use tracing::{error, info, trace, warn};

use crate::bindings::IOrderBookV6::{ClearV3, IOrderBookV6Instance, TakeOrderV3};
use crate::conductor::monitor::cutoff::get_cutoff_block;
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
/// WebSocket disconnection or errors. On every invocation — first
/// run and every restart — the monitor opens a fresh WS subscription,
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
        info!(ws_url = %self.evm_ctx.ws_rpc_url, "Connecting to DEX WebSocket");

        let ws = WsConnect::new(self.evm_ctx.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let orderbook = IOrderBookV6Instance::new(self.evm_ctx.orderbook, &provider);

        let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        self.prepare_and_listen(&provider, &mut clear_stream, &mut take_stream)
            .await
    }

    /// Computes the cutoff block, enqueues a `BackfillRange` job for
    /// the gap since the last checkpoint, and enters the live listen
    /// loop. Split out so unit tests can drive the post-subscribe
    /// path with mocked streams and a mocked provider.
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
        let cutoff_block = get_cutoff_block(clear_stream, take_stream, provider).await?;
        let from_block = backfill_start_block(&self.pool, &self.evm_ctx).await?;

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

        self.listen(clear_stream, take_stream).await
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

            self.enqueue_trade_event(event, &log).await?;
        }
    }

    async fn enqueue_trade_event(
        &mut self,
        event: RaindexTradeEvent,
        log: &alloy::rpc::types::Log,
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

        self.job_queue
            .push(AccountForDexTrade { trade: trade_event })
            .await?;

        // Apalis storage now durably owns this event. Advance the
        // backfill checkpoint to `block_number - 1` so a subsequent
        // reconnect-time backfill does not refetch already-enqueued
        // events. The checkpoint is monotonic via `MAX(...)` upsert,
        // so out-of-order writes from concurrent block streams cannot
        // rewind it.
        if let Some(advance_to) = block_number.checked_sub(1) {
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

        monitor.enqueue_trade_event(event, &log).await.unwrap();

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

        monitor
            .enqueue_trade_event(event, &invalid_log)
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
    async fn enqueue_trade_event_advances_checkpoint_to_block_minus_one() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;
        // create_log uses block_number = 12345 by default.
        let log = create_log(7);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));

        monitor.enqueue_trade_event(event, &log).await.unwrap();

        let checkpoint = crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
            .await
            .unwrap();

        assert_eq!(
            checkpoint,
            Some(12344),
            "checkpoint must advance to block_number - 1 once an event is durably enqueued"
        );
    }

    #[tokio::test]
    async fn enqueue_trade_event_checkpoint_is_monotonic() {
        let (mut monitor, pool, evm_ctx) = create_test_monitor_with_pool().await;

        let high_block_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: alloy::primitives::LogData::empty(),
            },
            block_hash: None,
            block_number: Some(200),
            block_timestamp: None,
            transaction_hash: Some(alloy::primitives::B256::ZERO),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };

        let low_block_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: alloy::primitives::LogData::empty(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(alloy::primitives::B256::ZERO),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };

        let event_high = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));
        let event_low = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));

        monitor
            .enqueue_trade_event(event_high, &high_block_log)
            .await
            .unwrap();
        monitor
            .enqueue_trade_event(event_low, &low_block_log)
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
        // which the monitor will use as the cutoff after the cutoff
        // helper times out waiting for live events.
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&serde_json::Value::from(105_u64));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        // prepare_and_listen returns Err(StreamsClosed) once it falls
        // through to the listen loop with empty streams; we don't
        // care about that — we only care that the backfill job was
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

        monitor
            .enqueue_trade_event(event, &invalid_log)
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
