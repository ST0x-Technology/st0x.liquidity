//! Supervised order fill monitor that subscribes to onchain events
//! and pushes them into apalis storage for processing.
//!
//! The WebSocket connection is created inside [`SupervisedTask::run`],
//! so a fresh connection is established on each restart. This provides
//! automatic reconnection via the supervisor's restart policy.

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::providers::WsConnect;
use alloy::rpc::types::Log;
use alloy::sol_types;
use apalis::prelude::TaskSink;
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::sync::Mutex;
use tracing::{error, info, trace};
use url::Url;

use crate::bindings::IOrderBookV6::{ClearV3, IOrderBookV6Instance, TakeOrderV3};
use crate::onchain::trade::RaindexTradeEvent;
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{AccountForDexTrade, DexTradeAccountingJobQueue};

/// Monitors DEX WebSocket streams and pushes
/// [`AccountForDexTrade`] jobs into apalis storage.
///
/// Implements [`SupervisedTask`] so the supervisor restarts it
/// on WebSocket disconnection or errors. On first run, it consumes
/// pre-established streams from `Conductor::start`; on subsequent
/// restarts it creates a fresh WS connection.
#[derive(Clone)]
pub(crate) struct OrderFillMonitor {
    ws_url: Url,
    orderbook: Address,
    job_queue: DexTradeAccountingJobQueue,
    dex_streams: Arc<Mutex<Option<DexEventStreams>>>,
}

impl OrderFillMonitor {
    pub(crate) fn new(
        ws_url: Url,
        orderbook: Address,
        job_queue: DexTradeAccountingJobQueue,
        dex_streams: DexEventStreams,
    ) -> Self {
        Self {
            ws_url,
            orderbook,
            job_queue,
            dex_streams: Arc::new(Mutex::new(Some(dex_streams))),
        }
    }
}

impl SupervisedTask for OrderFillMonitor {
    async fn run(&mut self) -> TaskResult {
        info!(ws_url = %self.ws_url, "Connecting to DEX WebSocket");
        let initial = self.dex_streams.lock().await.take();

        if let Some(streams) = initial {
            info!("Order fill monitor using pre-established streams");
            return self.listen(streams.clear, streams.take).await;
        }

        self.connect_and_listen().await
    }
}

#[derive(Debug, thiserror::Error)]
enum OrderFillMonitorError {
    #[error("DEX event streams closed unexpectedly")]
    StreamsClosed,
}

type BoxedStream<T, Err = sol_types::Error> = Pin<Box<dyn Stream<Item = Result<T, Err>> + Send>>;

/// DEX event streams (ClearV3 / TakeOrderV3) for monitoring order fills.
///
/// Passed from `Conductor::start` to avoid a gap between the WS
/// subscription (used for `get_cutoff_block`) and the monitor's first
/// `run()` invocation.
pub(crate) struct DexEventStreams {
    pub(crate) clear: BoxedStream<(ClearV3, Log)>,
    pub(crate) take: BoxedStream<(TakeOrderV3, Log)>,
}

impl OrderFillMonitor {
    async fn connect_and_listen(&mut self) -> TaskResult {
        info!(ws_url = %self.ws_url, "Reconnecting to DEX WebSocket");

        let ws = WsConnect::new(self.ws_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let orderbook = IOrderBookV6Instance::new(self.orderbook, &provider);

        let clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        info!("Order fill monitor connected and listening");

        self.listen(clear_stream, take_stream).await
    }
}

impl OrderFillMonitor {
    async fn listen(
        &mut self,
        mut clear_stream: impl Stream<
            Item = Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>,
        > + Unpin,
        mut take_stream: impl Stream<
            Item = Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>,
        > + Unpin,
    ) -> TaskResult {
        loop {
            let trade_event = tokio::select! {
                Some(result) = clear_stream.next() => {
                    let (event, log) = result?;
                    Some((RaindexTradeEvent::ClearV3(Box::new(event)), log))
                }
                Some(result) = take_stream.next() => {
                    let (event, log) = result?;
                    Some((RaindexTradeEvent::TakeOrderV3(Box::new(event)), log))
                }
                else => None,
            };

            let Some((event, log)) = trade_event else {
                error!("Both DEX event streams ended unexpectedly");
                return Err(OrderFillMonitorError::StreamsClosed.into());
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
                error!(%err, "Failed to extract block inclusion metadata from log");
                return Ok(());
            }
        };

        trace!(
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            "Enqueuing trade accounting job"
        );

        self.job_queue
            .push(AccountForDexTrade { trade: trade_event })
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address};
    use apalis_sqlite::SqliteStorage;
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

    async fn create_test_monitor_with_pool() -> (OrderFillMonitor, SqlitePool) {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();
        let job_queue: DexTradeAccountingJobQueue = SqliteStorage::new(&pool);

        let dex_streams = DexEventStreams {
            clear: Box::pin(stream::empty()),
            take: Box::pin(stream::empty()),
        };

        let monitor = OrderFillMonitor::new(
            Url::parse("ws://localhost:8545").unwrap(),
            Address::ZERO,
            job_queue,
            dex_streams,
        );

        (monitor, pool)
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
        let (mut monitor, pool) = create_test_monitor_with_pool().await;
        let log = create_log(0);

        let clear_stream = stream::iter(vec![Ok((test_clear_event(), log))]);
        let take_stream = stream::empty();

        let _result = monitor.listen(clear_stream, take_stream).await;

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn listen_enqueues_take_event() {
        let (mut monitor, pool) = create_test_monitor_with_pool().await;
        let log = create_log(1);

        let clear_stream = stream::empty();
        let take_stream = stream::iter(vec![Ok((test_take_event(), log))]);

        let _result = monitor.listen(clear_stream, take_stream).await;

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn listen_returns_error_when_both_streams_empty() {
        let (mut monitor, _pool) = create_test_monitor_with_pool().await;

        let clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let result = monitor.listen(clear_stream, take_stream).await;

        let error = result
            .unwrap_err()
            .downcast::<OrderFillMonitorError>()
            .expect("expected OrderFillMonitorError");
        assert!(matches!(*error, OrderFillMonitorError::StreamsClosed));
    }

    #[tokio::test]
    async fn enqueue_trade_event_persists_to_storage() {
        let (mut monitor, pool) = create_test_monitor_with_pool().await;
        let log = create_log(42);
        let event = RaindexTradeEvent::ClearV3(Box::new(test_clear_event()));

        monitor.enqueue_trade_event(event, &log).await.unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn enqueue_trade_event_survives_invalid_log() {
        let (mut monitor, pool) = create_test_monitor_with_pool().await;

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
        let (mut monitor, pool) = create_test_monitor_with_pool().await;

        let clear_log = create_log(0);
        let take_log = create_log(1);

        let clear_stream = stream::iter(vec![Ok((test_clear_event(), clear_log))]);
        let take_stream = stream::iter(vec![Ok((test_take_event(), take_log))]);

        let _result = monitor.listen(clear_stream, take_stream).await;

        assert_eq!(job_count(&pool).await, 2);
    }
}
