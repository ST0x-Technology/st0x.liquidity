//! Orchestrates the market-making bot: builds CQRS frameworks, wires up
//! apalis workers for event processing, and launches long-running tasks
//! under a task-supervisor for automatic restarts.
//!
//! The [`start`] function replaces the old `Conductor` struct. It connects
//! to the DEX WebSocket, backfills missed events, registers an apalis
//! Monitor for [`ProcessOnchainEventJob`], and launches supervised tasks
//! for order polling, position checking, and inventory polling.

mod event_pipeline;
pub(crate) mod job;
mod startup;
mod tasks;

use std::sync::Arc;

use alloy::providers::{ProviderBuilder, WsConnect};
use cqrs_es::{AggregateError, Query};
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use task_supervisor::SupervisorHandle;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tracing::info;

use st0x_dto::ServerMessage;
use st0x_execution::{
    AlpacaBrokerApiError, AlpacaTradingApiError, EmptySymbolError, ExecutionError, Executor,
    SchwabError,
};

use crate::bindings::IOrderBookV5::IOrderBookV5Instance;
use crate::config::{Ctx, CtxError};
use crate::dual_write::DualWriteContext;
use crate::inventory::{InventorySnapshotAggregate, InventorySnapshotQuery, InventoryView};
use crate::onchain::OnChainError;
use crate::onchain::backfill::backfill_events;
use crate::onchain::event_processor::ProcessOnchainEventCtx;
use crate::onchain::pyth::FeedIdCache;
use crate::queue::EventQueueError;
use crate::symbol::cache::SymbolCache;
use crate::vault_registry::VaultRegistryError;
use event_pipeline::{
    create_event_storage, drain_event_queue_to_storage, enqueue_live_events_to_storage,
};
pub(crate) use startup::get_cutoff_block;
use startup::spawn_rebalancing_infrastructure;
use tasks::{BackgroundHandles, build_and_run_supervisor, spawn_monitor_and_receiver};

/// Event processing errors for live event handling.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventProcessingError {
    #[error("Event queue error: {0}")]
    Queue(#[from] EventQueueError),
    #[error("Database transaction error: {0}")]
    Transaction(#[from] sqlx::Error),
    #[error("Execution with ID {0} not found")]
    ExecutionNotFound(i64),
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Schwab execution error: {0}")]
    Schwab(#[from] SchwabError),
    #[error("Alpaca Broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Alpaca Trading API error: {0}")]
    AlpacaTradingApi(#[from] AlpacaTradingApiError),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error("Config error: {0}")]
    Config(#[from] CtxError),
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] AggregateError<VaultRegistryError>),
}

/// Starts the bot: connects WS, backfills, wires up apalis Monitor
/// for event processing, and launches supervised background tasks.
///
/// Returns a `SupervisorHandle` that the caller should `.wait()` on.
/// All tasks (monitor, receiver, pollers) are registered with the
/// supervisor and kept alive until shutdown.
pub(crate) async fn start<E>(
    ctx: &Ctx,
    pool: &SqlitePool,
    executor: E,
    executor_maintenance: Option<JoinHandle<()>>,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<SupervisorHandle>
where
    E: Executor + Clone + Send + Sync + Unpin + 'static,
    EventProcessingError: From<E::Error>,
{
    let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
    let provider = ProviderBuilder::new().connect_ws(ws).await?;
    let orderbook = IOrderBookV5Instance::new(ctx.evm.orderbook, &provider);

    let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
    let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

    let cutoff_block =
        get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, pool).await?;

    if let Some(end_block) = cutoff_block.checked_sub(1) {
        backfill_events(pool, &provider, &ctx.evm, end_block).await?;
    }

    let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
    let position_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
    let offchain_order_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
    let dual_write_context = DualWriteContext::with_threshold(
        pool.clone(),
        onchain_trade_cqrs,
        position_cqrs,
        offchain_order_cqrs,
        ctx.execution_threshold,
    );

    let inventory = Arc::new(RwLock::new(InventoryView::default()));
    let snapshot_query = InventorySnapshotQuery::new(inventory.clone());
    let snapshot_cqrs = sqlite_cqrs(
        pool.clone(),
        vec![Box::new(snapshot_query) as Box<dyn Query<InventorySnapshotAggregate>>],
        (),
    );

    let rebalancer = match ctx.rebalancing_ctx() {
        Some(rebalancing_config) => Some(
            spawn_rebalancing_infrastructure(
                rebalancing_config,
                pool,
                ctx,
                &inventory,
                event_sender,
                &provider,
            )
            .await?,
        ),
        None => None,
    };

    let vault_registry_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));
    let process_ctx = ProcessOnchainEventCtx {
        pool: pool.clone(),
        ctx: ctx.clone(),
        executor: executor.clone(),
        provider: provider.clone(),
        dual_write_context: dual_write_context.clone(),
        cache: SymbolCache::default(),
        feed_id_cache: Arc::new(FeedIdCache::default()),
        vault_registry_cqrs,
    };

    let mut event_storage = create_event_storage(pool, &process_ctx);
    drain_event_queue_to_storage(pool, &ctx.evm, &mut event_storage).await;
    enqueue_live_events_to_storage(&mut clear_stream, &mut take_stream, &mut event_storage).await;

    let (monitor_handle, receiver_handle) =
        spawn_monitor_and_receiver(process_ctx, event_storage, clear_stream, take_stream);

    let supervisor_handle = build_and_run_supervisor(
        ctx,
        pool,
        executor,
        dual_write_context,
        &provider,
        snapshot_cqrs,
        BackgroundHandles {
            executor_maintenance,
            rebalancer,
            monitor: monitor_handle,
            receiver: receiver_handle,
        },
    );

    info!("Conductor started: supervisor running with all tasks");

    Ok(supervisor_handle)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, IntoLogData, U256, address, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;
    use alloy::sol_types;
    use apalis_sql::sqlite::SqliteStorage;
    use futures_util::stream;

    use st0x_execution::MockExecutor;

    use super::event_pipeline::reconstruct_log_from_queued_event;
    use super::startup::{buffer_live_events, wait_for_first_event_with_timeout};
    use super::*;
    use crate::bindings::IOrderBookV5::{ClearConfigV2, ClearV3, TakeOrderV3};
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::onchain::OnchainTrade;
    use crate::onchain::event_processor::ProcessOnchainEventJob;
    use crate::onchain::trade::TradeEvent;
    use crate::test_utils::{get_test_log, get_test_order, setup_test_db};

    #[tokio::test]
    async fn test_event_enqueued_when_trade_conversion_returns_none() {
        let pool = setup_test_db().await;

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let trade_count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(trade_count, 0);
    }

    #[tokio::test]
    async fn test_onchain_trade_duplicate_handling() {
        let pool = setup_test_db().await;

        let existing_trade = crate::test_utils::OnchainTradeBuilder::new()
            .with_tx_hash(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ))
            .with_log_index(293)
            .with_symbol("AAPL0x")
            .with_amount(5.0)
            .with_price(20000.0)
            .build();
        let mut sql_tx = pool.begin().await.unwrap();
        existing_trade
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let duplicate_trade = existing_trade.clone();
        let mut sql_tx2 = pool.begin().await.unwrap();
        let err = duplicate_trade
            .save_within_transaction(&mut sql_tx2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Persistence(st0x_execution::PersistenceError::Database(ref db_err))
                if matches!(db_err, sqlx::Error::Database(_))
            ),
            "Expected database constraint violation, got: {err:?}"
        );
        sql_tx2.rollback().await.unwrap();

        let count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_complete_event_processing_flow() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();

        if let TradeEvent::ClearV3(boxed_clear_event) = queued_event.event {
            let cache = SymbolCache::default();
            let http_provider =
                ProviderBuilder::new().connect_http("http://localhost:8545".parse().unwrap());

            let feed_id_cache = crate::onchain::pyth::FeedIdCache::default();
            let order_owner = ctx.order_owner().unwrap();
            if let Ok(Some(trade)) = OnchainTrade::try_from_clear_v3(
                &ctx.evm,
                &cache,
                &http_provider,
                *boxed_clear_event,
                log,
                &feed_id_cache,
                order_owner,
            )
            .await
            {
                let dual_write_context = DualWriteContext::new(pool.clone());

                let mut sql_tx = pool.begin().await.unwrap();
                let crate::onchain::accumulator::TradeProcessingResult { .. } =
                    crate::onchain::accumulator::process_onchain_trade(
                        &mut sql_tx,
                        &dual_write_context,
                        trade,
                        st0x_execution::SupportedExecutor::DryRun,
                    )
                    .await
                    .unwrap();
                sql_tx.commit().await.unwrap();
            }
        }

        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let remaining_count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(remaining_count, 0);
    }

    #[tokio::test]
    async fn test_idempotency_bot_restart_during_processing() {
        let pool = setup_test_db().await;

        let event1 = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log1 = crate::test_utils::get_test_log();

        crate::queue::enqueue(&pool, &event1, &log1).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();
        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        crate::queue::enqueue(&pool, &event1, &log1).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        let mut log2 = crate::test_utils::get_test_log();
        log2.log_index = Some(2);
        crate::queue::enqueue(&pool, &event1, &log2).await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 1);

        let next_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next_event.log_index, 2);
        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, next_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_deterministic_processing_order() {
        let pool = setup_test_db().await;

        let events_and_logs = vec![(100, 5), (99, 3), (100, 1), (101, 2), (99, 8)];

        for (block_num, log_idx) in &events_and_logs {
            let event = ClearV3 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: get_test_order(),
                bob: get_test_order(),
                clearConfig: ClearConfigV2 {
                    aliceInputIOIndex: U256::from(0),
                    aliceOutputIOIndex: U256::from(1),
                    bobInputIOIndex: U256::from(1),
                    bobOutputIOIndex: U256::from(0),
                    aliceBountyVaultId: B256::ZERO,
                    bobBountyVaultId: B256::ZERO,
                },
            };
            let mut log = crate::test_utils::get_test_log();
            log.block_number = Some(*block_num);
            log.log_index = Some(*log_idx);
            log.transaction_hash = Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ));

            crate::queue::enqueue(&pool, &event, &log).await.unwrap();
        }

        let expected_order = vec![(99, 3), (99, 8), (100, 1), (100, 5), (101, 2)];

        for (expected_block, expected_log_idx) in expected_order {
            let event = crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(event.block_number, expected_block);
            assert_eq!(event.log_index, expected_log_idx);
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
        }

        assert!(
            crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_restart_scenarios_edge_cases() {
        let pool = setup_test_db().await;

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        let mut events = vec![];
        for i in 0..5 {
            let event = ClearV3 {
                sender: address!("0x1111111111111111111111111111111111111111"),
                alice: get_test_order(),
                bob: get_test_order(),
                clearConfig: ClearConfigV2 {
                    aliceInputIOIndex: U256::from(0),
                    aliceOutputIOIndex: U256::from(1),
                    bobInputIOIndex: U256::from(1),
                    bobOutputIOIndex: U256::from(0),
                    aliceBountyVaultId: B256::ZERO,
                    bobBountyVaultId: B256::ZERO,
                },
            };
            let mut log = crate::test_utils::get_test_log();
            log.log_index = Some(i);
            let mut hash_bytes = [0u8; 32];
            hash_bytes[31] = u8::try_from(i).unwrap_or(0);
            log.transaction_hash = Some(B256::from(hash_bytes));

            crate::queue::enqueue(&pool, &event, &log).await.unwrap();
            events.push((event, log));
        }

        for _ in 0..2 {
            let event = crate::queue::get_next_unprocessed_event(&pool)
                .await
                .unwrap()
                .unwrap();
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
        }

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 3);

        let mut processed_count = 0;
        while let Some(event) = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
        {
            let mut sql_tx = pool.begin().await.unwrap();
            crate::queue::mark_event_processed(&mut sql_tx, event.id.unwrap())
                .await
                .unwrap();
            sql_tx.commit().await.unwrap();
            processed_count += 1;
        }

        assert_eq!(processed_count, 3);
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);

        for (event, log) in &events {
            crate::queue::enqueue(&pool, event, log).await.unwrap();
        }

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_process_queued_event_deserialization() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        let count = crate::queue::count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let queued_event = crate::queue::get_next_unprocessed_event(&pool)
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(queued_event.event, TradeEvent::ClearV3(_)));

        let reconstructed_log = reconstruct_log_from_queued_event(&ctx.evm, &queued_event);
        assert_eq!(reconstructed_log.inner.address, ctx.evm.orderbook);
        assert_eq!(
            reconstructed_log.transaction_hash.unwrap(),
            queued_event.tx_hash
        );
        assert_eq!(reconstructed_log.log_index.unwrap(), queued_event.log_index);
        assert_eq!(
            reconstructed_log.block_number.unwrap(),
            queued_event.block_number
        );

        let original_log_data = clear_event.into_log_data();
        assert_eq!(reconstructed_log.inner.data, original_log_data);

        let mut sql_tx = pool.begin().await.unwrap();
        crate::queue::mark_event_processed(&mut sql_tx, queued_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_get_cutoff_block_with_timeout() {
        let pool = setup_test_db().await;
        let asserter = Asserter::new();

        asserter.push_success(&serde_json::Value::from(12345u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream = futures_util::stream::empty();
        let mut take_stream = futures_util::stream::empty();

        let cutoff_block = get_cutoff_block(&mut clear_stream, &mut take_stream, &provider, &pool)
            .await
            .unwrap();

        assert_eq!(cutoff_block, 12345);
    }

    #[tokio::test]
    async fn test_wait_for_first_event_with_timeout_no_events() {
        let mut clear_stream = stream::empty();
        let mut take_stream = stream::empty();

        let result = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_millis(10),
        )
        .await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_wait_for_first_event_with_clear_event() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.block_number = Some(1000);

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log.clone()))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        let (events, block_number) = wait_for_first_event_with_timeout(
            &mut clear_stream,
            &mut take_stream,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(block_number, 1000);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].0, TradeEvent::ClearV3(_)));
    }

    #[tokio::test]
    async fn test_wait_for_first_event_missing_block_number() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.block_number = None;

        let mut clear_stream = stream::iter(vec![Ok((clear_event, log))]);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();

        assert!(
            wait_for_first_event_with_timeout(
                &mut clear_stream,
                &mut take_stream,
                std::time::Duration::from_millis(100),
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn test_buffer_live_events_filtering() {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut early_log = get_test_log();
        early_log.block_number = Some(99);

        let mut late_log = get_test_log();
        late_log.block_number = Some(101);

        let events = vec![
            Ok((clear_event.clone(), early_log)),
            Ok((clear_event, late_log)),
        ];

        let mut clear_stream = stream::iter(events);
        let mut take_stream = stream::empty::<Result<(TakeOrderV3, Log), sol_types::Error>>();
        let mut event_buffer = Vec::new();

        buffer_live_events(&mut clear_stream, &mut take_stream, &mut event_buffer, 100).await;

        assert_eq!(event_buffer.len(), 1);
        assert_eq!(event_buffer[0].1.block_number.unwrap(), 101);
    }

    #[tokio::test]
    async fn test_drain_event_queue_to_storage() {
        let pool = setup_test_db().await;
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        // Enqueue an event in the legacy event_queue
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };
        let log = get_test_log();
        crate::queue::enqueue(&pool, &clear_event, &log)
            .await
            .unwrap();

        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 1);

        // Run apalis migrations so SqliteStorage works
        SqliteStorage::<()>::migrations()
            .set_ignore_missing(true)
            .run(&pool)
            .await
            .unwrap();

        type TestJob = ProcessOnchainEventJob<alloy::providers::RootProvider, MockExecutor>;
        let mut storage: SqliteStorage<TestJob> = SqliteStorage::new(pool.clone());

        drain_event_queue_to_storage(&pool, &ctx.evm, &mut storage).await;

        // Legacy queue should now be empty
        assert_eq!(crate::queue::count_unprocessed(&pool).await.unwrap(), 0);
    }
}
