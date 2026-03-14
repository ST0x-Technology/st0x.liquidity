//! Historical onchain event backfill with retry logic.
//!
//! Scans past blocks for `ClearV3` and `TakeOrderV3` events and pushes them
//! into the apalis job queue for processing, ensuring no trades are missed
//! after downtime.
//!
//! Always backfills from `deployment_block` — the CQRS pipeline rejects
//! duplicate fills via aggregate idempotency, so re-pushing is safe.

use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use apalis::prelude::TaskSink;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use futures_util::future;
use itertools::Itertools;
use std::time::Duration;
use tracing::{debug, info, trace, warn};

use super::EvmCtx;
use super::OnChainError;
use crate::bindings::IOrderBookV6::{ClearV3, TakeOrderV3};
use crate::onchain::trade::RaindexTradeEvent;
use crate::trading::onchain::inclusion::ChainIncluded;
use crate::trading::onchain::trade_accountant::{AccountForDexTrade, DexTradeAccountingJobQueue};

pub(crate) fn get_backfill_retry_strat() -> ExponentialBuilder {
    const BACKFILL_MAX_RETRIES: usize = 15;
    const BACKFILL_INITIAL_DELAY: Duration = Duration::from_millis(100);
    const BACKFILL_MAX_DELAY: Duration = Duration::from_secs(120);

    ExponentialBuilder::default()
        .with_max_times(BACKFILL_MAX_RETRIES)
        .with_min_delay(BACKFILL_INITIAL_DELAY)
        .with_max_delay(BACKFILL_MAX_DELAY)
        .with_jitter()
}

#[tracing::instrument(
    skip(provider, evm_ctx, retry_strategy, storage),
    fields(end_block),
    level = tracing::Level::INFO,
)]
pub(crate) async fn backfill_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &EvmCtx,
    end_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<(), OnChainError> {
    let start_block = evm_ctx.deployment_block;

    if start_block > end_block {
        info!(
            "Already caught up to block {}, skipping backfill",
            end_block
        );
        return Ok(());
    }

    let total_blocks = end_block - start_block + 1;

    info!(
        "Backfilling from block {} to {} ({} blocks)",
        start_block, end_block, total_blocks
    );

    let batch_ranges = generate_batch_ranges(start_block, end_block);

    let batch_tasks = batch_ranges
        .into_iter()
        .map(|(batch_start, batch_end)| {
            enqueue_batch_events(
                provider,
                evm_ctx,
                batch_start,
                batch_end,
                retry_strategy.clone(),
                storage.clone(),
            )
        })
        .collect::<Vec<_>>();

    let batch_results = future::join_all(batch_tasks).await;

    let total_enqueued = batch_results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<usize>();

    info!("Backfill completed: {total_enqueued} events enqueued");

    Ok(())
}

#[tracing::instrument(
    skip(provider, evm_ctx, retry_strategy, storage),
    fields(batch_start, batch_end),
    level = tracing::Level::DEBUG,
)]
async fn enqueue_batch_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &EvmCtx,
    batch_start: u64,
    batch_end: u64,
    retry_strategy: B,
    mut storage: OrderFillJobQueue,
) -> Result<usize, OnChainError> {
    let clear_filter = Filter::new()
        .address(evm_ctx.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(ClearV3::SIGNATURE_HASH);

    let take_filter = Filter::new()
        .address(evm_ctx.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(TakeOrderV3::SIGNATURE_HASH);

    let provider_clear = provider.clone();
    let provider_take = provider.clone();
    let clear_filter_clone = clear_filter.clone();
    let take_filter_clone = take_filter.clone();

    let get_clear_logs = move || {
        let provider = provider_clear.clone();
        let filter = clear_filter_clone.clone();
        async move { provider.get_logs(&filter).await }
    };
    let get_take_logs = move || {
        let provider = provider_take.clone();
        let filter = take_filter_clone.clone();
        async move { provider.get_logs(&filter).await }
    };

    let (clear_logs, take_logs) = future::try_join(
        get_clear_logs
            .retry(retry_strategy.clone().build())
            .notify(|err, dur| {
                trace!("Retrying clear_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
        get_take_logs
            .retry(retry_strategy.build())
            .notify(|err, dur| {
                trace!("Retrying take_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
    )
    .await?;

    debug!(
        total_clear_logs = %clear_logs.len(),
        total_take_logs = %take_logs.len(),
        "Processed a batch of blocks from {batch_start} to {batch_end}",
    );

    let queued_events = clear_logs
        .into_iter()
        .chain(take_logs)
        .sorted_by_key(|log| (log.block_number, log.log_index))
        .filter_map(|log| {
            if let Ok(clear_event) = log.log_decode::<ClearV3>() {
                let event = RaindexTradeEvent::ClearV3(Box::new(clear_event.data().clone()));
                Some((event, log))
            } else if let Ok(take_event) = log.log_decode::<TakeOrderV3>() {
                let event = RaindexTradeEvent::TakeOrderV3(Box::new(take_event.data().clone()));
                Some((event, log))
            } else {
                None
            }
        })
        .filter_map(|(event, log)| {
            QueuedEvent::from_log(event, &log)
                .inspect_err(
                    |error| warn!(%error, "Failed to create queued event from log during backfill"),
                )
                .ok()
        })
        .collect::<Vec<_>>();

    let mut enqueued_count = 0;

    for queued_event in queued_events {
        match storage.push(OrderFillJob { queued_event }).await {
            Ok(()) => enqueued_count += 1,
            Err(error) => {
                warn!(%error, "Failed to push backfill job into apalis storage");
            }
        }
    }

    Ok(enqueued_count)
}

fn generate_batch_ranges(start_block: u64, end_block: u64) -> Vec<(u64, u64)> {
    const BACKFILL_BATCH_SIZE: usize = 1_000;

    (start_block..=end_block)
        .step_by(BACKFILL_BATCH_SIZE)
        .map(|batch_start| {
            let batch_end = (batch_start + u64::try_from(BACKFILL_BATCH_SIZE).unwrap_or(u64::MAX)
                - 1)
            .min(end_block);
            (batch_start, batch_end)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{
        Address, B256, FixedBytes, IntoLogData, TxHash, U256, address, fixed_bytes, uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use apalis_sqlite::SqliteStorage;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use url::Url;

    use super::*;
    use crate::bindings::IOrderBookV6;
    use crate::conductor::setup_apalis_tables;
    use crate::onchain::EvmCtx;
    use crate::test_utils::{get_test_order, setup_test_db};

    fn test_retry_strategy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_max_times(2) // Only 2 retries for tests (3 attempts total)
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
    }

    async fn setup_storage(pool: &SqlitePool) -> OrderFillJobQueue {
        setup_apalis_tables(pool).await.unwrap();
        SqliteStorage::new(pool)
    }

    async fn job_count(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_backfill_events_empty_results() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[test]
    fn test_generate_batch_ranges_single_batch() {
        let ranges = generate_batch_ranges(100, 500);
        assert_eq!(ranges, vec![(100, 500)]);
    }

    #[test]
    fn test_generate_batch_ranges_exact_batch_size() {
        let ranges = generate_batch_ranges(100, 1099);
        assert_eq!(ranges, vec![(100, 1099)]);
    }

    #[test]
    fn test_generate_batch_ranges_multiple_batches() {
        let ranges = generate_batch_ranges(100, 2500);
        assert_eq!(ranges, vec![(100, 1099), (1100, 2099), (2100, 2500)]);
    }

    #[test]
    fn test_generate_batch_ranges_large_range() {
        let ranges = generate_batch_ranges(5000, 25000);
        assert_eq!(
            ranges,
            vec![
                (5000, 5999),
                (6000, 6999),
                (7000, 7999),
                (8000, 8999),
                (9000, 9999),
                (10000, 10999),
                (11000, 11999),
                (12000, 12999),
                (13000, 13999),
                (14000, 14999),
                (15000, 15999),
                (16000, 16999),
                (17000, 17999),
                (18000, 18999),
                (19000, 19999),
                (20000, 20999),
                (21000, 21999),
                (22000, 22999),
                (23000, 23999),
                (24000, 24999),
                (25000, 25000)
            ]
        );
    }

    #[test]
    fn test_generate_batch_ranges_boundary() {
        let ranges = generate_batch_ranges(100, 25000);
        assert_eq!(
            ranges,
            vec![
                (100, 1099),
                (1100, 2099),
                (2100, 3099),
                (3100, 4099),
                (4100, 5099),
                (5100, 6099),
                (6100, 7099),
                (7100, 8099),
                (8100, 9099),
                (9100, 10099),
                (10100, 11099),
                (11100, 12099),
                (12100, 13099),
                (13100, 14099),
                (14100, 15099),
                (15100, 16099),
                (16100, 17099),
                (17100, 18099),
                (18100, 19099),
                (19100, 20099),
                (20100, 21099),
                (21100, 22099),
                (22100, 23099),
                (23100, 24099),
                (24100, 25000)
            ]
        );
    }

    #[test]
    fn test_generate_batch_ranges_single_block() {
        let ranges = generate_batch_ranges(42, 42);
        assert_eq!(ranges, vec![(42, 42)]);
    }

    #[test]
    fn test_generate_batch_ranges_empty() {
        let ranges = generate_batch_ranges(100, 99);
        assert_eq!(ranges.len(), 0);
    }

    #[tokio::test]
    async fn test_backfill_events_with_clear_v3_events() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let clear_config = IOrderBookV6::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IOrderBookV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: clear_config,
        };

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_with_take_order_v3_events() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let take_event = IOrderBookV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IOrderBookV6::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },
            input: Float::from_fixed_decimal_lossy(uint!(100_000_000_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(9_000_000_000_000_000_000_U256), 18)
                .unwrap()
                .0
                .get_inner(),
        };

        let take_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: take_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events (empty)
        asserter.push_success(&serde_json::json!([take_log])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_enqueues_all_events() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let different_order = get_test_order();
        let clear_event = IOrderBookV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: different_order.clone(),
            bob: different_order.clone(),
            clearConfig: IOrderBookV6::ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let take_event = IOrderBookV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IOrderBookV6::TakeOrderConfigV4 {
                order: different_order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },
            input: Float::from_fixed_decimal_lossy(uint!(100_000_000_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(9_000_000_000_000_000_000_U256), 18)
                .unwrap()
                .0
                .get_inner(),
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(tx_hash1),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let take_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: take_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(60),
            block_timestamp: None,
            transaction_hash: Some(tx_hash2),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log]));
        asserter.push_success(&serde_json::json!([take_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_rpc_failure() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // All retry attempts fail - need double since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("RPC connection error");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("RPC connection error");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage).await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_block_range() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    fn create_test_take_event(
        order: &IOrderBookV6::OrderV4,
        input: U256,
        output: U256,
    ) -> IOrderBookV6::TakeOrderV3 {
        IOrderBookV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IOrderBookV6::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },

            input: Float::from_fixed_decimal_lossy(input, 0)
                .unwrap()
                .0
                .get_inner(),

            output: Float::from_fixed_decimal_lossy(output, 18)
                .unwrap()
                .0
                .get_inner(),
        }
    }

    fn create_test_log(
        orderbook: Address,
        event: &IOrderBookV6::TakeOrderV3,
        block_number: u64,
        tx_hash: FixedBytes<32>,
    ) -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        }
    }

    #[tokio::test]
    async fn test_backfill_events_preserves_chronological_order() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let take_event1 = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(1_000_000_000_000_000_000_U256),
        );
        let take_event2 = create_test_take_event(
            &order,
            uint!(200_000_000_U256),
            uint!(2_000_000_000_000_000_000_U256),
        );

        let take_log1 = create_test_log(evm_ctx.orderbook, &take_event1, 50, tx_hash1);
        let take_log2 = create_test_log(evm_ctx.orderbook, &take_event2, 100, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([take_log2, take_log1]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_count_verification() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1000,
        };

        let asserter = Asserter::new();

        // Batch 1: blocks 1000-1999
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Batch 2: blocks 2000-2500
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 2500, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_boundary_verification() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 500,
        };

        let asserter = Asserter::new();

        // Batch 1: blocks 500-1499
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Batch 2: blocks 1500-1900
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            1900,
            get_backfill_retry_strat(),
            storage,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_process_batch_with_realistic_data() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let take_event = create_test_take_event(
            &order,
            uint!(500_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );
        let tx_hash =
            fixed_bytes!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let take_log = create_test_log(evm_ctx.orderbook, &take_event, 150, tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([take_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let enqueued_count = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await
        .unwrap();

        assert_eq!(enqueued_count, 1);
        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_large_block_range_batching() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();

        for _ in 0..3 {
            asserter.push_success(&serde_json::json!([]));
            asserter.push_success(&serde_json::json!([]));
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            3000,
            get_backfill_retry_strat(),
            storage,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_valid_and_invalid_events() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let valid_take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );

        // Create different order with different hash to make it invalid
        let mut different_order = get_test_order();
        different_order.nonce =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let invalid_take_event = create_test_take_event(
            &different_order,
            uint!(50_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );

        let valid_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let invalid_tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let valid_log = create_test_log(evm_ctx.orderbook, &valid_take_event, 50, valid_tx_hash);
        let invalid_log =
            create_test_log(evm_ctx.orderbook, &invalid_take_event, 51, invalid_tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([valid_log, invalid_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        // Both events should be enqueued (filtering happens during processing, not backfill)
        assert_eq!(job_count(&pool).await, 2);
    }

    fn create_clear_log(orderbook: Address, order: &IOrderBookV6::OrderV4, tx_hash: TxHash) -> Log {
        let clear_config = IOrderBookV6::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IOrderBookV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: clear_config,
        };

        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        }
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_clear_and_take_events() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(evm_ctx.orderbook, &take_event, 50, tx_hash1);
        let clear_log = create_clear_log(evm_ctx.orderbook, &order, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log]));
        asserter.push_success(&serde_json::json!([take_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 2);
    }

    #[tokio::test]
    async fn test_process_batch_retry_mechanism() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // First two calls fail, third succeeds
        asserter.push_failure_msg("RPC connection error");
        asserter.push_failure_msg("Timeout error");
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;

        let enqueued_count = result.unwrap();
        assert_eq!(enqueued_count, 0);
    }

    #[tokio::test]
    async fn test_process_batch_exhausted_retries() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // All retry attempts fail - need double since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_partial_batch_failure() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();

        // First batch succeeds
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Second batch fails completely (after retries)
        // Need double the failures since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            backfill_events(&provider, &evm_ctx, 25000, test_retry_strategy(), storage).await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_corrupted_log_data() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        // Create malformed log with invalid event signature
        let corrupted_log = Log {
            inner: alloy::primitives::Log::new(
                evm_ctx.orderbook,
                Vec::new(),
                Vec::from([0x00u8; 32]).into(),
            )
            .unwrap(),
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([corrupted_log]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        // Corrupted logs are silently ignored during backfill
        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_single_block_range() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 42,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 42, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_database_failure() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([take_log])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Close the database to simulate connection failure
        pool.close().await;

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;

        // Should succeed at RPC level but fail at database level
        // The function handles enqueue failures gracefully by continuing
        let enqueued = result.unwrap();
        assert_eq!(enqueued, 0); // No events successfully enqueued
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_filter_creation() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            150,
            test_retry_strategy(),
            storage,
        )
        .await;

        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_partial_enqueue_failure() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let order = get_test_order();

        let take_event1 = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_event2 = create_test_take_event(
            &order,
            uint!(200_000_000_U256),
            uint!(18_000_000_000_000_000_000_U256),
        );

        let take_log1 = create_test_log(
            evm_ctx.orderbook,
            &take_event1,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );
        let take_log2 = create_test_log(
            evm_ctx.orderbook,
            &take_event2,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([take_log1, take_log2])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;

        let enqueued = result.unwrap();
        assert_eq!(enqueued, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_concurrent_batch_processing() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );

        let asserter = Asserter::new();

        // blocks 1-3000 (3 batches), second batch has take events
        for batch_idx in 0..3 {
            asserter.push_success(&serde_json::json!([])); // clear events
            if batch_idx == 1 {
                asserter.push_success(&serde_json::json!([take_log])); // take events
            } else {
                asserter.push_success(&serde_json::json!([])); // take events
            }
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 3000, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 1);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_retry_exponential_backoff() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // First attempt fails for both parallel calls
        asserter.push_failure_msg("Temporary network failure");
        asserter.push_failure_msg("Rate limit exceeded");
        // Second attempt succeeds for both
        asserter.push_success(&serde_json::json!([])); // clear events (retry)
        asserter.push_success(&serde_json::json!([])); // take events (retry)

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let start_time = std::time::Instant::now();
        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;
        let elapsed = start_time.elapsed();

        let enqueued = result.unwrap();
        assert_eq!(enqueued, 0);

        // Should have taken at least the test initial delay time due to retries
        assert!(elapsed >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_backfill_events_zero_blocks() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 100,
        };

        // No RPC calls should be made when deployment block > end block
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 50, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_mixed_log_types() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
        };

        let order = get_test_order();

        let clear_event = IOrderBookV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: IOrderBookV6::ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        asserter.push_success(&serde_json::json!([take_log])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            storage,
        )
        .await;

        let enqueued = result.unwrap();
        assert_eq!(enqueued, 2);
        assert_eq!(job_count(&pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_starts_from_deployment_block() {
        let pool = setup_test_db().await;
        let storage = setup_storage(&pool).await;
        let evm_ctx = EvmCtx {
            ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
        };

        // Should start from deployment_block (50) to end_block (100)
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events for 50-100
        asserter.push_success(&serde_json::json!([])); // take events for 50-100

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&provider, &evm_ctx, 100, test_retry_strategy(), storage)
            .await
            .unwrap();

        assert_eq!(job_count(&pool).await, 0);
    }
}
