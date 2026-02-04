use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use futures_util::future;
use itertools::Itertools;
use sqlx::SqlitePool;
use std::time::Duration;
use tracing::{debug, info, trace, warn};

use super::EvmConfig;
use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::error::OnChainError;
use crate::queue::enqueue;

fn get_backfill_retry_strat() -> ExponentialBuilder {
    const BACKFILL_MAX_RETRIES: usize = 15;
    const BACKFILL_INITIAL_DELAY: Duration = Duration::from_millis(100);
    const BACKFILL_MAX_DELAY: Duration = Duration::from_secs(120);

    ExponentialBuilder::default()
        .with_max_times(BACKFILL_MAX_RETRIES)
        .with_min_delay(BACKFILL_INITIAL_DELAY)
        .with_max_delay(BACKFILL_MAX_DELAY)
        .with_jitter()
}

#[derive(Debug)]
enum EventData {
    ClearV3(Box<ClearV3>),
    TakeOrderV3(Box<TakeOrderV3>),
}

pub(crate) async fn backfill_events<P: Provider + Clone>(
    pool: &SqlitePool,
    provider: &P,
    config: &EvmConfig,
    end_block: u64,
) -> Result<(), OnChainError> {
    let retry_strat = get_backfill_retry_strat();
    backfill_events_with_retry_strat(pool, provider, config, end_block, retry_strat).await
}

#[tracing::instrument(skip(pool, provider, config, retry_strategy), fields(end_block), level = tracing::Level::INFO)]
async fn backfill_events_with_retry_strat<P: Provider + Clone, B: BackoffBuilder + Clone>(
    pool: &SqlitePool,
    provider: &P,
    config: &EvmConfig,
    end_block: u64,
    retry_strategy: B,
) -> Result<(), OnChainError> {
    // Query the last processed block from event_queue to determine start point
    let start_block = crate::queue::get_max_processed_block(pool)
        .await?
        .map_or_else(
            || {
                info!(
                    "Starting initial backfill from deployment block {}",
                    config.deployment_block
                );
                config.deployment_block
            },
            |max_block| {
                let resume_block = max_block + 1;
                info!(
                    "Resuming backfill from block {} (last processed: {})",
                    resume_block, max_block
                );
                resume_block
            },
        );

    // Skip if we're already caught up
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
                pool,
                provider,
                config,
                batch_start,
                batch_end,
                retry_strategy.clone(),
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

#[tracing::instrument(skip(pool, provider, config, retry_strategy), fields(batch_start, batch_end), level = tracing::Level::DEBUG)]
async fn enqueue_batch_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    pool: &SqlitePool,
    provider: &P,
    config: &EvmConfig,
    batch_start: u64,
    batch_end: u64,
    retry_strategy: B,
) -> Result<usize, OnChainError> {
    let clear_filter = Filter::new()
        .address(config.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(ClearV3::SIGNATURE_HASH);

    let take_filter = Filter::new()
        .address(config.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(TakeOrderV3::SIGNATURE_HASH);

    // Use the provided retry strategy

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
        "Found {} ClearV3 events and {} TakeOrderV3 events in batch {}-{}",
        clear_logs.len(),
        take_logs.len(),
        batch_start,
        batch_end
    );

    let all_logs = clear_logs
        .into_iter()
        .chain(take_logs.into_iter())
        .collect::<Vec<_>>();

    let enqueue_tasks = all_logs
        .into_iter()
        .sorted_by_key(|log| (log.block_number, log.log_index))
        .filter_map(|log| {
            // Try ClearV3 first
            if let Ok(clear_event) = log.log_decode::<ClearV3>() {
                let event_data = EventData::ClearV3(Box::new(clear_event.data().clone()));
                Some((event_data, log))
            // Then try TakeOrderV3
            } else if let Ok(take_event) = log.log_decode::<TakeOrderV3>() {
                let event_data = EventData::TakeOrderV3(Box::new(take_event.data().clone()));
                Some((event_data, log))
            } else {
                None
            }
        })
        .map(|(event_data, log)| async move {
            match event_data {
                EventData::ClearV3(event) => match enqueue(pool, &*event, &log).await {
                    Ok(()) => Some(()),
                    Err(e) => {
                        warn!("Failed to enqueue ClearV3 event during backfill: {e}");
                        None
                    }
                },
                EventData::TakeOrderV3(event) => match enqueue(pool, &*event, &log).await {
                    Ok(()) => Some(()),
                    Err(e) => {
                        warn!("Failed to enqueue TakeOrderV3 event during backfill: {e}");
                        None
                    }
                },
            }
        })
        .collect::<Vec<_>>();

    let enqueue_results = future::join_all(enqueue_tasks).await;

    let enqueued_count = enqueue_results.into_iter().flatten().count();

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
        Address, B256, FixedBytes, IntoLogData, U256, address, fixed_bytes, uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use super::*;
    use crate::bindings::IERC20::symbolCall;
    use crate::bindings::IOrderBookV5;
    use crate::onchain::EvmConfig;
    use crate::onchain::trade::TradeEvent;
    use crate::queue::{count_unprocessed, get_next_unprocessed_event, mark_event_processed};
    use crate::test_utils::{get_test_order, setup_test_db};

    fn test_retry_strategy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_max_times(2) // Only 2 retries for tests (3 attempts total)
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
    }

    #[tokio::test]
    async fn test_backfill_events_empty_results() {
        let pool = setup_test_db().await;
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
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
    async fn test_backfill_events_with_clear_v2_events() {
        let pool = setup_test_db().await;
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_config = IOrderBookV5::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IOrderBookV5::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: clear_config,
        };

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: config.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events (empty)

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        // Verify the enqueued event details
        let queued_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(queued_event.tx_hash, tx_hash);
        assert_eq!(queued_event.log_index, 1);
        assert!(matches!(queued_event.event, TradeEvent::ClearV3(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_with_take_order_v2_events() {
        let pool = setup_test_db().await;
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let take_event = IOrderBookV5::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IOrderBookV5::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },
            input: Float::from_fixed_decimal_lossy(uint!(100_000_000_U256), 0)
                .unwrap()
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(9_000_000_000_000_000_000_U256), 18)
                .unwrap()
                .get_inner(),
        };

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let take_log = Log {
            inner: alloy::primitives::Log {
                address: config.orderbook,
                data: take_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number
        asserter.push_success(&serde_json::json!([])); // clear events (empty)
        asserter.push_success(&serde_json::json!([take_log])); // take events
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"MSFT0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        // Check that one event was enqueued
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        // Verify the enqueued event details
        let queued_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(queued_event.tx_hash, tx_hash);
        assert_eq!(queued_event.log_index, 1);
        assert!(matches!(queued_event.event, TradeEvent::TakeOrderV3(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_enqueues_all_events() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let different_order = get_test_order();
        let clear_event = IOrderBookV5::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: different_order.clone(),
            bob: different_order.clone(),
            clearConfig: IOrderBookV5::ClearConfigV2 {
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
                address: config.orderbook,
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
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events (empty)

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        // Should enqueue the event (filtering happens during queue processing, not backfill)
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_handles_rpc_errors() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number call
        // Need 2 failures: one for clear_logs retry, one for take_logs retry (they run in parallel)
        asserter.push_failure_msg("RPC error"); // clear_logs failure
        asserter.push_failure_msg("RPC error"); // take_logs failure

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            backfill_events_with_retry_strat(&pool, &provider, &config, 100, test_retry_strategy())
                .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OnChainError::Alloy(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_block_range() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 50,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64)); // get_block_number
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    fn create_test_take_event(
        order: &IOrderBookV5::OrderV4,
        input: U256,
        output: U256,
    ) -> IOrderBookV5::TakeOrderV3 {
        IOrderBookV5::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IOrderBookV5::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },

            input: Float::from_fixed_decimal_lossy(input, 0)
                .unwrap()
                .get_inner(),

            output: Float::from_fixed_decimal_lossy(output, 18)
                .unwrap()
                .get_inner(),
        }
    }

    fn create_test_log(
        orderbook: Address,
        event: &IOrderBookV5::TakeOrderV3,
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
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
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

        let take_log1 = create_test_log(config.orderbook, &take_event1, 50, tx_hash1);
        let take_log2 = create_test_log(config.orderbook, &take_event2, 100, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(200u64));
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([take_log2, take_log1]));

        // Symbol calls for both trades
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"MSFT0x".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"MSFT0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        // Check that two events were enqueued
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 2);

        // Verify the first event (earlier block number)
        let first_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(first_event.tx_hash, tx_hash1);
        assert_eq!(first_event.block_number, 50);

        // Mark as processed and get the second event
        let mut sql_tx = pool.begin().await.unwrap();
        mark_event_processed(&mut sql_tx, first_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let second_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(second_event.tx_hash, tx_hash2);
        assert_eq!(second_event.block_number, 100);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_count_verification() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1000,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(2500u64));

        // Batch 1: blocks 1000-1999
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Batch 2: blocks 2000-2500
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        backfill_events(&pool, &provider, &config, 2500)
            .await
            .unwrap();

        // Verifies that batching correctly handles the expected number of RPC calls
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_boundary_verification() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 500,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(1900u64));

        // Batch 1: blocks 500-1499
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Batch 2: blocks 1500-1900
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events_with_retry_strat(
            &pool,
            &provider,
            &config,
            1900,
            get_backfill_retry_strat(),
        )
        .await
        .unwrap();

        // Verify the batching worked correctly for different deployment/current block combination
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_process_batch_with_realistic_data() {
        let pool = setup_test_db().await;
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let tx_hash =
            fixed_bytes!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let take_event = create_test_take_event(
            &order,
            uint!(500_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(config.orderbook, &take_event, 150, tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([take_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let enqueued_count =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy())
                .await
                .unwrap();

        assert_eq!(enqueued_count, 1);

        // Verify the enqueued event
        let queued_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(queued_event.tx_hash, tx_hash);
        assert_eq!(queued_event.block_number, 150);
    }

    #[tokio::test]
    async fn test_backfill_events_deployment_equals_current_block() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 100,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64));
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_large_block_range_batching() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(3000u64));

        for _ in 0..6 {
            asserter.push_success(&serde_json::json!([]));
            asserter.push_success(&serde_json::json!([]));
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events_with_retry_strat(
            &pool,
            &provider,
            &config,
            3000,
            get_backfill_retry_strat(),
        )
        .await
        .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_deployment_after_current_block() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 200,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events_with_retry_strat(&pool, &provider, &config, 100, test_retry_strategy())
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_valid_and_invalid_events() {
        let pool = setup_test_db().await;
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
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
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"); // Change nonce to make hash different
        let invalid_take_event = create_test_take_event(
            &different_order,
            uint!(50_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );

        let valid_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let invalid_tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let valid_log = create_test_log(config.orderbook, &valid_take_event, 50, valid_tx_hash);
        let invalid_log =
            create_test_log(config.orderbook, &invalid_take_event, 51, invalid_tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(100u64));
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([valid_log, invalid_log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        // Both events should be enqueued (filtering happens during processing, not backfill)
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 2);
    }

    fn create_clear_log(orderbook: Address, order: &IOrderBookV5::OrderV4, tx_hash: B256) -> Log {
        let clear_config = IOrderBookV5::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IOrderBookV5::ClearV3 {
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

    fn create_after_clear_log(orderbook: Address, tx_hash: B256) -> Log {
        let after_clear_event = IOrderBookV5::AfterClearV2 {
            sender: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            clearStateChange: IOrderBookV5::ClearStateChangeV2 {
                aliceOutput: Float::from_fixed_decimal_lossy(
                    uint!(5_000_000_000_000_000_000_U256),
                    18,
                )
                .unwrap()
                .get_inner(),

                bobOutput: Float::from_fixed_decimal_lossy(uint!(50_000_000_U256), 0)
                    .unwrap()
                    .get_inner(),

                aliceInput: Float::from_fixed_decimal_lossy(uint!(50_000_000_U256), 0)
                    .unwrap()
                    .get_inner(),

                bobInput: Float::from_fixed_decimal_lossy(
                    uint!(5_000_000_000_000_000_000_U256),
                    18,
                )
                .unwrap()
                .get_inner(),
            },
        };

        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        }
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_clear_and_take_events() {
        let pool = setup_test_db().await;
        let order = get_test_order();
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
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
        let take_log = create_test_log(config.orderbook, &take_event, 50, tx_hash1);
        let clear_log = create_clear_log(config.orderbook, &order, tx_hash2);
        let after_clear_log = create_after_clear_log(config.orderbook, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(150u64));
        asserter.push_success(&serde_json::json!([clear_log]));
        asserter.push_success(&serde_json::json!([take_log]));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        asserter.push_success(&serde_json::json!([after_clear_log]));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 2);

        let first_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(first_event.tx_hash, tx_hash1);
        assert_eq!(first_event.block_number, 50);

        let mut sql_tx = pool.begin().await.unwrap();
        mark_event_processed(&mut sql_tx, first_event.id.unwrap())
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let second_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(second_event.tx_hash, tx_hash2);
        assert_eq!(second_event.block_number, 100);
    }

    #[tokio::test]
    async fn test_process_batch_retry_mechanism() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // First two calls fail, third succeeds
        asserter.push_failure_msg("RPC connection error");
        asserter.push_failure_msg("Timeout error");
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;

        assert!(result.is_ok());
        let enqueued_count = result.unwrap();
        assert_eq!(enqueued_count, 0);
    }

    #[tokio::test]
    async fn test_process_batch_exhausted_retries() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // All retry attempts fail - need double since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error"); // clear_logs failures
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error"); // take_logs failures
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OnChainError::Alloy(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_partial_batch_failure() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(25000u64));

        // First batch succeeds
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        // Second batch fails completely (after retries)
        // Need double the failures since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure"); // clear_logs failures
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure"); // take_logs failures
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = backfill_events_with_retry_strat(
            &pool,
            &provider,
            &config,
            25000,
            test_retry_strategy(),
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OnChainError::Alloy(_)));
    }

    #[tokio::test]
    async fn test_backfill_events_corrupted_log_data() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        // Create malformed log with invalid event signature
        let corrupted_log = Log {
            inner: alloy::primitives::Log::new(
                config.orderbook,
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
        asserter.push_success(&serde_json::Value::from(100u64));
        asserter.push_success(&serde_json::json!([corrupted_log]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        // Corrupted logs are silently ignored during backfill
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_single_block_range() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 42,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(42u64));
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_database_failure() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            config.orderbook,
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

        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;

        // Should succeed at RPC level but fail at database level
        // The function handles enqueue failures gracefully by continuing
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // No events successfully enqueued
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_filter_creation() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Test with specific block range
        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 150, test_retry_strategy()).await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_partial_enqueue_failure() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let order = get_test_order();

        // Create multiple events
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
            config.orderbook,
            &take_event1,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );
        let take_log2 = create_test_log(
            config.orderbook,
            &take_event2,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        asserter.push_success(&serde_json::json!([take_log1, take_log2])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;

        // Should succeed with 2 events enqueued
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_backfill_events_concurrent_batch_processing() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            config.orderbook,
            &take_event,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );

        let asserter = Asserter::new();

        // Use a smaller range to avoid excessive mock responses: blocks 1-3000 (3 batches)
        // Multiple batches with events in different batches
        for batch_idx in 0..3 {
            // All batches start with clear events
            asserter.push_success(&serde_json::json!([])); // clear events
            if batch_idx == 1 {
                // Second batch has take events
                asserter.push_success(&serde_json::json!([take_log])); // take events
            } else {
                // Other batches have no take events
                asserter.push_success(&serde_json::json!([])); // take events
            }
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 3000)
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_retry_exponential_backoff() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let asserter = Asserter::new();
        // First attempt fails for both parallel calls
        asserter.push_failure_msg("Temporary network failure"); // clear_logs first attempt
        asserter.push_failure_msg("Rate limit exceeded"); // take_logs first attempt
        // Second attempt succeeds for both
        asserter.push_success(&serde_json::json!([])); // clear events (retry)
        asserter.push_success(&serde_json::json!([])); // take events (retry)

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let start_time = std::time::Instant::now();
        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;
        let elapsed = start_time.elapsed();

        // Should succeed after retries
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // Should have taken at least the test initial delay time due to retries
        assert!(elapsed >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_backfill_events_zero_blocks() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 100,
        };

        // No RPC calls should be made when deployment block > end block
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = backfill_events(&pool, &provider, &config, 50).await;
        assert!(result.is_ok());

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_mixed_log_types() {
        let pool = setup_test_db().await;
        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        let order = get_test_order();

        // Create a ClearV3 event
        let clear_event = IOrderBookV5::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: IOrderBookV5::ClearConfigV2 {
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
                address: config.orderbook,
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

        // Create a TakeOrderV3 event
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            config.orderbook,
            &take_event,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        asserter.push_success(&serde_json::json!([take_log])); // take events

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result =
            enqueue_batch_events(&pool, &provider, &config, 100, 200, test_retry_strategy()).await;

        // Should process both event types
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Verify both events were enqueued
        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_backfill_resumes_from_last_processed_block() {
        let pool = setup_test_db().await;

        // Insert a processed event at block 100
        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 100, '{}', 1)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 50, // Earlier than processed block
        };

        // Mock provider should only receive requests for blocks 101-200, not 50-200
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events for 101-200
        asserter.push_success(&serde_json::json!([])); // take events for 101-200

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Should start from block 101 (last processed + 1), not deployment_block
        backfill_events(&pool, &provider, &config, 200)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_backfill_initial_run_starts_from_deployment() {
        let pool = setup_test_db().await;

        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 50,
        };

        // No processed events exist, should start from deployment_block
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events for 50-100
        asserter.push_success(&serde_json::json!([])); // take events for 50-100

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 100)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_backfill_skips_when_caught_up() {
        let pool = setup_test_db().await;

        // Insert processed event at block 150
        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 150, '{}', 1)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 50,
        };

        // No RPC calls should be made since we're already caught up
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Last processed: 150, end_block: 150, so start would be 151 > 150
        backfill_events(&pool, &provider, &config, 150)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_backfill_handles_mixed_processed_states() {
        let pool = setup_test_db().await;

        // Insert events with mixed processed states - only processed ones should affect resume point
        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES 
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 50, '{}', 1),
                ('0x2222222222222222222222222222222222222222222222222222222222222222', 0, 75, '{}', 0),
                ('0x3333333333333333333333333333333333333333333333333333333333333333', 0, 100, '{}', 1),
                ('0x4444444444444444444444444444444444444444444444444444444444444444', 0, 125, '{}', 0)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let config = EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
            deployment_block: 1,
        };

        // Should resume from block 101 (max processed block 100 + 1)
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events for 101-200
        asserter.push_success(&serde_json::json!([])); // take events for 101-200

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(&pool, &provider, &config, 200)
            .await
            .unwrap();
    }
}
