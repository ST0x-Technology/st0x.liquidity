//! [`Job`] implementation for accounting onchain DEX trades.
//!
//! [`AccountForDexTrade`] carries a [`ChainIncluded`] raindex event.
//! Its [`perform`] method converts the event to a trade, discovers
//! vaults, and runs the hedging pipeline.
//!
//! [`perform`]: Job::perform

use alloy::primitives::{Address, IntoLogData};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::Arc;
use tracing::{debug, info};

use st0x_event_sorcery::{SendError, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::{ExecutionError, Executor};

use super::inclusion::EmittedOnChain;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::conductor::{
    TradeProcessingCqrs, VaultDiscoveryCtx, discover_vaults_for_trade, process_queued_trade,
};
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::trade::{RaindexTradeEvent, TradeValidationError};
use crate::onchain::{OnChainError, OnchainTrade};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::vault_registry::VaultRegistry;
use st0x_config::Ctx;

/// Persistent job queue for DEX trade accounting.
pub(crate) type DexTradeAccountingJobQueue = JobQueue<AccountForDexTrade>;

/// An accounting job for processing a single onchain raindex trade event.
/// It's the unified mechanism for processing both backfilled events as well as
/// live events from the monitor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountForDexTrade {
    /// Raindex trade event with block inclusion metadata
    pub(crate) trade: EmittedOnChain<RaindexTradeEvent>,
}

/// Bundles the shared dependencies needed by the trade accounting job.
pub(crate) struct AccountantCtx<Node, Exec> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) pyth_feed_ids: PythFeedIds,
    pub(crate) orderbook: Address,
    pub(crate) evm: ReadOnlyEvm<Node>,
    pub(crate) cqrs: TradeProcessingCqrs,
    pub(crate) vault_registry: Arc<Store<VaultRegistry>>,
    pub(crate) executor: Exec,
    pub(crate) pool: SqlitePool,
    pub(crate) job_queue: DexTradeAccountingJobQueue,
}

impl<Node, Exec> Job<AccountantCtx<Node, Exec>> for AccountForDexTrade
where
    Node: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + 'static,
    TradeAccountingError: From<Exec::Error>,
{
    type Output = ();
    type Error = TradeAccountingError;

    const WORKER_NAME: &'static str = "order-fill-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::OrderFill;

    fn label(&self) -> Label {
        let EmittedOnChain {
            event,
            block_number,
            log_index,
            ..
        } = &self.trade;

        Label::new(format!("{}:{block_number}:{log_index}", event.kind()))
    }

    #[allow(clippy::cognitive_complexity)]
    async fn perform(&self, ctx: &AccountantCtx<Node, Exec>) -> Result<Self::Output, Self::Error> {
        use RaindexTradeEvent::{ClearV3, TakeOrderV3};

        let trade_event = &self.trade;
        let order_owner = ctx.ctx.order_owner();
        let reconstructed_log = reconstruct_log(ctx.orderbook, trade_event);

        let trade_result = match &trade_event.event {
            ClearV3(clear_event) => {
                OnchainTrade::try_from_clear_v3(
                    &ctx.ctx.evm,
                    &ctx.cache,
                    &ctx.evm,
                    *clear_event.clone(),
                    reconstructed_log,
                    &ctx.pyth_feed_ids,
                    order_owner,
                )
                .await
            }

            TakeOrderV3(take_event) => {
                OnchainTrade::try_from_take_order_if_target_owner(
                    &ctx.cache,
                    &ctx.evm,
                    *take_event.clone(),
                    reconstructed_log,
                    order_owner,
                    &ctx.pyth_feed_ids,
                )
                .await
            }
        };

        let onchain_trade = match trade_result {
            Ok(trade) => trade,
            Err(OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(
                input,
                output,
            ))) => {
                info!(
                    target: "hedge",
                    event_type = trade_event.event.kind(),
                    tx_hash = ?trade_event.tx_hash,
                    log_index = trade_event.log_index,
                    input,
                    output,
                    "Skipping event with non-hedgeable token pair"
                );
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };

        let Some(trade) = onchain_trade else {
            debug!(
                target: "hedge",
                event_type = trade_event.event.kind(),
                tx_hash = ?trade_event.tx_hash,
                log_index = trade_event.log_index,
                "Event filtered out (no matching owner)"
            );

            return Ok(());
        };

        let vault_discovery_ctx = VaultDiscoveryCtx {
            vault_registry: &ctx.vault_registry,
            orderbook: ctx.orderbook,
            order_owner,
        };

        debug!(
            target: "hedge",
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            symbol = %trade.symbol,
            amount = %trade.amount,
            event_type = trade_event.event.kind(),
            "Processing trade event",
        );

        discover_vaults_for_trade(trade_event, &trade, &vault_discovery_ctx).await?;

        let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
        let _guard = symbol_lock.lock().await;

        let trading_enabled = ctx.ctx.is_trading_enabled(trade.symbol.base());

        process_queued_trade(
            &ctx.executor,
            trade_event,
            trade,
            &ctx.cqrs,
            trading_enabled,
        )
        .await?;

        Ok(())
    }
}

fn reconstruct_log(orderbook: Address, trade: &EmittedOnChain<RaindexTradeEvent>) -> Log {
    use RaindexTradeEvent::{ClearV3, TakeOrderV3};

    let log_data = match &trade.event {
        ClearV3(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TakeOrderV3(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = trade
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: orderbook,
            data: log_data,
        },
        block_hash: None,
        block_number: Some(trade.block_number),
        block_timestamp,
        transaction_hash: Some(trade.tx_hash),
        transaction_index: None,
        log_index: Some(trade.log_index),
        removed: false,
    }
}

/// Event processing errors for DEX trade accounting.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeAccountingError {
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Onchain trade command failed: {0}")]
    OnChainTradeCommand(#[from] SendError<crate::onchain_trade::OnChainTrade>),
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error("Position command failed: {0}")]
    PositionCommand(#[from] SendError<crate::position::Position>),
    #[error("Offchain order command failed: {0}")]
    OffchainOrderCommand(#[from] SendError<crate::offchain::order::OffchainOrder>),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    // TODO: TradeAccountingError should not be coupled to a concrete executor error type.
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Failed to enqueue PollOrderStatus job: {0}")]
    EnqueuePollJob(#[from] crate::conductor::job::QueuePushError),
    #[error("Missing block_timestamp for fill {trade_id}; cannot account for it")]
    MissingBlockTimestamp {
        trade_id: crate::onchain_trade::OnChainTradeId,
    },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::IERC20::{decimalsCall, symbolCall};
    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};

    use super::*;
    use crate::bindings::IRaindexV6;
    use crate::bindings::IRaindexV6::{
        ClearConfigV2, SignedContextV1, TakeOrderConfigV4, TakeOrderV3 as TakeOrderV3Event,
    };
    use crate::offchain::order::OffchainOrder;
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::test_utils::{get_test_log, get_test_order, setup_test_pools};
    use st0x_config::ExecutionThreshold;
    use st0x_config::create_test_ctx_with_order_owner;

    fn test_job() -> AccountForDexTrade {
        let log = get_test_log();
        let event = RaindexTradeEvent::ClearV3(Box::new(IRaindexV6::ClearV3 {
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
        }));

        AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        }
    }

    #[test]
    fn label_contains_event_type_and_block_info() {
        let job = test_job();
        let label: Label =
            <AccountForDexTrade as Job<AccountantCtx<RootProvider, MockExecutor>>>::label(&job);

        let label_str = label.to_string();
        assert_eq!(label_str, "ClearV3:12345:293");
    }

    #[tokio::test]
    async fn perform_returns_ok_when_event_filtered_out() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();

        // order_owner = Address::ZERO won't match test orders (owned by 0xdddd...)
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain::order::noop_order_placer())
                .await
                .unwrap();

        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
        };

        let job_queue = DexTradeAccountingJobQueue::new(&apalis_pool);

        let accountant_ctx = AccountantCtx {
            orderbook: ctx.evm.orderbook,
            ctx,
            cache: SymbolCache::default(),
            pyth_feed_ids: PythFeedIds::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
            pool: pool.clone(),
            job_queue,
        };

        let job = test_job();
        job.perform(&accountant_ctx).await.unwrap();
    }

    /// A TakeOrderV3 event with a non-hedgeable token pair (e.g. tNVDA/wtNVDA)
    /// should be skipped gracefully instead of causing a fatal error.
    #[tokio::test]
    async fn perform_skips_take_order_with_non_hedgeable_pair() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        // The order owner matches so the event passes the owner filter.
        let order = get_test_order();
        let order_owner = order.owner;

        let take_event = TakeOrderV3Event {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![SignedContextV1 {
                    signer: Address::ZERO,
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            input: Float::from_fixed_decimal_lossy(alloy::primitives::uint!(9_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(alloy::primitives::uint!(100_U256), 0)
                .unwrap()
                .0
                .get_inner(),
        };

        let log = get_test_log();
        let event = RaindexTradeEvent::TakeOrderV3(Box::new(take_event));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

        // Mock EVM responses: receipt first, then decimals()+symbol() for each token.
        let tx_hash = alloy::primitives::fixed_bytes!(
            "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );
        asserter.push_success(&serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x1",
            "blockHash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blockNumber": "0x1",
            "from": "0x1234567890123456789012345678901234567890",
            "to": "0x5678901234567890123456789012345678901234",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x77359400",
            "cumulativeGasUsed": "0x5208",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "logs": []
        }));

        // Input token (order's output due to TakeOrder perspective swap)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tNVDA".to_string(),
        ));

        // Output token (order's input)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"wtNVDA".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(order_owner);

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain::order::noop_order_placer())
                .await
                .unwrap();

        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
        };

        let job_queue = DexTradeAccountingJobQueue::new(&apalis_pool);

        let accountant_ctx = AccountantCtx {
            orderbook: ctx.evm.orderbook,
            ctx,
            cache: SymbolCache::default(),
            pyth_feed_ids: PythFeedIds::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
            pool: pool.clone(),
            job_queue,
        };

        // Should succeed (skip) rather than error.
        job.perform(&accountant_ctx).await.unwrap();
    }
}
