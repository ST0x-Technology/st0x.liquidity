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
use tracing::{debug, error, info};

use st0x_config::Ctx;
use st0x_event_sorcery::{SendError, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::{ExecutionError, Executor};
use st0x_registry::{SymbolCache, get_symbol_lock};

use super::inclusion::EmittedOnChain;
use super::skipped_fill::{SkipReason, record_skipped_fill};
use crate::conductor::job::{Job, JobQueue, Label};
use crate::conductor::{
    TradeProcessingCqrs, VaultDiscoveryCtx, discover_vaults_for_trade, process_queued_trade,
};
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::trade::{RaindexTradeEvent, TradeValidationError};
use crate::onchain::{OnChainError, OnchainTrade};
use crate::vault_registry::VaultRegistry;

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
        // The Raindex order/vault owner — the inventory contract post-migration,
        // the bot EOA before it. Used to match ClearV3/TakeOrderV3 fills to our
        // orders and to scope vault discovery.
        let order_owner = ctx.ctx.vault_owner();
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
                persist_skipped_fill(
                    &ctx.pool,
                    trade_event,
                    SkipReason::NonHedgeablePair,
                    &format!("input {input}, output {output}"),
                )
                .await;
                return Ok(());
            }
            Err(OnChainError::Validation(error @ TradeValidationError::NonPositivePrice(_))) => {
                // An unpriceable fill (non-zero equity moved at a non-positive
                // USDC/share price) is anomalous and possibly adversarial. Surface
                // it loudly and skip THIS fill only -- propagating the error would
                // exhaust the worker retries and trip the conductor-wide fail-stop,
                // letting one crafted on-chain fill take the whole bot down.
                error!(
                    target: "hedge",
                    event_type = trade_event.event.kind(),
                    tx_hash = ?trade_event.tx_hash,
                    log_index = trade_event.log_index,
                    %error,
                    "Skipping unpriceable on-chain fill; it is left unhedged and must be \
                     reconciled manually"
                );
                persist_skipped_fill(
                    &ctx.pool,
                    trade_event,
                    SkipReason::UnpriceableFill,
                    &error.to_string(),
                )
                .await;
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

        let trading_enabled = ctx.ctx.assets.is_trading_enabled(trade.symbol.base());

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

/// Best-effort durable record of a skipped fill for manual reconciliation. A
/// persistence failure is logged but never propagated: the whole point of the
/// skip is to not fail the job, so a write hiccup must not resurrect the
/// fail-stop it exists to avoid.
async fn persist_skipped_fill(
    pool: &SqlitePool,
    trade_event: &EmittedOnChain<RaindexTradeEvent>,
    reason: SkipReason,
    detail: &str,
) {
    if let Err(persist_error) = record_skipped_fill(
        pool,
        trade_event.tx_hash,
        trade_event.log_index,
        trade_event.event.kind(),
        reason,
        detail,
    )
    .await
    {
        error!(
            target: "hedge",
            ?persist_error,
            tx_hash = ?trade_event.tx_hash,
            log_index = trade_event.log_index,
            "Failed to persist skipped fill for reconciliation"
        );
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
    #[error("Failed to enqueue follow-up job: {0}")]
    EnqueueJob(#[from] crate::conductor::job::QueuePushError),
    #[error("Position fill lookup failed: {0}")]
    PositionFillLookup(#[from] crate::conductor::PositionFillLookupError),
    #[error("Missing block_timestamp for fill {trade_id}; cannot account for it")]
    MissingBlockTimestamp {
        trade_id: crate::onchain_trade::OnChainTradeId,
    },
    #[error(
        "Offchain order {offchain_order_id} in unexpected state {state:?} directly after Place; \
         retrying without clearing the position claim"
    )]
    UnexpectedPostPlaceState {
        offchain_order_id: crate::offchain::order::OffchainOrderId,
        state: crate::offchain::order::OffchainOrder,
    },
    #[error("Witness command for fill {trade_id} was rejected but the trade is missing on reload")]
    InconsistentOnChainTradeState {
        trade_id: crate::onchain_trade::OnChainTradeId,
    },
    #[error("Failed to determine market session before placing hedge for {symbol}")]
    MarketSessionCheck {
        symbol: st0x_execution::Symbol,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to fetch latest trade price for {symbol}")]
    LimitPriceFetch {
        symbol: st0x_execution::Symbol,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Executor does not support fetching trade prices for {symbol}")]
    LimitPriceUnavailable { symbol: st0x_execution::Symbol },
    #[error("Slippage calculation failed")]
    SlippageCalculation(#[from] crate::trading::offchain::hedge::SlippageError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use st0x_config::ExecutionThreshold;
    use st0x_config::create_test_ctx_with_order_owner;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::IERC20::{decimalsCall, symbolCall};
    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};

    use super::*;
    use crate::bindings::IRaindexV6;
    use crate::bindings::IRaindexV6::{
        AfterClearV2, ClearConfigV2, ClearStateChangeV2, SignedContextV1, TakeOrderConfigV4,
        TakeOrderV3 as TakeOrderV3Event,
    };
    use crate::offchain::order::{OffchainOrder, noop_order_placer};
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::test_utils::{get_test_log, get_test_order, setup_test_pools};

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
                .build(noop_order_placer())
                .await
                .unwrap();

        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            pool: pool.clone(),
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            order_placer: noop_order_placer(),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(&apalis_pool),
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
                .build(noop_order_placer())
                .await
                .unwrap();

        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            pool: pool.clone(),
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            order_placer: noop_order_placer(),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(&apalis_pool),
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

    /// A fill whose USDC/share price is non-positive (zero USDC against non-zero
    /// equity) is anomalous and possibly adversarial. `perform` must surface it
    /// and skip THIS fill only -- returning Ok(()) -- rather than propagating the
    /// error, which would exhaust the worker retries and trip the conductor-wide
    /// fail-stop, letting one crafted fill take the whole bot down.
    #[tokio::test]
    async fn perform_skips_unpriceable_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        let order = get_test_order();
        let order_owner = order.owner;
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash = alloy::primitives::fixed_bytes!(
            "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );

        let clear_event = IRaindexV6::ClearV3 {
            sender: orderbook,
            alice: order.clone(),
            bob: order,
            clearConfig: ClearConfigV2 {
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
                address: orderbook,
                data: clear_event.clone().into_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let event = RaindexTradeEvent::ClearV3(Box::new(clear_event));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &clear_log).unwrap(),
        };

        // AfterClearV2 crediting alice 9 shares (aliceOutput) for 0 USDC
        // (aliceInput): a non-zero equity movement at a zero price -> the price
        // per share computes to zero -> NonPositivePrice.
        let after_clear = AfterClearV2 {
            sender: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: Float::from_fixed_decimal(U256::from(9), 0)
                    .unwrap()
                    .get_inner(),
                bobOutput: Float::from_fixed_decimal(U256::from(100), 0)
                    .unwrap()
                    .get_inner(),
                aliceInput: Float::from_fixed_decimal(U256::from(0), 0)
                    .unwrap()
                    .get_inner(),
                bobInput: Float::from_fixed_decimal(U256::from(9), 0)
                    .unwrap()
                    .get_inner(),
            },
        };

        let after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear.into_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        // get_logs returns the AfterClearV2, then a receipt for gas. Symbols come
        // from the pre-seeded address cache below (not RPC mocks): resolving
        // input/output concurrently via tokio::try_join! means two positional
        // decimals()+symbol() mocks are not guaranteed to land on the token that
        // actually queried them, which can silently swap which side is "equity"
        // and mask the very zero-price case this test exists to catch.
        asserter.push_success(&serde_json::json!([after_clear_log]));
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

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(order_owner);
        let cache = SymbolCache::default();
        crate::test_utils::seed_get_test_order_token_symbols(&cache);

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
                .build(noop_order_placer())
                .await
                .unwrap();

        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            pool: pool.clone(),
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            order_placer: noop_order_placer(),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(&apalis_pool),
        };

        let job_queue = DexTradeAccountingJobQueue::new(&apalis_pool);

        let accountant_ctx = AccountantCtx {
            orderbook: ctx.evm.orderbook,
            ctx,
            cache,
            pyth_feed_ids: PythFeedIds::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
            pool: pool.clone(),
            job_queue,
        };

        // Should surface and skip (Ok) rather than propagate the error.
        job.perform(&accountant_ctx).await.unwrap();

        // ...and durably record the skipped fill for manual reconciliation,
        // not leave it visible only in a log line.
        let recorded = sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].log_index, 1);
        assert_eq!(recorded[0].reason, "unpriceable_fill");
    }
}
