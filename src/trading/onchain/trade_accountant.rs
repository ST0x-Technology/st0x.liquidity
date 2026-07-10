//! [`Job`] implementation for accounting onchain DEX trades.
//!
//! [`AccountForDexTrade`] carries a [`ChainIncluded`] raindex event.
//! Its [`perform`] method converts the event to a trade, discovers
//! vaults, and runs the hedging pipeline.
//!
//! [`perform`]: Job::perform

use alloy::primitives::IntoLogData;
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
use st0x_raindex::RaindexContracts;
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
    /// Orderbook and shared `RaindexInventory` addresses -- the latter is the
    /// source contract for `InventoryTrade` events
    /// (`OperatorDeposit`/`OperatorWithdraw`). Used to reconstruct the
    /// emitting log's address for downstream processing.
    pub(crate) contracts: RaindexContracts,
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
        use RaindexTradeEvent::{ClearV3, InventoryTrade, TakeOrderV3};

        let trade_event = &self.trade;
        // The Raindex order/vault owner -- the inventory contract post-migration,
        // the bot EOA before it. Used to match ClearV3/TakeOrderV3 fills to our
        // orders and to scope vault discovery.
        let order_owner = ctx.ctx.vault_owner();
        let reconstructed_log = reconstruct_log(ctx.contracts, trade_event);

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

            InventoryTrade(inv) => {
                // No pre-fetched receipt here (this event may be reconstructed
                // from a stored backfill log, not a fresh RPC round-trip), so
                // `try_from_inventory_trade` fetches its own receipt metadata.
                OnchainTrade::try_from_inventory_trade(
                    &ctx.cache,
                    &ctx.evm,
                    &ctx.ctx.assets,
                    inv.as_ref(),
                    reconstructed_log,
                    &ctx.pyth_feed_ids,
                    None,
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
            Err(OnChainError::Validation(
                error @ TradeValidationError::TokenIntrospectionFailed { .. },
            )) => {
                // InventoryTrade token addresses come from any OPERATOR_ROLE
                // holder on the shared inventory (Bebop hook, univ4 hook, or a
                // future venue), not the bot's own trusted order config. A
                // non-standard token there must not be allowed to exhaust
                // worker retries and trip the conductor-wide fail-stop --
                // skip THIS fill only, same as an unpriceable fill.
                error!(
                    target: "hedge",
                    event_type = trade_event.event.kind(),
                    tx_hash = ?trade_event.tx_hash,
                    log_index = trade_event.log_index,
                    %error,
                    "Skipping InventoryTrade fill with an unintrospectable token; it is \
                     left unhedged and must be reconciled manually"
                );
                persist_skipped_fill(
                    &ctx.pool,
                    trade_event,
                    SkipReason::UnintrospectableToken,
                    &error.to_string(),
                )
                .await;
                return Ok(());
            }
            Err(OnChainError::Validation(
                error @ TradeValidationError::UnrecognizedInventoryToken { .. },
            )) => {
                // An InventoryTrade leg's address didn't match the configured
                // canonical address for the symbol it claims to be (USDC or
                // the resolved equity). Same threat model as
                // TokenIntrospectionFailed: any OPERATOR_ROLE holder can
                // supply this token, so a spoofed/misconfigured one must not
                // exhaust worker retries and trip the conductor-wide
                // fail-stop -- skip THIS fill only.
                error!(
                    target: "hedge",
                    event_type = trade_event.event.kind(),
                    tx_hash = ?trade_event.tx_hash,
                    log_index = trade_event.log_index,
                    %error,
                    "Skipping InventoryTrade fill with an unrecognized token address; it is \
                     left unhedged and must be reconciled manually"
                );
                persist_skipped_fill(
                    &ctx.pool,
                    trade_event,
                    SkipReason::UnrecognizedInventoryToken,
                    &error.to_string(),
                )
                .await;
                return Ok(());
            }
            Err(OnChainError::Validation(
                error @ TradeValidationError::InvalidInventoryAmount(_),
            )) => {
                // InventoryTrade deposit/withdraw amounts come from any
                // OPERATOR_ROLE holder on the shared inventory, same threat
                // model as TokenIntrospectionFailed above: a malformed or
                // extreme amount must not exhaust worker retries and trip
                // the conductor-wide fail-stop -- skip THIS fill only.
                error!(
                    target: "hedge",
                    event_type = trade_event.event.kind(),
                    tx_hash = ?trade_event.tx_hash,
                    log_index = trade_event.log_index,
                    %error,
                    "Skipping InventoryTrade fill with an unconvertible amount; it is \
                     left unhedged and must be reconciled manually"
                );
                persist_skipped_fill(
                    &ctx.pool,
                    trade_event,
                    SkipReason::InvalidInventoryAmount,
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
            orderbook: ctx.contracts.orderbook,
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

fn reconstruct_log(contracts: RaindexContracts, trade: &EmittedOnChain<RaindexTradeEvent>) -> Log {
    use RaindexTradeEvent::{ClearV3, InventoryTrade, TakeOrderV3};

    // Pick the emitting contract for each event type. InventoryTrade events
    // are emitted by the inventory contract; the paired-event log data
    // encodes the OperatorWithdraw (the equity-side change the hedge cares
    // about), which is enough for the downstream parser.
    let (address, log_data) = match &trade.event {
        ClearV3(clear_event) => (
            contracts.orderbook,
            clear_event.as_ref().clone().into_log_data(),
        ),
        TakeOrderV3(take_event) => (
            contracts.orderbook,
            take_event.as_ref().clone().into_log_data(),
        ),
        InventoryTrade(inv) => (contracts.inventory, inv.withdraw.clone().into_log_data()),
    };

    let block_timestamp = trade
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address,
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
    use std::collections::HashMap;

    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use st0x_config::ExecutionThreshold;
    use st0x_config::create_test_ctx_with_order_owner;
    use st0x_config::{AssetsConfig, EquitiesConfig, EquityAssetConfig, OperationMode};
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::IERC20::{decimalsCall, symbolCall};
    use st0x_execution::{
        FractionalShares, MockExecutor, MockExecutorCtx, Positive, Symbol, TryIntoExecutor,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::bindings::IRaindexInventory::{OperatorDeposit, OperatorWithdraw};
    use crate::bindings::IRaindexV6;
    use crate::bindings::IRaindexV6::{
        AfterClearV2, ClearConfigV2, ClearStateChangeV2, SignedContextV1, TakeOrderConfigV4,
        TakeOrderV3 as TakeOrderV3Event,
    };
    use crate::offchain::order::{OffchainOrder, noop_order_placer};
    use crate::onchain::trade::{INVENTORY_TOKEN_DECIMALS_MAX_RETRIES, InventoryTrade};
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::test_utils::{get_test_log, get_test_order, panic_revert_payload, setup_test_pools};

    /// Builds the CQRS stores, job queue, and `AccountantCtx` shared by every
    /// `perform()` test in this module -- callers supply only what actually
    /// varies per test (config `ctx`, symbol cache, provider, executor, and
    /// execution threshold). Extracted because this ~30-line wiring block was
    /// duplicated verbatim across every InventoryTrade/ClearV3/TakeOrderV3
    /// `perform()` test.
    async fn build_test_accountant_ctx<Node, Exec>(
        pool: SqlitePool,
        apalis_pool: &apalis_sqlite::SqlitePool,
        ctx: Ctx,
        cache: SymbolCache,
        provider: Node,
        executor: Exec,
        execution_threshold: ExecutionThreshold,
    ) -> AccountantCtx<Node, Exec>
    where
        Node: Provider + Clone,
    {
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
            execution_threshold,
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(apalis_pool),
            hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(apalis_pool),
        };

        let job_queue = DexTradeAccountingJobQueue::new(apalis_pool);

        AccountantCtx {
            contracts: crate::onchain::raindex_contracts(&ctx.evm),
            ctx,
            cache,
            pyth_feed_ids: PythFeedIds::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
            pool,
            job_queue,
        }
    }

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

        let accountant_ctx = build_test_accountant_ctx(
            pool,
            &apalis_pool,
            ctx,
            SymbolCache::default(),
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

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

        let accountant_ctx = build_test_accountant_ctx(
            pool,
            &apalis_pool,
            ctx,
            SymbolCache::default(),
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

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

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

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

    /// An `InventoryTrade` whose two vault deltas are the same non-USDC token
    /// (a non-hedgeable pair) must be routed through `try_from_inventory_trade`
    /// and skipped gracefully -- exercising the InventoryTrade accounting arm
    /// end to end without tripping the fail-stop.
    #[tokio::test]
    async fn perform_skips_inventory_trade_with_non_hedgeable_pair() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        // Both sides are 0xbbbb... (wtAAPL): no USDC leg, so the pair is
        // non-hedgeable and TradeDetails::try_from_io rejects it.
        let equity_token = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator: Address::ZERO,
                token: equity_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(2000000000000000000_U256),
            },
            withdraw: OperatorWithdraw {
                operator: Address::ZERO,
                token: equity_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(2000000000000000000_U256),
            },
        };

        let log = get_test_log();
        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

        // Reconstructed log's tx_hash is get_test_log's; the parser fetches the
        // receipt, then the deposit- then withdraw-token decimals() (both 18).
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
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        crate::test_utils::seed_get_test_order_token_symbols(&cache);

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

        // Should succeed (skip) rather than error.
        job.perform(&accountant_ctx).await.unwrap();

        // ...and durably record the skipped inventory fill for reconciliation.
        let recorded = sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, "InventoryTrade");
        assert_eq!(recorded[0].reason, "non_hedgeable_pair");
    }

    /// An `InventoryTrade` whose USDC leg address does not match the
    /// configured canonical `USDC_BASE` -- even though its `symbol()`
    /// reports "USDC" -- must be classified as `UnrecognizedInventoryToken`
    /// and skipped gracefully through `perform()`, never hedged as if it
    /// were real USDC.
    #[tokio::test]
    async fn perform_skips_inventory_trade_with_unrecognized_token() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        // 0xaaaa... resolves to "USDC" via the seeded cache but is NOT the
        // configured canonical USDC_BASE address -- a spoof.
        let spoof_usdc = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let equity_token = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator: Address::ZERO,
                token: spoof_usdc,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(160000000_U256),
            },
            withdraw: OperatorWithdraw {
                operator: Address::ZERO,
                token: equity_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(2000000000000000000_U256),
            },
        };

        let log = get_test_log();
        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

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
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        crate::test_utils::seed_get_test_order_token_symbols(&cache);

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

        // Should succeed (skip) rather than error -- a spoofed token address
        // must not trip the fail-stop.
        job.perform(&accountant_ctx).await.unwrap();

        let recorded = sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, "InventoryTrade");
        assert_eq!(recorded[0].reason, "unrecognized_inventory_token");
    }

    /// An `InventoryTrade` whose deposit token fails `decimals()`
    /// introspection (a non-standard or reverting ERC20 -- these addresses
    /// come from any OPERATOR_ROLE holder, not the bot's own trusted order
    /// config) must be classified as `TokenIntrospectionFailed` and skipped
    /// gracefully through `perform()` rather than tripping the fail-stop.
    #[tokio::test]
    async fn perform_skips_inventory_trade_with_unintrospectable_token() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        let usdc_token = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let equity_token = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator: Address::ZERO,
                token: usdc_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(5_000_000_U256),
            },
            withdraw: OperatorWithdraw {
                operator: Address::ZERO,
                token: equity_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(2_000_000_000_000_000_000_U256),
            },
        };

        let log = get_test_log();
        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

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
        // Both token symbols are preloaded into the cache below, so the only
        // RPC introspection left is the two decimals() calls, each of which
        // makes 1 + INVENTORY_TOKEN_DECIMALS_MAX_RETRIES attempts. Seed a
        // genuine revert for every attempt of both legs: whichever leg
        // tokio::try_join! resolves first then fails with the same
        // TokenIntrospectionFailed classification, so the skip reason is
        // deterministic regardless of polling order. A bare push_failure_msg
        // would not do -- only a payload carrying revert data is classified as
        // a revert rather than a retryable transport blip.
        for _ in 0..2 * (1 + INVENTORY_TOKEN_DECIMALS_MAX_RETRIES) {
            asserter.push_failure(panic_revert_payload());
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        crate::test_utils::seed_get_test_order_token_symbols(&cache);

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

        // Should succeed (skip) rather than error, so a misbehaving
        // third-party token cannot exhaust retries and trip the fail-stop.
        job.perform(&accountant_ctx).await.unwrap();

        let recorded = sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, "InventoryTrade");
        assert_eq!(recorded[0].reason, "unintrospectable_token");
    }

    /// An `InventoryTrade` deposit amount that cannot be losslessly
    /// represented as a `rain_math_float` value (here `U256::MAX`, which
    /// deterministically overflows `Float::from_fixed_decimal` regardless of
    /// decimals) must be classified as `InvalidInventoryAmount` and skipped
    /// gracefully through `perform()` rather than tripping the fail-stop.
    #[tokio::test]
    async fn perform_skips_inventory_trade_with_invalid_amount() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        let usdc_token = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let equity_token = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator: Address::ZERO,
                token: usdc_token,
                vaultId: B256::ZERO,
                amount: U256::MAX,
            },
            withdraw: OperatorWithdraw {
                operator: Address::ZERO,
                token: equity_token,
                vaultId: B256::ZERO,
                amount: alloy::primitives::uint!(2_000_000_000_000_000_000_U256),
            },
        };

        let log = get_test_log();
        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

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
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let cache = SymbolCache::default();
        crate::test_utils::seed_get_test_order_token_symbols(&cache);

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            ExecutionThreshold::whole_share(),
        )
        .await;

        // Should succeed (skip) rather than error, so a malformed external
        // amount cannot exhaust retries and trip the fail-stop.
        job.perform(&accountant_ctx).await.unwrap();

        let recorded = sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, "InventoryTrade");
        assert_eq!(recorded[0].reason, "invalid_inventory_amount");
    }

    /// A hedgeable `InventoryTrade` (real USDC leg + real equity leg, from
    /// the real Bebop-routed prod settlement `0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9`
    /// also pinned at the parser level in `onchain::trade`'s
    /// `try_from_inventory_trade_real_bebop_fill_is_sell`) must flow all the
    /// way through `perform()`: the fill is recorded as an `OnChainTrade`,
    /// `Position` advances by the exact real fractional share amount, and --
    /// because that amount clears the (fractional, for this test)
    /// execution threshold -- a hedge order is placed inline. This is the
    /// InventoryTrade happy path; `perform_skips_inventory_trade_with_non_hedgeable_pair`
    /// above only covers the skip branch.
    #[tokio::test]
    async fn perform_hedges_inventory_trade_with_hedgeable_pair() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        // Real Base-mainnet USDC and wtCOIN addresses (same as
        // `onchain::trade`'s real-fixture tests).
        let usdc_token = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let equity_token = address!("0x5CdA0E1cA4ce2Af96315F7F8963c85399c172204");
        let operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");

        // The pool received 5 USDC (deposit) and sent 0.034172366621067031
        // wtCOIN (withdraw): it sold equity onchain, so it must hedge Sell.
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator,
                token: usdc_token,
                vaultId: alloy::primitives::b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000004"
                ),
                amount: alloy::primitives::uint!(5_000_000_U256), // 5 USDC, 6 decimals
            },
            withdraw: OperatorWithdraw {
                operator,
                token: equity_token,
                vaultId: alloy::primitives::b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000003"
                ),
                // 0.034172366621067031 wtCOIN, 18 decimals
                amount: alloy::primitives::uint!(34_172_366_621_067_031_U256),
            },
        };

        let tx_hash = alloy::primitives::fixed_bytes!(
            "0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9"
        );
        let mut log = crate::test_utils::create_log(0x97);
        log.transaction_hash = Some(tx_hash);
        log.block_number = Some(48_030_415);
        log.block_timestamp = Some(1_782_850_177);

        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

        // The parser fetches the receipt, then the deposit- (USDC, 6dp)
        // then withdraw-token (wtCOIN, 18dp) decimals().
        asserter.push_success(&serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x3b",
            "blockHash": "0x373307a0e2154c2de6b046e349fc27f9bb02b01fdddbb15eeba57f3ce3b24973",
            "blockNumber": "0x2dce2cf",
            "from": "0x679df30b30ac2947aa3143490add6717af81dcc3",
            "to": "0xbeb0009aca35087ce7ccf11637e24dd1aad3bf2a",
            "gasUsed": "0x7a62d",
            "effectiveGasPrice": "0x57bcf0",
            "cumulativeGasUsed": "0xa00b4e",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": []
        }));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtCOIN

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);

        // Trading must be explicitly enabled for COIN: `perform` computes
        // `trading_enabled` from `ctx.assets.is_trading_enabled`, which
        // defaults to disabled for any symbol absent from the config.
        // `tokenized_equity_derivative` must equal the real wtCOIN address
        // used above -- `try_from_inventory_trade` now validates the
        // InventoryTrade equity leg's address against this configured
        // canonical address, not just its self-reported symbol().
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("COIN").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: equity_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        ctx.assets = AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        let cache = SymbolCache::default();
        cache.preload_symbol(usdc_token, "USDC");
        cache.preload_symbol(equity_token, "wtCOIN");

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            // Below the real Bebop fill's 0.034172366621067031 wtCOIN so the
            // fractional real amount still clears the threshold and triggers
            // an inline hedge (the real fill is not a whole share).
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(0.01))).unwrap()),
        )
        .await;

        job.perform(&accountant_ctx).await.unwrap();

        // The fill was persisted as an OnChainTrade.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("OnChainTradeEvent::Filled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "the hedgeable fill must be recorded as an OnChainTrade"
        );

        // Position advanced by the exact real fractional amount (Sell
        // decreases net, increases accumulated_short), immediately hit the
        // execution threshold, and claimed a pending hedge.
        let expected_amount =
            Float::from_fixed_decimal(alloy::primitives::uint!(34_172_366_621_067_031_U256), 18)
                .unwrap();
        let expected_shares = FractionalShares::new(expected_amount);

        let symbol = Symbol::new("COIN").unwrap();
        let position = accountant_ctx
            .cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("Position should exist");
        assert_eq!(
            position.net,
            (FractionalShares::ZERO - expected_shares).unwrap()
        );
        assert_eq!(position.accumulated_short, expected_shares);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert!(
            position.pending_offchain_order_id.is_some(),
            "clearing the execution threshold must claim a pending hedge"
        );

        // ...and a hedge order was actually placed (not skipped).
        let (order_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE event_type = 'OffchainOrderEvent::Placed'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(order_count, 1, "the hedge order must be placed");
    }

    /// The real univ4-routed prod settlement (`0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805`,
    /// also pinned at the parser level in `onchain::trade`'s
    /// `try_from_inventory_trade_real_univ4_fill_is_buy`) must flow all the
    /// way through `perform()` too: the pool received wtCOIN (deposit) and
    /// sent USDC (withdraw), i.e. it bought equity onchain, so `Position`
    /// must advance in the Buy direction and a hedge order must be placed.
    /// `perform_hedges_inventory_trade_with_hedgeable_pair` above only
    /// exercises the Sell-direction real fixture; this is the only real
    /// fixture that proves the deposit-side-equity (Buy) wiring end to end.
    #[tokio::test]
    async fn perform_hedges_inventory_trade_with_real_univ4_buy_fixture() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let asserter = Asserter::new();

        // Real Base-mainnet wtCOIN and USDC addresses (same as
        // `onchain::trade`'s real-fixture tests).
        let equity_token = address!("0x5CdA0E1cA4ce2Af96315F7F8963c85399c172204");
        let usdc_token = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let operator = address!("0x36ebb1e5149c60111dd035f0417a4b00d39caa88");

        // The pool received 0.01 wtCOIN (deposit) and sent 2 USDC (withdraw):
        // it bought equity onchain, so it must hedge Buy.
        let inventory_trade = InventoryTrade {
            deposit: OperatorDeposit {
                operator,
                token: equity_token,
                vaultId: alloy::primitives::b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000003"
                ),
                amount: alloy::primitives::uint!(10_000_000_000_000_000_U256), // 0.01 wtCOIN, 18dp
            },
            withdraw: OperatorWithdraw {
                operator,
                token: usdc_token,
                vaultId: alloy::primitives::b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000004"
                ),
                amount: alloy::primitives::uint!(2_000_000_U256), // 2 USDC, 6dp
            },
        };

        let tx_hash = alloy::primitives::fixed_bytes!(
            "0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805"
        );
        let mut log = crate::test_utils::create_log(0xf4);
        log.transaction_hash = Some(tx_hash);
        log.block_number = Some(48_051_940);
        log.block_timestamp = Some(1_782_893_227);

        let event = RaindexTradeEvent::InventoryTrade(Box::new(inventory_trade));
        let job = AccountForDexTrade {
            trade: EmittedOnChain::from_log(event, &log).unwrap(),
        };

        // The parser fetches the receipt, then the deposit- (wtCOIN, 18dp)
        // then withdraw-token (USDC, 6dp) decimals().
        asserter.push_success(&serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x41",
            "blockHash": "0x4fb86ed2edeee5845f379874a140df6ed78a5553877a4a480878f2eb70f0efeb",
            "blockNumber": "0x2dd36e4",
            "from": "0x679df30b30ac2947aa3143490add6717af81dcc3",
            "to": "0xe23457189a0186b23e9f325eb11364b3733c2c89",
            "gasUsed": "0x54025",
            "effectiveGasPrice": "0x59a538",
            "cumulativeGasUsed": "0xa481c7",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": []
        }));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtCOIN
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);

        // Trading must be explicitly enabled for COIN: `perform` computes
        // `trading_enabled` from `ctx.assets.is_trading_enabled`, which
        // defaults to disabled for any symbol absent from the config.
        // `tokenized_equity_derivative` must equal the real wtCOIN address
        // used above -- `try_from_inventory_trade` now validates the
        // InventoryTrade equity leg's address against this configured
        // canonical address, not just its self-reported symbol().
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("COIN").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: equity_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        ctx.assets = AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        let cache = SymbolCache::default();
        cache.preload_symbol(equity_token, "wtCOIN");
        cache.preload_symbol(usdc_token, "USDC");

        let accountant_ctx = build_test_accountant_ctx(
            pool.clone(),
            &apalis_pool,
            ctx,
            cache,
            provider,
            executor,
            // Below the real univ4 fill's 0.01 wtCOIN so it still clears the
            // threshold and triggers an inline hedge.
            ExecutionThreshold::shares(
                Positive::new(FractionalShares::new(float!(0.001))).unwrap(),
            ),
        )
        .await;

        job.perform(&accountant_ctx).await.unwrap();

        // The fill was persisted as an OnChainTrade.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("OnChainTradeEvent::Filled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "the hedgeable fill must be recorded as an OnChainTrade"
        );

        // Position advanced by the exact real fractional amount (Buy
        // increases net and accumulated_long), immediately hit the
        // execution threshold, and claimed a pending hedge.
        let expected_amount =
            Float::from_fixed_decimal(alloy::primitives::uint!(10_000_000_000_000_000_U256), 18)
                .unwrap();
        let expected_shares = FractionalShares::new(expected_amount);

        let symbol = Symbol::new("COIN").unwrap();
        let position = accountant_ctx
            .cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("Position should exist");
        assert_eq!(position.net, expected_shares);
        assert_eq!(position.accumulated_long, expected_shares);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert!(
            position.pending_offchain_order_id.is_some(),
            "clearing the execution threshold must claim a pending hedge"
        );

        // ...and a hedge order was actually placed (not skipped).
        let (order_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE event_type = 'OffchainOrderEvent::Placed'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(order_count, 1, "the hedge order must be placed");
    }

    /// `reconstruct_log` must attribute an `InventoryTrade` to the inventory
    /// contract (not the orderbook) and encode the `OperatorWithdraw` -- the
    /// equity-side change the downstream parser decodes.
    #[test]
    fn reconstruct_log_for_inventory_trade_uses_inventory_address_and_round_trips_withdraw() {
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let inventory = address!("0x2222222222222222222222222222222222222222");

        let withdraw = OperatorWithdraw {
            operator: address!("0x3333333333333333333333333333333333333333"),
            token: address!("0x4444444444444444444444444444444444444444"),
            vaultId: B256::ZERO,
            amount: U256::from(42u64),
        };
        let deposit = OperatorDeposit {
            operator: withdraw.operator,
            token: address!("0x5555555555555555555555555555555555555555"),
            vaultId: B256::ZERO,
            amount: U256::from(100u64),
        };

        let event = RaindexTradeEvent::InventoryTrade(Box::new(InventoryTrade {
            deposit,
            withdraw: withdraw.clone(),
        }));
        let emitted = EmittedOnChain::from_log(event, &get_test_log()).unwrap();

        let reconstructed = reconstruct_log(
            RaindexContracts {
                orderbook,
                inventory,
            },
            &emitted,
        );

        assert_eq!(reconstructed.address(), inventory);

        let decoded = reconstructed
            .log_decode::<OperatorWithdraw>()
            .unwrap()
            .data()
            .clone();
        assert_eq!(decoded.operator, withdraw.operator);
        assert_eq!(decoded.token, withdraw.token);
        assert_eq!(decoded.vaultId, withdraw.vaultId);
        assert_eq!(decoded.amount, withdraw.amount);
    }
}
