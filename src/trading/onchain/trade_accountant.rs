//! [`Job`] implementation for processing order fill events.
//!
//! [`OrderFillJob`] carries a [`QueuedEvent`] through apalis
//! storage. Its [`execute`] method converts the event to a trade,
//! discovers vaults, and runs the hedging pipeline.
//!
//! [`execute`]: super::job::Job::execute

use alloy::providers::Provider;
use std::sync::Arc;
use tracing::{debug, info};

use st0x_event_sorcery::Store;
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;

use super::job::{Job, Label};
use super::order_fill_monitor::OrderFillJob;
use super::{
    OrderFillError, TradeProcessingCqrs, VaultDiscoveryCtx, convert_event_to_trade,
    discover_vaults_for_trade, process_queued_trade,
};
use crate::config::Ctx;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::TradeEvent;
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::vault_registry::VaultRegistry;

pub(crate) struct AccountForDexTrade {
    trade: ChainIncluded<TradeEvent>,
}

/// Bundles everything the order fill processing job needs.
///
/// Wrapped in an [`Arc`] and injected via apalis [`Data`] so the
/// handler can access shared state without generics leaking into
/// apalis.
///
/// [`Data`]: apalis::prelude::Data
pub(crate) struct AccountantCtx<Node, Exec> {
    pub(crate) cache: SymbolCache,
    pub(crate) feed_id_cache: FeedIdCache,
    pub(crate) orderbook: Address,
    pub(crate) evm: ReadOnlyEvm<Node>,
    pub(crate) cqrs: TradeProcessingCqrs,
    pub(crate) vault_registry: Arc<Store<VaultRegistry>>,
    pub(crate) executor: Exec,
}

impl<Node, Exec> Job for DexTradeAccountingJob
where
    Node: Provider + Clone + Send + Sync + 'static,
    Broker: Executor + Clone + Send + 'static,
    DexTradeAccountingError: From<Broker::Error>,
{
    type Error = DexTradeAccountingError;

    fn label(&self) -> Label {
        let (ChainIncluded {
            event,
            block_number: block,
            log_index: idx,
        }) = &self.queued_event;

        Label::new(format!("{event_type}:{block}:{idx}"))
    }

    async fn perform(&self, ctx: &Accou<Prov, Exec>) -> Result<(), Self::Error> {
        let queued_event = &self.queued_event;
        let order_owner = ctx.ctx.order_owner();

        let reconstructed_log = reconstruct_log(&ctx.orderbook, queued_event);

        let onchain_trade = match &queued_event.event {
            RaindexTradeEvent::ClearV3(clear_event) => {
                OnchainTrade::try_from_clear_v3(
                    &ctx.evm,
                    cache,
                    evm,
                    *clear_event.clone(),
                    reconstructed_log,
                    feed_id_cache,
                    order_owner,
                )
                .await?
            }

            RaindexTradeEvent::TakeOrderV3(take_event) => {
                OnchainTrade::try_from_take_order_if_target_owner(
                    cache,
                    evm,
                    *take_event.clone(),
                    reconstructed_log,
                    order_owner,
                    feed_id_cache,
                )
                .await?
            }
        };

        let Some(trade) = onchain_trade else {
            info!(
                event_type = match &queued_event.,
                tx_hash = ?queued_event.tx_hash,
                log_index = queued_event.log_index,
                "Event filtered out (no matching owner)"
            );

            return Ok(());
        };

        let vault_discovery_ctx = VaultDiscoveryCtx {
            vault_registry: &ctx.vault_registry,
            orderbook: ctx.ctx.evm.orderbook,
            order_owner,
        };

        debug!(
            tx_hash = trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            amount = %trade.amount,
            "Processing a {event_type} event"
            event_type = match &queued_event.event {
                TradeEvent::ClearV3(_) => "ClearV3",
                TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
            },
        );

        discover_vaults_for_trade(queued_event, &trade, &vault_discovery_ctx).await?;

        let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
        let _guard = symbol_lock.lock().await;

        let trading_enabled = ctx.ctx.is_trading_enabled(trade.symbol.base());

        process_queued_trade(
            &ctx.executor,
            queued_event,
            trade,
            &ctx.cqrs,
            trading_enabled,
        )
        .await?;

        Ok(())
    }
}

fn reconstruct_log(orderbook: Address, trade: &ChainIncluded<TradeEvent>) -> Log {
    use RaindexTradeEvent::{ClearV3, TakeOrderV3};

    let log_data = match &queued_event.event {
        ClearV3(clear_event) => clear_event.as_ref().clone().into_log_data(),
        TradeEvent::TakeOrderV3(take_event) => take_event.as_ref().clone().into_log_data(),
    };

    let block_timestamp = queued_event
        .block_timestamp
        .and_then(|dt| u64::try_from(dt.timestamp()).ok());

    Log {
        inner: alloy::primitives::Log {
            address: orderbook,
            data: log_data,
        },
        block_hash: None,
        block_number: Some(queued_event.block_number),
        block_timestamp,
        transaction_hash: Some(queued_event.tx_hash),
        transaction_index: None,
        log_index: Some(queued_event.log_index),
        removed: false,
    }
}

/// Event processing errors for live event handling.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DexTradeAccountingError {
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    // TODO: shouldn't be coupled to a concrete executor
    #[error("Alpaca trading API error: {0}")]
    AlpacaTradingApi(#[from] AlpacaTradingApiError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};

    use super::*;
    use crate::bindings::IOrderBookV6::ClearConfigV2;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::offchain_order::OffchainOrder;
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::queue::QueuedEvent;
    use crate::test_utils::{get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn test_order_fill_job() -> OrderFillJob {
        let log = get_test_log();
        let event = TradeEvent::ClearV3(Box::new(crate::bindings::IOrderBookV6::ClearV3 {
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

        OrderFillJob {
            queued_event: QueuedEvent::from_log(event, &log).unwrap(),
        }
    }

    #[test]
    fn label_returns_order_fill() {
        let job = test_order_fill_job();
        let label: Label =
            <OrderFillJob as Job<OrderFillCtx<RootProvider, MockExecutor>>>::label(&job);
        assert_eq!(label.to_string(), "order-fill");
    }

    #[tokio::test]
    async fn execute_returns_ok_when_event_filtered_out() {
        let pool = setup_test_db().await;
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
                .build(crate::offchain_order::noop_order_placer())
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
        };

        let order_fill_ctx = OrderFillCtx {
            ctx,
            cache: SymbolCache::default(),
            feed_id_cache: FeedIdCache::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
        };

        let job = test_order_fill_job();
        let result = job.perform(&order_fill_ctx).await;

        assert!(
            result.is_ok(),
            "Filtered event should return Ok, got: {result:?}"
        );
    }
}
