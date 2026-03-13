//! [`Job`] implementation for processing order fill events.
//!
//! [`OrderFillJob`] carries a [`QueuedEvent`] through apalis
//! storage. Its [`execute`] method converts the event to a trade,
//! discovers vaults, and runs the hedging pipeline.
//!
//! [`execute`]: super::job::Job::execute

use std::sync::Arc;

use alloy::providers::Provider;
use tracing::info;

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

/// Bundles everything the order fill processing job needs.
///
/// Wrapped in an [`Arc`] and injected via apalis [`Data`] so the
/// handler can access shared state without generics leaking into
/// apalis.
///
/// [`Data`]: apalis::prelude::Data
pub(crate) struct OrderFillCtx<Prov, Exec> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) feed_id_cache: FeedIdCache,
    pub(crate) evm: ReadOnlyEvm<Prov>,
    pub(crate) cqrs: TradeProcessingCqrs,
    pub(crate) vault_registry: Arc<Store<VaultRegistry>>,
    pub(crate) executor: Exec,
}

impl<Prov, Exec> Job<OrderFillCtx<Prov, Exec>> for OrderFillJob
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + 'static,
    OrderFillError: From<Exec::Error>,
{
    type Error = OrderFillError;

    fn label(&self) -> Label {
        Label::new("order-fill")
    }

    async fn execute(&self, ctx: &OrderFillCtx<Prov, Exec>) -> Result<(), Self::Error> {
        let queued_event = &self.queued_event;
        let order_owner = ctx.ctx.order_owner();

        let onchain_trade = convert_event_to_trade(
            &ctx.ctx,
            &ctx.cache,
            &ctx.evm,
            queued_event,
            &ctx.feed_id_cache,
            order_owner,
        )
        .await?;

        let Some(trade) = onchain_trade else {
            info!(
                event_type = match &queued_event.event {
                    TradeEvent::ClearV3(_) => "ClearV3",
                    TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
                },
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

        info!(
            event_type = match &queued_event.event {
                TradeEvent::ClearV3(_) => "ClearV3",
                TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
            },
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            amount = %trade.amount,
            "Event converted to trade, processing"
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
        let result = job.execute(&order_fill_ctx).await;

        assert!(
            result.is_ok(),
            "Filtered event should return Ok, got: {result:?}"
        );
    }
}
