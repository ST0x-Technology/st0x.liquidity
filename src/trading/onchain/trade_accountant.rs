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
use apalis_codec::json::JsonCodec;
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{CompactType, SqliteStorage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, trace};

use st0x_event_sorcery::{SendError, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::ExecutionError;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::alpaca_trading_api::AlpacaTradingApiError;

use super::inclusion::EmittedOnChain;
use crate::conductor::job::{Job, Label};
use crate::conductor::{
    TradeProcessingCqrs, VaultDiscoveryCtx, discover_vaults_for_trade, process_queued_trade,
};
use crate::config::Ctx;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::RaindexTradeEvent;
use crate::onchain::{OnChainError, OnchainTrade};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::vault_registry::VaultRegistry;

/// Persistent job queue for DEX trade accounting.
pub(crate) type DexTradeAccountingJobQueue =
    SqliteStorage<AccountForDexTrade, JsonCodec<CompactType>, SqliteFetcher>;

/// An accounting job for processing a single onchain raindex trade event.
/// It's the unified mechanism for processing both backfilled events as well as
/// live events from the monitor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AccountForDexTrade {
    /// Raindex trade event with block inclusion metadata
    pub(crate) trade: EmittedOnChain<RaindexTradeEvent>,
}

/// Bundles the shared dependencies needed by the trade accounting job.
pub(crate) struct AccountantCtx<Node> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) feed_id_cache: FeedIdCache,
    pub(crate) orderbook: Address,
    pub(crate) evm: ReadOnlyEvm<Node>,
    pub(crate) cqrs: TradeProcessingCqrs,
    pub(crate) vault_registry: Arc<Store<VaultRegistry>>,
}

impl<Node> Job<AccountantCtx<Node>> for AccountForDexTrade
where
    Node: Provider + Clone + Send + Sync + 'static,
{
    type Error = TradeAccountingError;

    fn label(&self) -> Label {
        let EmittedOnChain {
            event,
            block_number,
            log_index,
            ..
        } = &self.trade;

        Label::new(format!("{}:{block_number}:{log_index}", event.kind()))
    }

    async fn perform(&self, ctx: &AccountantCtx<Node>) -> Result<(), Self::Error> {
        use RaindexTradeEvent::{ClearV3, TakeOrderV3};

        let trade_event = &self.trade;
        let order_owner = ctx.ctx.order_owner();
        let reconstructed_log = reconstruct_log(ctx.orderbook, trade_event);

        let onchain_trade = match &trade_event.event {
            ClearV3(clear_event) => {
                OnchainTrade::try_from_clear_v3(
                    &ctx.ctx.evm,
                    &ctx.cache,
                    &ctx.evm,
                    *clear_event.clone(),
                    reconstructed_log,
                    &ctx.feed_id_cache,
                    order_owner,
                )
                .await?
            }

            TakeOrderV3(take_event) => {
                OnchainTrade::try_from_take_order_if_target_owner(
                    &ctx.cache,
                    &ctx.evm,
                    *take_event.clone(),
                    reconstructed_log,
                    order_owner,
                    &ctx.feed_id_cache,
                )
                .await?
            }
        };

        let Some(trade) = onchain_trade else {
            info!(
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

        trace!(
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

        process_queued_trade(trade_event, trade, &ctx.cqrs).await?;

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
    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    // TODO: shouldn't be coupled to a concrete executor
    #[error("Alpaca trading API error: {0}")]
    AlpacaTradingApi(#[from] AlpacaTradingApiError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Position projection error: {0}")]
    PositionProjection(#[from] st0x_event_sorcery::ProjectionError<crate::position::Position>),
    #[error("Position error: {0}")]
    Position(#[from] crate::position::PositionError),
    #[error(transparent)]
    InvalidShares(#[from] st0x_execution::NotPositive<st0x_execution::FractionalShares>),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};

    use super::*;
    use crate::bindings::IOrderBookV6;
    use crate::bindings::IOrderBookV6::ClearConfigV2;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::offchain_order::OffchainOrder;
    use crate::onchain_trade::OnChainTrade;
    use crate::position::Position;
    use crate::test_utils::{get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;

    fn test_job() -> AccountForDexTrade {
        let log = get_test_log();
        let event = RaindexTradeEvent::ClearV3(Box::new(IOrderBookV6::ClearV3 {
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
        let label: Label = <AccountForDexTrade as Job<AccountantCtx<RootProvider>>>::label(&job);

        let label_str = label.to_string();
        assert_eq!(label_str, "ClearV3:12345:293");
    }

    #[tokio::test]
    async fn perform_returns_ok_when_event_filtered_out() {
        let pool = setup_test_db().await;
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        // order_owner = Address::ZERO won't match test orders (owned by 0xdddd...)
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (_offchain_order, _offchain_order_projection) =
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
            execution_threshold: ExecutionThreshold::whole_share(),
        };

        let accountant_ctx = AccountantCtx {
            orderbook: ctx.evm.orderbook,
            ctx,
            cache: SymbolCache::default(),
            feed_id_cache: FeedIdCache::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
        };

        let job = test_job();
        job.perform(&accountant_ctx).await.unwrap();
    }
}
