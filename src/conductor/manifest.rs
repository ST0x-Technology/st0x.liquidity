//! Query processor manifest with compile-time wiring guarantees.
//!
//! This module enumerates ALL query processors that must be wired to
//! CQRS frameworks. The exhaustive destructuring in
//! [`QueryManifest::build_frameworks()`] ensures that adding a new
//! query to `QueryManifest` forces you to wire it.
//!
//! # Adding a new query processor
//!
//! 1. Add field to [`QueryManifest`]
//! 2. Create it in [`QueryManifest::new()`]
//! 3. Wire it in [`QueryManifest::build()`] -
//!    destructuring forces you to handle it
//! 4. Add output to [`BuiltFrameworks`] if needed

use sqlx::SqlitePool;
use std::sync::Arc;

use st0x_event_sorcery::{Projection, Store, StoreBuilder};

use crate::dashboard::Broadcaster;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::InventorySnapshot;
use crate::performance::HedgeLatencyProjection;
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::performance::reliability::LifecycleFailureProjection;
use crate::position::Position;
use crate::rebalancing::{RebalancingService, equity::EquityTransferServices};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

/// All query processors that must be created and wired when
/// rebalancing is enabled.
///
/// Exhaustive destructuring in [`Self::build()`]
/// ensures every field is handled.
pub(super) struct QueryManifest {
    rebalancing_service: Arc<RebalancingService>,
    broadcaster: Arc<Broadcaster>,
    hedge_latency: Arc<HedgeLatencyProjection>,
    rebalance_timing: Arc<RebalanceTimingProjection>,
    lifecycle_failure: Arc<LifecycleFailureProjection>,
}

/// Built CQRS frameworks from the wiring process.
///
/// `WrappedEquityRecovery` is built outside this manifest because its
/// services include `CrossVenueEquityTransfer`, which is constructed
/// downstream (from the `mint`/`redemption` stores this manifest produces).
pub(super) struct BuiltFrameworks {
    pub(super) position: Arc<Store<Position>>,
    pub(super) position_projection: Arc<Projection<Position>>,
    pub(super) mint: Arc<Store<TokenizedEquityMint>>,
    pub(super) redemption: Arc<Store<EquityRedemption>>,
    pub(super) usdc: Arc<Store<UsdcRebalance>>,
    pub(super) snapshot: Arc<Store<InventorySnapshot>>,
}

impl QueryManifest {
    pub(super) fn new(
        rebalancing_service: Arc<RebalancingService>,
        broadcaster: Arc<Broadcaster>,
        hedge_latency: Arc<HedgeLatencyProjection>,
        rebalance_timing: Arc<RebalanceTimingProjection>,
        lifecycle_failure: Arc<LifecycleFailureProjection>,
    ) -> Self {
        Self {
            rebalancing_service,
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        }
    }

    /// Builds all CQRS frameworks, wiring query processors to each.
    ///
    /// Destructures `self` to ensure every field is handled. If you
    /// add a new query to the manifest, this method won't compile
    /// until you wire it.
    pub(super) async fn build(
        self,
        pool: SqlitePool,
        services: EquityTransferServices,
    ) -> anyhow::Result<BuiltFrameworks> {
        let Self {
            rebalancing_service,
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        } = self;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(rebalancing_service.clone())
            .with(broadcaster.clone())
            .with(hedge_latency)
            .build(())
            .await?;

        let mint = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(rebalancing_service.clone())
            .with(broadcaster.clone())
            .with(lifecycle_failure.clone())
            .build(())
            .await?;

        let redemption = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(rebalancing_service.clone())
            .with(broadcaster.clone())
            .with(lifecycle_failure.clone())
            .build(services)
            .await?;

        let usdc = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .with(rebalancing_service.clone())
            .with(broadcaster)
            .with(rebalance_timing)
            .with(lifecycle_failure)
            .build(())
            .await?;

        // The reactor's underlying trigger owns the snapshot projection
        // internally, so it's the sole subscriber here.
        let snapshot = StoreBuilder::<InventorySnapshot>::new(pool.clone())
            .with(rebalancing_service)
            .build(())
            .await?;

        Ok(BuiltFrameworks {
            position,
            position_projection,
            mint,
            redemption,
            usdc,
            snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, fixed_bytes};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::sync::broadcast;

    use st0x_dto::Statement;
    use st0x_event_sorcery::test_store;
    use st0x_execution::{Direction, FractionalShares, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::inventory::snapshot::{InventorySnapshotCommand, InventorySnapshotId};
    use crate::inventory::{
        BroadcastingInventory, ImbalanceThreshold, Inventory, InventoryView, Operator, Venue,
    };
    use crate::onchain::mock::MockRaindex;
    use crate::position::{PositionCommand, TradeId};
    use crate::rebalancing::equity::TransferEquityToMarketMaking;
    use crate::rebalancing::{
        RebalancingSchedulers, RebalancingService, RebalancingServiceConfig, drain_pending_jobs,
    };
    use crate::test_utils::{rebalancing_enabled_equities, setup_test_pools};
    use crate::tokenization::mock::MockTokenizer;
    use crate::vault_lookup::MockVaultLookup;
    use crate::vault_registry::{VaultRegistryCommand, VaultRegistryId};
    use st0x_config::{AssetsConfig, ExecutionThreshold};

    fn test_trigger_config() -> RebalancingServiceConfig {
        RebalancingServiceConfig {
            equity: ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.2),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!(0.6),
                deviation: float!(0.15),
            }),
            transfer_timeout: Duration::from_secs(30 * 60),
            assets: AssetsConfig {
                equities: rebalancing_enabled_equities(&["AAPL"]),
                cash: None,
            },
        }
    }

    #[tokio::test]
    async fn build_frameworks_produces_working_stores() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (event_sender, _event_receiver) = broadcast::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender.clone(),
        ));

        let rebalancing_service = Arc::new(RebalancingService::new(
            test_trigger_config(),
            vault_registry,
            Address::ZERO,
            Address::ZERO,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
        let hedge_latency = Arc::new(HedgeLatencyProjection::new(pool.clone()));
        let rebalance_timing = Arc::new(RebalanceTimingProjection::new(pool.clone()));
        let lifecycle_failure = Arc::new(LifecycleFailureProjection::new(pool.clone()));
        let manifest = QueryManifest::new(
            rebalancing_service,
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        );

        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(MockVaultLookup::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };

        let frameworks = manifest.build(pool, services).await.unwrap();

        // Verify stores are usable by checking that loading a
        // nonexistent position returns None
        let result = frameworks
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    /// Live snapshot commands dispatched through the built store must
    /// reach `BroadcastingInventory` via the trigger's internal
    /// projection. Historical replay on restart is out of scope —
    /// `InventorySnapshot` is non-projected, so `StoreBuilder::build`
    /// does not `catch_up` reactor subscribers.
    #[tokio::test]
    async fn build_frameworks_dispatches_live_snapshot_events_to_view() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (event_sender, _event_receiver) = broadcast::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender.clone(),
        ));

        let rebalancing_service = Arc::new(RebalancingService::new(
            test_trigger_config(),
            vault_registry,
            Address::ZERO,
            Address::ZERO,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
        let hedge_latency = Arc::new(HedgeLatencyProjection::new(pool.clone()));
        let rebalance_timing = Arc::new(RebalanceTimingProjection::new(pool.clone()));
        let lifecycle_failure = Arc::new(LifecycleFailureProjection::new(pool.clone()));
        let manifest = QueryManifest::new(
            rebalancing_service,
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        );
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(MockVaultLookup::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };
        let built = manifest.build(pool, services).await.unwrap();

        // Dispatch a live snapshot command through the built store and
        // verify it lands in the shared BroadcastingInventory via the
        // trigger's internal projection.
        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0xAB),
            owner: Address::repeat_byte(0xCD),
        };
        let mut balances = BTreeMap::new();
        balances.insert(
            Symbol::new("RKLB").unwrap(),
            FractionalShares::new(float!(7)),
        );
        built
            .snapshot
            .send(&id, InventorySnapshotCommand::OnchainEquity { balances })
            .await
            .unwrap();

        let symbol = Symbol::new("RKLB").unwrap();
        let available = inventory
            .read()
            .await
            .equity_available(&symbol, Venue::MarketMaking);
        assert_eq!(
            available,
            Some(FractionalShares::new(float!(7))),
            "manifest build must wire the InventorySnapshot store so that \
             live commands dispatch through the trigger's projection into \
            BroadcastingInventory",
        );
    }

    #[tokio::test]
    async fn build_frameworks_broadcasts_live_position_updates() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (event_sender, mut event_receiver) = broadcast::channel(10);

        let symbol = Symbol::new("AAPL").unwrap();
        let orderbook = Address::repeat_byte(0xAB);
        let owner = Address::repeat_byte(0xCD);
        let token = Address::repeat_byte(0xEF);
        let vault_registry = Arc::new(test_store(pool.clone(), ()));
        vault_registry
            .send(
                &VaultRegistryId { orderbook, owner },
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();

        let initial_inventory = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc::new(float!(1000000)), Usdc::new(float!(1000000)))
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::MarketMaking,
                    Operator::Add,
                    FractionalShares::new(float!(20)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::Hedging,
                    Operator::Add,
                    FractionalShares::new(float!(80)),
                ),
                chrono::Utc::now(),
            )
            .unwrap();
        let inventory = Arc::new(BroadcastingInventory::new(
            initial_inventory,
            event_sender.clone(),
        ));

        let rebalancing_service = Arc::new(RebalancingService::new(
            test_trigger_config(),
            vault_registry,
            orderbook,
            owner,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
        let hedge_latency = Arc::new(HedgeLatencyProjection::new(pool.clone()));
        let rebalance_timing = Arc::new(RebalanceTimingProjection::new(pool.clone()));
        let lifecycle_failure = Arc::new(LifecycleFailureProjection::new(pool.clone()));
        let manifest = QueryManifest::new(
            rebalancing_service.clone(),
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        );
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(MockVaultLookup::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };
        let built = manifest.build(pool.clone(), services).await.unwrap();

        built
            .position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(2)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let message = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let message = event_receiver
                    .recv()
                    .await
                    .expect("position command should broadcast dashboard update");

                if matches!(message, Statement::PositionUpdate(_)) {
                    return message;
                }
            }
        })
        .await
        .expect("position command should broadcast dashboard update");

        match message {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(position.net.eq(float!(2)).unwrap());
                assert!(
                    position
                        .last_price_usdc
                        .expect("position update should include last price")
                        .eq(float!(150))
                        .unwrap()
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }

        drain_pending_jobs(&rebalancing_service).await.unwrap();
        let pending_mint_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            pending_mint_jobs, 1,
            "expected the rebalancing subscriber to enqueue a mint job on the position event"
        );
    }
}
