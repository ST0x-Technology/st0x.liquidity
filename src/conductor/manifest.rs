//! Query processor manifest with compile-time wiring guarantees.
//!
//! This module enumerates ALL query processors that must be wired to
//! CQRS frameworks. The exhaustive destructuring in
//! [`QueryManifest::wire()`] ensures that adding a new query to
//! `QueryManifest` forces you to wire it.
//!
//! # Adding a new query processor
//!
//! 1. Add field to [`QueryManifest`] with dependencies encoded in
//!    the type
//! 2. Add corresponding field to [`WiredQueries`]
//! 3. Create it in [`QueryManifest::new()`]
//! 4. Wire it in [`QueryManifest::wire()`] - destructuring forces
//!    you to handle it
//! 5. Extract and return it in [`WiredQueries`]

use alloy::primitives::Address;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

use st0x_dto::ServerMessage;
use st0x_event_sorcery::{Cons, Nil, Projection, ProjectionError, Store, StoreBuilder, Unwired};

use crate::dashboard::EventBroadcaster;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::InventoryView;
use crate::position::Position;
use crate::rebalancing::{RebalancingTrigger, RebalancingTriggerConfig, TriggeredOperation};
use crate::tokenized_equity_mint::MintServices;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

type RebalancingTriggerDeps =
    Cons<Position, Cons<TokenizedEquityMint, Cons<EquityRedemption, Cons<UsdcRebalance, Nil>>>>;

type EventBroadcasterDeps =
    Cons<TokenizedEquityMint, Cons<EquityRedemption, Cons<UsdcRebalance, Nil>>>;

/// All query processors that must be created and wired when
/// rebalancing is enabled.
///
/// Exhaustive destructuring in [`Self::wire()`] ensures every field
/// is handled.
pub(super) struct QueryManifest {
    rebalancing_trigger: Unwired<RebalancingTrigger, RebalancingTriggerDeps>,
    event_broadcaster: Unwired<EventBroadcaster, EventBroadcasterDeps>,
    position_view: Projection<Position>,
}

/// All query processors after wiring is complete.
pub(super) struct WiredQueries {
    pub(super) position_view: Projection<Position>,
}

/// Built CQRS frameworks from the wiring process.
pub(super) struct BuiltFrameworks {
    pub(super) position: Store<Position>,
    pub(super) mint: Store<TokenizedEquityMint>,
    pub(super) redemption: Store<EquityRedemption>,
    pub(super) usdc: Store<UsdcRebalance>,
}

impl QueryManifest {
    pub(super) fn new(
        config: RebalancingTriggerConfig,
        pool: SqlitePool,
        orderbook: Address,
        market_maker_wallet: Address,
        inventory: Arc<RwLock<InventoryView>>,
        operation_sender: mpsc::Sender<TriggeredOperation>,
        event_sender: broadcast::Sender<ServerMessage>,
    ) -> Result<Self, ProjectionError<Position>> {
        let rebalancing_trigger = RebalancingTrigger::new(
            config,
            pool.clone(),
            orderbook,
            market_maker_wallet,
            inventory,
            operation_sender,
        );

        let event_broadcaster = EventBroadcaster::new(event_sender);

        let position_view = Projection::<Position>::sqlite(pool)?;

        Ok(Self {
            rebalancing_trigger: Unwired::new(rebalancing_trigger),
            event_broadcaster: Unwired::new(event_broadcaster),
            position_view,
        })
    }

    /// Wires all query processors and builds their CQRS frameworks.
    ///
    /// Destructures `self` to ensure every field is handled. If you
    /// add a new query to the manifest, this method won't compile
    /// until you wire it.
    pub(super) async fn wire(
        self,
        pool: SqlitePool,
    ) -> anyhow::Result<(BuiltFrameworks, WiredQueries)> {
        let Self {
            rebalancing_trigger,
            event_broadcaster,
            position_view,
        } = self;

        let (position, (rebalancing_trigger, ())) = StoreBuilder::<Position>::new(pool.clone())
            .with(position_view.clone())
            .wire(rebalancing_trigger)
            .build(())
            .await?;

        let (mint, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
                .wire(rebalancing_trigger)
                .wire(event_broadcaster)
                .build(())
                .await?;

        let (redemption, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .wire(rebalancing_trigger)
                .wire(event_broadcaster)
                .build(())
                .await?;

        let (usdc, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<UsdcRebalance>::new(pool)
                .wire(rebalancing_trigger)
                .wire(event_broadcaster)
                .build(())
                .await?;

        let _rebalancing_trigger = rebalancing_trigger.into_inner();
        let _event_broadcaster = event_broadcaster.into_inner();

        Ok((
            BuiltFrameworks {
                position,
                mint,
                redemption,
                usdc,
            },
            WiredQueries { position_view },
        ))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use rust_decimal_macros::dec;
    use tokio::sync::{RwLock, broadcast, mpsc};

    use super::*;
    use crate::inventory::{ImbalanceThreshold, InventoryView};
    use crate::test_utils::setup_test_db;

    fn test_trigger_config() -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
            equity_threshold: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc_threshold: ImbalanceThreshold {
                target: dec!(0.6),
                deviation: dec!(0.15),
            },
        }
    }

    #[tokio::test]
    async fn wire_produces_working_stores() {
        let pool = setup_test_db().await;
        let (operation_sender, _operation_receiver) = mpsc::channel(10);
        let (event_sender, _event_receiver) = broadcast::channel(10);

        let manifest = QueryManifest::new(
            test_trigger_config(),
            pool.clone(),
            Address::ZERO,
            Address::ZERO,
            Arc::new(RwLock::new(InventoryView::default())),
            operation_sender,
            event_sender,
        )
        .unwrap();

        let (_frameworks, queries) = manifest.wire(pool).await.unwrap();

        // Verify stores are usable by checking that loading a
        // nonexistent position returns None
        let result = queries
            .position_view
            .load(&st0x_execution::Symbol::new("AAPL").unwrap())
            .await
            .unwrap();
        assert!(result.is_none());
    }
}
