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
use st0x_event_sorcery::{Cons, Nil, Projection, Store, StoreBuilder, Unwired};

use crate::dashboard::EventBroadcaster;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::InventoryView;
use crate::position::Position;
use crate::rebalancing::{RebalancingTrigger, RebalancingTriggerConfig, TriggeredOperation};
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
    ) -> Self {
        let rebalancing_trigger = RebalancingTrigger::new(
            config,
            pool.clone(),
            orderbook,
            market_maker_wallet,
            inventory,
            operation_sender,
        );

        let event_broadcaster = EventBroadcaster::new(event_sender);

        let position_view = Projection::<Position>::sqlite(pool, "position_view");

        Self {
            rebalancing_trigger: Unwired::new(rebalancing_trigger),
            event_broadcaster: Unwired::new(event_broadcaster),
            position_view,
        }
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
            .with_projection(&position_view)
            .wire_reactor(rebalancing_trigger)
            .build(())
            .await?;

        let (mint, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
                .wire_reactor(rebalancing_trigger)
                .wire_reactor(event_broadcaster)
                .build(())
                .await?;

        let (redemption, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<EquityRedemption>::new(pool.clone())
                .wire_reactor(rebalancing_trigger)
                .wire_reactor(event_broadcaster)
                .build(())
                .await?;

        let (usdc, (event_broadcaster, (rebalancing_trigger, ()))) =
            StoreBuilder::<UsdcRebalance>::new(pool)
                .wire_reactor(rebalancing_trigger)
                .wire_reactor(event_broadcaster)
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
