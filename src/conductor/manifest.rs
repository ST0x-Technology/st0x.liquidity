//! Query processor manifest with compile-time wiring guarantees.
//!
//! This module enumerates ALL query processors that must be wired to CQRS
//! frameworks. The exhaustive destructuring in [`QueryManifest::wire()`]
//! ensures that adding a new query to `QueryManifest` forces you to wire it.
//!
//! # Adding a new query processor
//!
//! 1. Add field to [`QueryManifest`] with dependencies encoded in the type
//! 2. Add corresponding field to [`WiredQueries`]
//! 3. Create it in [`QueryManifest::new()`]
//! 4. Wire it in [`QueryManifest::wire()`] - destructuring forces you to handle it
//! 5. Extract and return it in [`WiredQueries`]

use alloy::primitives::Address;
use cqrs_es::persist::GenericQuery;
use sqlite_es::{SqliteCqrs, SqliteViewRepository};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

use super::wire::{Cons, CqrsBuilder, Nil, UnwiredQuery};
use crate::dashboard::{EventBroadcaster, ServerMessage};
use crate::equity_redemption::{EquityRedemption, RedemptionServices};
use crate::inventory::InventoryView;
use crate::lifecycle::Lifecycle;
use crate::position::{PositionAggregate, PositionQuery};
use crate::rebalancing::{RebalancingTrigger, RebalancingTriggerConfig, TriggeredOperation};
use crate::tokenized_equity_mint::MintServices;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::wrapper::Wrapper;

type RebalancingTriggerDeps = Cons<
    PositionAggregate,
    Cons<
        Lifecycle<TokenizedEquityMint>,
        Cons<Lifecycle<EquityRedemption>, Cons<Lifecycle<UsdcRebalance>, Nil>>,
    >,
>;

type EventBroadcasterDeps = Cons<
    Lifecycle<TokenizedEquityMint>,
    Cons<Lifecycle<EquityRedemption>, Cons<Lifecycle<UsdcRebalance>, Nil>>,
>;

type RedemptionViewRepository =
    SqliteViewRepository<Lifecycle<EquityRedemption>, Lifecycle<EquityRedemption>>;

type RedemptionViewQuery = GenericQuery<
    RedemptionViewRepository,
    Lifecycle<EquityRedemption>,
    Lifecycle<EquityRedemption>,
>;

/// All query processors that must be created and wired.
///
/// Exhaustive destructuring in [`Self::wire()`] ensures every field is handled.
pub(super) struct QueryManifest {
    rebalancing_trigger: UnwiredQuery<RebalancingTrigger, RebalancingTriggerDeps>,
    event_broadcaster: UnwiredQuery<EventBroadcaster, EventBroadcasterDeps>,
    position_view: UnwiredQuery<PositionQuery, Cons<PositionAggregate, Nil>>,
    redemption_view: UnwiredQuery<RedemptionViewQuery, Cons<Lifecycle<EquityRedemption>, Nil>>,
}

/// All query processors after wiring is complete.
pub(super) struct WiredQueries {
    pub(super) rebalancing_trigger: Arc<RebalancingTrigger>,
    pub(super) position_view: Arc<PositionQuery>,
    pub(super) redemption_view: Arc<RedemptionViewQuery>,
}

/// Built CQRS frameworks from the wiring process.
pub(super) struct BuiltFrameworks {
    pub(super) position: Arc<SqliteCqrs<PositionAggregate>>,
    pub(super) mint: Arc<SqliteCqrs<Lifecycle<TokenizedEquityMint>>>,
    pub(super) redemption: Arc<SqliteCqrs<Lifecycle<EquityRedemption>>>,
    pub(super) usdc: Arc<SqliteCqrs<Lifecycle<UsdcRebalance>>>,
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
        wrapper: Arc<dyn Wrapper>,
    ) -> Self {
        let rebalancing_trigger = RebalancingTrigger::new(
            config,
            pool.clone(),
            orderbook,
            market_maker_wallet,
            inventory,
            operation_sender,
            wrapper,
        );

        let event_broadcaster = EventBroadcaster::new(event_sender);

        let position_view_repo = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "position_view".to_string(),
        ));
        let position_view = GenericQuery::new(position_view_repo);

        let redemption_view_repo = Arc::new(SqliteViewRepository::new(
            pool,
            "equity_redemption_view".to_string(),
        ));
        let redemption_view = GenericQuery::new(redemption_view_repo);

        Self {
            rebalancing_trigger: UnwiredQuery::new(rebalancing_trigger),
            event_broadcaster: UnwiredQuery::new(event_broadcaster),
            position_view: UnwiredQuery::new(position_view),
            redemption_view: UnwiredQuery::new(redemption_view),
        }
    }

    /// Wires all query processors and builds their CQRS frameworks.
    ///
    /// Destructures `self` to ensure every field is handled. If you add a new
    /// query to the manifest, this method won't compile until you wire it.
    pub(super) fn wire(
        self,
        pool: SqlitePool,
        mint_services: MintServices,
        redemption_services: RedemptionServices,
    ) -> (BuiltFrameworks, WiredQueries) {
        let Self {
            rebalancing_trigger,
            event_broadcaster,
            position_view,
            redemption_view,
        } = self;

        let (position, (position_view, (rebalancing_trigger, ()))) = CqrsBuilder::new(pool.clone())
            .wire(rebalancing_trigger)
            .wire(position_view)
            .build(());

        let (mint, (event_broadcaster, (rebalancing_trigger, ()))) = CqrsBuilder::new(pool.clone())
            .wire(rebalancing_trigger)
            .wire(event_broadcaster)
            .build(mint_services);

        let (redemption, (redemption_view, (event_broadcaster, (rebalancing_trigger, ())))) =
            CqrsBuilder::new(pool.clone())
                .wire(rebalancing_trigger)
                .wire(event_broadcaster)
                .wire(redemption_view)
                .build(redemption_services);

        let (usdc, (event_broadcaster, (rebalancing_trigger, ()))) = CqrsBuilder::new(pool)
            .wire(rebalancing_trigger)
            .wire(event_broadcaster)
            .build(());

        let rebalancing_trigger = rebalancing_trigger.into_inner();
        let _event_broadcaster = event_broadcaster.into_inner();
        let position_view = position_view.into_inner();
        let redemption_view = redemption_view.into_inner();

        (
            BuiltFrameworks {
                position: Arc::new(position),
                mint: Arc::new(mint),
                redemption: Arc::new(redemption),
                usdc: Arc::new(usdc),
            },
            WiredQueries {
                rebalancing_trigger,
                position_view,
                redemption_view,
            },
        )
    }
}
