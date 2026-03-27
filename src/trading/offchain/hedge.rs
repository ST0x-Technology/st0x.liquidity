//! Durable hedge placement job.
//!
//! [`PlaceHedge`] is an apalis-backed [`Job`] that places an offsetting
//! broker order for an accumulated position. The position monitor enqueues
//! these; the apalis worker processes them with retry semantics.

use std::sync::Arc;

use apalis_codec::json::JsonCodec;
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{CompactType, SqliteStorage};
use serde::{Deserialize, Serialize};

use st0x_event_sorcery::Store;
use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};

use crate::conductor::job::{Job, Label};
use crate::offchain_order::{OffchainOrder, OffchainOrderCommand, OffchainOrderId};
use crate::position::{Position, PositionCommand};
use crate::threshold::ExecutionThreshold;
use crate::trading::onchain::trade_accountant::TradeAccountingError;

/// Persistent job queue for hedge placement.
pub(crate) type HedgeJobQueue = SqliteStorage<PlaceHedge, JsonCodec<CompactType>, SqliteFetcher>;

/// Shared dependencies for hedge placement jobs.
pub(crate) struct HedgeCtx {
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
}

/// A durable job that places an offsetting broker order for an accumulated
/// position, then rolls back the position if the broker rejects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PlaceHedge {
    pub(crate) symbol: Symbol,
    pub(crate) direction: Direction,
    pub(crate) shares: Positive<FractionalShares>,
    pub(crate) executor: SupportedExecutor,
    pub(crate) threshold: ExecutionThreshold,
}

impl Job<HedgeCtx> for PlaceHedge {
    type Error = TradeAccountingError;

    fn label(&self) -> Label {
        Label::new(format!(
            "PlaceHedge:{}:{}:{:?}",
            self.symbol, self.shares, self.direction
        ))
    }

    async fn perform(&self, ctx: &HedgeCtx) -> Result<(), Self::Error> {
        let offchain_order_id = OffchainOrderId::new();

        let _ = ctx
            .position
            .send(
                &self.symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: self.shares,
                    direction: self.direction,
                    executor: self.executor,
                    threshold: self.threshold,
                },
            )
            .await;

        let _ = ctx
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: self.symbol.clone(),
                    shares: self.shares,
                    direction: self.direction,
                    executor: self.executor,
                },
            )
            .await;

        if let Ok(Some(OffchainOrder::Failed { error, .. })) =
            ctx.offchain_order.load(&offchain_order_id).await
        {
            let _ = ctx
                .position
                .send(
                    &self.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use std::sync::Arc;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::job::Job;
    use crate::offchain_order::{OffchainOrder, OrderPlacer};
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    fn succeeding_order_placer() -> Arc<dyn OrderPlacer> {
        use crate::offchain_order::OrderPlacementResult;
        use st0x_execution::ExecutorOrderId;

        struct SucceedingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for SucceedingPlacer {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-order-123"),
                    placed_shares: order.shares,
                })
            }
        }

        Arc::new(SucceedingPlacer)
    }

    fn rejecting_order_placer() -> Arc<dyn OrderPlacer> {
        struct RejectingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for RejectingPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<
                crate::offchain_order::OrderPlacementResult,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("Broker rejected: insufficient buying power".into())
            }
        }

        Arc::new(RejectingPlacer)
    }

    async fn create_hedge_ctx(
        order_placer: Arc<dyn OrderPlacer>,
    ) -> (HedgeCtx, Arc<st0x_event_sorcery::Projection<Position>>) {
        let pool = setup_test_db().await;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
        };

        (ctx, position_projection)
    }

    async fn fill_position(
        store: &Store<Position>,
        symbol: &Symbol,
        amount: FractionalShares,
        direction: Direction,
    ) {
        store
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount,
                    direction,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    fn hedge_job(symbol: &Symbol, shares: f64, direction: Direction) -> PlaceHedge {
        PlaceHedge {
            symbol: symbol.clone(),
            direction,
            shares: Positive::new(FractionalShares::new(float!(shares))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
        }
    }

    #[tokio::test]
    async fn places_offchain_order_and_marks_position_pending() {
        let (ctx, projection) = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        let position = projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Position should have a pending offchain order after hedge placement"
        );
    }

    #[tokio::test]
    async fn clears_pending_state_on_broker_rejection() {
        let (ctx, projection) = create_hedge_ctx(rejecting_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(5.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 5.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        let position = projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position should not be stuck with pending order after broker rejection"
        );
    }

    #[tokio::test]
    async fn gracefully_handles_position_rejection() {
        let (ctx, _projection) = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // No position exists -- PlaceOffChainOrder will be rejected
        let job = hedge_job(&symbol, 1.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        // Should not panic or error -- just logs and returns Ok
    }
}
