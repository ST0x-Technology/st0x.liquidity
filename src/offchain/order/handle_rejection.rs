//! [`HandleOrderRejection`] job: marks an order failed at the
//! [`OffchainOrder`] aggregate and clears the
//! [`Position`](crate::position::Position) aggregate's pending state.
//!
//! Split out from
//! [`PollOrderStatus`](crate::offchain::order::PollOrderStatus) so the CQRS
//! write happens in its own retryable unit -- a transient DB failure here
//! does not force us to re-call the broker.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use st0x_event_sorcery::Store;
use st0x_execution::Positive;

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{JobError, OffchainOrder, OffchainOrderCommand, OffchainOrderId};
use crate::position::{Position, PositionCommand};

pub(crate) type HandleOrderRejectionJobQueue = JobQueue<HandleOrderRejection>;

/// Dependencies [`HandleOrderRejection`] needs to record a rejection: the
/// two aggregate stores it writes to.
pub(crate) struct HandleOrderRejectionCtx {
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) position: Arc<Store<Position>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HandleOrderRejection {
    pub(crate) offchain_order_id: OffchainOrderId,
    pub(crate) error: String,
}

impl Job<HandleOrderRejectionCtx> for HandleOrderRejection {
    type Output = ();
    type Error = JobError;

    const WORKER_NAME: &'static str = "handle-order-rejection-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::HandleOrderRejection;

    fn label(&self) -> Label {
        Label::new(format!("HandleOrderRejection:{}", self.offchain_order_id))
    }

    async fn perform(&self, ctx: &HandleOrderRejectionCtx) -> Result<Self::Output, Self::Error> {
        let Some(order) = ctx.offchain_order.load(&self.offchain_order_id).await? else {
            warn!(
                offchain_order_id = %self.offchain_order_id,
                "HandleOrderRejection: order not found, skipping"
            );
            return Ok(());
        };

        let symbol = order.symbol().clone();

        // Retry-safe: the two writes (OffchainOrder MarkFailed +
        // Position FailOffChainOrder) are not atomic. If a prior attempt
        // completed step 1 but failed step 2, apalis re-runs us with the
        // order already in `Failed`. Re-sending `MarkFailed` would surface
        // `AlreadyCompleted` and stall the job forever, so we only run
        // step 1 when the order has not yet been marked failed.
        use OffchainOrder::{
            Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
        };
        match &order {
            Failed { .. } => {
                info!(
                    offchain_order_id = %self.offchain_order_id,
                    "HandleOrderRejection: order already Failed, resuming position update"
                );
            }

            Pending { .. } | Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. } => {
                ctx.offchain_order
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::MarkFailed {
                            error: self.error.clone(),
                        },
                    )
                    .await?;
            }

            Filled { .. } => {
                warn!(
                    offchain_order_id = %self.offchain_order_id,
                    "HandleOrderRejection: order already Filled, cannot mark failed -- skipping"
                );
                return Ok(());
            }

            Cancelled { .. } => {
                info!(
                    offchain_order_id = %self.offchain_order_id,
                    "HandleOrderRejection: order already Cancelled -- skipping MarkFailed, resuming position update"
                );
            }
        }

        // Retry-safe step 2: if a prior attempt or the startup recovery
        // job already cleared the position's pending id, sending the
        // command again would fail `validate_pending_execution`. Detect
        // and no-op instead.
        let position_pending = ctx
            .position
            .load(&symbol)
            .await?
            .and_then(|position| position.pending_offchain_order_id);
        if position_pending != Some(self.offchain_order_id) {
            info!(
                offchain_order_id = %self.offchain_order_id,
                ?position_pending,
                "HandleOrderRejection: position no longer expecting this order, skipping"
            );
            return Ok(());
        }

        let position_command = match &order {
            PartiallyFilled {
                shares_filled,
                direction,
                executor_order_id,
                avg_price,
                ..
            }
            | Cancelling {
                shares_filled,
                direction,
                executor_order_id,
                avg_price: Some(avg_price),
                ..
            }
            | Failed {
                shares_filled: Some(shares_filled),
                avg_price: Some(avg_price),
                executor_order_id: Some(executor_order_id),
                direction,
                ..
            } => position_command_for_retained_fill(
                self.offchain_order_id,
                *shares_filled,
                *direction,
                executor_order_id.clone(),
                *avg_price,
                self.error.clone(),
            ),
            Pending { .. }
            | Submitted { .. }
            | Cancelling {
                avg_price: None, ..
            }
            | Failed {
                shares_filled: None,
                ..
            }
            | Failed {
                avg_price: None, ..
            }
            | Failed {
                executor_order_id: None,
                ..
            }
            | Cancelled { .. } => PositionCommand::FailOffChainOrder {
                offchain_order_id: self.offchain_order_id,
                error: self.error.clone(),
            },
            Filled { .. } => unreachable!("filled orders return before position update"),
        };

        ctx.position.send(&symbol, position_command).await?;

        Ok(())
    }
}

fn position_command_for_retained_fill(
    offchain_order_id: OffchainOrderId,
    shares_filled: st0x_execution::FractionalShares,
    direction: st0x_execution::Direction,
    executor_order_id: st0x_execution::ExecutorOrderId,
    avg_price: st0x_finance::Usd,
    fallback_error: String,
) -> PositionCommand {
    Positive::new(shares_filled).map_or_else(
        |_| PositionCommand::FailOffChainOrder {
            offchain_order_id,
            error: fallback_error,
        },
        |positive_filled| PositionCommand::CompleteOffChainOrder {
            offchain_order_id,
            shares_filled: positive_filled,
            direction,
            executor_order_id,
            price: avg_price,
            broker_timestamp: chrono::Utc::now(),
        },
    )
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, FractionalShares, Positive, SupportedExecutor, Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::offchain::order::noop_order_placer;
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use st0x_config::ExecutionThreshold;

    struct TestInfra {
        ctx: HandleOrderRejectionCtx,
    }

    async fn build_test_infra() -> TestInfra {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let (offchain_order, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        TestInfra {
            ctx: HandleOrderRejectionCtx {
                offchain_order,
                position,
            },
        }
    }

    async fn submit_offchain_order(
        infra: &TestInfra,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
    ) -> OffchainOrderId {
        let onchain = OnchainTradeBuilder::new()
            .with_symbol(tokenized_symbol)
            .with_amount(shares.inner().inner())
            .build();
        let trade_id = TradeId {
            tx_hash: onchain.tx_hash,
            log_index: onchain.log_index,
        };

        infra
            .ctx
            .position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id,
                    amount: onchain.amount,
                    direction: Direction::Buy,
                    price_usdc: onchain.price.value(),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let offchain_order_id = OffchainOrderId::new();

        infra
            .ctx
            .position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        infra
            .ctx
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    #[tokio::test]
    async fn handle_order_rejection_emits_offchain_and_position_commands() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let error_message = "broker rejected: insufficient buying power".to_string();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: error_message.clone(),
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let offchain = infra
            .ctx
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::Failed {
            error: stored_error,
            ..
        } = offchain
        else {
            panic!("expected OffchainOrder::Failed, got {offchain:?}");
        };
        assert_eq!(stored_error, error_message);

        let position = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position must clear pending state after rejection"
        );
    }

    /// Simulates apalis retrying after step 1 (MarkFailed) succeeded but
    /// step 2 (Position update) failed. The order is already `Failed` and
    /// the position still has `pending_offchain_order_id` set. The retry
    /// must resume step 2 without re-applying step 1, which would surface
    /// `AlreadyCompleted` and stall the job.
    #[tokio::test]
    async fn retry_after_position_failure_resumes_position_update() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        // Simulate a prior attempt having completed step 1 by manually
        // driving the OffchainOrder to Failed while leaving the position's
        // pending state set.
        let original_error = "broker rejected: insufficient buying power".to_string();
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: original_error.clone(),
                },
            )
            .await
            .unwrap();

        let position_before = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position_before.pending_offchain_order_id,
            Some(order_id),
            "test setup: position must still be expecting this order"
        );

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: original_error,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position_after = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position_after.pending_offchain_order_id, None,
            "Retry must clear the position's pending state by running step 2"
        );
    }

    #[tokio::test]
    async fn retry_after_failed_partial_fill_completes_position_fill() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(1)),
                    avg_price: st0x_finance::Usd::new(float!(150)),
                },
            )
            .await
            .unwrap();
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker failed after partial fill".to_string(),
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker failed after partial fill".to_string(),
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position_after = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position_after.pending_offchain_order_id, None,
            "Retry must clear pending by completing the retained partial fill"
        );
    }

    /// Simulates apalis retrying after both steps succeeded -- the order is
    /// `Failed` and the position no longer has a pending id. The retry
    /// must no-op rather than surface `NoPendingExecution`.
    #[tokio::test]
    async fn retry_after_full_success_is_noop() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let error_message = "broker rejected".to_string();

        // First run drives both order and position to terminal state.
        HandleOrderRejection {
            offchain_order_id: order_id,
            error: error_message.clone(),
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        // Second run: identical job, both writes already applied.
        HandleOrderRejection {
            offchain_order_id: order_id,
            error: error_message,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position must remain cleared after no-op retry"
        );
    }
}
