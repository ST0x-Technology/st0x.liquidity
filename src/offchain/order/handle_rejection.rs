//! [`HandleOrderRejection`] job: marks an order failed at the
//! [`OffchainOrder`] aggregate and clears the
//! [`Position`](crate::position::Position) aggregate's pending state.
//!
//! Split out from
//! [`PollOrderStatus`](crate::offchain::order::PollOrderStatus) so the CQRS
//! write happens in its own retryable unit -- a transient DB failure here
//! does not force us to re-call the broker.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use st0x_event_sorcery::Store;
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, OrderFailureTerminality, Positive,
};

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{
    JobError, NoFillOutcome, OffchainOrder, OffchainOrderCommand, OffchainOrderId, RetainedFill,
    TerminalPositionFinalization, terminal_position_finalization,
};
use crate::position::{AnchorDisposition, Position, PositionCommand};

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
    /// Broker-reported cumulative fill quantity. `None` means this rejection
    /// carries no persisted evidence for the actual fill.
    #[serde(default)]
    pub(crate) broker_filled_shares: Option<FractionalShares>,
    /// Broker-reported failure time, when the enqueuing poll observed a
    /// broker `Failed` state. `None` when the rejection has no broker
    /// timestamp (the job then stamps its own observation time).
    /// `#[serde(default)]` so payloads already in the queue without it still deserialize.
    #[serde(default)]
    pub(crate) broker_failed_at: Option<DateTime<Utc>>,
    /// Broker-terminality classification, when the enqueuing poll observed a
    /// broker `Failed` state carrying one. `None` when this rejection has no
    /// broker-terminality evidence (e.g. cleanup paths) -- `AnchorDisposition`
    /// must never release the idempotency anchor on `None`.
    /// `#[serde(default)]` so jobs queued before this field existed still
    /// deserialize.
    #[serde(default)]
    pub(crate) broker_terminality: Option<OrderFailureTerminality>,
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
                            filled_shares: self.broker_filled_shares,
                            // Prefer the broker's failure time; rejections
                            // without one (e.g. cleanup paths) fall back to
                            // this job's observation time.
                            failed_at: self.broker_failed_at.unwrap_or_else(Utc::now),
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

        // `broker_timestamp` must be the broker event time the matched state
        // recorded, not the wall-clock time this recovery job happens to run --
        // it flows into `Position.last_updated` and any recency/ordering logic
        // keyed off it. Each state carries its own broker timestamp.
        let position_command = match &order {
            PartiallyFilled {
                shares_filled,
                direction,
                executor_order_id,
                avg_price,
                partially_filled_at,
                ..
            }
            | Cancelling {
                retained_fill:
                    Some(RetainedFill::Priced {
                        shares_filled,
                        avg_price,
                        partially_filled_at,
                    }),
                direction,
                executor_order_id,
                ..
            }
            | Failed {
                retained_fill:
                    Some(RetainedFill::Priced {
                        shares_filled,
                        avg_price,
                        partially_filled_at,
                    }),
                executor_order_id: Some(executor_order_id),
                direction,
                ..
            } => position_command_for_retained_fill(
                self.offchain_order_id,
                *shares_filled,
                *direction,
                executor_order_id.clone(),
                *avg_price,
                *partially_filled_at,
                self.error.clone(),
                self.broker_terminality,
            ),

            // A locally-`Cancelled` order is already terminal and must NOT be
            // recorded as a broker failure: that would set the failure /
            // idempotency anchor for an intentional cancellation and drop any
            // partial fill it retained. Route through the shared terminal
            // finalization so this mapping cannot drift from the recovery and
            // cancel-and-replace paths.
            cancelled @ Cancelled { .. } => {
                match terminal_position_finalization(cancelled) {
                    Some(TerminalPositionFinalization::Complete {
                        shares_filled,
                        direction,
                        executor_order_id,
                        price,
                        broker_timestamp,
                    }) => PositionCommand::CompleteOffChainOrder {
                        offchain_order_id: self.offchain_order_id,
                        shares_filled,
                        direction,
                        executor_order_id,
                        price,
                        broker_timestamp,
                    },
                    // Zero-fill cancellation: release the slot without the
                    // failure anchor.
                    Some(TerminalPositionFinalization::NoFill(NoFillOutcome::Cancelled {
                        reason,
                        cancelled_at,
                    })) => PositionCommand::CancelOffChainOrder {
                        offchain_order_id: self.offchain_order_id,
                        reason,
                        cancelled_at,
                    },
                    // A Cancelled order always classifies as the Cancelled
                    // outcome, so a Failed outcome cannot occur here; a
                    // positive fill with no price cannot be applied. Both fail
                    // so the slot is released rather than left pending forever.
                    Some(
                        TerminalPositionFinalization::NoFill(NoFillOutcome::Failed { .. })
                        | TerminalPositionFinalization::UnpricedFill { .. },
                    )
                    | None => PositionCommand::FailOffChainOrder {
                        offchain_order_id: self.offchain_order_id,
                        error: self.error.clone(),
                        anchor: AnchorDisposition::Preserve,
                    },
                }
            }

            Pending { .. } | Submitted { .. } | Cancelling { .. } | Failed { .. } => {
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: self.offchain_order_id,
                    error: self.error.clone(),
                    anchor: AnchorDisposition::from_broker_terminality(self.broker_terminality),
                }
            }

            Filled { .. } => unreachable!("filled orders return before position update"),
        };

        ctx.position.send(&symbol, position_command).await?;

        Ok(())
    }
}

fn position_command_for_retained_fill(
    offchain_order_id: OffchainOrderId,
    shares_filled: FractionalShares,
    direction: Direction,
    executor_order_id: ExecutorOrderId,
    avg_price: st0x_finance::Usd,
    broker_timestamp: chrono::DateTime<chrono::Utc>,
    fallback_error: String,
    broker_terminality: Option<OrderFailureTerminality>,
) -> PositionCommand {
    let anchor = AnchorDisposition::from_broker_terminality(broker_terminality);

    // Only a confirmed-Terminal broker failure may finalize the position off a
    // retained partial fill. `NotTerminal` and `None` (no evidence, e.g. a
    // legacy job payload from before this field existed) both mean the broker
    // order may still resume and fill the remainder -- completing the
    // position here would lock in the partial quantity as final and lose any
    // later fill once this order is retried under a fresh id.
    if broker_terminality != Some(OrderFailureTerminality::Terminal) {
        return PositionCommand::FailOffChainOrder {
            offchain_order_id,
            error: fallback_error,
            anchor,
        };
    }

    Positive::new(shares_filled).map_or_else(
        |_| PositionCommand::FailOffChainOrder {
            offchain_order_id,
            error: fallback_error,
            anchor,
        },
        |positive_filled| PositionCommand::CompleteOffChainOrder {
            offchain_order_id,
            shares_filled: positive_filled,
            direction,
            executor_order_id,
            price: avg_price,
            broker_timestamp,
        },
    )
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{
        ClientOrderId, Direction, FractionalShares, MarketSession, Positive, SupportedExecutor,
        Symbol,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};

    struct TestInfra {
        ctx: HandleOrderRejectionCtx,
    }

    async fn build_test_infra() -> TestInfra {
        let pool = setup_test_db().await;

        let (offchain_order, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
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
            chain: Chain::Base,
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
                    block_number: None,
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

        infra
            .ctx
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                    placed_shares: shares,
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    async fn mark_order_accepted(
        infra: &TestInfra,
        order_id: OffchainOrderId,
        shares: Positive<FractionalShares>,
    ) {
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("TEST-ACCEPT"),
                    placed_shares: shares,
                    submitted_at: Utc::now(),
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
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
            broker_filled_shares: Some(FractionalShares::ZERO),
            broker_failed_at: None,
            broker_terminality: None,
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
            filled_shares,
            ..
        } = offchain
        else {
            panic!("expected OffchainOrder::Failed, got {offchain:?}");
        };
        assert_eq!(stored_error, error_message);
        assert_eq!(filled_shares, Some(FractionalShares::ZERO));

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

    /// Without terminal broker evidence, a partial fill's rejection must NOT
    /// finalize the position at the partial quantity: the broker order may
    /// still be suspended/live rather than genuinely done, and completing
    /// here would lock in the partial as final and lose any later fill. Only
    /// `Some(OrderFailureTerminality::Terminal)` may complete off a retained
    /// partial (see `rejection_with_terminal_broker_terminality_releases_the_anchor`
    /// for that path).
    #[tokio::test]
    async fn partial_fill_rejection_without_terminal_evidence_does_not_complete_the_position() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;
        mark_order_accepted(&infra, order_id, shares).await;

        let position_before = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(0.75)),
                    avg_price: Usd::new(float!(150.25)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker cancelled after partial fill".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
        assert!(
            matches!(offchain, OffchainOrder::Failed { .. }),
            "the rejection must still mark the offchain order failed locally"
        );

        let position = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.net, position_before.net,
            "without terminal broker evidence, the retained partial fill must \
             NOT be applied to net -- the broker order may still resume and \
             fill the remainder under a fresh retry"
        );
        assert_eq!(
            position.pending_offchain_order_id, None,
            "the rejection must still clear the pending slot"
        );
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(order_id),
            "with no terminal evidence, the anchor must be preserved rather \
             than released"
        );
    }

    /// An order partially fills, then the broker suspends it (`NotTerminal`).
    /// The suspended order may still resume and fill the remainder, so this
    /// rejection must NOT complete the position off the partial quantity,
    /// and must preserve the anchor.
    #[tokio::test]
    async fn suspended_partial_fill_rejection_does_not_complete_the_position() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;
        mark_order_accepted(&infra, order_id, shares).await;

        let position_before = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(0.75)),
                    avg_price: Usd::new(float!(150.25)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker suspended the order".to_string(),
            broker_filled_shares: Some(FractionalShares::new(float!(0.75))),
            broker_failed_at: Some(Utc::now()),
            broker_terminality: Some(OrderFailureTerminality::NotTerminal),
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
            position.net, position_before.net,
            "a NotTerminal broker failure must NOT complete the position off \
             the partial fill -- the broker order may still resume and fill \
             the remainder"
        );
        assert_eq!(
            position.pending_offchain_order_id, None,
            "the rejection must still clear the pending slot"
        );
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(order_id),
            "a NotTerminal classification must preserve the anchor"
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
                    filled_shares: None,
                    failed_at: Utc::now(),
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
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
        assert_eq!(
            position_after.last_failed_offchain_order_id,
            Some(order_id),
            "the order's own executor_order_id (recorded by the earlier \
             attempt's step 1) is not broker-terminality evidence, so with \
             no broker_terminality classification the anchor must preserve"
        );
    }

    /// A retry that lands after step 1 already committed (order `Failed`,
    /// `executor_order_id` recorded) but whose `broker_terminality` is
    /// `Some(NotTerminal)` -- the enqueuing poll observed the broker order
    /// as suspended/replaced, i.e. still able to resume or fill. The
    /// explicit `NotTerminal` classification must win over the
    /// `executor_order_id` presence and preserve the anchor; releasing it
    /// would let a fresh retry double-hedge alongside an order the broker
    /// says can still fill.
    #[tokio::test]
    async fn retry_of_suspended_order_rejection_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let original_error = "broker suspended the order".to_string();
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: original_error.clone(),
                    filled_shares: None,
                    failed_at: Utc::now(),
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
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: Some(OrderFailureTerminality::NotTerminal),
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
        assert_eq!(
            position_after.last_failed_offchain_order_id,
            Some(order_id),
            "an explicit NotTerminal classification must be authoritative and \
             preserve the anchor even though the order carries an \
             executor_order_id from the earlier attempt's step 1"
        );
    }

    /// Simulates a retry landing after step 1 (`MarkFailed`) already
    /// committed a retained partial fill, but with no broker-terminality
    /// evidence carried on the job (`None`, e.g. a legacy payload or a
    /// cleanup path). Release now depends only on `broker_terminality`, so
    /// this must preserve the anchor and NOT complete the position off the
    /// retained partial -- the broker order may still resume.
    #[tokio::test]
    async fn retry_of_failed_partial_fill_without_terminal_evidence_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;
        mark_order_accepted(&infra, order_id, shares).await;

        let position_before = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(1)),
                    avg_price: st0x_finance::Usd::new(float!(150)),
                    partially_filled_at: Utc::now(),
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
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker failed after partial fill".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
            "Retry must clear pending state regardless of anchor disposition"
        );
        assert_eq!(
            position_after.net, position_before.net,
            "without terminal evidence, the retained partial fill must NOT be \
             applied to net"
        );
        assert_eq!(
            position_after.last_failed_offchain_order_id,
            Some(order_id),
            "with no broker-terminality evidence, the anchor must be preserved"
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
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        // Second run: identical job, both writes already applied.
        HandleOrderRejection {
            offchain_order_id: order_id,
            error: error_message,
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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

    /// The broker's failure time (carried on the job from the status poll)
    /// must be the timestamp persisted on the `Failed` event, not the wall
    /// clock at which this recovery job happens to run.
    #[tokio::test]
    async fn rejection_records_broker_failure_time_on_failed_event() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let broker_failed_at = Utc::now() - chrono::Duration::hours(2);

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker rejected".to_string(),
            broker_filled_shares: None,
            broker_failed_at: Some(broker_failed_at),
            broker_terminality: Some(OrderFailureTerminality::Terminal),
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra
            .ctx
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::Failed { failed_at, .. } = order else {
            panic!("expected Failed, got {order:?}");
        };
        assert_eq!(
            failed_at, broker_failed_at,
            "Failed event must carry the broker-reported failure time"
        );
    }

    #[tokio::test]
    async fn rejection_with_terminal_broker_terminality_releases_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();

        let first_order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;
        infra
            .ctx
            .position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: first_order_id,
                    error: "first attempt lost in flight".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();

        // A second order under the same net position (already at threshold
        // from the first fill) rather than a second onchain fill, since
        // `submit_offchain_order`'s fixed trade id would collide on reuse.
        let second_order_id = OffchainOrderId::new();
        infra
            .ctx
            .position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: second_order_id,
                    shares,
                    direction: Direction::Sell,
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
                &second_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(second_order_id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        mark_order_accepted(&infra, second_order_id, shares).await;

        HandleOrderRejection {
            offchain_order_id: second_order_id,
            error: "order expired".to_string(),
            broker_filled_shares: None,
            broker_failed_at: Some(Utc::now()),
            broker_terminality: Some(OrderFailureTerminality::Terminal),
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
            "rejection must clear the pending slot"
        );
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "a broker-observed terminal failure must release the anchor, \
             even one stashed by an earlier attempt"
        );
    }

    /// An `executor_order_id` proves nothing about broker terminality by
    /// itself; only direct `broker_terminality` evidence from the enqueuing
    /// poll may release the anchor here.
    #[tokio::test]
    async fn rejection_of_submitted_order_without_broker_terminality_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let offchain = infra
            .ctx
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::Submitted { .. } = offchain else {
            panic!(
                "expected OffchainOrder::Submitted carrying an executor_order_id, \
                 got {offchain:?}"
            );
        };

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker rejected: insufficient buying power".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
            position.last_failed_offchain_order_id,
            Some(order_id),
            "with no broker-terminality evidence, the Submitted order's own \
             executor_order_id must NOT release the anchor -- the broker order \
             may still be live"
        );
    }

    /// A `Submitted` order's rejection with an explicit `NotTerminal`
    /// classification must preserve the anchor.
    #[tokio::test]
    async fn rejection_of_submitted_order_with_not_terminal_evidence_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker suspended the order".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: Some(OrderFailureTerminality::NotTerminal),
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
            position.last_failed_offchain_order_id,
            Some(order_id),
            "a NotTerminal classification must preserve the anchor -- the \
             broker order may still resume or fill"
        );
    }

    /// A `Cancelling` order with no retained fill must route through the
    /// same `Submitted | Cancelling` arm as a plain `Submitted` rejection
    /// (not the retained-fill arm), so the anchor disposition depends only
    /// on broker-terminality evidence.
    #[tokio::test]
    async fn cancelling_without_retained_fill_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: crate::offchain::order::CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let offchain = infra
            .ctx
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::Cancelling { retained_fill, .. } = offchain else {
            panic!("expected OffchainOrder::Cancelling, got {offchain:?}");
        };
        assert_eq!(
            retained_fill, None,
            "test setup: this order must carry no retained fill so the \
             rejection routes through the Submitted | Cancelling arm"
        );

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker rejected during cancellation".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
            "rejection must still clear the pending slot"
        );
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(order_id),
            "with no retained fill and no broker-terminality evidence, the \
             anchor must be preserved rather than released"
        );
    }

    #[tokio::test]
    async fn expired_order_rejection_releases_anchor_and_pending() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "expired".to_string(),
            broker_filled_shares: Some(FractionalShares::ZERO),
            broker_failed_at: Some(Utc::now()),
            broker_terminality: Some(OrderFailureTerminality::Terminal),
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
            "an expired order's rejection must clear the pending slot"
        );
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "an expired order's rejection must release the idempotency \
             anchor so the next hedge derives a fresh key"
        );
    }

    /// A rejection that lands while the order is `Cancelling` with a retained
    /// fill, carrying `Some(Terminal)` broker-terminality evidence, must
    /// finalize the position with the fill's broker timestamp (the
    /// `partially_filled_at` carried onto the Cancelling state), not the
    /// local cancel-request wall clock. Terminal evidence is required here:
    /// completing the position off a retained partial is only correct once
    /// the broker order is confirmed done (see
    /// `partial_fill_rejection_without_terminal_evidence_does_not_complete_the_position`
    /// for the non-terminal/no-evidence case).
    #[tokio::test]
    async fn cancelling_rejection_finalizes_position_with_fill_broker_time() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        let broker_fill_time = Utc::now() - chrono::Duration::minutes(30);
        mark_order_accepted(&infra, order_id, shares).await;
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(1)),
                    avg_price: st0x_finance::Usd::new(float!(150)),
                    partially_filled_at: broker_fill_time,
                },
            )
            .await
            .unwrap();
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: crate::offchain::order::CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker rejected during cancellation".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: Some(OrderFailureTerminality::Terminal),
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
            "Retained fill must complete the position and release the slot"
        );
        assert_eq!(
            position.last_updated,
            Some(broker_fill_time),
            "Position must be stamped with the fill's broker time, not the \
             cancel-request wall clock"
        );
    }

    #[tokio::test]
    async fn zero_partial_fill_rejection_without_broker_terminality_preserves_the_anchor() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtTSLA", shares, Direction::Sell).await;

        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::ZERO,
                    avg_price: Usd::new(float!(150)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        HandleOrderRejection {
            offchain_order_id: order_id,
            error: "broker rejected".to_string(),
            broker_filled_shares: None,
            broker_failed_at: None,
            broker_terminality: None,
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
            position.last_failed_offchain_order_id,
            Some(order_id),
            "the order's own executor_order_id is not broker-terminality \
             evidence, so with no broker_terminality classification the \
             anchor must preserve"
        );
    }
}
