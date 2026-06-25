//! [`PollOrderStatus`] job: polls the broker for one order's state, then
//! either self-reschedules (still pending) or enqueues one of the reconcile
//! jobs.
//!
//! Each order gets its own job instance so a transient broker failure for
//! one order does not block polling of another, and unprocessed jobs survive
//! a crash/restart through apalis's SQLite-backed queue.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use st0x_event_sorcery::{Projection, Store};
use st0x_execution::{
    Executor, ExecutorOrderId, FractionalShares, OrderState, Positive, SupportedExecutor, Symbol,
};
use st0x_finance::Usd;

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::handle_rejection::{
    HandleOrderRejection, HandleOrderRejectionJobQueue,
};
use crate::offchain::order::reconcile_fill::{ReconcileOrderFill, ReconcileOrderFillJobQueue};
use crate::offchain::order::{
    CancellationReason, ExecutorOrderPlacer, JobError, OffchainOrder, OffchainOrderCommand,
    OffchainOrderId, RetainedFill, TerminalPositionFinalization, cancel_offchain_order,
    position_command_for_finalization, terminal_position_finalization,
};
use crate::position::Position;

pub(crate) type PollOrderStatusJobQueue = JobQueue<PollOrderStatus>;

/// Dependencies [`PollOrderStatus`] needs to query the broker and route the
/// result. The `Filled` and `Failed` terminal transitions are deferred to
/// [`ReconcileOrderFill`] / [`HandleOrderRejection`] via their queues, but the
/// cancellation-confirmation path finalizes the owning `Position` inline (the
/// broker only reports a cancelled order once, so there is no separate
/// terminal-Cancelled queue to defer to) -- hence the direct `position_store`.
pub(crate) struct PollOrderStatusCtx<E: Executor + Clone + Send + Sync + 'static> {
    pub(crate) executor: E,
    pub(crate) offchain_order_projection: Arc<Projection<OffchainOrder>>,
    /// Store for sending `UpdatePartialFill` directly when the broker
    /// reports a partial fill. Cannot use the reconcile queue because
    /// that's gated to terminal-Filled transitions.
    pub(crate) offchain_order_store: Arc<Store<OffchainOrder>>,
    /// Store for finalizing the owning `Position` when a cancellation is
    /// confirmed terminal. Without this the position keeps
    /// `pending_offchain_order_id = Some(..)` forever after a cancellation,
    /// permanently blocking the symbol from new hedges.
    pub(crate) position_store: Arc<Store<Position>>,
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
    pub(crate) reconcile_queue: ReconcileOrderFillJobQueue,
    pub(crate) rejection_queue: HandleOrderRejectionJobQueue,
    pub(crate) poll_interval: Duration,
}

/// Polls the broker for the status of a single offchain order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PollOrderStatus {
    pub(crate) offchain_order_id: OffchainOrderId,
}

impl<E> Job<PollOrderStatusCtx<E>> for PollOrderStatus
where
    E: Executor + Clone + Send + Sync + 'static,
    JobError: From<E::Error>,
{
    type Output = ();
    type Error = JobError;

    const WORKER_NAME: &'static str = "poll-order-status-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::PollOrderStatus;

    fn label(&self) -> Label {
        Label::new(format!("PollOrderStatus:{}", self.offchain_order_id))
    }

    async fn perform(&self, ctx: &PollOrderStatusCtx<E>) -> Result<Self::Output, Self::Error> {
        let Some(order) = ctx
            .offchain_order_projection
            .load(&self.offchain_order_id)
            .await?
        else {
            warn!(
                target: "broker",
                offchain_order_id = %self.offchain_order_id,
                "PollOrderStatus: order not found in projection, skipping"
            );
            return Ok(());
        };

        let configured_executor = ctx.executor.to_supported_executor();
        let order_executor = order.executor();
        if order_executor != configured_executor {
            warn!(
                target: "broker",
                offchain_order_id = %self.offchain_order_id,
                symbol = %order.symbol(),
                ?order_executor,
                ?configured_executor,
                "PollOrderStatus: order placed via a different executor than the \
                 one currently configured, dropping job without polling"
            );
            return Ok(());
        }

        match dispatch_for_order_state(self.offchain_order_id, &order) {
            PollAction::Drop => Ok(()),
            PollAction::FinalizeCancelled => {
                self.finalize_position_for_terminal_order(ctx, order.symbol())
                    .await
            }
            PollAction::Reschedule => reschedule_self(ctx, self.offchain_order_id).await,
            PollAction::Query { executor_order_id } => {
                self.query_broker_and_dispatch(ctx, &order, executor_order_id)
                    .await
            }
        }
    }
}

impl PollOrderStatus {
    async fn query_broker_and_dispatch<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        order: &OffchainOrder,
        executor_order_id: ExecutorOrderId,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
        JobError: From<E::Error>,
    {
        let parsed_order_id = ctx.executor.parse_order_id(executor_order_id.as_ref())?;
        let order_state = ctx.executor.get_order_status(&parsed_order_id).await?;

        self.dispatch_for_broker_state(ctx, order, order_state)
            .await
    }

    async fn dispatch_for_broker_state<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        order: &OffchainOrder,
        order_state: OrderState,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
        JobError: From<E::Error>,
    {
        use OrderState::{Cancelled, Failed, Filled, PartiallyFilled, Pending, Submitted};
        let symbol = order.symbol();
        match order_state {
            Filled {
                price,
                order_id,
                executed_at,
            } => {
                self.enqueue_reconcile(ctx, symbol, price, order_id, executed_at)
                    .await
            }

            Failed {
                error_reason,
                shares_filled: Some(shares_filled),
                avg_price: Some(avg_price),
                failed_at,
            } => {
                self.record_partial_fill(ctx, symbol, shares_filled, Some(avg_price), failed_at)
                    .await?;
                self.enqueue_rejection(ctx, symbol, error_reason, Some(failed_at))
                    .await
            }

            // Broker-*terminal* Failed with a positive fill it never priced.
            // A terminal order's fill data is immutable, so re-polling can
            // never surface the missing price -- the old `reschedule_self`
            // looped forever. Fail closed instead: leave the position pending
            // (NEVER clear it -- clearing under-accounts the executed shares and
            // double-hedges on the next scan) and stop re-polling. The order
            // stays non-terminal, so startup orphan-recovery re-polls it on the
            // next restart without an in-process tight loop; the error log
            // surfaces it for manual reconciliation.
            Failed {
                error_reason: _,
                shares_filled: Some(shares_filled),
                avg_price: None,
                ..
            } if Positive::new(shares_filled).is_ok() => {
                error!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    %shares_filled,
                    "Broker reports Failed with filled shares but no avg_price; \
                     leaving the position pending (fail closed) and not re-polling"
                );
                Ok(())
            }

            Failed {
                error_reason,
                failed_at,
                ..
            } => {
                self.enqueue_rejection(ctx, symbol, error_reason, Some(failed_at))
                    .await
            }

            Cancelled {
                shares_filled,
                avg_price: Some(avg_price),
                cancelled_at,
                ..
            } if Positive::new(shares_filled).is_ok() => {
                self.record_partial_fill(ctx, symbol, shares_filled, Some(avg_price), cancelled_at)
                    .await?;
                self.confirm_cancellation(ctx, symbol, cancelled_at).await
            }

            // Broker-terminal Cancelled with a positive unpriced fill -- same
            // fail-closed handling as the Failed arm above: leave the position
            // pending and stop re-polling rather than looping forever or
            // clearing the executed shares.
            Cancelled {
                cancelled_at: _,
                shares_filled,
                avg_price: None,
                ..
            } if Positive::new(shares_filled).is_ok() => {
                error!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    %shares_filled,
                    "Broker reports Cancelled with filled shares but no avg_price; \
                     leaving the position pending (fail closed) and not re-polling"
                );
                Ok(())
            }

            Cancelled { cancelled_at, .. } => {
                self.confirm_cancellation(ctx, symbol, cancelled_at).await
            }

            PartiallyFilled {
                shares_filled,
                avg_price,
                partially_filled_at,
                ..
            } => {
                self.record_partial_fill(
                    ctx,
                    symbol,
                    shares_filled,
                    avg_price,
                    partially_filled_at,
                )
                .await?;

                debug!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    "PollOrderStatus: broker reports partial fill, re-enqueuing with delay"
                );

                reschedule_self(ctx, self.offchain_order_id).await
            }

            Pending | Submitted { .. } => {
                debug!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    "PollOrderStatus: broker reports still pending, re-enqueuing with delay"
                );

                reschedule_self(ctx, self.offchain_order_id).await
            }
        }
    }

    async fn record_partial_fill<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        broker_shares_filled: FractionalShares,
        avg_price: Option<Usd>,
        partially_filled_at: DateTime<Utc>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        if broker_shares_filled == FractionalShares::ZERO {
            return Ok(());
        }

        let Some(price) = avg_price else {
            warn!(
                target: "broker",
                %symbol,
                offchain_order_id = %self.offchain_order_id,
                "Broker reports filled shares but no avg_price; skipping UpdatePartialFill"
            );
            return Ok(());
        };

        // Avoid no-op or stale writes when the local aggregate already
        // reflects this partial-fill quantity or a newer cumulative quantity.
        // Partial fills are cumulative and must never move backwards.
        let local_filled = ctx
            .offchain_order_projection
            .load(&self.offchain_order_id)
            .await?
            .and_then(|local_order| match local_order {
                OffchainOrder::PartiallyFilled { shares_filled, .. } => Some(shares_filled),
                OffchainOrder::Cancelling { retained_fill, .. } => {
                    retained_fill.map(RetainedFill::shares_filled)
                }
                OffchainOrder::Pending { .. }
                | OffchainOrder::Submitted { .. }
                | OffchainOrder::Filled { .. }
                | OffchainOrder::Failed { .. }
                | OffchainOrder::Cancelled { .. } => None,
            });

        if let Some(local_filled) = local_filled {
            match super::broker_fill_exceeds_local(broker_shares_filled, local_filled) {
                Ok(true) => {}
                Ok(false) => {
                    debug!(
                        target: "broker",
                        %symbol,
                        offchain_order_id = %self.offchain_order_id,
                        local_shares_filled = %local_filled,
                        broker_shares_filled = ?broker_shares_filled,
                        "Skipping duplicate or stale broker partial fill"
                    );
                    return Ok(());
                }
                // Fail the whole dispatch (apalis retries it) instead of
                // swallowing the error: the callers that follow this with
                // confirm_cancellation / enqueue_rejection would otherwise
                // finalize the position from a possibly-stale local fill,
                // under-accounting executed shares (double hedge). Mirrors
                // the fail-closed contract of the aggregate's own
                // fill-comparison guard.
                Err(error) => {
                    warn!(
                        target: "broker",
                        %symbol,
                        offchain_order_id = %self.offchain_order_id,
                        %error,
                        "Failed to compare broker and local partial fills; failing the poll so it retries"
                    );
                    return Err(error.into());
                }
            }
        }

        info!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            shares_filled = ?broker_shares_filled,
            "PollOrderStatus: broker reports PartiallyFilled, recording UpdatePartialFill"
        );

        ctx.offchain_order_store
            .send(
                &self.offchain_order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: broker_shares_filled,
                    avg_price: price,
                    partially_filled_at,
                },
            )
            .await
            .inspect_err(|error| {
                warn!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    %error,
                    "Failed to send UpdatePartialFill; leaving order non-terminal for retry"
                );
            })?;

        Ok(())
    }

    async fn enqueue_reconcile<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        price: Usd,
        order_id: ExecutorOrderId,
        executed_at: DateTime<Utc>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        info!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            "PollOrderStatus: broker reports Filled, enqueueing ReconcileOrderFill"
        );

        let mut queue = ctx.reconcile_queue.clone();
        queue
            .push(ReconcileOrderFill {
                offchain_order_id: self.offchain_order_id,
                price,
                executor_order_id: order_id,
                broker_timestamp: executed_at,
            })
            .await?;

        Ok(())
    }

    /// `broker_failed_at` is the broker-reported failure time when this
    /// rejection stems from a broker `Failed` state; `None` when no broker
    /// timestamp exists for the rejection (the job then stamps its own
    /// observation time).
    async fn enqueue_rejection<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        error_reason: Option<String>,
        broker_failed_at: Option<DateTime<Utc>>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        let error_message =
            error_reason.unwrap_or_else(|| "Order failed with no error reason".to_string());
        info!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            %error_message,
            "PollOrderStatus: broker reports Failed, enqueueing HandleOrderRejection"
        );

        let mut queue = ctx.rejection_queue.clone();
        queue
            .push(HandleOrderRejection {
                offchain_order_id: self.offchain_order_id,
                error: error_message,
                broker_failed_at,
            })
            .await?;

        Ok(())
    }

    async fn confirm_cancellation<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        cancelled_at: DateTime<Utc>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        let Some(order) = ctx
            .offchain_order_projection
            .load(&self.offchain_order_id)
            .await?
        else {
            warn!(
                target: "broker",
                %symbol,
                offchain_order_id = %self.offchain_order_id,
                "PollOrderStatus: order disappeared before confirming cancellation"
            );
            return Ok(());
        };

        match order {
            OffchainOrder::Cancelling { .. } => {
                info!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    "PollOrderStatus: broker confirms Cancelled, marking order terminal"
                );

                ctx.offchain_order_store
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::ConfirmCancellation { cancelled_at },
                    )
                    .await?;

                self.finalize_position_for_terminal_order(ctx, symbol).await
            }

            // The broker reports Cancelled while the local order is still
            // live. Two ways here: the `CancelOrder` crash window (DELETE
            // succeeded, process died before `CancelRequested` persisted) or
            // a cancellation that never had a local request at all (operator
            // dashboard cancel, broker-initiated cancel). Either way this is
            // a cancellation, NOT a broker rejection -- routing it to
            // HandleOrderRejection would set `last_failed_offchain_order_id`
            // and let the next replacement adopt the cancelled broker order
            // via duplicate-client-order-id recovery.
            OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. } => {
                self.recover_unrequested_cancellation(ctx, symbol, cancelled_at)
                    .await
            }

            OffchainOrder::Pending { .. }
            | OffchainOrder::Filled { .. }
            | OffchainOrder::Failed { .. }
            | OffchainOrder::Cancelled { .. } => {
                warn!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    state = ?order,
                    "Broker reports Cancelled for order that is neither live nor \
                     Cancelling; enqueueing rejection cleanup"
                );
                self.enqueue_rejection(
                    ctx,
                    symbol,
                    Some("Broker reported Cancelled before local cancellation request".to_string()),
                    None,
                )
                .await
            }
        }
    }

    /// Recovers a broker-side cancellation observed while the local order is
    /// still `Submitted`/`PartiallyFilled` (the cancel request did not survive
    /// a crash). Re-drives `CancelOrder`: its pre-cancel reconcile reads the
    /// broker's `Cancelled` state and emits terminal `Cancelled` directly,
    /// preserving any priced fill. If the aggregate's own broker read raced
    /// and still saw the order live, the order lands in `Cancelling` instead
    /// and is confirmed with the broker timestamp this poll already holds.
    async fn recover_unrequested_cancellation<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        cancelled_at: DateTime<Utc>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        warn!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            "Broker reports Cancelled for a live local order (cancel request \
             lost, e.g. crash before persisting CancelRequested); recovering \
             as cancellation instead of failure"
        );

        // This path also catches cancellations that never had a local
        // request at all (operator dashboard cancels, broker-initiated
        // cancels), so record `Unrequested` rather than guessing a specific
        // local reason -- mislabelling a 2pm manual cancel as a market-open
        // replacement would corrupt the cancellation analytics the reason
        // field exists for.
        cancel_offchain_order(
            &ctx.offchain_order_store,
            &ExecutorOrderPlacer(ctx.executor.clone()),
            &self.offchain_order_id,
            CancellationReason::Unrequested,
        )
        .await?;

        let Some(recovered) = ctx
            .offchain_order_store
            .load(&self.offchain_order_id)
            .await?
        else {
            warn!(
                target: "broker",
                %symbol,
                offchain_order_id = %self.offchain_order_id,
                "PollOrderStatus: order disappeared during cancellation recovery"
            );
            return Ok(());
        };

        match recovered {
            OffchainOrder::Cancelling { .. } => {
                ctx.offchain_order_store
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::ConfirmCancellation { cancelled_at },
                    )
                    .await?;
            }
            // `CancelOrder`'s pre-cancel reconcile may have read the broker's
            // Cancelled state and driven the order terminal directly, skipping
            // `Cancelling`. Either way the finalization below classifies the
            // resulting terminal state and finalizes the position.
            OffchainOrder::Pending { .. }
            | OffchainOrder::Submitted { .. }
            | OffchainOrder::PartiallyFilled { .. }
            | OffchainOrder::Filled { .. }
            | OffchainOrder::Failed { .. }
            | OffchainOrder::Cancelled { .. } => {}
        }

        self.finalize_position_for_terminal_order(ctx, symbol).await
    }

    /// Reloads the order and, if it has reached a terminal state, finalizes the
    /// owning `Position` via the shared
    /// [`terminal_position_finalization`] mapping -- the same classification
    /// the startup orphan-recovery sweep and rejection path use, so the
    /// terminal-state -> position-command mapping cannot drift between them.
    ///
    /// Fails closed in two ways, never clearing the position incorrectly:
    /// - an `UnpricedFill` (a positive fill the broker never priced) leaves the
    ///   position pending and logs loudly -- dropping or force-clearing it
    ///   would under-account the executed shares (double hedge on the next
    ///   scan);
    /// - a still-non-terminal order leaves the position pending for the next
    ///   poll/recovery pass.
    async fn finalize_position_for_terminal_order<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        let Some(order) = ctx
            .offchain_order_projection
            .load(&self.offchain_order_id)
            .await?
        else {
            warn!(
                target: "broker",
                %symbol,
                offchain_order_id = %self.offchain_order_id,
                "PollOrderStatus: order disappeared before finalizing its position"
            );
            return Ok(());
        };

        match terminal_position_finalization(&order) {
            Some(TerminalPositionFinalization::UnpricedFill { shares_filled }) => {
                error!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    %shares_filled,
                    "Terminal order carries a positive fill the broker never priced; \
                     leaving the position pending (fail closed) rather than clearing it"
                );

                Ok(())
            }

            Some(finalization) => {
                let Some(command) =
                    position_command_for_finalization(finalization, self.offchain_order_id)
                else {
                    // `UnpricedFill` is the only `None` case and is handled
                    // above; this arm is unreachable but stays fail-closed.
                    return Ok(());
                };

                let position_pending = ctx
                    .position_store
                    .load(symbol)
                    .await?
                    .and_then(|position| position.pending_offchain_order_id);
                if position_pending != Some(self.offchain_order_id) {
                    info!(
                        target: "broker",
                        %symbol,
                        offchain_order_id = %self.offchain_order_id,
                        ?position_pending,
                        "PollOrderStatus: position no longer expecting this terminal order, skipping"
                    );
                    return Ok(());
                }

                ctx.position_store.send(symbol, command).await?;
                info!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    "PollOrderStatus: finalized the owning position for a terminal order"
                );

                Ok(())
            }

            None => {
                warn!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    state = ?order,
                    "PollOrderStatus: order not terminal after cancellation handling; \
                     leaving the position pending"
                );

                Ok(())
            }
        }
    }
}

/// Decision derived from the order's current aggregate state, before any
/// broker call. Lets [`PollOrderStatus::perform`] stay flat: branch on this,
/// then either return or do the broker round-trip.
enum PollAction {
    /// Already terminal or unrecoverable -- discard this poll job.
    Drop,
    /// Terminal cancellation whose position finalization may need retrying.
    FinalizeCancelled,
    /// Not yet submitted to the broker. Re-queue and wait.
    Reschedule,
    /// Submitted (or partially filled). Ask the broker for status.
    Query { executor_order_id: ExecutorOrderId },
}

fn dispatch_for_order_state(
    offchain_order_id: OffchainOrderId,
    order: &OffchainOrder,
) -> PollAction {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };
    let symbol = order.symbol();
    match order {
        Filled { .. } | Failed { .. } => {
            debug!(
                target: "broker",
                %symbol,
                %offchain_order_id,
                "PollOrderStatus: order already in terminal state, dropping"
            );

            PollAction::Drop
        }

        Cancelled { .. } => {
            debug!(
                target: "broker",
                %symbol,
                %offchain_order_id,
                "PollOrderStatus: order already Cancelled, retrying position finalization"
            );

            PollAction::FinalizeCancelled
        }

        Pending { .. } => {
            debug!(
                target: "broker",
                %symbol,
                %offchain_order_id,
                "PollOrderStatus: order still Pending (not yet submitted), \
                 re-enqueuing with delay"
            );

            PollAction::Reschedule
        }

        Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. } => {
            order.executor_order_id().map_or_else(
                || {
                    warn!(
                        target: "broker",
                        %symbol,
                        %offchain_order_id,
                        "PollOrderStatus: missing executor_order_id on submitted order"
                    );

                    PollAction::Drop
                },
                |executor_order_id| PollAction::Query {
                    executor_order_id: executor_order_id.clone(),
                },
            )
        }
    }
}

async fn reschedule_self<E>(
    ctx: &PollOrderStatusCtx<E>,
    offchain_order_id: OffchainOrderId,
) -> Result<(), JobError>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    let mut queue = ctx.poll_status_queue.clone();
    queue
        .push_with_delay(PollOrderStatus { offchain_order_id }, ctx.poll_interval)
        .await?;

    Ok(())
}

/// Re-enqueue a [`PollOrderStatus`] job for every offchain order still in a
/// non-terminal post-submission state. Idempotent: workers that pick up a
/// duplicate poll job for an already-reconciled order observe the terminal
/// state and exit cleanly.
///
/// Closes the gap between
/// [`OffchainOrderCommand::Place`](crate::offchain::order::OffchainOrderCommand::Place)
/// succeeding and the in-process push of the poll job that follows it -- if
/// the bot crashes between those two writes, the order would otherwise sit in
/// `Submitted` forever waiting for a poll that never comes.
pub(crate) async fn recover_submitted_offchain_orders(
    offchain_order_projection: &Projection<OffchainOrder>,
    poll_status_queue: &mut PollOrderStatusJobQueue,
    executor_type: SupportedExecutor,
) -> Result<(), JobError> {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };
    let orders = offchain_order_projection.load_all().await?;

    let pending_poll: Vec<_> = orders
        .into_iter()
        .filter_map(|(id, order)| match order {
            Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }
                if order.executor() == executor_type =>
            {
                Some(id)
            }
            Submitted { .. }
            | PartiallyFilled { .. }
            | Cancelling { .. }
            | Pending { .. }
            | Filled { .. }
            | Failed { .. }
            | Cancelled { .. } => None,
        })
        .collect();

    if pending_poll.is_empty() {
        return Ok(());
    }

    info!(
        count = pending_poll.len(),
        "Re-enqueuing PollOrderStatus jobs for offchain orders awaiting broker fill"
    );

    for offchain_order_id in pending_poll {
        poll_status_queue
            .push(PollOrderStatus { offchain_order_id })
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;

    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, ExecutionError, FractionalShares, MockExecutor, Positive, Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::{
        NoFillOutcome, OffchainOrderCommand, TerminalPositionFinalization,
        terminal_position_finalization,
    };
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::{OnchainTradeBuilder, setup_test_pools};

    const TEST_POLL_INTERVAL: Duration = Duration::from_secs(15);

    struct TestInfra<E: Executor + Clone + Send + Sync + 'static> {
        ctx: PollOrderStatusCtx<E>,
        apalis_pool: apalis_sqlite::SqlitePool,
        offchain_order: Arc<st0x_event_sorcery::Store<OffchainOrder>>,
        position: Arc<st0x_event_sorcery::Store<Position>>,
    }

    async fn build_test_infra<E: Executor + Clone + Send + Sync + 'static>(
        executor: E,
    ) -> TestInfra<E> {
        let (pool, apalis_pool) = setup_test_pools().await;

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build()
                .await
                .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let ctx = PollOrderStatusCtx {
            executor,
            offchain_order_projection,
            offchain_order_store: offchain_order.clone(),
            position_store: position.clone(),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            reconcile_queue: ReconcileOrderFillJobQueue::new(&apalis_pool),
            rejection_queue: HandleOrderRejectionJobQueue::new(&apalis_pool),
            poll_interval: TEST_POLL_INTERVAL,
        };

        TestInfra {
            ctx,
            apalis_pool,
            offchain_order,
            position,
        }
    }

    async fn submit_offchain_order<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
    ) -> OffchainOrderId {
        submit_offchain_order_with_executor(
            infra,
            symbol,
            tokenized_symbol,
            shares,
            direction,
            SupportedExecutor::DryRun,
        )
        .await
    }

    async fn submit_offchain_order_with_executor<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
        order_executor: SupportedExecutor,
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
            .position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor: order_executor,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        infra
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor: order_executor,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        infra
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("test-accept"),
                    placed_shares: shares,
                    submitted_at: Utc::now(),
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    async fn count_jobs(apalis_pool: &apalis_sqlite::SqlitePool, job_type: &str) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(job_type)
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    fn poll_order_status_job_type() -> &'static str {
        std::any::type_name::<PollOrderStatus>()
    }

    fn reconcile_order_fill_job_type() -> &'static str {
        std::any::type_name::<ReconcileOrderFill>()
    }

    fn handle_order_rejection_job_type() -> &'static str {
        std::any::type_name::<HandleOrderRejection>()
    }

    async fn min_run_at(apalis_pool: &apalis_sqlite::SqlitePool, job_type: &str) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT MIN(run_at) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(job_type)
        .fetch_one(apalis_pool)
        .await
        .unwrap()
    }

    async fn assert_partially_filled_order<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        order_id: OffchainOrderId,
        expected_shares_filled: FractionalShares,
        expected_avg_price: Usd,
    ) {
        let order = infra
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::PartiallyFilled {
            shares_filled,
            avg_price,
            ..
        } = order
        else {
            panic!("expected OffchainOrder::PartiallyFilled, got {order:?}");
        };

        assert_eq!(shares_filled, expected_shares_filled);
        assert_eq!(avg_price, expected_avg_price);
    }

    #[tokio::test]
    async fn poll_with_filled_broker_enqueues_reconcile_fill() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            1,
            "PollOrderStatus must enqueue exactly one ReconcileOrderFill on Filled"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Filled state must not enqueue HandleOrderRejection"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Filled state must not re-enqueue PollOrderStatus"
        );
    }

    #[tokio::test]
    async fn poll_with_pending_broker_reschedules_self_with_delay() {
        let executor = MockExecutor::new().with_order_status(OrderState::Pending);
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let before_now: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "Pending broker state must self-reschedule a PollOrderStatus job"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            0
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0
        );

        let scheduled_at = min_run_at(&infra.apalis_pool, poll_order_status_job_type()).await;
        let poll_interval_secs: i64 = TEST_POLL_INTERVAL.as_secs().try_into().unwrap();
        let expected_min = before_now + poll_interval_secs;
        assert!(
            scheduled_at >= expected_min,
            "Re-enqueued poll should run at >= now + poll_interval ({expected_min}s); got {scheduled_at}s"
        );
    }

    /// Asserts that polling a confirmed broker cancellation finalized the owning
    /// position INLINE -- releasing the pending slot with no fill and no failure
    /// anchor -- rather than leaving it stuck. The terminal order's
    /// classification is checked against the shared
    /// `terminal_position_finalization` mapping (the same one the conductor
    /// recovery and rejection paths use), and the position is asserted already
    /// released by `PollOrderStatus::perform`, NOT finalized by the test.
    async fn assert_position_released_without_fill<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        symbol: &Symbol,
        order: &OffchainOrder,
    ) {
        let OffchainOrder::Cancelled {
            reason,
            cancelled_at,
            ..
        } = order
        else {
            panic!("expected a terminal Cancelled order, got: {order:?}");
        };

        let finalization = terminal_position_finalization(order)
            .expect("terminal Cancelled order must classify for finalization");
        assert_eq!(
            finalization,
            TerminalPositionFinalization::NoFill(NoFillOutcome::Cancelled {
                reason: *reason,
                cancelled_at: *cancelled_at,
            }),
            "Zero-fill cancellation must classify as NoFill with the broker's cancellation outcome"
        );

        let position = infra.position.load(symbol).await.unwrap().unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Polling a confirmed cancellation must release the position's pending slot inline"
        );
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "Intentional cancellation must NOT set the failure anchor"
        );
    }

    #[tokio::test]
    async fn poll_with_cancelled_broker_confirms_local_cancellation() {
        let cancelled_at = Utc::now();
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::ZERO,
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(order, OffchainOrder::Cancelled { .. }),
            "Broker Cancelled must mark local Cancelling order terminal, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Terminal cancellation must not re-enqueue PollOrderStatus"
        );

        assert_position_released_without_fill(&infra, &symbol, &order).await;
    }

    #[tokio::test]
    async fn poll_with_cancelled_zero_fill_without_avg_price_confirms_cancellation() {
        let cancelled_at = Utc::now();
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::ZERO,
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(order, OffchainOrder::Cancelled { .. }),
            "Zero-fill broker cancellation without avg_price must mark local order terminal, got: {order:?}"
        );

        assert_position_released_without_fill(&infra, &symbol, &order).await;
    }

    #[tokio::test]
    async fn retry_after_confirmed_cancellation_finalizes_position() {
        let executor = MockExecutor::new();
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let position_before = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_before.pending_offchain_order_id,
            Some(order_id),
            "test setup should leave the position pending after order-only confirmation"
        );

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position_after = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_after.pending_offchain_order_id, None,
            "Retrying a terminal Cancelled poll must finish the position finalization"
        );
        assert_eq!(
            position_after.last_failed_offchain_order_id, None,
            "Cancellation finalization must not create a failure idempotency anchor"
        );
    }

    /// Broker reports Cancelled with a positive priced partial fill while the
    /// order is locally `Cancelling`: the poll must record the fill
    /// (`UpdatePartialFill`) BEFORE confirming the cancellation, so the
    /// terminal `Cancelled` state carries the executed shares and the
    /// position finalization completes the hedge for them instead of
    /// double-hedging on the next scan.
    #[tokio::test]
    async fn poll_with_cancelled_partial_fill_records_fill_before_confirming_cancellation() {
        let cancelled_at = Utc::now();
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::new(float!(50)),
            avg_price: Some(Usd::new(float!(150.0))),
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(
                &order,
                OffchainOrder::Cancelled {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            avg_price,
                            ..
                        }),
                    ..
                } if *shares_filled == FractionalShares::new(float!(50))
                    && *avg_price == Usd::new(float!(150.0))
            ),
            "Cancelled order must retain the broker-reported priced fill, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Cancellation with a partial fill must not be routed to rejection"
        );

        // Position-side: the shared terminal classification must complete the
        // hedge for the filled quantity, stamped with the broker's
        // cancellation time.
        let finalization = terminal_position_finalization(&order)
            .expect("terminal Cancelled order must classify for finalization");
        assert_eq!(
            finalization,
            TerminalPositionFinalization::Complete {
                shares_filled: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("test-accept"),
                price: Usd::new(float!(150.0)),
                broker_timestamp: cancelled_at,
            },
        );

        // `perform` must finalize the owning position inline -- the retained
        // priced fill is completed and the pending slot released, stamped with
        // the broker cancellation time (not the local wall clock).
        let position = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Polling the priced cancellation must complete the retained fill and release the slot inline"
        );
        assert_eq!(
            position.last_updated,
            Some(cancelled_at),
            "Position must be stamped with the broker cancellation time, not local wall clock"
        );

        // The priced cancellation path finalizes the owning position inline,
        // so no follow-up poll is needed. A duplicate poll against the
        // terminal aggregate must still no-op rather than loop or re-route.
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Inline cancellation finalization must not enqueue a redundant follow-up poll"
        );
        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();
        let after_followup = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(after_followup, OffchainOrder::Cancelled { .. }),
            "Follow-up poll against a terminal order must leave it Cancelled"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Follow-up poll must not enqueue rejection for a terminal order"
        );
    }

    /// Crash window in `CancelOrder`: the broker accepted the DELETE but the
    /// process died before `CancelRequested` was persisted, so recovery polls
    /// a locally-`Submitted` order whose broker state is already Cancelled.
    /// The poll must record it as an intentional cancellation -- NOT route it
    /// to `HandleOrderRejection`, which would set the position's
    /// `last_failed_offchain_order_id` anchor and let the next replacement
    /// adopt the cancelled broker order via duplicate-client-order-id
    /// recovery.
    #[tokio::test]
    async fn poll_with_cancelled_broker_without_local_cancel_request_records_cancelled_not_failed()
    {
        let cancelled_at = Utc::now();
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::ZERO,
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        // No CancelOrder command: the local order is still Submitted.

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(order, OffchainOrder::Cancelled { .. }),
            "Broker-confirmed cancellation must become terminal Cancelled even \
             without a surviving local cancel request, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Broker-side cancellation must NOT be misclassified as a failure"
        );

        assert_position_released_without_fill(&infra, &symbol, &order).await;
    }

    /// Same crash window as above, but the order partially filled before the
    /// broker cancelled it: recovery must record the priced fill AND resolve
    /// the order as Cancelled, preserving both properties at once.
    #[tokio::test]
    async fn poll_with_cancelled_partial_fill_after_crash_recovers_fill_and_cancellation() {
        let cancelled_at = Utc::now();
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::new(float!(50)),
            avg_price: Some(Usd::new(float!(150.0))),
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        // No CancelOrder command: the local order is still Submitted.

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(
                &order,
                OffchainOrder::Cancelled {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            avg_price,
                            ..
                        }),
                    ..
                } if *shares_filled == FractionalShares::new(float!(50))
                    && *avg_price == Usd::new(float!(150.0))
            ),
            "Crash recovery must preserve the priced fill on the Cancelled order, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Broker-side cancellation must NOT be misclassified as a failure"
        );

        let finalization = terminal_position_finalization(&order)
            .expect("terminal Cancelled order must classify for finalization");
        assert_eq!(
            finalization,
            TerminalPositionFinalization::Complete {
                shares_filled: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("test-accept"),
                price: Usd::new(float!(150.0)),
                broker_timestamp: cancelled_at,
            },
            "Recovered cancellation must complete the position for the filled shares"
        );
    }

    #[tokio::test]
    async fn poll_with_failed_broker_enqueues_handle_rejection() {
        let executor = MockExecutor::new().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("broker rejected".to_string()),
            shares_filled: None,
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            1,
            "Failed broker state must enqueue exactly one HandleOrderRejection"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            0,
            "Failed state must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Terminal state must not re-enqueue PollOrderStatus"
        );
    }

    #[tokio::test]
    async fn poll_with_failed_partial_fill_records_fill_before_rejection() {
        let shares_filled = FractionalShares::new(float!(1));
        let avg_price = Usd::new(float!(150.0));
        let executor = MockExecutor::new().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("broker rejected after partial fill".to_string()),
            shares_filled: Some(shares_filled),
            avg_price: Some(avg_price),
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_partially_filled_order(&infra, order_id, shares_filled, avg_price).await;
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            1,
            "Failed partial fill must still enqueue HandleOrderRejection"
        );
    }

    #[tokio::test]
    async fn poll_with_cancelled_broker_confirms_cancellation_without_rejection() {
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at: Utc::now(),
            order_id: ExecutorOrderId::new("some-broker-order-id"),
            shares_filled: FractionalShares::ZERO,
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        // A broker Cancelled (no-fill) for a still-live local order is a
        // cancellation, not a rejection: 2b routes it through the
        // cancellation-confirmation path, driving the order terminal Cancelled
        // rather than enqueueing a HandleOrderRejection cleanup job.
        let order = infra.offchain_order.load(&order_id).await.unwrap().unwrap();
        assert!(
            matches!(order, OffchainOrder::Cancelled { .. }),
            "Broker Cancelled must drive a live local order terminal Cancelled, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Cancellation must NOT enqueue a HandleOrderRejection job"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            0,
            "Cancelled state must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Terminal state must not re-enqueue PollOrderStatus"
        );

        // The cancellation must also finalize the owning position inline:
        // recovering a broker-side cancel on a still-live order releases the
        // pending slot (without the failure anchor) so the symbol is not
        // permanently blocked from new hedges.
        let position = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Confirming a broker-side cancellation must release the position's pending slot inline"
        );
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "An intentional cancellation must NOT set the failure anchor"
        );
    }

    /// Broker reports a terminal `Cancelled` with a positive fill it never
    /// priced (`avg_price: None`). The old behavior re-polled forever; the fix
    /// fails closed -- it must NOT reschedule (a terminal order's price will not
    /// appear on a later poll) and must leave the position pending rather than
    /// clearing it, since the executed shares cannot be recorded without a price
    /// and clearing would double-hedge on the next scan.
    #[tokio::test]
    async fn poll_with_cancelled_unpriced_fill_fails_closed_without_reschedule() {
        let executor = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at: Utc::now(),
            order_id: ExecutorOrderId::new("noop"),
            shares_filled: FractionalShares::new(float!(50)),
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Unpriced terminal fill must NOT reschedule a poll (no infinite loop)"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Unpriced terminal fill must not be routed to rejection"
        );

        let position = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position.pending_offchain_order_id,
            Some(order_id),
            "Unpriced fill must leave the position pending (fail closed), never cleared"
        );
    }

    /// Same fail-closed contract as the cancelled case, but the broker reports a
    /// terminal `Failed` carrying a positive unpriced fill.
    #[tokio::test]
    async fn poll_with_failed_unpriced_fill_fails_closed_without_reschedule() {
        let executor = MockExecutor::new().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("broker rejected after partial fill".to_string()),
            shares_filled: Some(FractionalShares::new(float!(50))),
            avg_price: None,
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Unpriced terminal fill must NOT reschedule a poll (no infinite loop)"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Unpriced terminal fill must not be routed to rejection"
        );

        let position = infra.position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position.pending_offchain_order_id,
            Some(order_id),
            "Unpriced fill must leave the position pending (fail closed), never cleared"
        );
    }

    #[tokio::test]
    async fn poll_with_partially_filled_broker_reschedules_self_with_delay() {
        let shares_filled = FractionalShares::new(float!(1));
        let avg_price = Usd::new(float!(150.0));
        let executor = MockExecutor::new().with_order_status(OrderState::PartiallyFilled {
            order_id: ExecutorOrderId::new("some-broker-order-id"),
            shares_filled,
            avg_price: Some(avg_price),
            partially_filled_at: Utc::now(),
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let before_now: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_partially_filled_order(&infra, order_id, shares_filled, avg_price).await;
        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "PartiallyFilled broker state must self-reschedule a PollOrderStatus job"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            0,
            "PartiallyFilled state must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "PartiallyFilled state must not enqueue HandleOrderRejection"
        );

        let scheduled_at = min_run_at(&infra.apalis_pool, poll_order_status_job_type()).await;
        let poll_interval_secs: i64 = TEST_POLL_INTERVAL.as_secs().try_into().unwrap();
        let expected_min = before_now + poll_interval_secs;
        assert!(
            scheduled_at >= expected_min,
            "Re-enqueued poll should run at >= now + poll_interval ({expected_min}s); got {scheduled_at}s"
        );
    }

    /// Two orders, two PollOrderStatus jobs. The broker fails the call for the
    /// first order and fills the second. The failure of the first must not
    /// prevent the second from progressing to ReconcileOrderFill -- the whole
    /// point of converting from a single-sweep poller to per-order jobs.
    #[tokio::test]
    async fn poll_jobs_for_different_orders_run_independently() {
        #[derive(Clone)]
        struct FailFirstExecutor {
            counter: Arc<AtomicUsize>,
            inner: MockExecutor,
        }

        #[async_trait::async_trait]
        impl Executor for FailFirstExecutor {
            type Error = ExecutionError;
            type OrderId = String;
            type Ctx = ();

            async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
                unimplemented!()
            }

            async fn is_market_open(&self) -> Result<bool, Self::Error> {
                self.inner.is_market_open().await
            }

            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<st0x_execution::OrderPlacement<Self::OrderId>, Self::Error> {
                self.inner.place_market_order(order).await
            }

            async fn get_order_status(
                &self,
                order_id: &Self::OrderId,
            ) -> Result<OrderState, Self::Error> {
                let n = self.counter.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    return Err(ExecutionError::MockFailure {
                        message: "simulated broker outage for first call".to_string(),
                    });
                }
                self.inner.get_order_status(order_id).await
            }

            fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
                self.inner.parse_order_id(order_id_str)
            }

            async fn get_inventory(&self) -> Result<st0x_execution::InventoryResult, Self::Error> {
                self.inner.get_inventory().await
            }

            fn to_supported_executor(&self) -> SupportedExecutor {
                SupportedExecutor::DryRun
            }

            async fn preflight_counter_trade(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<st0x_execution::CounterTradePreflight, Self::Error> {
                self.inner.preflight_counter_trade(order).await
            }

            async fn place_limit_order(
                &self,
                order: st0x_execution::LimitOrder,
            ) -> Result<st0x_execution::OrderPlacement<Self::OrderId>, Self::Error> {
                self.inner.place_limit_order(order).await
            }

            async fn cancel_order(
                &self,
                order_id: &Self::OrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Self::Error> {
                self.inner.cancel_order(order_id).await
            }
        }

        let infra = build_test_infra(FailFirstExecutor {
            counter: Arc::new(AtomicUsize::new(0)),
            inner: MockExecutor::new(),
        })
        .await;

        let symbol_failing = Symbol::new("AAPL").unwrap();
        let symbol_succeeding = Symbol::new("MSFT").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let failing_id =
            submit_offchain_order(&infra, &symbol_failing, "wtAAPL", shares, Direction::Sell).await;
        let succeeding_id = submit_offchain_order(
            &infra,
            &symbol_succeeding,
            "wtMSFT",
            shares,
            Direction::Sell,
        )
        .await;

        let first_result = PollOrderStatus {
            offchain_order_id: failing_id,
        }
        .perform(&infra.ctx)
        .await;
        assert!(
            matches!(
                first_result,
                Err(JobError::Execution(ExecutionError::MockFailure { .. })),
            ),
            "first poll should propagate broker error, got {first_result:?}"
        );

        PollOrderStatus {
            offchain_order_id: succeeding_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            1,
            "Second order's poll must enqueue ReconcileOrderFill despite first order's failure"
        );
    }

    #[tokio::test]
    async fn recover_with_no_orders_enqueues_nothing() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let mut queue = infra.ctx.poll_status_queue.clone();

        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Empty projection must not enqueue any poll jobs"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_poll_for_submitted_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let _ = submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "Submitted order must trigger exactly one PollOrderStatus on recovery"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_poll_for_partially_filled_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(10))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(5)),
                    avg_price: Usd::new(float!(150.0)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "PartiallyFilled order must trigger one PollOrderStatus on recovery"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_poll_for_cancelling_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(10))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: crate::offchain::order::CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "Cancelling order must trigger one PollOrderStatus on recovery"
        );
    }

    #[tokio::test]
    async fn recover_skips_filled_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Filled order must not be re-polled"
        );
    }

    #[tokio::test]
    async fn recover_skips_failed_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected".to_string(),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Failed order must not be re-polled"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_one_poll_per_eligible_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();

        let aapl = Symbol::new("AAPL").unwrap();
        let _ = submit_offchain_order(&infra, &aapl, "wtAAPL", shares, Direction::Sell).await;

        let tsla = Symbol::new("TSLA").unwrap();
        let _ = submit_offchain_order(&infra, &tsla, "wtTSLA", shares, Direction::Sell).await;

        let msft = Symbol::new("MSFT").unwrap();
        let msft_id = submit_offchain_order(&infra, &msft, "wtMSFT", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &msft_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            2,
            "Two Submitted orders + one Filled must enqueue exactly two polls"
        );
    }

    #[tokio::test]
    async fn recover_skips_orders_from_different_executor() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();

        let aapl = Symbol::new("AAPL").unwrap();
        let _ = submit_offchain_order_with_executor(
            &infra,
            &aapl,
            "wtAAPL",
            shares,
            Direction::Sell,
            SupportedExecutor::AlpacaBrokerApi,
        )
        .await;

        let tsla = Symbol::new("TSLA").unwrap();
        let _ = submit_offchain_order_with_executor(
            &infra,
            &tsla,
            "wtTSLA",
            shares,
            Direction::Sell,
            SupportedExecutor::DryRun,
        )
        .await;

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "Only the order placed by the currently-configured executor must be re-polled"
        );
    }

    #[tokio::test]
    async fn poll_drops_job_when_order_belongs_to_different_executor() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id = submit_offchain_order_with_executor(
            &infra,
            &symbol,
            "wtAAPL",
            shares,
            Direction::Sell,
            SupportedExecutor::AlpacaBrokerApi,
        )
        .await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            0,
            "Executor mismatch must drop the job without rescheduling"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, reconcile_order_fill_job_type()).await,
            0,
            "Executor mismatch must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.apalis_pool, handle_order_rejection_job_type()).await,
            0,
            "Executor mismatch must not enqueue HandleOrderRejection"
        );
    }
}
