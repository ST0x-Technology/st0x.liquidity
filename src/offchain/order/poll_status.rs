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
    CancellationReason, JobError, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
    RetainedFill, TerminalPositionFinalization, position_command_for_finalization,
    terminal_position_finalization,
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
        ctx.offchain_order_store
            .send(
                &self.offchain_order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::Unrequested,
                },
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

/// How many poll intervals a `Queued`/`Running` [`PollOrderStatus`] row may
/// sit without visible progress before [`reconcile_live_poll_jobs`] presumes
/// it stranded (dropped in-memory fetch buffer, a cancelled task, a latched
/// worker) rather than a poll genuinely in flight. A poll is a single broker
/// round-trip, so a live worker clears it within a small multiple of the
/// interval.
const STRANDED_POLL_JOB_INTERVAL_MULTIPLIER: u32 = 5;

/// Reconciles the live [`PollOrderStatus`] rows for `offchain_order_id`
/// toward a single-live-row invariant and returns whether one exists
/// afterward -- the caller should skip pushing a new job when this is
/// `true`.
///
/// "Live" means `Pending`; `Queued`/`Running` with `lock_at` newer than
/// `stale_after`; or retryable-`Failed` (`attempts < max_attempts`) with
/// `done_at` newer than `stale_after`. `lock_at`/`done_at` are read as
/// INTEGER unix-epoch-seconds values, which is what apalis-sqlite 1.0.0-rc.8
/// always writes there -- `queries/backend/fetch_next.sql` and
/// `queries/task/lock.sql` both set `lock_at = strftime('%s', 'now')`,
/// `queries/task/ack.sql` sets `done_at` the same way on every ack including
/// a `Failed` one, and `migrations/20251018164941_move_to_bytes.sql` declares
/// both columns `INTEGER` -- pinned in-repo by
/// `reconcile_live_poll_jobs_lock_at_from_a_real_apalis_lock_reads_back_as_recent_epoch_seconds`
/// (`src/offchain/order/poll_status.rs`), which pushes a job through apalis's
/// own `fetch_next`/`lock` path rather than a hand-seeded fixture. A
/// `Queued`/`Running` row past `stale_after` is presumed stranded (dropped
/// in-memory fetch buffer, a cancelled task, a latched worker) rather than a
/// poll genuinely in flight: nothing else ever ages such a row out under a
/// deterministic worker name (see `docs/conductor.md`'s "Gotcha" on orphaned
/// in-flight rows).
///
/// A retryable-`Failed` row is a live, immediately re-dispatchable chain head
/// under apalis's own `fetch_next.sql` (`status = 'Failed' AND attempts <
/// max_attempts`, ignoring `run_at`'s original value once already elapsed),
/// not an inert parked row -- so it must count as live here too, or recovery
/// forks a second chain alongside the one apalis is about to re-run the
/// moment a worker is free. But `ack.sql` never reschedules a `Failed` row
/// (no `run_at` bump), so a row stuck behind a latched worker (e.g. RAI-1495's
/// circuit breaker) would otherwise sit `Failed` forever and, if counted as
/// live unconditionally, would permanently suppress recovery's re-push for
/// that order. Bounding it by `done_at` freshness resolves both: a
/// just-failed row still counts as live (no duplicate chain forked while a
/// worker is about to retry it), but once it has sat `Failed` past
/// `stale_after` without a fresh ack, it stops suppressing recovery.
///
/// Beyond gating the return value, this also atomically converges
/// pre-existing duplicate `Pending` chains (each an independent,
/// self-perpetuating chain a prior unguarded recovery tick forked, RAI-1493)
/// back down to one: it keeps the survivor apalis's own dispatch order would
/// run first, and marks every other `Pending` row `Done` (with `done_at`
/// set). This is a single `UPDATE` whose `WHERE` clause selects the survivor
/// via a subquery, rather than a `SELECT` to pick a survivor followed by a
/// separately-predicated `UPDATE` -- SQLite evaluates the whole statement,
/// subquery included, as one atomic unit under its own write lock, so there
/// is no window between "decide the survivor" and "collapse the rest" for a
/// concurrent writer (another guard call racing this one, or
/// `reschedule_self` pushing a legitimate successor) to land in: it either
/// commits before this statement starts, and is included in the survivor
/// decision, or after this statement ends, and is left untouched -- never
/// observed half-written mid-decision. The old two-statement shape captured
/// a survivor id from a separate `SELECT`, then re-ran `id != <that id>` in a
/// later `UPDATE`; a successor landing in the gap between them matched that
/// stale predicate and was collapsed too, leaving zero live rows for the
/// order despite this function returning `true`. Pinned by
/// `reconcile_live_poll_jobs_atomic_collapse_never_leaves_zero_live_rows_when_a_successor_`
/// `commits_while_the_collapse_is_blocked`.
///
/// This single-statement collapse is always safe to run, even while a
/// `Queued`/`Running` row for this order is still fresh: it only ever
/// inspects/touches `Pending` rows, so a currently-executing row is never a
/// candidate, and if its `reschedule_self` successor has not landed yet
/// there is nothing else to collapse (a lone `Pending` row is trivially its
/// own survivor).
///
/// Apalis's `fetch_next.sql` (apalis-sqlite 1.0.0-rc.8) only ever ranks rows
/// it has already made eligible -- its `WHERE` clause requires `run_at IS
/// NULL OR run_at <= strftime('%s', 'now')` before its `ORDER BY priority
/// DESC, run_at ASC, id ASC` ever runs -- so a due row always beats a
/// not-yet-due one there, regardless of priority. This guard's own candidate
/// set is deliberately wider (every `Pending` row, not just due ones: the
/// normal steady-state successor `reschedule_self` pushes is one poll
/// interval in the future, and excluding it would make the guard return
/// `false` and push a duplicate). Textually replaying apalis's `ORDER BY`
/// alone over that wider set would therefore diverge from what apalis would
/// actually dispatch first whenever a due row and a higher-priority
/// not-yet-due row coexist, so due-ness is ranked first here too
/// (`(run_at IS NULL OR run_at <= strftime('%s', 'now')) DESC`) before
/// `priority DESC, run_at ASC, id ASC` -- the same tie-break apalis applies
/// among rows it would actually consider dispatching. `Jobs.run_at` is
/// epoch-*seconds*, so rows pushed within the same second need apalis's own
/// `id ASC` tie-break -- SQLite's row order among ties is otherwise
/// unspecified. `Done`, not `Killed`, matches `JobQueue::cancel_all_pending`'s
/// precedent for discarding superseded queue rows: this is routine dedupe,
/// not a non-retryable abort, and `load_job_queue_health`'s operator-facing
/// `killed` counter (`status = 'Killed' AND attempts < max_attempts`) exists
/// to surface the latter.
///
/// `json_extract` requires `CAST(job AS TEXT)` -- `Jobs.job` is a `BLOB`
/// (apalis's `JsonCodec`), and a bare BLOB argument is not guaranteed to be
/// read as JSON text on every SQLite build. `json_extract` also raises a hard
/// SQL error (not NULL) on a row whose `job` blob is not valid JSON at all,
/// and the order-id equality is not guaranteed to filter such a row out
/// before `json_extract` runs on it -- so every predicate here also requires
/// `json_valid(CAST(job AS TEXT))`, skipping a corrupt/foreign-codec row
/// instead of failing the whole guard (and, on the boot path, `Conductor::run()`)
/// for every order sharing this job type.
async fn reconcile_live_poll_jobs(
    apalis_pool: &apalis_sqlite::SqlitePool,
    offchain_order_id: OffchainOrderId,
    stale_after: Duration,
) -> Result<bool, JobError> {
    let job_type = std::any::type_name::<PollOrderStatus>();
    let stale_after_secs = i64::try_from(stale_after.as_secs())?;

    let collapsed = sqlx_apalis::query(
        "UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now') \
         WHERE job_type = ? \
           AND json_valid(CAST(job AS TEXT)) \
           AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
           AND status = 'Pending' \
           AND id != ( \
             SELECT id FROM Jobs \
             WHERE job_type = ? \
               AND json_valid(CAST(job AS TEXT)) \
               AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
               AND status = 'Pending' \
             ORDER BY (run_at IS NULL OR run_at <= strftime('%s', 'now')) DESC, \
                      priority DESC, run_at ASC, id ASC \
             LIMIT 1 \
           )",
    )
    .bind(job_type)
    .bind(offchain_order_id.to_string())
    .bind(job_type)
    .bind(offchain_order_id.to_string())
    .execute(apalis_pool)
    .await?;

    if collapsed.rows_affected() > 0 {
        warn!(
            target: "broker",
            %offchain_order_id,
            collapsed = collapsed.rows_affected(),
            "Collapsed duplicate live PollOrderStatus rows for one order down to one"
        );
    }

    let is_live: bool = sqlx_apalis::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM Jobs \
         WHERE job_type = ? \
           AND json_valid(CAST(job AS TEXT)) \
           AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
           AND ( \
             status = 'Pending' \
             OR (status IN ('Queued', 'Running') \
                  AND lock_at IS NOT NULL AND lock_at > strftime('%s', 'now') - ?) \
             OR (status = 'Failed' AND attempts < max_attempts \
                  AND done_at IS NOT NULL AND done_at > strftime('%s', 'now') - ?) \
           ))",
    )
    .bind(job_type)
    .bind(offchain_order_id.to_string())
    .bind(stale_after_secs)
    .bind(stale_after_secs)
    .fetch_one(apalis_pool)
    .await?;

    Ok(is_live)
}

/// Wrapper over [`reconcile_live_poll_jobs`] that computes `stale_after` from
/// `poll_interval` so every caller shares the same overflow-checked
/// staleness bound instead of repeating it. [`push_poll_job_if_absent`] is
/// the only caller, and every push site outside this module goes through
/// that, not this function directly.
///
/// Despite reading like a pure predicate, this call can also **write**: when
/// more than one `Pending` row already exists for `offchain_order_id` (a
/// pre-existing duplicate, RAI-1493), it collapses every non-survivor row to
/// `Done` as a side effect before returning `true`. Every call site must treat
/// this as a query-with-a-possible-write, not a read-only check.
///
/// RAI-1493's guard originally covered only the periodic recovery sweep
/// ([`recover_submitted_offchain_orders`]); four other push sites -- the
/// post-`Place` enqueue and per-fill reconciliation in `conductor.rs`'s
/// `dispatch_post_place_state`, the `PendingExecution` retry in
/// `recover_claimed_offchain_order`, the hedge job's
/// `recover_pending_poll_status`, and that recovery's own `Pending` re-drive
/// follow-up (`route_placement_outcome`, shared with the hedge job's primary
/// placement path) -- pushed unconditionally, so an order that stayed open
/// while its symbol kept trading could still fork one independent,
/// self-perpetuating chain per fill. This wrapper lets all five sites share
/// one guard, consolidated via [`push_poll_job_if_absent`] so the
/// check-then-push shape only needs to be implemented once.
async fn reconcile_and_check_live_poll_job(
    apalis_pool: &apalis_sqlite::SqlitePool,
    offchain_order_id: OffchainOrderId,
    poll_interval: Duration,
) -> Result<bool, JobError> {
    let stale_after = poll_interval
        .checked_mul(STRANDED_POLL_JOB_INTERVAL_MULTIPLIER)
        .ok_or(JobError::StaleAfterOverflow)?;

    reconcile_live_poll_jobs(apalis_pool, offchain_order_id, stale_after).await
}

/// Outcome of [`push_poll_job_if_absent`]: whether a new [`PollOrderStatus`]
/// job was pushed, or one was already live and the push was skipped.
pub(crate) enum PollJobPushOutcome {
    AlreadyLive,
    Pushed,
}

/// Pushes a [`PollOrderStatus`] job for `offchain_order_id` unless one is
/// already live ([`reconcile_and_check_live_poll_job`], which may also
/// collapse pre-existing duplicate `Pending` rows for this order to `Done`
/// as a side effect first). Consolidates the check-then-push shape every
/// `PollOrderStatus` push site shares (RAI-1493) so a future change to the
/// push step (a metric, different logging, another guard condition) only
/// needs to land once instead of being replicated by hand at every call
/// site -- exactly the class of gap that originally left the fifth push
/// site (`route_placement_outcome`) unguarded.
pub(crate) async fn push_poll_job_if_absent(
    mut queue: PollOrderStatusJobQueue,
    offchain_order_id: OffchainOrderId,
    poll_interval: Duration,
) -> Result<PollJobPushOutcome, JobError> {
    if reconcile_and_check_live_poll_job(queue.pool(), offchain_order_id, poll_interval).await? {
        return Ok(PollJobPushOutcome::AlreadyLive);
    }

    queue.push(PollOrderStatus { offchain_order_id }).await?;

    Ok(PollJobPushOutcome::Pushed)
}

/// Re-enqueue a [`PollOrderStatus`] job for every offchain order still in a
/// non-terminal post-submission state that does not already have a live poll
/// job ([`push_poll_job_if_absent`]), which also collapses any pre-existing
/// duplicate live rows for an order back down to one. Without the guard,
/// every periodic tick that finds an order still open would fork a
/// brand-new, independent, self-perpetuating poll chain (via
/// [`reschedule_self`]) on top of whatever chain(s) already exist for that
/// order, growing the live population without bound the longer the order
/// stays open (RAI-1493). The same guard also covers the other four
/// unconditional `PollOrderStatus` push sites (`conductor.rs`'s
/// `dispatch_post_place_state` and `recover_claimed_offchain_order`, and the
/// hedge job's `recover_pending_poll_status` and `route_placement_outcome`)
/// -- this sweep is only the periodic backstop, not the sole guarded path.
///
/// Closes the gap between
/// [`OffchainOrderCommand::Place`](crate::offchain::order::OffchainOrderCommand::Place)
/// succeeding and the in-process push of the poll job that follows it -- if
/// the bot crashes between those two writes, the order would otherwise sit in
/// `Submitted` forever waiting for a poll that never comes.
pub(crate) async fn recover_submitted_offchain_orders(
    offchain_order_projection: &Projection<OffchainOrder>,
    poll_status_queue: &PollOrderStatusJobQueue,
    executor_type: SupportedExecutor,
    poll_interval: Duration,
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

    let candidate_count = pending_poll.len();
    let mut pushed = 0usize;
    let mut skipped = 0usize;

    for offchain_order_id in pending_poll {
        match push_poll_job_if_absent(poll_status_queue.clone(), offchain_order_id, poll_interval)
            .await?
        {
            PollJobPushOutcome::AlreadyLive => {
                skipped += 1;
            }
            PollJobPushOutcome::Pushed => {
                pushed += 1;

                debug!(
                    target: "broker",
                    %offchain_order_id,
                    "PollOrderStatus: recovery armed polling for an order with no live poll job"
                );
            }
        }
    }

    info!(
        candidate_count,
        pushed,
        skipped,
        "Periodic submitted-order poll recovery swept offchain orders awaiting broker fill"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use sqlx_apalis::ConnectOptions;

    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, ExecutionError, FractionalShares, MockExecutor, Positive, Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::{
        NoFillOutcome, OffchainOrderCommand, TerminalPositionFinalization, noop_order_placer,
        terminal_position_finalization,
    };
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::{
        OnchainTradeBuilder, TEST_POLL_INTERVAL, setup_file_backed_test_db, setup_test_pools,
    };

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
                .build(noop_order_placer())
                .await
                .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
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
        let queue = infra.ctx.poll_status_queue.clone();

        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
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

    #[tokio::test]
    async fn json_extract_reads_the_offchain_order_id_from_a_real_pushed_job() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let mut queue = infra.ctx.poll_status_queue.clone();
        queue
            .push(PollOrderStatus { offchain_order_id })
            .await
            .unwrap();

        let extracted: String = sqlx_apalis::query_scalar(
            "SELECT json_extract(CAST(job AS TEXT), '$.offchain_order_id') FROM Jobs \
             WHERE job_type = ?",
        )
        .bind(poll_order_status_job_type())
        .fetch_one(&infra.apalis_pool)
        .await
        .unwrap();

        assert_eq!(
            extracted,
            offchain_order_id.to_string(),
            "json_extract(CAST(job AS TEXT), ...) must recover the exact offchain_order_id \
             UUID string from a real pushed job's BLOB payload"
        );
    }

    /// Pins the assumption `reconcile_live_poll_jobs`'s staleness predicate
    /// depends on (`lock_at > strftime('%s', 'now') - ?`): that apalis writes
    /// `lock_at` as an INTEGER unix-epoch-seconds value. Drives a real job
    /// through apalis's own `fetch_next` (`queries/backend/fetch_next.sql`,
    /// which sets `lock_at = strftime('%s', 'now')`) rather than a hand-seeded
    /// fixture, so an apalis upgrade that changed the representation would
    /// fail this test instead of silently passing every hand-seeded one.
    #[tokio::test]
    async fn reconcile_live_poll_jobs_lock_at_from_a_real_apalis_lock_reads_back_as_recent_epoch_seconds()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let mut queue = infra.ctx.poll_status_queue.clone();
        queue
            .push(PollOrderStatus { offchain_order_id })
            .await
            .unwrap();

        // `Jobs.lock_by` has a foreign key onto `Workers.id`; fetch_next
        // writes `lock_by` to this worker's name, so a matching Worker row
        // must exist first, exactly as apalis's own worker registration would
        // create in production.
        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, ?, 'test')",
        )
        .bind("test-worker")
        .bind(poll_order_status_job_type())
        .execute(&infra.apalis_pool)
        .await
        .unwrap();

        let config = apalis_sqlite::Config::new(poll_order_status_job_type());
        let worker = apalis_core::worker::context::WorkerContext::new::<()>("test-worker");
        let fetched = apalis_sqlite::fetcher::fetch_next(infra.apalis_pool.clone(), config, worker)
            .await
            .unwrap();

        assert_eq!(
            fetched.len(),
            1,
            "apalis's own fetch_next must lock exactly the one pushed job"
        );

        let lock_at: i64 = sqlx_apalis::query_scalar("SELECT lock_at FROM Jobs WHERE job_type = ?")
            .bind(poll_order_status_job_type())
            .fetch_one(&infra.apalis_pool)
            .await
            .unwrap();

        let now = Utc::now().timestamp();
        assert!(
            lock_at <= now && lock_at >= now - 5,
            "apalis's fetch_next must write lock_at as INTEGER unix-epoch-seconds close to \
             now (got {lock_at}, now {now}); reconcile_live_poll_jobs's staleness predicate \
             assumes this representation"
        );
    }

    /// The staleness bound `recover_submitted_offchain_orders` would derive
    /// from `TEST_POLL_INTERVAL` in production.
    const TEST_STALE_AFTER: Duration = Duration::from_secs(
        TEST_POLL_INTERVAL.as_secs() * STRANDED_POLL_JOB_INTERVAL_MULTIPLIER as u64,
    );

    async fn seed_poll_job_row(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
        status: &str,
        attempts: i64,
        lock_at: Option<i64>,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(&PollOrderStatus { offchain_order_id })
            .expect("serialize PollOrderStatus payload");
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at, lock_at) \
             VALUES (?, ?, ?, ?, ?, 25, strftime('%s', 'now'), ?)",
        )
        .bind(&id)
        .bind(poll_order_status_job_type())
        .bind(payload)
        .bind(status)
        .bind(attempts)
        .bind(lock_at)
        .execute(apalis_pool)
        .await
        .expect("seed PollOrderStatus job row");

        id
    }

    /// Seeds a `Pending` row with an explicit `run_at`, used to control which
    /// of several duplicate rows for one order is the earliest.
    async fn seed_pending_poll_job_row_at(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
        run_at: i64,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(&PollOrderStatus { offchain_order_id })
            .expect("serialize PollOrderStatus payload");
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at) \
             VALUES (?, ?, ?, 'Pending', 0, 25, ?)",
        )
        .bind(&id)
        .bind(poll_order_status_job_type())
        .bind(payload)
        .bind(run_at)
        .execute(apalis_pool)
        .await
        .expect("seed PollOrderStatus job row");

        id
    }

    /// Seeds a retryable-`Failed` row (`attempts < max_attempts`, the default
    /// `max_attempts` of 25) with an explicit `done_at`, used to control
    /// whether `reconcile_live_poll_jobs`'s staleness predicate reads it as
    /// fresh or stale. A real apalis ack (`queries/task/ack.sql`) always sets
    /// `done_at` to the ack time -- this only hand-seeds the value so a test
    /// can pin the staleness boundary precisely, unlike the fresh case, which
    /// is pinned against a real ack instead (see
    /// `reconcile_live_poll_jobs_true_when_a_fresh_retryable_failed_row_exists_via_a_real_apalis_ack`).
    async fn seed_failed_poll_job_row_at(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
        done_at: i64,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(&PollOrderStatus { offchain_order_id })
            .expect("serialize PollOrderStatus payload");
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at, done_at) \
             VALUES (?, ?, ?, 'Failed', 1, 25, strftime('%s', 'now'), ?)",
        )
        .bind(&id)
        .bind(poll_order_status_job_type())
        .bind(payload)
        .bind(done_at)
        .execute(apalis_pool)
        .await
        .expect("seed retryable Failed PollOrderStatus job row");

        id
    }

    /// Seeds a `Pending` row with an explicit `run_at` and `priority`, used to
    /// pin the survivor query's tie-break ordering
    /// (`priority DESC, run_at ASC, id ASC`, mirroring apalis's own
    /// `fetch_next.sql`).
    async fn seed_pending_poll_job_row_with_priority(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
        run_at: i64,
        priority: i64,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(&PollOrderStatus { offchain_order_id })
            .expect("serialize PollOrderStatus payload");
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, ?, 'Pending', 0, 25, ?, ?)",
        )
        .bind(&id)
        .bind(poll_order_status_job_type())
        .bind(payload)
        .bind(run_at)
        .bind(priority)
        .execute(apalis_pool)
        .await
        .expect("seed PollOrderStatus job row");

        id
    }

    async fn live_poll_job_ids(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
    ) -> Vec<String> {
        sqlx_apalis::query_scalar(
            "SELECT id FROM Jobs \
             WHERE job_type = ? \
               AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
               AND status IN ('Pending', 'Queued', 'Running')",
        )
        .bind(poll_order_status_job_type())
        .bind(offchain_order_id.to_string())
        .fetch_all(apalis_pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_true_when_a_pending_row_exists_for_the_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        seed_poll_job_row(&infra.apalis_pool, offchain_order_id, "Pending", 0, None).await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_true_when_a_fresh_queued_row_exists_for_the_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        seed_poll_job_row(
            &infra.apalis_pool,
            offchain_order_id,
            "Queued",
            0,
            Some(Utc::now().timestamp()),
        )
        .await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_true_when_a_fresh_running_row_exists_for_the_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        seed_poll_job_row(
            &infra.apalis_pool,
            offchain_order_id,
            "Running",
            0,
            Some(Utc::now().timestamp()),
        )
        .await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_false_when_the_only_running_row_is_stale() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let stale_lock_at =
            Utc::now().timestamp() - i64::try_from(TEST_STALE_AFTER.as_secs()).unwrap() - 60;
        seed_poll_job_row(
            &infra.apalis_pool,
            offchain_order_id,
            "Running",
            0,
            Some(stale_lock_at),
        )
        .await;

        assert!(
            !reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap(),
            "a Running row whose lock_at is older than stale_after must not count as live \
             (RAI-1493: a stranded row must not permanently suppress recovery's re-push)"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_false_when_no_row_exists_for_the_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();

        assert!(
            !reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_false_when_only_a_different_orders_row_exists() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let other_order_id = OffchainOrderId::new();
        seed_poll_job_row(&infra.apalis_pool, other_order_id, "Pending", 0, None).await;

        let offchain_order_id = OffchainOrderId::new();
        assert!(
            !reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_false_when_the_only_row_is_terminal_done() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        seed_poll_job_row(&infra.apalis_pool, offchain_order_id, "Done", 1, None).await;

        assert!(
            !reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );
    }

    /// Pins apalis's own contract that a retryable-`Failed` row
    /// (`attempts < max_attempts`) is a live, immediately re-dispatchable
    /// chain head, not an inert parked row: `fetch_next.sql` re-selects it
    /// (`status = 'Failed' AND attempts < max_attempts`, ignoring `run_at`'s
    /// original elapsed value) the moment a worker is free. Independent of
    /// `reconcile_live_poll_jobs`'s own staleness bound -- this only proves
    /// the external fact that bound is built on.
    #[tokio::test]
    async fn fetch_next_re_dispatches_a_retryable_failed_row_pinning_the_apalis_contract() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();

        // Push through the real queue (rather than hand-seeding an id) so
        // the row carries a real apalis-generated ULID id -- fetch_next's
        // own row decoding requires that format, and a hand-seeded id in a
        // foreign format would fail to decode regardless of this test's
        // point (apalis's re-dispatch contract), not because of it.
        let mut queue = infra.ctx.poll_status_queue.clone();
        queue
            .push(PollOrderStatus { offchain_order_id })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed', attempts = 1 WHERE job_type = ?")
            .bind(poll_order_status_job_type())
            .execute(&infra.apalis_pool)
            .await
            .unwrap();

        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, ?, 'test')",
        )
        .bind("test-worker")
        .bind(poll_order_status_job_type())
        .execute(&infra.apalis_pool)
        .await
        .unwrap();

        let config = apalis_sqlite::Config::new(poll_order_status_job_type());
        let worker = apalis_core::worker::context::WorkerContext::new::<()>("test-worker");
        let fetched = apalis_sqlite::fetcher::fetch_next(infra.apalis_pool.clone(), config, worker)
            .await
            .unwrap();

        assert_eq!(
            fetched.len(),
            1,
            "apalis's own fetch_next must re-dispatch a retryable-Failed row \
             (attempts < max_attempts), not treat it as an inert parked row"
        );
    }

    /// Drives a real apalis fail/ack path (rather than a hand-seeded fixture)
    /// so the guard's assumption about apalis's retry state machine cannot
    /// silently encode a wrong model: `ack.sql` parks an errored, still-
    /// retryable task as `Failed` in place, without rescheduling `run_at`,
    /// and `fetch_next.sql` will immediately re-select it once a worker is
    /// free. A just-failed row must therefore count as live here too, or
    /// recovery forks a duplicate chain alongside the one apalis is about to
    /// re-run.
    #[tokio::test]
    async fn reconcile_live_poll_jobs_true_when_a_fresh_retryable_failed_row_exists_via_a_real_apalis_ack()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let mut queue = infra.ctx.poll_status_queue.clone();
        queue
            .push(PollOrderStatus { offchain_order_id })
            .await
            .unwrap();

        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, ?, 'test')",
        )
        .bind("test-worker")
        .bind(poll_order_status_job_type())
        .execute(&infra.apalis_pool)
        .await
        .unwrap();

        let config = apalis_sqlite::Config::new(poll_order_status_job_type());
        let worker = apalis_core::worker::context::WorkerContext::new::<()>("test-worker");
        let fetched = apalis_sqlite::fetcher::fetch_next(infra.apalis_pool.clone(), config, worker)
            .await
            .unwrap();
        assert_eq!(fetched.len(), 1);

        let (_args, parts) = fetched.into_iter().next().unwrap().take();
        let task_id = parts.task_id.unwrap().to_string();
        let worker_id = parts.ctx.lock_by().clone().unwrap();

        apalis_sqlite::queries::ack_task::ack_task(
            &infra.apalis_pool,
            &task_id,
            &worker_id,
            "simulated transient broker error",
            &apalis_core::task::status::Status::Failed,
            1,
        )
        .await
        .unwrap();

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap(),
            "a just-failed retryable row (attempts < max_attempts) is a live, immediately \
             re-dispatchable chain head per apalis's own fetch_next.sql -- it must count as \
             live so recovery does not fork a duplicate chain alongside it"
        );
    }

    /// The RAI-1495 decoupling half of the same tradeoff: once a retryable-
    /// Failed row has sat past `stale_after` without a fresh ack (e.g. stuck
    /// behind a latched worker), it must stop suppressing recovery, or a
    /// stuck row would permanently block a fresh push for that order.
    #[tokio::test]
    async fn reconcile_live_poll_jobs_false_when_the_only_row_is_a_stale_retryable_failed_row() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let stale_done_at =
            Utc::now().timestamp() - i64::try_from(TEST_STALE_AFTER.as_secs()).unwrap() - 60;
        seed_failed_poll_job_row_at(&infra.apalis_pool, offchain_order_id, stale_done_at).await;

        assert!(
            !reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap(),
            "a retryable-Failed row whose done_at is older than stale_after must not \
             permanently suppress recovery (RAI-1493/1495 decoupling): a row stuck behind a \
             latched worker must eventually allow a fresh push"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_collapses_duplicate_pending_rows_to_the_earliest() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let now = Utc::now().timestamp();
        // Three independent forked chains for one order (RAI-1493), each a
        // Pending row with a different run_at.
        let earliest_id =
            seed_pending_poll_job_row_at(&infra.apalis_pool, offchain_order_id, now - 120).await;
        seed_pending_poll_job_row_at(&infra.apalis_pool, offchain_order_id, now - 60).await;
        seed_pending_poll_job_row_at(&infra.apalis_pool, offchain_order_id, now).await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, offchain_order_id).await,
            vec![earliest_id.clone()],
            "reconcile_live_poll_jobs must collapse duplicate live rows for one order down to \
             exactly the one with the earliest run_at"
        );

        let collapsed_rows: Vec<(String, Option<i64>)> = sqlx_apalis::query_as(
            "SELECT status, done_at FROM Jobs \
             WHERE job_type = ? \
               AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
               AND id != ?",
        )
        .bind(poll_order_status_job_type())
        .bind(offchain_order_id.to_string())
        .bind(&earliest_id)
        .fetch_all(&infra.apalis_pool)
        .await
        .unwrap();

        assert_eq!(
            collapsed_rows.len(),
            2,
            "both non-earliest duplicate rows must still exist, now collapsed"
        );
        let now = Utc::now().timestamp();
        for (status, done_at) in collapsed_rows {
            assert_eq!(
                status, "Done",
                "a collapsed duplicate must be marked Done, matching \
                 JobQueue::cancel_all_pending's precedent for discarding superseded queue rows, \
                 not Killed (which would pollute load_job_queue_health's operator-facing abort \
                 metric)"
            );
            assert!(
                matches!(done_at, Some(value) if value <= now && value >= now - 5),
                "a collapsed duplicate's done_at must be a fresh unix-epoch-seconds timestamp \
                 close to now (got {done_at:?}, now {now}), matching every other terminal row \
                 apalis itself writes -- not merely non-NULL, which would miss a regression \
                 that dropped or replaced the UPDATE's strftime('%s', 'now') expression"
            );
        }
    }

    /// Proves the collapse is atomic with the survivor decision by
    /// reproducing the exact precondition that broke the old two-statement
    /// implementation: a legitimate successor (as `reschedule_self` would
    /// push) committing while `reconcile_live_poll_jobs`'s own collapse
    /// write is already blocked in flight. The old code captured
    /// `pending_ids` in one `SELECT`, then re-ran `id != <captured
    /// survivor>` in a later `UPDATE`; a successor committing in that
    /// window matched the stale predicate and was wrongly collapsed
    /// alongside the real duplicate, leaving zero live rows despite the
    /// function returning `true`. Holding SQLite's own write lock before
    /// spawning the guard call, then committing the successor only after
    /// the guard has had a chance to start blocking on that same lock,
    /// proves the new single-statement collapse has no such window:
    /// whichever row survives, there is always exactly one -- never zero.
    #[tokio::test]
    async fn reconcile_live_poll_jobs_atomic_collapse_never_leaves_zero_live_rows_when_a_successor_commits_while_the_collapse_is_blocked()
     {
        let (_pool, apalis_pool, db_path, _dir) =
            setup_file_backed_test_db(Duration::from_secs(2)).await;
        let offchain_order_id = OffchainOrderId::new();
        let now = Utc::now().timestamp();

        // A pre-existing duplicate chain (the RAI-1493 steady state this
        // guard exists to collapse), due now so due-ness-first ordering
        // picks it as survivor over the not-yet-due successor below.
        let due_duplicate_id =
            seed_pending_poll_job_row_at(&apalis_pool, offchain_order_id, now - 60).await;

        let mut locker = sqlx_apalis::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .connect()
            .await
            .unwrap();
        sqlx_apalis::query("BEGIN IMMEDIATE")
            .execute(&mut locker)
            .await
            .unwrap();

        let pool_for_task = apalis_pool.clone();
        let collapse_task = tokio::spawn(async move {
            reconcile_live_poll_jobs(&pool_for_task, offchain_order_id, TEST_STALE_AFTER).await
        });

        // Give the spawned collapse a chance to reach and start blocking on
        // the write lock we already hold before we commit the successor --
        // the assertions below hold regardless of whether it actually got
        // there in time, since the collapse either observes the successor
        // fully committed or not at all, never mid-write.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // `reschedule_self`'s legitimate successor: not yet due, pushed one
        // poll interval in the future -- committed under the lock the
        // blocked collapse above is waiting to acquire.
        let successor_payload = serde_json::to_vec(&PollOrderStatus { offchain_order_id }).unwrap();
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at) \
             VALUES (?, ?, ?, 'Pending', 0, 25, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(poll_order_status_job_type())
        .bind(successor_payload)
        .bind(now + 300)
        .execute(&mut locker)
        .await
        .unwrap();

        sqlx_apalis::query("COMMIT")
            .execute(&mut locker)
            .await
            .unwrap();

        assert!(
            collapse_task.await.unwrap().unwrap(),
            "a live Pending row must remain after the collapse"
        );

        let live_ids = live_poll_job_ids(&apalis_pool, offchain_order_id).await;
        assert_eq!(
            live_ids.len(),
            1,
            "the collapse must never leave zero live rows for an order even when a legitimate \
             successor commits while the collapse's own write is blocked in flight; got \
             {live_ids:?}"
        );
        assert_eq!(
            live_ids[0], due_duplicate_id,
            "due-ness-first ordering must still pick the due pre-existing row over the \
             not-yet-due successor as survivor"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_survivor_selection_prefers_higher_priority_over_earlier_run_at()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let now = Utc::now().timestamp();

        // Lower priority but earlier run_at: apalis's fetch_next.sql dispatches
        // by `priority DESC, run_at ASC, id ASC`, so this row loses despite
        // firing sooner -- `run_at` alone would have kept it instead.
        seed_pending_poll_job_row_with_priority(
            &infra.apalis_pool,
            offchain_order_id,
            now - 120,
            0,
        )
        .await;
        let higher_priority_id =
            seed_pending_poll_job_row_with_priority(&infra.apalis_pool, offchain_order_id, now, 1)
                .await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, offchain_order_id).await,
            vec![higher_priority_id],
            "the survivor must be the row apalis's fetch_next.sql would dispatch first \
             (priority DESC before run_at ASC), not merely the earliest run_at"
        );
    }

    /// RAI-1493: apalis's own `fetch_next.sql` only ranks rows its `WHERE`
    /// clause has already made eligible (`run_at IS NULL OR run_at <=
    /// strftime('%s', 'now')`), so a due row always beats a not-yet-due one
    /// there regardless of priority -- it is never a candidate for
    /// `priority DESC` to lose against. Replaying only the `ORDER BY` over
    /// this guard's wider candidate set (every `Pending` row, not just due
    /// ones) would pick the not-yet-due higher-priority row instead, which
    /// apalis itself would never dispatch first. The survivor query must
    /// therefore rank due-ness ahead of priority.
    #[tokio::test]
    async fn reconcile_live_poll_jobs_survivor_selection_prefers_a_due_row_over_a_higher_priority_not_yet_due_row()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let now = Utc::now().timestamp();

        let due_id =
            seed_pending_poll_job_row_with_priority(&infra.apalis_pool, offchain_order_id, now, 0)
                .await;
        seed_pending_poll_job_row_with_priority(
            &infra.apalis_pool,
            offchain_order_id,
            now + 600,
            1,
        )
        .await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, offchain_order_id).await,
            vec![due_id],
            "the survivor must be the due, lower-priority row -- apalis would never dispatch \
             the not-yet-due higher-priority row first"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_survivor_selection_breaks_a_run_at_tie_by_id_asc() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        let run_at = Utc::now().timestamp();

        // Both rows share one run_at (pushed within the same second): apalis's
        // fetch_next.sql breaks that tie deterministically by `id ASC`, unlike
        // SQLite's otherwise-unspecified row order among ties.
        let mut ids = [
            seed_pending_poll_job_row_at(&infra.apalis_pool, offchain_order_id, run_at).await,
            seed_pending_poll_job_row_at(&infra.apalis_pool, offchain_order_id, run_at).await,
        ];
        ids.sort();
        let expected_survivor = ids[0].clone();

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, offchain_order_id).await,
            vec![expected_survivor],
            "rows tied on run_at must break the tie by id ASC, matching apalis's own dispatch \
             order, not SQLite's unspecified row order among ties"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_skips_a_malformed_job_row_instead_of_erroring() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();

        // A corrupt/foreign-codec `job` blob for this job type: not valid JSON,
        // so `json_extract` would raise a hard "malformed JSON" SQL error
        // without the `json_valid` guard -- and the order-id equality does not
        // filter this row out first, so an unrelated order's guard query would
        // fail too, not just this row's own (nonexistent) order.
        sqlx_apalis::query(
            "INSERT INTO Jobs (id, job_type, job, status, attempts, max_attempts, run_at) \
             VALUES (?, ?, ?, 'Pending', 0, 25, strftime('%s', 'now'))",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(poll_order_status_job_type())
        .bind(b"not json".to_vec())
        .execute(&infra.apalis_pool)
        .await
        .unwrap();

        let live =
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap();

        assert!(
            !live,
            "a malformed job row (for a different, or no, order) must not fail the guard \
             query for an unrelated order id"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_returns_int_conversion_when_stale_after_secs_exceeds_i64_max()
    {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        // `Duration::checked_mul` (the STRANDED_POLL_JOB_INTERVAL_MULTIPLIER
        // guard in `reconcile_and_check_live_poll_job`) can succeed here since u64 has roughly
        // double i64's range, but `stale_after.as_secs()` itself still exceeds
        // i64::MAX -- a narrower, deeper failure than `StaleAfterOverflow`,
        // guarded separately by this function's own `i64::try_from`.
        // `JobError::ApalisDatabase` (the other variant this function can
        // return) is a bare `#[from]` passthrough of a library error and is
        // deliberately left without a dedicated test.
        let stale_after = Duration::from_secs(u64::try_from(i64::MAX).unwrap() + 1);

        let error = reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, stale_after)
            .await
            .unwrap_err();

        assert!(
            matches!(error, JobError::IntConversion(_)),
            "a stale_after whose as_secs() exceeds i64::MAX must fail fast with a typed \
             conversion error instead of silently truncating or panicking"
        );
    }

    #[tokio::test]
    async fn reconcile_live_poll_jobs_does_not_collapse_pending_rows_while_a_fresh_running_row_exists()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let offchain_order_id = OffchainOrderId::new();
        seed_poll_job_row(
            &infra.apalis_pool,
            offchain_order_id,
            "Running",
            0,
            Some(Utc::now().timestamp()),
        )
        .await;
        seed_pending_poll_job_row_at(
            &infra.apalis_pool,
            offchain_order_id,
            Utc::now().timestamp(),
        )
        .await;

        assert!(
            reconcile_live_poll_jobs(&infra.apalis_pool, offchain_order_id, TEST_STALE_AFTER)
                .await
                .unwrap()
        );

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, offchain_order_id)
                .await
                .len(),
            2,
            "a fresh Running row and its Pending successor may briefly coexist for one \
             legitimate chain; reconcile_live_poll_jobs must not collapse them (doing so could \
             kill the successor instead of an actual duplicate, see doc comment)"
        );
    }

    #[tokio::test]
    async fn recover_submitted_offchain_orders_is_a_noop_when_a_live_poll_job_already_exists() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        seed_poll_job_row(&infra.apalis_pool, order_id, "Pending", 0, None).await;

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "recovery must not push a second job when a live poll job already exists"
        );
    }

    /// Drives a real apalis fail/ack path, matching
    /// `reconcile_live_poll_jobs_true_when_a_fresh_retryable_failed_row_exists_via_a_real_apalis_ack`:
    /// a just-failed retryable row is a live, immediately re-dispatchable
    /// chain head, so recovery must not fork a duplicate `Pending` chain
    /// alongside it.
    #[tokio::test]
    async fn recover_submitted_offchain_orders_does_not_push_when_only_a_fresh_retryable_failed_row_exists()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let mut queue = infra.ctx.poll_status_queue.clone();
        queue
            .push(PollOrderStatus {
                offchain_order_id: order_id,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, ?, 'test')",
        )
        .bind("test-worker")
        .bind(poll_order_status_job_type())
        .execute(&infra.apalis_pool)
        .await
        .unwrap();

        let config = apalis_sqlite::Config::new(poll_order_status_job_type());
        let worker = apalis_core::worker::context::WorkerContext::new::<()>("test-worker");
        let fetched = apalis_sqlite::fetcher::fetch_next(infra.apalis_pool.clone(), config, worker)
            .await
            .unwrap();
        let (_args, parts) = fetched.into_iter().next().unwrap().take();
        let task_id = parts.task_id.unwrap().to_string();
        let worker_id = parts.ctx.lock_by().clone().unwrap();

        apalis_sqlite::queries::ack_task::ack_task(
            &infra.apalis_pool,
            &task_id,
            &worker_id,
            "simulated transient broker error",
            &apalis_core::task::status::Status::Failed,
            1,
        )
        .await
        .unwrap();

        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            1,
            "a just-failed retryable row counts as live, so recovery must not fork a second, \
             duplicate chain alongside the one apalis is about to re-run"
        );
    }

    /// The RAI-1495 decoupling half: once the retryable-Failed row has gone
    /// stale (past `stale_after` without a fresh ack -- e.g. stuck behind a
    /// latched worker), recovery must push a fresh row rather than staying
    /// permanently suppressed.
    #[tokio::test]
    async fn recover_submitted_offchain_orders_re_pushes_once_the_only_retryable_failed_row_goes_stale()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        let stale_done_at =
            Utc::now().timestamp() - i64::try_from(TEST_STALE_AFTER.as_secs()).unwrap() - 60;
        seed_failed_poll_job_row_at(&infra.apalis_pool, order_id, stale_done_at).await;

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            2,
            "a stale retryable-Failed row must not permanently suppress recovery: recovery \
             must push a fresh Pending row alongside it, a bounded tradeoff (see \
             reconcile_live_poll_jobs)"
        );

        // A second tick, with the stale retryable-Failed row still present
        // untouched, must not push a third row: the Pending row pushed on
        // the first tick is what now makes the order look live, not the
        // Failed row -- proving the tradeoff is a single bounded extra row,
        // not an unbounded backlog that keeps growing every tick.
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            2,
            "a second recovery tick with the stale retryable-Failed row still present must not \
             push a third row: the first tick's freshly-pushed Pending row is what suppresses \
             further pushes, so the tradeoff stays bounded across ticks, not unbounded"
        );
    }

    #[tokio::test]
    async fn recover_submitted_offchain_orders_re_pushes_when_the_only_row_is_a_stale_running_row()
    {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        let stale_lock_at =
            Utc::now().timestamp() - i64::try_from(TEST_STALE_AFTER.as_secs()).unwrap() - 60;
        seed_poll_job_row(
            &infra.apalis_pool,
            order_id,
            "Running",
            0,
            Some(stale_lock_at),
        )
        .await;

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.apalis_pool, poll_order_status_job_type()).await,
            2,
            "a stranded Running row must not permanently suppress recovery's re-push \
             (RAI-1493): recovery must push a fresh Pending row alongside the stale one"
        );
    }

    #[tokio::test]
    async fn recover_submitted_offchain_orders_converges_pre_existing_duplicates_to_one_live_row() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        let now = Utc::now().timestamp();
        // Simulates the pre-fix incident: several independent forked chains
        // already live for one order before this fix's guard ever ran.
        seed_pending_poll_job_row_at(&infra.apalis_pool, order_id, now - 180).await;
        seed_pending_poll_job_row_at(&infra.apalis_pool, order_id, now - 90).await;
        seed_pending_poll_job_row_at(&infra.apalis_pool, order_id, now).await;

        let queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            TEST_POLL_INTERVAL,
        )
        .await
        .unwrap();

        assert_eq!(
            live_poll_job_ids(&infra.apalis_pool, order_id).await.len(),
            1,
            "recovery must converge pre-existing duplicate live PollOrderStatus rows for one \
             order down to exactly one instead of leaving each to self-perpetuate forever"
        );
    }

    #[tokio::test]
    async fn recover_submitted_offchain_orders_returns_stale_after_overflow_on_oversized_poll_interval()
     {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let queue = infra.ctx.poll_status_queue.clone();
        let error = recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &queue,
            SupportedExecutor::DryRun,
            Duration::from_secs(u64::MAX),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, JobError::StaleAfterOverflow),
            "an oversized order_polling_interval must fail fast with a typed overflow error \
             rather than panicking inside Duration's checked_mul"
        );
    }
}
