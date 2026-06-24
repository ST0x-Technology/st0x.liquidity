//! OffchainOrder CQRS/ES aggregate for tracking broker order lifecycle
//! (Pending -> Submitted -> Filled/Failed) plus the per-job machinery that
//! drives that lifecycle to a terminal state.
//!
//! # Per-job module layout
//!
//! Each apalis job that operates on an `OffchainOrder` lives in its own file
//! and carries its own `*Ctx` containing only the dependencies that job
//! actually uses. There is no shared umbrella context -- bag-of-everything
//! contexts obscure which job needs which dependency.
//!
//! - [`poll_status`] -- polls the broker for status and routes the result.
//! - [`reconcile_fill`] -- records a successful fill on the aggregate and
//!   position.
//! - [`handle_rejection`] -- records a broker rejection on the aggregate and
//!   position.
//!
//! [`JobError`] is shared across all three jobs because every job converts
//! the same upstream error sources (executor errors, aggregate send failures,
//! queue push failures). Splitting it per-job would duplicate `#[from]`
//! conversions without adding type safety.

pub(crate) mod handle_rejection;
pub(crate) mod poll_status;
pub(crate) mod reconcile_fill;

pub(crate) use handle_rejection::{HandleOrderRejection, HandleOrderRejectionJobQueue};
pub(crate) use poll_status::{
    PollOrderStatus, PollOrderStatusJobQueue, recover_submitted_offchain_orders,
};
pub(crate) use reconcile_fill::{ReconcileOrderFill, ReconcileOrderFillJobQueue};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
#[cfg(test)]
use std::sync::Arc;
use tracing::{debug, warn};
use uuid::Uuid;

use st0x_dto::{Direction, Trade, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, SendError, Store, Table};
use st0x_execution::{
    AlpacaBrokerApiError, ClientOrderId, ExecutionError, Executor, ExecutorOrderId,
    FractionalShares, MarketOrder, NotPositive, PersistenceError, Positive, SupportedExecutor,
    Symbol,
};
use st0x_finance::Usd;

use crate::conductor::job::QueuePushError;
use crate::onchain::OnChainError;
use crate::position::Position;

/// Errors surfaced by the per-order job pipeline.
///
/// Each concrete executor error gets its own variant via `#[from]` rather
/// than being boxed: the [`Job`](crate::conductor::job::Job) impls bound
/// `JobError: From<E::Error>` so `?` lifts whichever executor error the
/// caller picked. Adding a new executor means adding one variant here.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Onchain error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Projection query error: {0}")]
    OffchainOrderProjection(#[from] st0x_event_sorcery::ProjectionError<OffchainOrder>),
    #[error("Offchain order aggregate error: {0}")]
    OffchainOrderAggregate(#[from] st0x_event_sorcery::SendError<OffchainOrder>),
    #[error("Position aggregate error: {0}")]
    PositionAggregate(#[from] st0x_event_sorcery::SendError<Position>),
    #[error("Failed to enqueue follow-up job: {0}")]
    Enqueue(#[from] QueuePushError),
    #[error(
        "Broker reported partial fill of {shares_filled} for offchain order \
         {offchain_order_id} without an average price"
    )]
    MissingPartialFillPrice {
        offchain_order_id: OffchainOrderId,
        shares_filled: FractionalShares,
    },
    #[error("Broker reported invalid partial fill for offchain order {offchain_order_id}")]
    InvalidPartialFill {
        offchain_order_id: OffchainOrderId,
        #[source]
        source: NotPositive<FractionalShares>,
    },
}

/// Drives one offchain order placement end to end: records intent via `Place`,
/// performs the external `place_market_order` while the order is still
/// `Pending`, then feeds the broker outcome back as
/// `MarkAccepted`/`MarkPlacementFailed`.
///
/// This is the single broker call site, lifted out of the (now pure) `Place`
/// handler so the side effect lives in the durable placement path rather than a
/// command handler. At-least-once safe: a retry whose order already
/// left `Pending` skips the broker call entirely, and the broker dedupes on
/// `client_order_id` as a second line of defense. Returns the final order state
/// so callers can react (roll the position back on `Failed`, poll on
/// `Submitted`).
///
/// **Concurrency invariant:** a placement-failure outcome is recorded via
/// `MarkPlacementFailed`, which the aggregate honours only while the order is
/// still `Pending`. A stale attempt whose broker call errored after a concurrent
/// attempt already advanced the order past `Pending` is rejected by the handler
/// against the aggregate's authoritative state, so it can never strand a live
/// order. Callers on the live concurrent path (the trade-processing path and
/// `PlaceHedge`) still hold `counter_trade_submission_lock` to serialise
/// placement attempts; startup orphan recovery and the CLI `test-trade` command
/// run without it, which is safe because no concurrent placement runs there.
pub(crate) async fn place_offchain_order_at_broker(
    store: &Store<OffchainOrder>,
    order_placer: &dyn OrderPlacer,
    offchain_order_id: &OffchainOrderId,
    symbol: Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    executor: SupportedExecutor,
    client_order_id: ClientOrderId,
) -> Result<Option<OffchainOrder>, SendError<OffchainOrder>> {
    store
        .send(
            offchain_order_id,
            OffchainOrderCommand::Place {
                symbol: symbol.clone(),
                shares,
                direction,
                executor,
            },
        )
        .await?;

    // Only call the broker while the order is still Pending. A retry whose
    // outcome already landed (Submitted, or a terminal state) must not place a
    // second time. An exhaustive match forces a conscious decision for any
    // future state rather than letting it silently skip placement.
    let placed = store.load(offchain_order_id).await?;
    match placed {
        Some(OffchainOrder::Pending { .. }) => {}
        settled @ (Some(
            OffchainOrder::Submitted { .. }
            | OffchainOrder::PartiallyFilled { .. }
            | OffchainOrder::Filled { .. }
            | OffchainOrder::Failed { .. },
        )
        | None) => return Ok(settled),
    }

    // Capture metric labels before `symbol` is moved into the market order.
    let symbol_label = symbol.to_string();
    let direction_label = match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    };

    let market_order = MarketOrder {
        symbol,
        shares,
        direction,
        client_order_id,
    };
    let outcome = match order_placer.place_market_order(market_order).await {
        Ok(result) => {
            counter!(
                "hedge_trades_total",
                "symbol" => symbol_label,
                "direction" => direction_label
            )
            .increment(1);

            if result.placed_shares > shares {
                // A broker over-fill is real shares we now hold; record it as an
                // acceptance (ADR 0009) so the order keeps its executor_order_id
                // and is polled to a terminal state, rather than failed and
                // re-driven into an unbounded loop around a live, unpolled order.
                // The excess is reconciled as ordinary net exposure by the
                // periodic CheckPositions scan.
                warn!(
                    %offchain_order_id,
                    placed = %result.placed_shares,
                    requested = %shares,
                    "Broker placed more shares than requested; recording the over-fill as accepted"
                );
            }

            OffchainOrderCommand::MarkAccepted {
                executor_order_id: result.executor_order_id,
                placed_shares: result.placed_shares,
                submitted_at: Utc::now(),
            }
        }
        Err(error) => {
            counter!(
                "broker_errors_total",
                "symbol" => symbol_label,
                "kind" => "place_order_failed"
            )
            .increment(1);

            OffchainOrderCommand::MarkPlacementFailed {
                error: error.to_string(),
            }
        }
    };

    store.send(offchain_order_id, outcome).await?;
    store.load(offchain_order_id).await
}

/// Derives the broker-side [`ClientOrderId`] for a placement attempt.
///
/// When a prior attempt failed after the broker accepted the order, the
/// position aggregate stashes that attempt's [`OffchainOrderId`] as the
/// idempotency anchor. Retries must reuse its UUID as `client_order_id` so the
/// broker dedupes rather than double-submitting. Lives next to
/// [`place_offchain_order_at_broker`] so every placement path -- the
/// trade-processing path, the hedge job, the CLI, and startup orphan recovery --
/// derives the key the same way.
pub(crate) fn client_order_id_for_placement(
    offchain_order_id: OffchainOrderId,
    last_failed_offchain_order_id: Option<OffchainOrderId>,
) -> ClientOrderId {
    let idempotency_source = last_failed_offchain_order_id.unwrap_or(offchain_order_id);
    ClientOrderId::from_uuid(idempotency_source.as_uuid())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OffchainOrder {
    Pending {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        placed_at: DateTime<Utc>,
    },
    /// `shares` carries the broker-accepted quantity for orders placed after the
    /// durable-job extraction (built from [`OffchainOrderEvent::Accepted`]'s
    /// `placed_shares`), but the originally-requested quantity for pre-extraction
    /// orders that replay the legacy [`OffchainOrderEvent::Submitted`] event. The
    /// two can differ when the broker truncated to its precision; consumers that
    /// compare fills against `shares` should treat the legacy value as the
    /// request, not a guaranteed broker-accepted amount.
    Submitted {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        shares_filled: FractionalShares,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        avg_price: Usd,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        price: Usd,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        error: String,
        placed_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl EventSourced for OffchainOrder {
    type Id = OffchainOrderId;
    type Event = OffchainOrderEvent;
    type Command = OffchainOrderCommand;
    type Error = OffchainOrderError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "OffchainOrder";
    const PROJECTION: Table = Table("offchain_order_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use OffchainOrderEvent::*;
        match event {
            Placed {
                symbol,
                shares,
                direction,
                executor,
                placed_at,
            } => Some(Self::Pending {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                executor: *executor,
                placed_at: *placed_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use OffchainOrderEvent::*;
        match event {
            Placed { .. } => Ok(None),

            Submitted {
                executor_order_id,
                submitted_at,
            } => {
                let Self::Pending {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at,
                } = entity
                else {
                    return Ok(None);
                };

                Ok(Some(Self::Submitted {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                }))
            }

            Accepted {
                executor_order_id,
                placed_shares,
                submitted_at,
            } => {
                let Self::Pending {
                    symbol,
                    direction,
                    executor,
                    placed_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                // The broker-accepted quantity supersedes the requested quantity
                // the pure `Placed` event recorded, so the order carries the
                // amount actually working at the broker from here on.
                Ok(Some(Self::Submitted {
                    symbol: symbol.clone(),
                    shares: *placed_shares,
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                }))
            }

            PartiallyFilled {
                shares_filled,
                avg_price,
                partially_filled_at,
            } => Ok(match entity {
                Self::Submitted {
                    symbol,
                    shares,
                    direction,
                    executor,
                    executor_order_id,
                    placed_at,
                    submitted_at,
                }
                | Self::PartiallyFilled {
                    symbol,
                    shares,
                    direction,
                    executor,
                    executor_order_id,
                    placed_at,
                    submitted_at,
                    ..
                } => Some(Self::PartiallyFilled {
                    symbol: symbol.clone(),
                    shares: *shares,
                    shares_filled: *shares_filled,
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    avg_price: *avg_price,
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    partially_filled_at: *partially_filled_at,
                }),

                Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => None,
            }),

            Filled { price, filled_at } => Ok(match entity {
                Self::Submitted {
                    symbol,
                    shares,
                    direction,
                    executor,
                    executor_order_id,
                    placed_at,
                    submitted_at,
                }
                | Self::PartiallyFilled {
                    symbol,
                    shares,
                    direction,
                    executor,
                    executor_order_id,
                    placed_at,
                    submitted_at,
                    ..
                } => Some(Self::Filled {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    price: *price,
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    filled_at: *filled_at,
                }),

                Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => None,
            }),

            Failed { error, failed_at } => Ok(match entity {
                Self::Pending {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at,
                }
                | Self::Submitted {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at,
                    ..
                }
                | Self::PartiallyFilled {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at,
                    ..
                } => Some(Self::Failed {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    executor: *executor,
                    error: error.clone(),
                    placed_at: *placed_at,
                    failed_at: *failed_at,
                }),

                Self::Filled { .. } | Self::Failed { .. } => None,
            }),
        }
    }

    async fn initialize(
        command: Self::Command,
        (): &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OffchainOrderCommand::*;
        match command {
            // Pure intent: record that an order was requested and enter
            // `Pending`. The broker `place_market_order` call no longer happens
            // here -- the durable placement path performs it and feeds the
            // outcome back via `MarkAccepted`/`MarkFailed`.
            Place {
                symbol,
                shares,
                direction,
                executor,
            } => Ok(vec![OffchainOrderEvent::Placed {
                symbol,
                shares,
                direction,
                executor,
                placed_at: Utc::now(),
            }]),

            _ => Err(OffchainOrderError::NotPlaced),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        (): &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            // Idempotent against a placement retry: the durable path re-sends
            // `Place` for an order it already created, so an exact replay records
            // nothing. A payload that diverges from the existing order is a caller
            // bug, not a replay, and must fail fast rather than silently no-op
            // (financial integrity). `shares` is excluded because a broker
            // over-fill (ADR 0009) legitimately changes the recorded quantity from
            // the originally requested amount.
            OffchainOrderCommand::Place {
                symbol,
                direction,
                executor,
                shares: _,
            } => {
                if symbol != *self.symbol()
                    || direction != self.direction()
                    || executor != self.executor()
                {
                    return Err(OffchainOrderError::PlacePayloadMismatch);
                }
                Ok(vec![])
            }

            OffchainOrderCommand::UpdatePartialFill {
                shares_filled,
                avg_price,
            } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price,
                        partially_filled_at: Utc::now(),
                    }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::CompleteFill { price } => match self {
                Self::Submitted {
                    symbol, placed_at, ..
                }
                | Self::PartiallyFilled {
                    symbol, placed_at, ..
                } => {
                    let filled_at = Utc::now();
                    // Wall-clock placement-to-fill latency. A negative delta can
                    // only come from clock skew (never a real latency), so
                    // `to_std()` rejects it and the sample is skipped rather than
                    // recorded as a bogus value -- logged so the skew is not
                    // silently swallowed.
                    match (filled_at - *placed_at).to_std() {
                        Ok(latency) => {
                            histogram!("hedge_fill_latency_seconds", "symbol" => symbol.to_string())
                                .record(latency.as_secs_f64());
                        }
                        Err(error) => {
                            debug!(
                                %symbol, %error,
                                "Skipping hedge_fill_latency sample: fill precedes placement (clock skew)"
                            );
                        }
                    }

                    Ok(vec![OffchainOrderEvent::Filled { price, filled_at }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::MarkAccepted {
                executor_order_id,
                placed_shares,
                submitted_at,
            } => match self {
                Self::Pending { .. } => Ok(vec![OffchainOrderEvent::Accepted {
                    executor_order_id,
                    placed_shares,
                    submitted_at,
                }]),
                // Idempotent: a retried placement whose order already left
                // `Pending` (acceptance recorded, or already terminal) is a no-op.
                Self::Submitted { .. }
                | Self::PartiallyFilled { .. }
                | Self::Filled { .. }
                | Self::Failed { .. } => Ok(vec![]),
            },

            // Placement-initiated failure. Unlike `MarkFailed` (the poll-rejection
            // path, which may fail a live order), this is honoured only while the
            // order is still `Pending`: a stale placement attempt whose broker call
            // errored must never clobber a `Submitted`/`PartiallyFilled` order a
            // concurrent attempt already accepted. Enforcing it here -- against the
            // aggregate's authoritative state -- closes the load-then-send race a
            // caller-side re-check could not.
            OffchainOrderCommand::MarkPlacementFailed { error } => match self {
                Self::Pending { .. } => Ok(vec![OffchainOrderEvent::Failed {
                    error,
                    failed_at: Utc::now(),
                }]),
                Self::Submitted { symbol, .. }
                | Self::PartiallyFilled { symbol, .. }
                | Self::Filled { symbol, .. }
                | Self::Failed { symbol, .. } => {
                    warn!(
                        %symbol,
                        "Skipping placement-initiated failure: the order is no longer Pending \
                         (a concurrent placement attempt advanced it); leaving it untouched"
                    );
                    Ok(vec![])
                }
            },

            OffchainOrderCommand::MarkFailed { error } => match self {
                Self::Pending { .. } | Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Failed {
                        error,
                        failed_at: Utc::now(),
                    }])
                }
                // Idempotent: re-failing an already-failed order records nothing.
                Self::Failed { .. } => Ok(vec![]),
                Self::Filled { .. } => Err(OffchainOrderError::AlreadyCompleted),
            },
        }
    }
}

impl OffchainOrder {
    /// Renders this order as a dashboard [`Trade`]. Only the `Filled` state
    /// has the executed price and fill timestamp needed for a Trade; every
    /// other state returns [`NotFilled`] so callers must acknowledge the
    /// conversion can fail. Callers that want a silently-discarded `Option`
    /// can write `.ok()`.
    pub(crate) fn try_to_trade(&self, id: &OffchainOrderId) -> Result<Trade, NotFilled> {
        use OffchainOrder::{Failed, PartiallyFilled, Pending, Submitted};
        let Self::Filled {
            symbol,
            shares,
            direction,
            executor,
            filled_at,
            ..
        } = self
        else {
            return Err(match self {
                Pending { .. } => NotFilled { state: "Pending" },
                Submitted { .. } => NotFilled { state: "Submitted" },
                PartiallyFilled { .. } => NotFilled {
                    state: "PartiallyFilled",
                },
                Failed { .. } => NotFilled { state: "Failed" },
                Self::Filled { .. } => unreachable!(),
            });
        };

        Ok(Trade {
            id: id.to_string(),
            filled_at: *filled_at,
            venue: match executor {
                SupportedExecutor::AlpacaBrokerApi => TradingVenue::Alpaca,
                SupportedExecutor::DryRun => TradingVenue::DryRun,
            },
            direction: *direction,
            symbol: symbol.clone(),
            shares: FractionalShares::new(shares.inner().inner()),
        })
    }

    pub(crate) fn symbol(&self) -> &Symbol {
        use OffchainOrder::*;
        match self {
            Pending { symbol, .. }
            | Submitted { symbol, .. }
            | PartiallyFilled { symbol, .. }
            | Filled { symbol, .. }
            | Failed { symbol, .. } => symbol,
        }
    }

    pub(crate) fn shares(&self) -> Positive<FractionalShares> {
        use OffchainOrder::*;
        match self {
            Pending { shares, .. }
            | Submitted { shares, .. }
            | PartiallyFilled { shares, .. }
            | Filled { shares, .. }
            | Failed { shares, .. } => *shares,
        }
    }

    pub(crate) fn direction(&self) -> Direction {
        use OffchainOrder::*;
        match self {
            Pending { direction, .. }
            | Submitted { direction, .. }
            | PartiallyFilled { direction, .. }
            | Filled { direction, .. }
            | Failed { direction, .. } => *direction,
        }
    }

    pub(crate) fn executor(&self) -> SupportedExecutor {
        use OffchainOrder::*;
        match self {
            Pending { executor, .. }
            | Submitted { executor, .. }
            | PartiallyFilled { executor, .. }
            | Filled { executor, .. }
            | Failed { executor, .. } => *executor,
        }
    }

    pub(crate) fn executor_order_id(&self) -> Option<&ExecutorOrderId> {
        use OffchainOrder::*;
        match self {
            Submitted {
                executor_order_id, ..
            }
            | PartiallyFilled {
                executor_order_id, ..
            }
            | Filled {
                executor_order_id, ..
            } => Some(executor_order_id),

            Pending { .. } | Failed { .. } => None,
        }
    }
}

/// Result of a successful order placement, with the executor-assigned ID
/// and the actual quantity placed (which may differ from the requested
/// quantity due to broker precision limits).
pub struct OrderPlacementResult {
    pub executor_order_id: ExecutorOrderId,
    pub placed_shares: Positive<FractionalShares>,
}

/// Type-erased order placement capability used by the durable placement path
/// ([`place_offchain_order_at_broker`]) -- the trade-processing context, the
/// hedge job, and the CLI each hold one. It is no longer a cqrs-es `Service` of
/// the `OffchainOrder` aggregate: the broker call was lifted out of the now-pure
/// `Place` handler, whose `Services` is `()`.
///
/// This trait exists because the `Executor` trait has associated types
/// (`Error`, `OrderId`, `Ctx`) which make it non-object-safe - you cannot
/// write `Arc<dyn Executor>`. This trait provides the minimal surface needed
/// by the aggregate (just `place_market_order`) with erased error/ID types,
/// allowing different executor implementations to be used
/// via `Arc<dyn OrderPlacer>`.
#[async_trait]
pub trait OrderPlacer: Send + Sync {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>;
}

/// Bridges `Executor` (which has associated types and is not object-safe)
/// to `OrderPlacer` (object-safe).
pub(crate) struct ExecutorOrderPlacer<E>(pub E);

#[async_trait]
impl<E: Executor> OrderPlacer for ExecutorOrderPlacer<E> {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        let placement = self.0.place_market_order(order).await?;
        Ok(OrderPlacementResult {
            executor_order_id: ExecutorOrderId::new(&placement.order_id),
            placed_shares: placement.shares,
        })
    }
}

#[cfg(test)]
pub(crate) fn noop_order_placer() -> Arc<dyn OrderPlacer> {
    struct Noop;

    #[async_trait]
    impl OrderPlacer for Noop {
        async fn place_market_order(
            &self,
            order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("noop"),
                placed_shares: noop_placed_shares(order.shares),
            })
        }
    }

    Arc::new(Noop)
}

/// Returns a placed_shares value distinct from the requested shares,
/// simulating broker truncation. Used so tests can verify the system
/// persists the broker-accepted quantity, not the original request.
#[cfg(test)]
pub(crate) fn noop_placed_shares(
    requested: Positive<FractionalShares>,
) -> Positive<FractionalShares> {
    let original = requested.inner().inner();
    let offset = st0x_float_macro::float!(0.001);
    let truncated = (original - offset).expect("subtraction should not fail");

    Positive::new(FractionalShares::new(truncated)).expect("truncated shares should be positive")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    },
    UpdatePartialFill {
        shares_filled: FractionalShares,
        avg_price: Usd,
    },
    CompleteFill {
        price: Usd,
    },
    /// Outcome command: the broker accepted the placement. Fed back by the
    /// durable placement path after `place_market_order`, carrying the
    /// executor-assigned id and the broker-accepted `placed_shares`. Idempotent
    /// against a job retry: a no-op once the order has left `Pending`.
    MarkAccepted {
        executor_order_id: ExecutorOrderId,
        placed_shares: Positive<FractionalShares>,
        submitted_at: DateTime<Utc>,
    },
    /// Outcome command for a placement-initiated failure: the broker call errored
    /// while the placement path still held a `Pending` order. Honoured only from
    /// `Pending`, so a stale attempt cannot fail a live order a concurrent attempt
    /// already accepted. The poll-rejection path instead uses `MarkFailed`, which
    /// may fail a live `Submitted`/`PartiallyFilled` order.
    MarkPlacementFailed {
        error: String,
    },
    MarkFailed {
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OffchainOrderEvent {
    Placed {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        placed_at: DateTime<Utc>,
    },
    /// Legacy broker-acceptance event. Predates the durable-job extraction,
    /// where `Place` did the broker call inline and emitted this alongside
    /// `Placed`. Retained only so orders placed before the extraction still
    /// replay; new placements emit [`Accepted`](Self::Accepted) instead.
    Submitted {
        executor_order_id: ExecutorOrderId,
        submitted_at: DateTime<Utc>,
    },
    /// The broker accepted the placement, assigning `executor_order_id` and the
    /// `placed_shares` it actually accepted. This is usually <= requested (the
    /// broker may truncate to its precision) but can exceed the request on a
    /// broker over-fill, which ADR 0009 records as an acceptance rather than a
    /// failure. Emitted by the durable placement path after the external
    /// `place_market_order` call, now that `Place` only records intent. Carries
    /// `placed_shares` because the now-pure `Placed` event can only record the
    /// requested quantity.
    Accepted {
        executor_order_id: ExecutorOrderId,
        placed_shares: Positive<FractionalShares>,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        shares_filled: FractionalShares,
        avg_price: Usd,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        price: Usd,
        filled_at: DateTime<Utc>,
    },
    Failed {
        error: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for OffchainOrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Placed { .. } => "OffchainOrderEvent::Placed".to_string(),
            Self::Submitted { .. } => "OffchainOrderEvent::Submitted".to_string(),
            Self::Accepted { .. } => "OffchainOrderEvent::Accepted".to_string(),
            Self::PartiallyFilled { .. } => "OffchainOrderEvent::PartiallyFilled".to_string(),
            Self::Filled { .. } => "OffchainOrderEvent::Filled".to_string(),
            Self::Failed { .. } => "OffchainOrderEvent::Failed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct OffchainOrderId(Uuid);

impl std::fmt::Display for OffchainOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for OffchainOrderId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl OffchainOrderId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Exposes the wrapped UUID so callers can derive other identifiers
    /// (e.g. a broker-side `client_order_id`) without going through a
    /// fallible string roundtrip.
    pub(crate) fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Returned by [`OffchainOrder::try_to_trade`] when the order isn't in the
/// `Filled` state. `state` is the variant name as a `&'static str` so
/// diagnostics get the specific state without dragging the variant's data
/// (which would include opaque error strings on `Failed`).
#[derive(Debug, thiserror::Error)]
#[error("OffchainOrder cannot be rendered as a Trade: current state is {state}")]
pub(crate) struct NotFilled {
    pub(crate) state: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub enum OffchainOrderError {
    #[error(
        "Cannot update or complete fill: order has not been \
         submitted to broker yet"
    )]
    NotSubmitted,
    #[error("Cannot update order: order has already been completed (filled or failed)")]
    AlreadyCompleted,
    #[error("Order has not been placed yet")]
    NotPlaced,
    #[error(
        "Place retry payload diverges from the existing order: a re-`Place` must \
         replay the original symbol, direction, and executor"
    )]
    PlacePayloadMismatch,
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, TestStore, replay};

    use super::*;
    use st0x_float_macro::float;

    fn failing_order_placer() -> Arc<dyn OrderPlacer> {
        struct Failing;

        #[async_trait]
        impl OrderPlacer for Failing {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("Broker rejected order".into())
            }
        }

        Arc::new(Failing)
    }

    fn place_command() -> OffchainOrderCommand {
        OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
        }
    }

    /// Drives an order to `Submitted` the way the durable placement path does:
    /// a pure `Place` (-> Pending) followed by the broker-acceptance feedback
    /// `MarkAccepted`. The accepted quantity is `noop_placed_shares(100)`, so
    /// the resulting `Submitted` carries the broker-working amount, not 100.
    async fn place_and_submit(store: &TestStore<OffchainOrder>, id: &OffchainOrderId) {
        let requested = Positive::new(FractionalShares::new(float!(100))).unwrap();
        store.send(id, place_command()).await.unwrap();
        store
            .send(
                id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("TEST-ACCEPT"),
                    placed_shares: noop_placed_shares(requested),
                    submitted_at: Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    /// Builds a real store and runs the durable placement path against
    /// `placer`, returning the resulting order state. This is the single broker
    /// call site, so the placement outcomes are exercised here rather than
    /// through the (now pure) `Place` handler.
    async fn place_at_broker(placer: &dyn OrderPlacer) -> Option<OffchainOrder> {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        place_offchain_order_at_broker(
            &store,
            placer,
            &OffchainOrderId::new(),
            Symbol::new("AAPL").unwrap(),
            Positive::new(FractionalShares::new(float!(100))).unwrap(),
            Direction::Buy,
            SupportedExecutor::DryRun,
            ClientOrderId::from_uuid(Uuid::new_v4()),
        )
        .await
        .unwrap()
    }

    // These tests install the process-global Prometheus recorder via
    // crate::metrics::setup() and assert on its rendered output. nextest (which
    // CI and local runs use) runs each test in its own process, so the
    // install-once recorder is fresh per test and the negative assertions below
    // are not contaminated by the sibling test's counter. The counters live in
    // the durable placement path (place_offchain_order_at_broker), so the tests
    // drive that path rather than the now-pure Place handler.

    #[tokio::test]
    async fn placement_increments_hedge_trades_counter_on_success() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = noop_order_placer();

        place_at_broker(placer.as_ref()).await;

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_trades_total{") && rendered.contains("direction=\"buy\""),
            "a successful placement must increment hedge_trades_total{{direction=buy}}, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("broker_errors_total{"),
            "a successful placement must not touch broker_errors_total, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn placement_increments_broker_errors_counter_on_failure() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = failing_order_placer();

        place_at_broker(placer.as_ref()).await;

        let rendered = handle.render();
        assert!(
            rendered.contains("broker_errors_total{")
                && rendered.contains("kind=\"place_order_failed\""),
            "a failed placement must increment broker_errors_total{{kind=place_order_failed}}, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("hedge_trades_total{"),
            "a failed placement must not increment hedge_trades_total, got:\n{rendered}"
        );
    }

    fn overfilling_order_placer() -> Arc<dyn OrderPlacer> {
        struct Overfill;

        #[async_trait]
        impl OrderPlacer for Overfill {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                let original = order.shares.inner().inner();
                let extra = st0x_float_macro::float!(1);
                let overfilled = (original + extra).expect("addition should not fail");

                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("OVERFILL"),
                    placed_shares: Positive::new(FractionalShares::new(overfilled)).unwrap(),
                })
            }
        }

        Arc::new(Overfill)
    }

    #[tokio::test]
    async fn place_at_broker_records_broker_accepted_shares() {
        let placer = noop_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        let Some(OffchainOrder::Submitted { shares, .. }) = order else {
            panic!("expected Submitted, got {order:?}");
        };
        assert_eq!(
            shares,
            noop_placed_shares(Positive::new(FractionalShares::new(float!(100))).unwrap()),
            "persisted shares should reflect the broker-accepted quantity, not the request"
        );
    }

    #[tokio::test]
    async fn place_at_broker_with_failing_broker_marks_failed() {
        let placer = failing_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        assert!(
            matches!(
                &order,
                Some(OffchainOrder::Failed { error, .. }) if error.contains("Broker rejected")
            ),
            "expected Failed with the broker error, got {order:?}"
        );
    }

    #[tokio::test]
    async fn place_at_broker_with_overfill_records_acceptance() {
        let placer = overfilling_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        // Per ADR 0009 a broker over-fill is recorded as an acceptance carrying
        // the actual broker-placed quantity, not a failure -- the live order must
        // stay poll-able instead of being failed and re-driven into a loop.
        let Some(OffchainOrder::Submitted {
            shares,
            executor_order_id,
            ..
        }) = order
        else {
            panic!("expected Submitted for an over-filling broker, got {order:?}");
        };
        let expected = Positive::new(FractionalShares::new(float!(101))).unwrap();
        assert_eq!(
            shares, expected,
            "over-fill should record the broker-placed quantity (requested 100 + 1)"
        );
        assert_eq!(
            executor_order_id,
            ExecutorOrderId::new("OVERFILL"),
            "over-fill must preserve the broker order id so the live order stays poll-able"
        );
    }

    #[tokio::test]
    async fn place_retry_with_divergent_payload_is_rejected() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // First `Place` creates the Pending order.
        store.send(&id, place_command()).await.unwrap();

        // An exact replay (the durable path re-sending `Place`) is an idempotent
        // no-op and must not error.
        store.send(&id, place_command()).await.unwrap();

        // A retry whose identity fields diverge from the original is a caller bug,
        // not a replay: it must fail fast rather than silently no-op.
        let mismatched = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
        };
        let error = store.send(&id, mismatched).await.unwrap_err();
        assert!(matches!(
            error,
            AggregateError::UserError(LifecycleError::Apply(
                OffchainOrderError::PlacePayloadMismatch
            ))
        ));
    }

    #[tokio::test]
    async fn place_at_broker_skips_broker_when_order_left_pending() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // At-least-once safety: a retry whose order already left `Pending` must
        // skip the broker call entirely (the broker dedupes, but we should not
        // even reach it). Drive the order to Submitted directly on the store.
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("SETUP"),
                    placed_shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
                    submitted_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        struct PanicIfCalled;

        #[async_trait]
        impl OrderPlacer for PanicIfCalled {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("broker must not be called for an order that already left Pending");
            }
        }

        let result = place_offchain_order_at_broker(
            &store,
            &PanicIfCalled,
            &id,
            Symbol::new("AAPL").unwrap(),
            Positive::new(FractionalShares::new(float!(100))).unwrap(),
            Direction::Buy,
            SupportedExecutor::DryRun,
            ClientOrderId::from_uuid(Uuid::new_v4()),
        )
        .await
        .unwrap();

        assert!(
            matches!(result, Some(OffchainOrder::Submitted { .. })),
            "re-drive of an already-Submitted order must return it untouched, got {result:?}"
        );
    }

    #[tokio::test]
    async fn place_at_broker_skips_markfailed_when_order_advanced_concurrently() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // Concurrency narrowing: a concurrent attempt advances the order past
        // `Pending` (MarkAccepted) while this attempt's broker call is in flight
        // and then errors. The resulting `MarkPlacementFailed` must NOT clobber
        // the now-`Submitted` order -- the handler honours it only from `Pending`.
        struct AdvanceThenFail {
            store: Arc<st0x_event_sorcery::Store<OffchainOrder>>,
            id: OffchainOrderId,
        }

        #[async_trait]
        impl OrderPlacer for AdvanceThenFail {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                self.store
                    .send(
                        &self.id,
                        OffchainOrderCommand::MarkAccepted {
                            executor_order_id: ExecutorOrderId::new("CONCURRENT"),
                            placed_shares: Positive::new(FractionalShares::new(float!(100)))
                                .unwrap(),
                            submitted_at: Utc::now(),
                        },
                    )
                    .await
                    .unwrap();

                Err("broker error after a concurrent attempt already succeeded".into())
            }
        }

        let placer = AdvanceThenFail {
            store: store.clone(),
            id,
        };

        let result = place_offchain_order_at_broker(
            &store,
            &placer,
            &id,
            Symbol::new("AAPL").unwrap(),
            Positive::new(FractionalShares::new(float!(100))).unwrap(),
            Direction::Buy,
            SupportedExecutor::DryRun,
            ClientOrderId::from_uuid(Uuid::new_v4()),
        )
        .await
        .unwrap();

        assert!(
            matches!(result, Some(OffchainOrder::Submitted { .. })),
            "a stale broker error must not clobber an order a concurrent attempt advanced, \
             got {result:?}"
        );
    }

    #[tokio::test]
    async fn place_is_idempotent_once_placed() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        // The durable placement path re-sends `Place` on retry; an existing
        // aggregate -- live or terminal -- must record nothing rather than error.
        store.send(&id, place_command()).await.unwrap();
        let pending = store.load(&id).await.unwrap().unwrap();
        store.send(&id, place_command()).await.unwrap();
        assert_eq!(
            pending,
            store.load(&id).await.unwrap().unwrap(),
            "re-placing a pending order is a no-op"
        );

        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Market closed".to_string(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(failed, OffchainOrder::Failed { .. }));

        store.send(&id, place_command()).await.unwrap();
        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "re-placing a terminal order stays a no-op"
        );
    }

    #[tokio::test]
    async fn mark_accepted_is_idempotent_on_submitted() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        // The durable placement path re-sends `MarkAccepted` on retry (the broker
        // dedupes the duplicate submission); a second acceptance on an already
        // -`Submitted` order must record nothing rather than overwrite it.
        place_and_submit(&store, &id).await;
        let submitted = store.load(&id).await.unwrap().unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("SECOND-ACCEPT"),
                    placed_shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                    submitted_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            submitted,
            store.load(&id).await.unwrap().unwrap(),
            "a duplicate MarkAccepted must not overwrite the working Submitted order"
        );
    }

    #[tokio::test]
    async fn mark_failed_is_idempotent_on_failed() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "first failure".to_string(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();

        // A retried placement whose broker call errored again must not overwrite
        // the recorded failure (which would churn the failed_at / error).
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "second failure".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "a duplicate MarkFailed must leave the original failure untouched"
        );
    }

    #[tokio::test]
    async fn mark_accepted_after_failed_is_noop() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected".to_string(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();

        // A late acceptance arriving after the order was already failed must not
        // resurrect it to Submitted.
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("LATE-ACCEPT"),
                    placed_shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
                    submitted_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "a MarkAccepted after Failed must not resurrect the order"
        );
    }

    #[tokio::test]
    async fn partial_fill_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::PartiallyFilled { .. }));
    }

    #[tokio::test]
    async fn partial_fill_updates_shares() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(75)),
                    avg_price: Usd::new(float!(150.50)),
                },
            )
            .await
            .unwrap();

        let OffchainOrder::PartiallyFilled { shares_filled, .. } =
            store.load(&id).await.unwrap().unwrap()
        else {
            panic!("Expected PartiallyFilled state");
        };
        assert_eq!(shares_filled, FractionalShares::new(float!(75)));
    }

    #[tokio::test]
    async fn complete_fill_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn complete_fill_from_partially_filled() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(75)),
                    avg_price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.25)),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn complete_fill_records_hedge_fill_latency_sample() {
        // Process-isolated under nextest; only this fill is sampled. The default
        // Prometheus summary rendering emits a `_count` series we can assert on.
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_fill_latency_seconds_count{symbol=\"AAPL\"} 1"),
            "completing a fill must record exactly one hedge_fill_latency_seconds sample, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn cannot_fill_uninitialized_order() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::NotPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_fill_already_filled() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyCompleted))
        ));
    }

    #[tokio::test]
    async fn mark_failed_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Insufficient funds".to_string(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_failed_from_partially_filled() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Order cancelled".to_string(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_placement_failed_fails_a_pending_order() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        // The placement path's broker call errored while the order was still
        // Pending, so the placement-initiated failure is recorded.
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker unreachable".to_string(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_placement_failed_leaves_a_live_order_untouched() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        // A stale placement attempt's broker error must never fail a live order a
        // concurrent attempt already drove past Pending: the handler enforces the
        // Pending-only narrowing against the aggregate's authoritative state, so a
        // MarkPlacementFailed on a Submitted order is a no-op (no version-gate
        // race, unlike the old caller-side load-then-send guard).
        place_and_submit(&store, &id).await;
        let submitted = store.load(&id).await.unwrap().unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "stale broker error".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            submitted,
            store.load(&id).await.unwrap().unwrap(),
            "MarkPlacementFailed must leave a live Submitted order untouched"
        );
    }

    #[tokio::test]
    async fn cannot_fail_already_filled() {
        let store = TestStore::<OffchainOrder>::new(());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Test error".to_string(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyCompleted))
        ));
    }

    #[test]
    fn non_genesis_event_on_uninitialized_produces_error() {
        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD123"),
            submitted_at: Utc::now(),
        };

        let error = replay::<OffchainOrder>(vec![event]).unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn usd_serializes_as_decimal_string() {
        let usd = Usd::new(float!(150.25));
        let json = serde_json::to_value(usd).unwrap();
        assert_eq!(json, serde_json::json!("150.25"));
    }

    #[test]
    fn usd_deserializes_from_string() {
        let usd: Usd = serde_json::from_value(serde_json::json!("150.25")).unwrap();
        assert_eq!(usd, Usd::new(float!(150.25)));
    }

    #[test]
    fn usd_deserializes_from_number() {
        let usd: Usd = serde_json::from_value(serde_json::json!(150.25)).unwrap();
        assert_eq!(usd, Usd::new(float!(150.25)));
    }

    #[test]
    fn usd_round_trips_through_json() {
        let original = Usd::new(float!(99.99));
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Usd = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[tokio::test]
    async fn offchain_order_view_status_maps_all_lifecycle_states() {
        let pool = crate::test_utils::setup_test_db().await;

        let cases: &[(&str, Option<&str>)] = &[
            (r#"{"Live":{"Pending":{}}}"#, Some("Pending")),
            (r#"{"Live":{"Submitted":{}}}"#, Some("Submitted")),
            (
                r#"{"Live":{"PartiallyFilled":{}}}"#,
                Some("PartiallyFilled"),
            ),
            (r#"{"Live":{"Cancelling":{}}}"#, Some("Cancelling")),
            (r#"{"Live":{"Filled":{}}}"#, Some("Filled")),
            (r#"{"Live":{"Failed":{}}}"#, Some("Failed")),
            (r#"{"Live":{"Cancelled":{}}}"#, Some("Cancelled")),
            // Unrecognized payload — CASE has no ELSE, so status column must be NULL.
            (r#"{"Live":{"UnknownFutureState":{}}}"#, None),
        ];

        for (index, (payload, expected_status)) in cases.iter().enumerate() {
            let view_id = format!("view-{index}");

            sqlx::query(
                "INSERT INTO offchain_order_view (view_id, version, payload) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&view_id)
            .bind(1_i64)
            .bind(*payload)
            .execute(&pool)
            .await
            .unwrap();

            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM offchain_order_view WHERE view_id = $1")
                    .bind(&view_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(
                status.as_deref(),
                *expected_status,
                "payload {payload} should produce status {expected_status:?}"
            );
        }
    }
}
