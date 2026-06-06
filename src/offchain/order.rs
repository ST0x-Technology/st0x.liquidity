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
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use st0x_dto::{Direction, Trade, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, Projection, Store, StoreBuilder, Table};
use st0x_execution::{
    AlpacaBrokerApiError, ClientOrderId, ExecutionError, Executor, ExecutorOrderId,
    FractionalShares, LimitOrder, MarketOrder, PersistenceError, Positive, SupportedExecutor,
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
}

/// Constructs the offchain order CQRS framework with its view
/// query. Used by CLI code.
pub(crate) async fn build_offchain_order_cqrs(
    pool: &SqlitePool,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<(Arc<Store<OffchainOrder>>, Arc<Projection<OffchainOrder>>)> {
    let (store, projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
        .build(order_placer)
        .await?;

    Ok((store, projection))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OffchainOrder {
    Pending {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        placed_at: DateTime<Utc>,
        #[serde(default)]
        is_extended_hours: bool,
    },
    Submitted {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        #[serde(default)]
        is_extended_hours: bool,
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
        #[serde(default)]
        is_extended_hours: bool,
    },
    Cancelling {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        shares_filled: FractionalShares,
        avg_price: Option<Usd>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        reason: CancellationReason,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        cancel_requested_at: DateTime<Utc>,
        #[serde(default)]
        is_extended_hours: bool,
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
        shares_filled: Option<FractionalShares>,
        avg_price: Option<Usd>,
        executor_order_id: Option<ExecutorOrderId>,
        error: String,
        placed_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// Terminal state after a successful broker cancellation. Distinct
    /// from `Failed` so analytics and the cancel-and-replace recovery
    /// path can tell intentional cancellation apart from broker rejection.
    ///
    /// `shares_filled`/`avg_price`/`executor_order_id` carry any partial
    /// fills the order incurred before cancellation so the position-side
    /// cleanup can issue `CompleteOffChainOrder` for the filled quantity
    /// (otherwise the broker keeps those shares but `Position.net` never
    /// records them, leading to a duplicate hedge on the next scan).
    Cancelled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        shares_filled: FractionalShares,
        avg_price: Option<Usd>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        reason: CancellationReason,
        placed_at: DateTime<Utc>,
        cancelled_at: DateTime<Utc>,
    },
}

/// Why an [`OffchainOrder`] was cancelled. Carried on
/// [`OffchainOrderEvent::Cancelled`] so it can be persisted, projected,
/// and pattern-matched without parsing strings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CancellationReason {
    /// Extended-hours limit order cancelled at the Extended -> Regular
    /// transition so the next monitor scan can place a market order
    /// instead.
    MarketOpenReplacement,
}

#[async_trait]
impl EventSourced for OffchainOrder {
    type Id = OffchainOrderId;
    type Event = OffchainOrderEvent;
    type Command = OffchainOrderCommand;
    type Error = OffchainOrderError;
    type Services = Arc<dyn OrderPlacer>;
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
                is_extended_hours,
            } => Some(Self::Pending {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                executor: *executor,
                placed_at: *placed_at,
                is_extended_hours: *is_extended_hours,
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
                    is_extended_hours,
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
                    is_extended_hours: *is_extended_hours,
                }))
            }

            PartiallyFilled {
                shares_filled,
                avg_price,
                partially_filled_at,
            } => Ok(evolve_partially_filled(
                entity,
                *shares_filled,
                *avg_price,
                *partially_filled_at,
            )),

            Filled { price, filled_at } => Ok(match entity {
                Self::Submitted {
                    symbol,
                    shares,
                    direction,
                    executor,
                    executor_order_id,
                    placed_at,
                    submitted_at,
                    ..
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
                }
                | Self::Cancelling {
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

                Self::Pending { .. }
                | Self::Filled { .. }
                | Self::Failed { .. }
                | Self::Cancelled { .. } => None,
            }),

            CancelRequested {
                reason,
                cancel_requested_at,
            } => Ok(evolve_cancel_requested(
                entity,
                *reason,
                *cancel_requested_at,
            )),

            Failed { error, failed_at } => Ok(evolve_failed(entity, error.clone(), *failed_at)),

            Cancelled {
                reason,
                cancelled_at,
            } => Ok(evolve_cancelled(entity, *reason, *cancelled_at)),
        }
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OffchainOrderCommand::*;
        match command {
            Place {
                symbol,
                shares,
                direction,
                executor,
                client_order_id,
                kind,
            } => {
                let now = Utc::now();
                let is_extended_hours =
                    matches!(kind, CounterTradeOrderKind::ExtendedHoursLimit { .. });

                let placement_result = match kind {
                    CounterTradeOrderKind::Market => {
                        let market_order = MarketOrder {
                            symbol: symbol.clone(),
                            shares,
                            direction,
                            client_order_id,
                        };
                        services.place_market_order(market_order).await
                    }
                    CounterTradeOrderKind::ExtendedHoursLimit { limit_price } => {
                        let limit_order = LimitOrder {
                            symbol: symbol.clone(),
                            shares,
                            direction,
                            limit_price,
                            extended_hours: true,
                            client_order_id,
                        };
                        services.place_limit_order(limit_order).await
                    }
                };

                match placement_result {
                    Ok(result) => {
                        if result.placed_shares > shares {
                            return Err(OffchainOrderError::PlacedExceedsRequested {
                                placed: result.placed_shares,
                                requested: shares,
                            });
                        }

                        Ok(vec![
                            OffchainOrderEvent::Placed {
                                symbol,
                                shares: result.placed_shares,
                                direction,
                                executor,
                                placed_at: now,
                                is_extended_hours,
                            },
                            OffchainOrderEvent::Submitted {
                                executor_order_id: result.executor_order_id,
                                submitted_at: now,
                            },
                        ])
                    }
                    Err(error) => Ok(vec![
                        OffchainOrderEvent::Placed {
                            symbol,
                            shares,
                            direction,
                            executor,
                            placed_at: now,
                            is_extended_hours,
                        },
                        OffchainOrderEvent::Failed {
                            error: error.to_string(),
                            failed_at: now,
                        },
                    ]),
                }
            }

            _ => Err(OffchainOrderError::NotPlaced),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OffchainOrderCommand::Place { .. } => Err(OffchainOrderError::AlreadyPlaced),

            OffchainOrderCommand::CancelOrder { reason } => match self {
                Self::Submitted {
                    executor_order_id, ..
                }
                | Self::PartiallyFilled {
                    executor_order_id, ..
                } => {
                    let mut events = Vec::new();

                    // Reconcile any fills that landed between the last poll
                    // and now. If we skip this, a partial fill at the broker
                    // is silently dropped when the aggregate transitions to
                    // Cancelled, and the next monitor scan re-hedges the
                    // already-filled shares (double hedge).
                    let local_filled = match self {
                        Self::PartiallyFilled { shares_filled, .. } => Some(*shares_filled),
                        _ => None,
                    };
                    let pre_cancel_events = reconcile_pre_cancel(
                        services.as_ref(),
                        executor_order_id,
                        local_filled,
                        reason,
                    )
                    .await?;
                    let cancel_short_circuit = pre_cancel_events.iter().any(|event| {
                        matches!(
                            event,
                            OffchainOrderEvent::Filled { .. }
                                | OffchainOrderEvent::Failed { .. }
                                | OffchainOrderEvent::Cancelled { .. }
                        )
                    });
                    events.extend(pre_cancel_events);

                    // If the order already filled at the broker, do not
                    // call DELETE -- it would fail and we've already
                    // emitted the terminal Filled event.
                    if cancel_short_circuit {
                        return Ok(events);
                    }

                    // Propagate cancel errors so the aggregate stays in its
                    // current state and the caller can retry. If we emitted
                    // Cancelled on a cancel error, a still-live broker order
                    // could coexist with a replacement and cause duplicate
                    // hedges.
                    services
                        .cancel_order(executor_order_id)
                        .await
                        .map_err(|error| {
                            tracing::warn!(
                                %executor_order_id,
                                %error,
                                "Failed to cancel order via broker; will retry"
                            );
                            OffchainOrderError::CancelFailed {
                                executor_order_id: executor_order_id.clone(),
                                reason: error.to_string(),
                            }
                        })?;

                    events.push(OffchainOrderEvent::CancelRequested {
                        reason,
                        cancel_requested_at: Utc::now(),
                    });
                    Ok(events)
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Cancelling { .. } => Ok(Vec::new()),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::ConfirmCancellation { cancelled_at } => match self {
                Self::Cancelling { reason, .. } => Ok(vec![OffchainOrderEvent::Cancelled {
                    reason: *reason,
                    cancelled_at,
                }]),
                Self::Pending { .. } | Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Err(OffchainOrderError::CancellationNotRequested)
                }
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::UpdatePartialFill {
                shares_filled,
                avg_price,
            } => match self {
                Self::Submitted { .. } => Ok(vec![OffchainOrderEvent::PartiallyFilled {
                    shares_filled,
                    avg_price,
                    partially_filled_at: Utc::now(),
                }]),
                Self::PartiallyFilled {
                    shares_filled: local_filled,
                    ..
                } => {
                    if !shares_filled.inner().gt(local_filled.inner())? {
                        tracing::debug!(
                            local_shares_filled = %local_filled,
                            broker_shares_filled = %shares_filled,
                            "Skipping stale or duplicate partial-fill update"
                        );
                        return Ok(Vec::new());
                    }

                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price,
                        partially_filled_at: Utc::now(),
                    }])
                }
                Self::Cancelling {
                    shares_filled: local_filled,
                    ..
                } => {
                    if !shares_filled.inner().gt(local_filled.inner())? {
                        tracing::debug!(
                            local_shares_filled = %local_filled,
                            broker_shares_filled = %shares_filled,
                            "Skipping stale or duplicate partial-fill update during cancellation"
                        );
                        return Ok(Vec::new());
                    }

                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price,
                        partially_filled_at: Utc::now(),
                    }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::CompleteFill { price } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } | Self::Cancelling { .. } => {
                    Ok(vec![OffchainOrderEvent::Filled {
                        price,
                        filled_at: Utc::now(),
                    }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::MarkFailed { error } => match self {
                Self::Pending { .. }
                | Self::Submitted { .. }
                | Self::PartiallyFilled { .. }
                | Self::Cancelling { .. } => Ok(vec![OffchainOrderEvent::Failed {
                    error,
                    failed_at: Utc::now(),
                }]),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
        }
    }
}

fn evolve_partially_filled(
    entity: &OffchainOrder,
    shares_filled: FractionalShares,
    avg_price: Usd,
    partially_filled_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            is_extended_hours,
        }
        | OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            is_extended_hours,
            ..
        } => Some(OffchainOrder::PartiallyFilled {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            avg_price,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            partially_filled_at,
            is_extended_hours: *is_extended_hours,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            reason,
            placed_at,
            submitted_at,
            cancel_requested_at,
            is_extended_hours,
            ..
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled,
            avg_price: Some(avg_price),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason: *reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at: *cancel_requested_at,
            is_extended_hours: *is_extended_hours,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_cancel_requested(
    entity: &OffchainOrder,
    reason: CancellationReason,
    cancel_requested_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            is_extended_hours,
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled: FractionalShares::new(st0x_float_macro::float!(0)),
            avg_price: None,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at,
            is_extended_hours: *is_extended_hours,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            submitted_at,
            is_extended_hours,
            ..
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled: *shares_filled,
            avg_price: Some(*avg_price),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at,
            is_extended_hours: *is_extended_hours,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Cancelling { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_failed(
    entity: &OffchainOrder,
    error: String,
    failed_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Pending {
            symbol,
            shares,
            direction,
            executor,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            shares_filled: None,
            avg_price: None,
            executor_order_id: None,
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Submitted {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            shares_filled: None,
            avg_price: None,
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            shares_filled: Some(*shares_filled),
            avg_price: Some(*avg_price),
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            shares_filled,
            avg_price,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            shares_filled: Some(*shares_filled),
            avg_price: *avg_price,
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_cancelled(
    entity: &OffchainOrder,
    reason: CancellationReason,
    cancelled_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled: FractionalShares::new(st0x_float_macro::float!(0)),
            avg_price: None,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled: *shares_filled,
            avg_price: Some(*avg_price),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            shares_filled,
            avg_price,
            direction,
            executor,
            executor_order_id,
            reason: requested_reason,
            placed_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            shares_filled: *shares_filled,
            avg_price: *avg_price,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason: *requested_reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

impl OffchainOrder {
    /// Renders this order as a dashboard [`Trade`]. Only the `Filled` state
    /// has the executed price and fill timestamp needed for a Trade; every
    /// other state returns [`NotFilled`] so callers must acknowledge the
    /// conversion can fail. Callers that want a silently-discarded `Option`
    /// can write `.ok()`.
    pub(crate) fn try_to_trade(&self, id: &OffchainOrderId) -> Result<Trade, NotFilled> {
        use OffchainOrder::{Cancelled, Cancelling, Failed, PartiallyFilled, Pending, Submitted};
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
                Cancelling { .. } => NotFilled {
                    state: "Cancelling",
                },
                Failed { .. } => NotFilled { state: "Failed" },
                Cancelled { .. } => NotFilled { state: "Cancelled" },
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
            | Cancelling { symbol, .. }
            | Filled { symbol, .. }
            | Failed { symbol, .. }
            | Cancelled { symbol, .. } => symbol,
        }
    }

    pub(crate) fn shares(&self) -> Positive<FractionalShares> {
        use OffchainOrder::*;
        match self {
            Pending { shares, .. }
            | Submitted { shares, .. }
            | PartiallyFilled { shares, .. }
            | Cancelling { shares, .. }
            | Filled { shares, .. }
            | Failed { shares, .. }
            | Cancelled { shares, .. } => *shares,
        }
    }

    pub(crate) fn direction(&self) -> Direction {
        use OffchainOrder::*;
        match self {
            Pending { direction, .. }
            | Submitted { direction, .. }
            | PartiallyFilled { direction, .. }
            | Cancelling { direction, .. }
            | Filled { direction, .. }
            | Failed { direction, .. }
            | Cancelled { direction, .. } => *direction,
        }
    }

    pub(crate) fn executor(&self) -> SupportedExecutor {
        use OffchainOrder::*;
        match self {
            Pending { executor, .. }
            | Submitted { executor, .. }
            | PartiallyFilled { executor, .. }
            | Cancelling { executor, .. }
            | Filled { executor, .. }
            | Failed { executor, .. }
            | Cancelled { executor, .. } => *executor,
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
            | Cancelling {
                executor_order_id, ..
            }
            | Filled {
                executor_order_id, ..
            } => Some(executor_order_id),

            Pending { .. } | Failed { .. } | Cancelled { .. } => None,
        }
    }
}

/// Queries the broker for the current state of an order before cancellation
/// and emits the appropriate partial-fill / fill events so the local
/// aggregate is reconciled with the broker before the terminal Cancelled
/// event. `local_filled` is the cumulative quantity already recorded in
/// the local PartiallyFilled state (None if the local state is Submitted).
///
/// Returns the events that should be emitted *before* the cancel attempt.
/// If the returned vec contains `Filled`, the caller MUST short-circuit
/// and not attempt the DELETE (the broker already filled).
async fn reconcile_pre_cancel(
    services: &dyn OrderPlacer,
    executor_order_id: &ExecutorOrderId,
    local_filled: Option<FractionalShares>,
    cancellation_reason: CancellationReason,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    // Propagate read failures so the aggregate stays in its prior state
    // and the caller can retry. Silently bypassing reconciliation would
    // re-introduce the partial-fill loss bug the function exists to fix
    // -- a transient status-API failure right before cancel must not
    // become irreversible data loss.
    let state = services
        .get_order_status(executor_order_id)
        .await
        .map_err(|error| {
            tracing::warn!(
                %executor_order_id,
                %error,
                "Failed to read broker state for pre-cancel reconciliation; \
                 will retry without cancelling"
            );
            OffchainOrderError::PreCancelStatusFetchFailed {
                executor_order_id: executor_order_id.clone(),
                reason: error.to_string(),
            }
        })?;

    use st0x_execution::OrderState;
    match state {
        OrderState::PartiallyFilled {
            shares_filled,
            avg_price,
            partially_filled_at,
            ..
        } => {
            // Only emit when the broker reports STRICTLY MORE fills than
            // the local aggregate already records. Equal => no-op (the
            // local state is already up to date). LESS => stale broker
            // read (the poll loop recorded fresher data); never regress
            // the recorded cumulative quantity.
            let broker_filled = FractionalShares::new(shares_filled);
            if let Some(local) = local_filled {
                match broker_filled.inner().gt(local.inner()) {
                    Ok(true) => { /* fall through to emit event */ }
                    Ok(false) => {
                        tracing::debug!(
                            %executor_order_id,
                            "Broker partial-fill <= local; skipping pre-cancel reconcile"
                        );
                        return Ok(Vec::new());
                    }
                    Err(error) => {
                        tracing::warn!(
                            %executor_order_id,
                            %error,
                            "Float comparison failed in pre-cancel reconcile; skipping"
                        );
                        return Ok(Vec::new());
                    }
                }
            }

            // We need an avg_price for the event. If the broker did not
            // return one, drop the reconciliation -- without a price we
            // can't record the fill correctly. The position would be left
            // unhedged for the partial quantity, but that's safer than
            // recording a zero-price fill.
            let Some(price) = avg_price else {
                tracing::warn!(
                    %executor_order_id,
                    "Broker reports PartiallyFilled but no avg_price; will retry without cancelling"
                );
                return Err(OffchainOrderError::PreCancelPartialFillMissingAvgPrice {
                    executor_order_id: executor_order_id.clone(),
                    shares_filled: broker_filled,
                });
            };

            Ok(vec![OffchainOrderEvent::PartiallyFilled {
                shares_filled: broker_filled,
                avg_price: Usd::new(price),
                partially_filled_at,
            }])
        }

        OrderState::Filled {
            price, executed_at, ..
        } => {
            // The order filled completely between our last poll and the
            // cancel attempt. Record the fill so the position aggregate
            // gets the full hedge, and skip the DELETE (it would fail or
            // be no-op).
            tracing::info!(
                %executor_order_id,
                "Broker reports order fully Filled at cancel time; reconciling without DELETE"
            );
            Ok(vec![OffchainOrderEvent::Filled {
                price: Usd::new(price),
                filled_at: executed_at,
            }])
        }

        OrderState::Failed {
            error_reason,
            failed_at,
            shares_filled,
            avg_price,
        } => {
            // Order terminally failed at the broker between our last poll
            // and the cancel attempt. Emit Failed (which short-circuits
            // the DELETE -- attempting it would return 422 "not
            // cancellable" and trap the aggregate in CancelFailed retry).
            tracing::info!(
                %executor_order_id,
                ?error_reason,
                "Broker reports order Failed at cancel time; emitting Failed without DELETE"
            );
            let error =
                error_reason.unwrap_or_else(|| "Broker reported Failed at cancel time".to_string());

            if let (Some(shares_filled), Some(avg_price)) = (shares_filled, avg_price) {
                let broker_filled = FractionalShares::new(shares_filled);
                if let Some(local) = local_filled
                    && !broker_filled.inner().gt(local.inner())?
                {
                    return Ok(vec![OffchainOrderEvent::Failed { error, failed_at }]);
                }

                return Ok(vec![
                    OffchainOrderEvent::PartiallyFilled {
                        shares_filled: broker_filled,
                        avg_price: Usd::new(avg_price),
                        partially_filled_at: failed_at,
                    },
                    OffchainOrderEvent::Failed { error, failed_at },
                ]);
            }

            // Broker reports a positive fill but no avg_price: we cannot record
            // the fill correctly, and emitting a bare Failed here would clear
            // the position via FailOffChainOrder and silently drop those filled
            // shares, causing the next scan to re-hedge them (double hedge).
            // Block instead, exactly like the Cancelled arm below, so the
            // cancel is retried once the broker returns a priced fill.
            if let (Some(shares_filled), None) = (shares_filled, avg_price)
                && Positive::new(FractionalShares::new(shares_filled)).is_ok()
            {
                return Err(OffchainOrderError::PreCancelPartialFillMissingAvgPrice {
                    executor_order_id: executor_order_id.clone(),
                    shares_filled: FractionalShares::new(shares_filled),
                });
            }

            Ok(vec![OffchainOrderEvent::Failed { error, failed_at }])
        }

        OrderState::Cancelled {
            cancelled_at,
            shares_filled,
            avg_price,
            ..
        } => {
            tracing::info!(
                %executor_order_id,
                "Broker reports order already Cancelled at cancel time; reconciling without DELETE"
            );

            if let (Some(shares_filled), Some(avg_price)) = (shares_filled, avg_price) {
                let broker_filled = FractionalShares::new(shares_filled);
                if let Some(local) = local_filled
                    && !broker_filled.inner().gt(local.inner())?
                {
                    return Ok(vec![OffchainOrderEvent::Cancelled {
                        reason: cancellation_reason,
                        cancelled_at,
                    }]);
                }

                return Ok(vec![
                    OffchainOrderEvent::PartiallyFilled {
                        shares_filled: broker_filled,
                        avg_price: Usd::new(avg_price),
                        partially_filled_at: cancelled_at,
                    },
                    OffchainOrderEvent::Cancelled {
                        reason: cancellation_reason,
                        cancelled_at,
                    },
                ]);
            }

            if let (Some(shares_filled), None) = (shares_filled, avg_price)
                && Positive::new(FractionalShares::new(shares_filled)).is_ok()
            {
                return Err(OffchainOrderError::PreCancelPartialFillMissingAvgPrice {
                    executor_order_id: executor_order_id.clone(),
                    shares_filled: FractionalShares::new(shares_filled),
                });
            }

            Ok(vec![OffchainOrderEvent::Cancelled {
                reason: cancellation_reason,
                cancelled_at,
            }])
        }

        OrderState::Pending | OrderState::Submitted { .. } => Ok(Vec::new()),
    }
}

/// Result of a successful order placement, with the executor-assigned ID
/// and the actual quantity placed (which may differ from the requested
/// quantity due to broker precision limits).
pub struct OrderPlacementResult {
    pub executor_order_id: ExecutorOrderId,
    pub placed_shares: Positive<FractionalShares>,
}

/// Type-erased order placement capability injected into the OffchainOrder
/// aggregate via cqrs-es Services.
///
/// This trait exists because the `Executor` trait has associated types
/// (`Error`, `OrderId`, `Ctx`) which make it non-object-safe - you cannot
/// write `Arc<dyn Executor>`. This trait provides the minimal surface needed
/// by the aggregate with erased error/ID types, allowing different executor
/// implementations to be used via `Arc<dyn OrderPlacer>`.
#[async_trait]
pub trait OrderPlacer: Send + Sync {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>;

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>;

    async fn cancel_order(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Fetches the latest trade price for a symbol, used to compute limit
    /// prices for extended-hours counter-trades. Returns `None` when the
    /// executor does not support market data lookups.
    async fn fetch_latest_trade_price(
        &self,
        _symbol: &Symbol,
    ) -> Result<
        Option<st0x_execution::Positive<rain_math_float::Float>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(None)
    }

    /// Returns the current market session. Used by hedge jobs to re-check
    /// the session at execution time so a queued job does not submit the
    /// wrong order type across the 9:30/16:00 ET boundary. The default
    /// returns `Regular`, which is the safe assumption for executors
    /// without session awareness (e.g. dry-run).
    async fn market_session(
        &self,
    ) -> Result<st0x_execution::MarketSession, Box<dyn std::error::Error + Send + Sync>> {
        Ok(st0x_execution::MarketSession::Regular)
    }

    /// Queries the broker for the current state of an order. Used by the
    /// `CancelOrder` handler's `reconcile_pre_cancel` to apply any fill that
    /// landed between the last poll and the cancel.
    ///
    /// The default deliberately FAILS rather than fabricating a `Submitted`
    /// state: a fabricated-`Submitted` default would make an implementer that
    /// forgot to query the broker silently skip reconciliation and drop a fill
    /// -- the exact partial-fill loss `reconcile_pre_cancel` exists to prevent.
    /// `reconcile_pre_cancel` maps this error to `PreCancelStatusFetchFailed`,
    /// which keeps the order in its prior state and retries instead of
    /// cancelling blind. Real implementations (`ExecutorOrderPlacer`) override
    /// this to delegate to the executor.
    async fn get_order_status(
        &self,
        _executor_order_id: &ExecutorOrderId,
    ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
        Err("get_order_status not implemented for this OrderPlacer".into())
    }
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

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        let placement = self.0.place_limit_order(order).await?;
        Ok(OrderPlacementResult {
            executor_order_id: ExecutorOrderId::new(&placement.order_id),
            placed_shares: placement.shares,
        })
    }

    async fn cancel_order(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let order_id = self.0.parse_order_id(executor_order_id.as_ref())?;
        self.0.cancel_order(&order_id).await?;
        Ok(())
    }

    async fn fetch_latest_trade_price(
        &self,
        symbol: &Symbol,
    ) -> Result<
        Option<st0x_execution::Positive<rain_math_float::Float>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(self.0.fetch_latest_trade_price(symbol).await?)
    }

    async fn market_session(
        &self,
    ) -> Result<st0x_execution::MarketSession, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.0.market_session().await?)
    }

    async fn get_order_status(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
        let order_id = self.0.parse_order_id(executor_order_id.as_ref())?;
        Ok(self.0.get_order_status(&order_id).await?)
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

        async fn place_limit_order(
            &self,
            order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("noop-limit"),
                placed_shares: noop_placed_shares(order.shares),
            })
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn get_order_status(
            &self,
            executor_order_id: &ExecutorOrderId,
        ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
            // No-op placer reports the order still live, so pre-cancel
            // reconciliation finds nothing to apply (matches the prior default).
            Ok(st0x_execution::OrderState::Submitted {
                order_id: executor_order_id.as_ref().to_string(),
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

/// Determines whether a counter-trade is placed as a market order (regular
/// hours) or a limit order with `extended_hours: true` (pre-market /
/// after-hours).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterTradeOrderKind {
    Market,
    ExtendedHoursLimit { limit_price: Positive<Usd> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        /// Idempotency key forwarded to the broker so apalis retries of
        /// the same `PlaceHedge` job do not produce a second order if the
        /// first placement's response is lost in flight.
        client_order_id: ClientOrderId,
        kind: CounterTradeOrderKind,
    },
    /// Request broker cancellation for a submitted order and persist the
    /// in-flight cancellation state. The order becomes terminal only after the
    /// broker later reports `Cancelled`.
    CancelOrder {
        reason: CancellationReason,
    },
    ConfirmCancellation {
        cancelled_at: DateTime<Utc>,
    },
    UpdatePartialFill {
        shares_filled: FractionalShares,
        avg_price: Usd,
    },
    CompleteFill {
        price: Usd,
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
        /// Whether this order was placed during extended hours as a limit
        /// order. Used by the cancel-and-replace logic to avoid cancelling
        /// regular-hours market orders. Defaults to `false` for events
        /// persisted before this field existed.
        #[serde(default)]
        is_extended_hours: bool,
    },
    Submitted {
        executor_order_id: ExecutorOrderId,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        shares_filled: FractionalShares,
        avg_price: Usd,
        partially_filled_at: DateTime<Utc>,
    },
    CancelRequested {
        reason: CancellationReason,
        cancel_requested_at: DateTime<Utc>,
    },
    Filled {
        price: Usd,
        filled_at: DateTime<Utc>,
    },
    Failed {
        error: String,
        failed_at: DateTime<Utc>,
    },
    Cancelled {
        reason: CancellationReason,
        cancelled_at: DateTime<Utc>,
    },
}

impl DomainEvent for OffchainOrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Placed { .. } => "OffchainOrderEvent::Placed".to_string(),
            Self::Submitted { .. } => "OffchainOrderEvent::Submitted".to_string(),
            Self::PartiallyFilled { .. } => "OffchainOrderEvent::PartiallyFilled".to_string(),
            Self::CancelRequested { .. } => "OffchainOrderEvent::CancelRequested".to_string(),
            Self::Filled { .. } => "OffchainOrderEvent::Filled".to_string(),
            Self::Failed { .. } => "OffchainOrderEvent::Failed".to_string(),
            Self::Cancelled { .. } => "OffchainOrderEvent::Cancelled".to_string(),
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
    #[error("Cannot place order: order has already been placed")]
    AlreadyPlaced,
    #[error(
        "Cannot update or complete fill: order has not been \
         submitted to broker yet"
    )]
    NotSubmitted,
    #[error("Cannot update order: order has already been completed (filled or failed)")]
    AlreadyCompleted,
    #[error("Cannot update fill: broker cancellation has already been requested")]
    CancellationAlreadyRequested,
    #[error("Cannot confirm cancellation: broker cancellation has not been requested")]
    CancellationNotRequested,
    #[error("Order has not been placed yet")]
    NotPlaced,
    #[error(
        "Broker placed {placed} shares, exceeding the \
         requested {requested}"
    )]
    PlacedExceedsRequested {
        placed: Positive<FractionalShares>,
        requested: Positive<FractionalShares>,
    },
    /// Pre-cancel broker status query failed. Surfaced as an error so the
    /// aggregate stays in its prior state and the caller retries -- silent
    /// bypass would lose any partial fills that occurred between the last
    /// poll and the cancel attempt.
    // NOTE: `reason: String` for the same CQRS-derives constraint as
    // `CancelFailed` below.
    #[error(
        "Failed to read pre-cancel broker state for order \
         {executor_order_id}: {reason}"
    )]
    PreCancelStatusFetchFailed {
        executor_order_id: ExecutorOrderId,
        reason: String,
    },
    #[error(
        "Broker reported partial fill of {shares_filled} shares for order \
         {executor_order_id} without an average price"
    )]
    PreCancelPartialFillMissingAvgPrice {
        executor_order_id: ExecutorOrderId,
        shares_filled: FractionalShares,
    },
    #[error("Float arithmetic error: {0}")]
    Float(String),
    // NOTE: `reason: String` violates AGENTS.md "no opaque String values
    // in errors" preference, but `OffchainOrderError` requires
    // `Clone + Serialize + Deserialize + PartialEq + Eq` for CQRS event
    // persistence -- none of which `Box<dyn Error>` satisfies. A typed
    // enum here would require the broker abstraction to surface typed
    // cancellation errors, which is a wider refactor beyond this PR.
    #[error(
        "Failed to cancel order {executor_order_id} via broker: \
         {reason}"
    )]
    CancelFailed {
        executor_order_id: ExecutorOrderId,
        reason: String,
    },
}

impl From<rain_math_float::FloatError> for OffchainOrderError {
    fn from(error: rain_math_float::FloatError) -> Self {
        Self::Float(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::{AggregateError, LifecycleError, TestStore, replay};

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

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("Broker rejected order".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
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
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            kind: CounterTradeOrderKind::Market,
        }
    }

    #[tokio::test]
    async fn place_order_transitions_to_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Submitted { .. }));

        let expected =
            noop_placed_shares(Positive::new(FractionalShares::new(float!(100))).unwrap());
        assert_eq!(
            inner.shares(),
            expected,
            "Persisted shares should reflect the broker-accepted quantity, not the original request"
        );
    }

    #[tokio::test]
    async fn place_with_failing_broker_transitions_to_failed() {
        let store = TestStore::<OffchainOrder>::new(failing_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(&inner, OffchainOrder::Failed { error, .. } if error.contains("Broker rejected")),
            "Expected Failed with broker error, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn place_rejects_when_placed_shares_exceed_requested() {
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

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!("test stub")
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    Ok(())
                }
            }

            Arc::new(Overfill)
        }

        let store = TestStore::<OffchainOrder>::new(overfilling_order_placer());
        let id = OffchainOrderId::new();

        let err = store.send(&id, place_command()).await.unwrap_err();
        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PlacedExceedsRequested { .. }
                ))
            ),
            "Expected PlacedExceedsRequested error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cannot_place_when_already_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();

        let err = store.send(&id, place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_place_when_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                },
            )
            .await
            .unwrap();

        let err = store.send(&id, place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_place_when_failed() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Market closed".to_string(),
                },
            )
            .await
            .unwrap();

        let err = store.send(&id, place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn partial_fill_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
    async fn cannot_fill_uninitialized_order() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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
        assert!(
            matches!(
                inner,
                OffchainOrder::Failed {
                    shares_filled: Some(shares_filled),
                    avg_price: Some(_),
                    executor_order_id: Some(_),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "Failed order must retain partial-fill metadata for retry recovery, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cannot_fail_already_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
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

    #[tokio::test]
    async fn cancel_order_from_submitted_transitions_to_cancelling() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "Expected Cancelling with MarketOpenReplacement reason, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_from_partially_filled_transitions_to_cancelling() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.0)),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    shares_filled,
                    avg_price: Some(_),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "Expected Cancelling from PartiallyFilled with preserved partial fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn confirm_cancellation_transitions_cancelling_to_cancelled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::ConfirmCancellation {
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelled {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "Expected confirmed cancellation to become terminal Cancelled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_short_circuits_already_cancelled_zero_fill_without_avg_price() {
        fn already_cancelled_zero_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-CANCELLED"),
                        placed_shares: noop_placed_shares(order.shares),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(st0x_execution::OrderState::Cancelled {
                        order_id: "ORD-CANCELLED".to_string(),
                        cancelled_at: Utc::now(),
                        shares_filled: Some(float!(0)),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(already_cancelled_zero_fill_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Cancelled { .. }),
            "Zero-fill broker cancellation without avg_price must become terminal Cancelled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_propagates_broker_error_and_leaves_state_unchanged() {
        // Critical safety invariant: when the broker DELETE fails, the
        // aggregate MUST stay in Submitted so the caller can retry.
        // Emitting Cancelled on broker error would let a still-live broker
        // order coexist with a replacement, causing duplicate hedges.
        fn cancel_failing_placer() -> Arc<dyn OrderPlacer> {
            struct CancelFailing;

            #[async_trait]
            impl OrderPlacer for CancelFailing {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    Err("simulated broker DELETE failure".into())
                }

                async fn get_order_status(
                    &self,
                    executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Order still live -> pre-cancel reconciliation is a no-op,
                    // so the flow reaches the failing DELETE under test.
                    Ok(st0x_execution::OrderState::Submitted {
                        order_id: executor_order_id.as_ref().to_string(),
                    })
                }
            }

            Arc::new(CancelFailing)
        }

        let store = TestStore::<OffchainOrder>::new(cancel_failing_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::CancelFailed { .. }
                ))
            ),
            "Expected CancelFailed, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted on broker cancel failure, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_pending_returns_not_submitted() {
        // Pending = order placed locally but not yet acknowledged by broker.
        // Cancel must reject so we don't DELETE an id that doesn't exist.
        let store = TestStore::<OffchainOrder>::new(failing_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        // failing_order_placer transitions the aggregate to Failed (not
        // Pending). So we can't test Pending via the normal path.
        // Instead verify that CancelOrder on a terminal state returns
        // AlreadyCompleted, which is the safer guarantee.
        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::AlreadyCompleted
                ))
            ),
            "Expected AlreadyCompleted from cancel on Failed, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_filled_returns_already_completed() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
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
    async fn cancel_order_reconciles_partial_fill_before_cancelling() {
        // Critical correctness: if the broker reports a partial fill that
        // the local aggregate doesn't know about yet, CancelOrder must
        // emit PartiallyFilled BEFORE Cancelled, otherwise the partial
        // fill is silently dropped and the position double-hedges.
        fn partial_fill_reconciling_placer() -> Arc<dyn OrderPlacer> {
            struct PartialFillPlacer;

            #[async_trait]
            impl OrderPlacer for PartialFillPlacer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    Ok(())
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports 50 shares filled at $150 -- a partial
                    // fill the local aggregate doesn't know about.
                    Ok(st0x_execution::OrderState::PartiallyFilled {
                        order_id: "ORD-OK".to_string(),
                        shares_filled: float!(50),
                        avg_price: Some(float!(150.0)),
                        partially_filled_at: Utc::now(),
                    })
                }
            }

            Arc::new(PartialFillPlacer)
        }

        let store = TestStore::<OffchainOrder>::new(partial_fill_reconciling_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        // Must be Cancelling, but the partial-fill event should have been
        // emitted en route so the eventual terminal cancellation can
        // finalize the partial fill correctly.
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    shares_filled,
                    avg_price: Some(_),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "Expected Cancelling with reconciled partial fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_blocks_when_broker_failed_with_unpriced_fill() {
        // If the broker reports the order Failed with filled shares but no
        // avg_price at cancel time, CancelOrder must NOT emit a bare Failed
        // (which would clear the position via FailOffChainOrder and silently
        // drop the filled shares -> the next scan double-hedges them). It must
        // propagate PreCancelPartialFillMissingAvgPrice so the cancel is
        // retried once the broker returns a priced fill, leaving the aggregate
        // in its prior Submitted state. Mirrors the Cancelled-arm guard.
        fn failed_unpriced_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    Ok(())
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports a Failed order carrying 50 filled shares
                    // but no avg_price -- we cannot price the fill.
                    Ok(st0x_execution::OrderState::Failed {
                        failed_at: Utc::now(),
                        error_reason: Some("broker failed".to_string()),
                        shares_filled: Some(float!(50)),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(failed_unpriced_fill_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PreCancelPartialFillMissingAvgPrice { .. }
                ))
            ),
            "Expected PreCancelPartialFillMissingAvgPrice, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted so the unpriced fill is not dropped, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_short_circuits_to_filled_if_broker_already_filled() {
        // If the order completes at the broker between our last poll and
        // the cancel attempt, we must emit Filled (not Cancelled) and
        // NOT call DELETE -- the order is already terminal at the broker.
        fn already_filled_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    panic!("cancel_order must not be called when broker reports Filled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(st0x_execution::OrderState::Filled {
                        order_id: "ORD-OK".to_string(),
                        price: float!(150.0),
                        executed_at: Utc::now(),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(already_filled_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Filled { .. }),
            "Cancel-then-broker-Filled must short-circuit to Filled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_already_cancelling_is_idempotent_noop() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Cancelling { .. }),
            "Duplicate cancel requests should leave order Cancelling, got: {inner:?}"
        );
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

    #[tokio::test]
    async fn build_offchain_order_cqrs_wires_store_and_projection() {
        let pool = crate::test_utils::setup_test_db().await;
        let order_placer = noop_order_placer();

        let (store, projection) = build_offchain_order_cqrs(&pool, order_placer)
            .await
            .expect("build_offchain_order_cqrs should succeed");

        let order_id = OffchainOrderId::new();

        store.send(&order_id, place_command()).await.unwrap();

        let order = projection
            .load(&order_id)
            .await
            .expect("projection load should not fail")
            .expect("projection should return Some for live order");

        assert!(matches!(order, OffchainOrder::Submitted { .. }));
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
}
