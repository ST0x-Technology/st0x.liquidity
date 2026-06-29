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
use sqlx::SqlitePool;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use st0x_dto::{Direction, Trade, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, Projection, Store, StoreBuilder, Table};
use st0x_execution::{
    AlpacaBrokerApiError, ClientOrderId, ExecutionError, Executor, ExecutorOrderId,
    FractionalShares, MarketOrder, PersistenceError, Positive, SupportedExecutor, Symbol,
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
    },
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
            } => {
                let now = Utc::now();
                let market_order = MarketOrder {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    client_order_id,
                };

                let direction_label = match direction {
                    Direction::Buy => "buy",
                    Direction::Sell => "sell",
                };
                let symbol_label = symbol.to_string();

                match services.place_market_order(market_order).await {
                    Ok(result) => {
                        if result.placed_shares > shares {
                            return Err(OffchainOrderError::PlacedExceedsRequested {
                                placed: result.placed_shares,
                                requested: shares,
                            });
                        }

                        counter!(
                            "hedge_trades_total",
                            "symbol" => symbol_label,
                            "direction" => direction_label
                        )
                        .increment(1);

                        Ok(vec![
                            OffchainOrderEvent::Placed {
                                symbol,
                                shares: result.placed_shares,
                                direction,
                                executor,
                                placed_at: now,
                            },
                            OffchainOrderEvent::Submitted {
                                executor_order_id: result.executor_order_id,
                                submitted_at: now,
                            },
                        ])
                    }
                    Err(error) => {
                        counter!(
                            "broker_errors_total",
                            "symbol" => symbol_label,
                            "kind" => "place_order_failed"
                        )
                        .increment(1);

                        Ok(vec![
                            OffchainOrderEvent::Placed {
                                symbol,
                                shares,
                                direction,
                                executor,
                                placed_at: now,
                            },
                            OffchainOrderEvent::Failed {
                                error: error.to_string(),
                                failed_at: now,
                            },
                        ])
                    }
                }
            }

            _ => Err(OffchainOrderError::NotPlaced),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OffchainOrderCommand::Place { .. } => Err(OffchainOrderError::AlreadyPlaced),

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
                    // recorded as a bogus value.
                    if let Ok(latency) = (filled_at - *placed_at).to_std() {
                        histogram!("hedge_fill_latency_seconds", "symbol" => symbol.to_string())
                            .record(latency.as_secs_f64());
                    }

                    Ok(vec![OffchainOrderEvent::Filled { price, filled_at }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::MarkFailed { error } => match self {
                Self::Pending { .. } | Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Failed {
                        error,
                        failed_at: Utc::now(),
                    }])
                }
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
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

/// Type-erased order placement capability injected into the OffchainOrder
/// aggregate via cqrs-es Services.
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
        /// Idempotency key forwarded to the broker so apalis retries of
        /// the same `PlaceHedge` job do not produce a second order if the
        /// first placement's response is lost in flight.
        client_order_id: ClientOrderId,
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
    #[error("Cannot place order: order has already been placed")]
    AlreadyPlaced,
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
        "Broker placed {placed} shares, exceeding the \
         requested {requested}"
    )]
    PlacedExceedsRequested {
        placed: Positive<FractionalShares>,
        requested: Positive<FractionalShares>,
    },
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

    // These two tests install the process-global Prometheus recorder via
    // crate::metrics::setup() and assert on its rendered output. nextest (which
    // CI and local runs use) runs each test in its own process, so the
    // install-once recorder is fresh per test and the negative assertions below
    // are not contaminated by the sibling test's counter.

    #[tokio::test]
    async fn place_increments_hedge_trades_counter_on_success() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();

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
    async fn place_increments_broker_errors_counter_on_failure() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let store = TestStore::<OffchainOrder>::new(failing_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();

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
    async fn complete_fill_records_hedge_fill_latency_sample() {
        // Process-isolated under nextest; only this fill is sampled. The default
        // Prometheus summary rendering emits a `_count` series we can assert on.
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
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

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_fill_latency_seconds_count{symbol=\"AAPL\"} 1"),
            "completing a fill must record exactly one hedge_fill_latency_seconds sample, got:\n{rendered}"
        );
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
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
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
