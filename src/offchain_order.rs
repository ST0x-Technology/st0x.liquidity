//! OffchainOrder CQRS/ES aggregate for tracking broker
//! order lifecycle: Pending -> Submitted -> Filled/Failed.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::GenericQuery;
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use std::sync::Arc;
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Lifecycle, SqliteQuery};
use st0x_execution::{
    Direction, Executor, ExecutorOrderId, FractionalShares, MarketOrder, Positive,
    SupportedExecutor, Symbol,
};

/// Constructs the offchain order CQRS framework with its view
/// query. Used by `Conductor::start`, CLI, and tests.
pub(crate) fn build_offchain_order_cqrs(
    pool: &SqlitePool,
    order_placer: Arc<dyn OrderPlacer>,
) -> (
    Arc<SqliteCqrs<Lifecycle<OffchainOrder>>>,
    Arc<SqliteQuery<OffchainOrder>>,
) {
    let view_repo = Arc::new(SqliteViewRepository::new(
        pool.clone(),
        "offchain_order_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo.clone());

    let cqrs = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(GenericQuery::new(view_repo))],
        order_placer,
    ));

    (cqrs, Arc::new(query))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum OffchainOrder {
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
        avg_price_cents: PriceCents,
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
        price_cents: PriceCents,
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

    const AGGREGATE_TYPE: &'static str = "OffchainOrder";
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

    fn evolve(event: &Self::Event, state: &Self) -> Result<Option<Self>, Self::Error> {
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
                } = state
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
                avg_price_cents,
                partially_filled_at,
            } => Ok(match state {
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
                    avg_price_cents: *avg_price_cents,
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    partially_filled_at: *partially_filled_at,
                }),

                Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => None,
            }),

            Filled {
                price_cents,
                filled_at,
            } => Ok(match state {
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
                    price_cents: *price_cents,
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    filled_at: *filled_at,
                }),

                Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => None,
            }),

            Failed { error, failed_at } => Ok(match state {
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
                    error: error.to_string(),
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
            } => {
                let now = Utc::now();
                let market_order = MarketOrder {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                };

                let placed = OffchainOrderEvent::Placed {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at: now,
                };

                match services.place_market_order(market_order).await {
                    Ok(executor_order_id) => Ok(vec![
                        placed,
                        OffchainOrderEvent::Submitted {
                            executor_order_id,
                            submitted_at: now,
                        },
                    ]),
                    Err(error) => Ok(vec![
                        placed,
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
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OffchainOrderCommand::Place { .. } => Err(OffchainOrderError::AlreadyPlaced),

            OffchainOrderCommand::UpdatePartialFill {
                shares_filled,
                avg_price_cents,
            } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price_cents,
                        partially_filled_at: Utc::now(),
                    }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::CompleteFill { price_cents } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Filled {
                        price_cents,
                        filled_at: Utc::now(),
                    }])
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
pub(crate) trait OrderPlacer: Send + Sync {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>>;
}

/// Bridges `Executor` (which has associated types and is not object-safe)
/// to `OrderPlacer` (object-safe).
pub(crate) struct ExecutorOrderPlacer<E>(pub E);

#[async_trait]
impl<E: Executor> OrderPlacer for ExecutorOrderPlacer<E> {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
        let placement = self.0.place_market_order(order).await?;
        Ok(ExecutorOrderId::new(&placement.order_id))
    }
}

#[cfg(test)]
pub(crate) fn noop_order_placer() -> Arc<dyn OrderPlacer> {
    struct Noop;

    #[async_trait]
    impl OrderPlacer for Noop {
        async fn place_market_order(
            &self,
            _order: MarketOrder,
        ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
            Ok(ExecutorOrderId::new("noop"))
        }
    }

    Arc::new(Noop)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PriceCents(pub(crate) u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    },
    UpdatePartialFill {
        shares_filled: FractionalShares,
        avg_price_cents: PriceCents,
    },
    CompleteFill {
        price_cents: PriceCents,
    },
    MarkFailed {
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OffchainOrderEvent {
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
        avg_price_cents: PriceCents,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        price_cents: PriceCents,
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
pub(crate) struct OffchainOrderId(Uuid);

impl std::fmt::Display for OffchainOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OffchainOrderId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl OffchainOrderId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OffchainOrderError {
    #[error("Cannot place order: order has already been placed")]
    AlreadyPlaced,
    #[error("Cannot confirm submission: order has not been submitted to broker yet")]
    NotSubmitted,
    #[error("Cannot update order: order has already been completed (filled or failed)")]
    AlreadyCompleted,
    #[error("Order has not been placed yet")]
    NotPlaced,
}

impl TryFrom<i64> for PriceCents {
    type Error = std::num::TryFromIntError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::{Aggregate, AggregateContext, AggregateError, CqrsFramework, EventStore};
    use rust_decimal_macros::dec;

    use st0x_event_sorcery::LifecycleError;

    use super::*;

    type TestCqrs = CqrsFramework<Lifecycle<OffchainOrder>, MemStore<Lifecycle<OffchainOrder>>>;

    fn test_cqrs() -> (MemStore<Lifecycle<OffchainOrder>>, TestCqrs) {
        test_cqrs_with(noop_order_placer())
    }

    fn test_cqrs_with(
        order_placer: Arc<dyn OrderPlacer>,
    ) -> (MemStore<Lifecycle<OffchainOrder>>, TestCqrs) {
        let store = MemStore::default();
        let cqrs = CqrsFramework::new(store.clone(), vec![], order_placer);
        (store, cqrs)
    }

    fn failing_order_placer() -> Arc<dyn OrderPlacer> {
        struct Failing;

        #[async_trait]
        impl OrderPlacer for Failing {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
                Err("Broker rejected order".into())
            }
        }

        Arc::new(Failing)
    }

    fn place_command() -> OffchainOrderCommand {
        OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
        }
    }

    async fn load_order(
        store: &MemStore<Lifecycle<OffchainOrder>>,
        id: &str,
    ) -> Lifecycle<OffchainOrder> {
        store.load_aggregate(id).await.unwrap().aggregate().clone()
    }

    #[tokio::test]
    async fn place_order_transitions_to_submitted() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Submitted { .. }));
    }

    #[tokio::test]
    async fn place_with_failing_broker_transitions_to_failed() {
        let (store, cqrs) = test_cqrs_with(failing_order_placer());

        cqrs.execute("order-1", place_command()).await.unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(
            matches!(&inner, OffchainOrder::Failed { error, .. } if error.contains("Broker rejected")),
            "Expected Failed with broker error, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cannot_place_when_already_submitted() {
        let (_store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();

        let err = cqrs.execute("order-1", place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_place_when_filled() {
        let (_store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::CompleteFill {
                price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();

        let err = cqrs.execute("order-1", place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_place_when_failed() {
        let (_store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::MarkFailed {
                error: "Market closed".to_string(),
            },
        )
        .await
        .unwrap();

        let err = cqrs.execute("order-1", place_command()).await.unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyPlaced))
        ));
    }

    #[tokio::test]
    async fn partial_fill_from_submitted() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(dec!(50)),
                avg_price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::PartiallyFilled { .. }));
    }

    #[tokio::test]
    async fn partial_fill_updates_shares() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(dec!(50)),
                avg_price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(dec!(75)),
                avg_price_cents: PriceCents(15050),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(OffchainOrder::PartiallyFilled { shares_filled, .. }) =
            load_order(&store, "order-1").await
        else {
            panic!("Expected Live PartiallyFilled state");
        };
        assert_eq!(shares_filled, FractionalShares::new(dec!(75)));
    }

    #[tokio::test]
    async fn complete_fill_from_submitted() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::CompleteFill {
                price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn complete_fill_from_partially_filled() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(dec!(75)),
                avg_price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::CompleteFill {
                price_cents: PriceCents(15025),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn cannot_fill_uninitialized_order() {
        let (_store, cqrs) = test_cqrs();

        let err = cqrs
            .execute(
                "order-1",
                OffchainOrderCommand::CompleteFill {
                    price_cents: PriceCents(15000),
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
        let (_store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::CompleteFill {
                price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();

        let err = cqrs
            .execute(
                "order-1",
                OffchainOrderCommand::CompleteFill {
                    price_cents: PriceCents(15000),
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
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::MarkFailed {
                error: "Insufficient funds".to_string(),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_failed_from_partially_filled() {
        let (store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled: FractionalShares::new(dec!(50)),
                avg_price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::MarkFailed {
                error: "Order cancelled".to_string(),
            },
        )
        .await
        .unwrap();

        let Lifecycle::Live(inner) = load_order(&store, "order-1").await else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn cannot_fail_already_filled() {
        let (_store, cqrs) = test_cqrs();

        cqrs.execute("order-1", place_command()).await.unwrap();
        cqrs.execute(
            "order-1",
            OffchainOrderCommand::CompleteFill {
                price_cents: PriceCents(15000),
            },
        )
        .await
        .unwrap();

        let err = cqrs
            .execute(
                "order-1",
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
    fn transition_on_uninitialized_corrupts_state() {
        let mut order = Lifecycle::<OffchainOrder>::default();

        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD123"),
            submitted_at: Utc::now(),
        };

        order.apply(event);

        assert!(matches!(order, Lifecycle::Failed { .. }));
    }
}
