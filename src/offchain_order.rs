//! OffchainOrder CQRS/ES aggregate for tracking broker
//! order lifecycle: Pending -> Submitted -> Filled/Failed.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteCqrs;
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

use st0x_execution::{
    Direction, Executor, ExecutorOrderId, FractionalShares, MarketOrder, Positive,
    SupportedExecutor, Symbol,
};

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

pub(crate) type OffchainOrderCqrs = SqliteCqrs<Lifecycle<OffchainOrder>>;

#[async_trait]
impl Aggregate for Lifecycle<OffchainOrder> {
    type Command = OffchainOrderCommand;
    type Event = OffchainOrderEvent;
    type Error = OffchainOrderError;
    type Services = Arc<dyn OrderPlacer>;

    fn aggregate_type() -> String {
        "OffchainOrder".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, OffchainOrder::apply_transition)
            .or_initialize(&event, OffchainOrder::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (
                Err(LifecycleError::Uninitialized),
                OffchainOrderCommand::Place {
                    symbol,
                    shares,
                    direction,
                    executor,
                },
            ) => {
                let now = Utc::now();
                let market_order = MarketOrder {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                };

                let placed = OffchainOrderEvent::Placed {
                    symbol: symbol.clone(),
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
                    Err(e) => Ok(vec![
                        placed,
                        OffchainOrderEvent::Failed {
                            error: e.to_string(),
                            failed_at: now,
                        },
                    ]),
                }
            }

            (Err(e), _) => Err(e.into()),

            (Ok(order), OffchainOrderCommand::ConfirmSubmission { executor_order_id }) => {
                handle_confirm_submission(order, executor_order_id)
            }

            (
                Ok(order),
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled,
                    avg_price_cents,
                },
            ) => match order {
                OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled: *shares_filled,
                        avg_price_cents: *avg_price_cents,
                        partially_filled_at: Utc::now(),
                    }])
                }
                OffchainOrder::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                OffchainOrder::Filled { .. } | OffchainOrder::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            (Ok(order), OffchainOrderCommand::CompleteFill { price_cents }) => match order {
                OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Filled {
                        price_cents: *price_cents,
                        filled_at: Utc::now(),
                    }])
                }
                OffchainOrder::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                OffchainOrder::Filled { .. } | OffchainOrder::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            (Ok(order), OffchainOrderCommand::MarkFailed { error }) => match order {
                OffchainOrder::Pending { .. }
                | OffchainOrder::Submitted { .. }
                | OffchainOrder::PartiallyFilled { .. } => Ok(vec![OffchainOrderEvent::Failed {
                    error: error.clone(),
                    failed_at: Utc::now(),
                }]),
                OffchainOrder::Filled { .. } | OffchainOrder::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
        }
    }
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

impl OffchainOrder {
    pub(crate) fn aggregate_id(id: OffchainOrderId) -> String {
        id.to_string()
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

    pub(crate) fn apply_transition(
        event: &OffchainOrderEvent,
        order: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            OffchainOrderEvent::Submitted {
                executor_order_id,
                submitted_at,
            } => Self::apply_submitted(order, executor_order_id, *submitted_at, event),

            OffchainOrderEvent::PartiallyFilled {
                shares_filled,
                avg_price_cents,
                partially_filled_at,
            } => Self::apply_partially_filled(
                order,
                *shares_filled,
                *avg_price_cents,
                *partially_filled_at,
                event,
            ),

            OffchainOrderEvent::Filled {
                price_cents,
                filled_at,
            } => Self::apply_filled(order, *price_cents, *filled_at, event),

            OffchainOrderEvent::Failed { error, failed_at } => {
                Self::apply_failed(order, error, *failed_at, event)
            }

            OffchainOrderEvent::Placed { .. } => Err(LifecycleError::Mismatch {
                state: format!("{order:?}"),
                event: event.event_type(),
            }),
        }
    }

    fn apply_submitted(
        order: &Self,
        executor_order_id: &ExecutorOrderId,
        submitted_at: DateTime<Utc>,
        event: &OffchainOrderEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Pending {
            symbol,
            shares,
            direction,
            executor,
            placed_at,
        } = order
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{order:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Submitted {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            placed_at: *placed_at,
            submitted_at,
        })
    }

    fn apply_partially_filled(
        order: &Self,
        shares_filled: FractionalShares,
        avg_price_cents: PriceCents,
        partially_filled_at: DateTime<Utc>,
        event: &OffchainOrderEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        match order {
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
            } => Ok(Self::PartiallyFilled {
                symbol: symbol.clone(),
                shares: *shares,
                shares_filled,
                direction: *direction,
                executor: *executor,
                executor_order_id: executor_order_id.clone(),
                avg_price_cents,
                placed_at: *placed_at,
                submitted_at: *submitted_at,
                partially_filled_at,
            }),

            Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => {
                Err(LifecycleError::Mismatch {
                    state: format!("{order:?}"),
                    event: event.event_type(),
                })
            }
        }
    }

    fn apply_filled(
        order: &Self,
        price_cents: PriceCents,
        filled_at: DateTime<Utc>,
        event: &OffchainOrderEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        match order {
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
            } => Ok(Self::Filled {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                executor: *executor,
                executor_order_id: executor_order_id.clone(),
                price_cents,
                placed_at: *placed_at,
                submitted_at: *submitted_at,
                filled_at,
            }),

            Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => {
                Err(LifecycleError::Mismatch {
                    state: format!("{order:?}"),
                    event: event.event_type(),
                })
            }
        }
    }

    fn apply_failed(
        order: &Self,
        error: &str,
        failed_at: DateTime<Utc>,
        event: &OffchainOrderEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        match order {
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
            } => Ok(Self::Failed {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                executor: *executor,
                error: error.to_string(),
                placed_at: *placed_at,
                failed_at,
            }),

            Self::Filled { .. } | Self::Failed { .. } => Err(LifecycleError::Mismatch {
                state: format!("{order:?}"),
                event: event.event_type(),
            }),
        }
    }

    pub(crate) fn from_event(event: &OffchainOrderEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            OffchainOrderEvent::Placed {
                symbol,
                shares,
                direction,
                executor,
                placed_at,
            } => Ok(Self::Pending {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                executor: *executor,
                placed_at: *placed_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
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

fn handle_confirm_submission(
    order: &OffchainOrder,
    executor_order_id: &ExecutorOrderId,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    match order {
        OffchainOrder::Pending { .. } => Ok(vec![OffchainOrderEvent::Submitted {
            executor_order_id: executor_order_id.clone(),
            submitted_at: Utc::now(),
        }]),
        OffchainOrder::Submitted {
            executor_order_id: existing_id,
            ..
        } => {
            if existing_id == executor_order_id {
                Ok(vec![])
            } else {
                Err(OffchainOrderError::ConflictingExecutorOrderId {
                    existing: existing_id.clone(),
                    attempted: executor_order_id.clone(),
                })
            }
        }
        OffchainOrder::PartiallyFilled { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. } => Err(OffchainOrderError::AlreadySubmitted),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    },
    ConfirmSubmission {
        executor_order_id: ExecutorOrderId,
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum OffchainOrderError {
    #[error("Cannot place order: order has already been placed")]
    AlreadyPlaced,
    #[error("Cannot confirm submission: order has not been submitted to broker yet")]
    NotSubmitted,
    #[error("Cannot update order: order has already been completed (filled or failed)")]
    AlreadyCompleted,
    #[error("Cannot submit order: order has already been submitted")]
    AlreadySubmitted,
    #[error(
        "Cannot confirm submission: order already submitted with different executor_order_id \
         (existing: {existing:?}, attempted: {attempted:?})"
    )]
    ConflictingExecutorOrderId {
        existing: ExecutorOrderId,
        attempted: ExecutorOrderId,
    },
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, thiserror::Error)]
#[error("Price in cents cannot be negative: {0}")]
pub(crate) struct NegativePriceCents(pub i64);

impl TryFrom<i64> for PriceCents {
    type Error = NegativePriceCents;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value)
            .map(Self)
            .map_err(|_| NegativePriceCents(value))
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_place_order() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let command = OffchainOrderCommand::Place {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Placed { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Pending { .. }));
    }

    #[tokio::test]
    async fn test_cannot_place_when_already_pending() {
        let order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyPlaced)
        ));
    }

    #[tokio::test]
    async fn test_cannot_place_when_filled() {
        let order = Lifecycle::Live(OffchainOrder::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyPlaced)
        ));
    }

    #[tokio::test]
    async fn test_cannot_place_when_failed() {
        let order = Lifecycle::Live(OffchainOrder::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            error: "Market closed".to_string(),
            placed_at: Utc::now(),
            failed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyPlaced)
        ));
    }

    #[tokio::test]
    async fn test_confirm_submission_after_place() {
        let mut order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::ConfirmSubmission {
            executor_order_id: ExecutorOrderId::new("ORD123"),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Submitted { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_cannot_confirm_submission_if_not_placed() {
        let order = Lifecycle::<OffchainOrder, Never>::default();

        let command = OffchainOrderCommand::ConfirmSubmission {
            executor_order_id: ExecutorOrderId::new("ORD123"),
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::State(LifecycleError::Uninitialized))
        ));
    }

    #[tokio::test]
    async fn test_submit_with_different_order_id_fails() {
        let order = Lifecycle::Live(OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::ConfirmSubmission {
            executor_order_id: ExecutorOrderId::new("ORD456"),
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::ConflictingExecutorOrderId { .. })
        ));
    }

    #[tokio::test]
    async fn test_partial_fill_from_submitted() {
        let mut order = Lifecycle::Live(OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: FractionalShares::new(dec!(50)),
            avg_price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            OffchainOrderEvent::PartiallyFilled { .. }
        ));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::PartiallyFilled { .. }));
    }

    #[tokio::test]
    async fn test_partial_fill_updates_from_partially_filled() {
        let mut order = Lifecycle::Live(OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            shares_filled: FractionalShares::new(dec!(50)),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: FractionalShares::new(dec!(75)),
            avg_price_cents: PriceCents(15050),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(OffchainOrder::PartiallyFilled { shares_filled, .. }) = order else {
            panic!("Expected Live PartiallyFilled state");
        };

        assert_eq!(shares_filled, FractionalShares::new(dec!(75)));
    }

    #[tokio::test]
    async fn test_complete_fill_from_submitted() {
        let mut order = Lifecycle::Live(OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Filled { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_complete_fill_from_partially_filled() {
        let mut order = Lifecycle::Live(OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            shares_filled: FractionalShares::new(dec!(75)),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15025),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fill_if_not_submitted() {
        let order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::NotSubmitted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fill_already_filled() {
        let order = Lifecycle::Live(OffchainOrder::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_mark_failed_from_pending() {
        let mut order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Market closed".to_string(),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Failed { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_submitted() {
        let mut order = Lifecycle::Live(OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Insufficient funds".to_string(),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_partially_filled() {
        let mut order = Lifecycle::Live(OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            shares_filled: FractionalShares::new(dec!(50)),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Order cancelled".to_string(),
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_filled() {
        let order = Lifecycle::Live(OffchainOrder::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Test error".to_string(),
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_migrated_event_pending_status() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Pending { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_submitted_status() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD123")),
            price_cents: None,
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_filled_status() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Filled,
            executor_order_id: Some(ExecutorOrderId::new("ORD123")),
            price_cents: Some(PriceCents(15000)),
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_failed_status() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Failed {
                error: "Insufficient funds".to_string(),
            },
            executor_order_id: None,
            price_cents: None,
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(OffchainOrder::Failed { error, .. }) = order else {
            panic!("Expected Live Failed state");
        };
        assert_eq!(error, "Insufficient funds");
    }

    #[test]
    fn test_view_update_from_migrated_event_pending_status() {
        let execution_id = ExecutionId(42);
        let migrated_at = chrono::Utc::now();
        let symbol = Symbol::new("AAPL").unwrap();

        let event = OffchainOrderEvent::Migrated {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(100.5))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OffchainOrderView::default();

        assert!(matches!(view, OffchainOrderView::Unavailable));

        view.update(&envelope);

        let OffchainOrderView::Execution {
            execution_id: view_execution_id,
            symbol: view_symbol,
            shares,
            direction,
            executor,
            status,
            executor_order_id,
            price_cents,
            initiated_at,
            completed_at,
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(view_symbol, symbol);
        assert_eq!(
            shares,
            Positive::new(FractionalShares::new(dec!(100.5))).unwrap()
        );
        assert_eq!(direction, Direction::Buy);
        assert_eq!(executor, SupportedExecutor::Schwab);
        assert_eq!(status, ExecutionStatus::Pending);
        assert_eq!(executor_order_id, None);
        assert_eq!(price_cents, None);
        assert_eq!(initiated_at, migrated_at);
        assert_eq!(completed_at, None);
    }

    #[test]
    fn test_view_update_from_migrated_event_submitted_status() {
        let execution_id = ExecutionId(43);
        let migrated_at = chrono::Utc::now();
        let symbol = Symbol::new("TSLA").unwrap();

        let event = OffchainOrderEvent::Migrated {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(50.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::AlpacaTradingApi,
            status: MigratedOrderStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD123")),
            price_cents: None,
            executed_at: None,
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OffchainOrderView::default();
        view.update(&envelope);

        let OffchainOrderView::Execution {
            execution_id: view_execution_id,
            status,
            executor_order_id,
            price_cents,
            initiated_at,
            completed_at,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(status, ExecutionStatus::Submitted);
        assert_eq!(executor_order_id, Some(ExecutorOrderId::new("ORD123")));
        assert_eq!(price_cents, None);
        assert_eq!(initiated_at, migrated_at);
        assert_eq!(completed_at, None);
    }

    #[test]
    fn test_view_update_from_migrated_event_filled_status() {
        let execution_id = ExecutionId(44);
        let executed_at = chrono::Utc::now();
        let migrated_at = executed_at + chrono::Duration::seconds(10);
        let symbol = Symbol::new("NVDA").unwrap();

        let event = OffchainOrderEvent::Migrated {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(25.75))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Filled,
            executor_order_id: Some(ExecutorOrderId::new("ORD456")),
            price_cents: Some(PriceCents(45025)),
            executed_at: Some(executed_at),
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OffchainOrderView::default();
        view.update(&envelope);

        let OffchainOrderView::Execution {
            execution_id: view_execution_id,
            status,
            executor_order_id,
            price_cents,
            initiated_at,
            completed_at,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(executor_order_id, Some(ExecutorOrderId::new("ORD456")));
        assert_eq!(price_cents, Some(PriceCents(45025)));
        assert_eq!(initiated_at, executed_at);
        assert_eq!(completed_at, Some(executed_at));
    }

    #[test]
    fn test_view_update_from_migrated_event_failed_status() {
        let execution_id = ExecutionId(45);
        let executed_at = chrono::Utc::now();
        let migrated_at = executed_at + chrono::Duration::seconds(5);
        let symbol = Symbol::new("AMZN").unwrap();

        let event = OffchainOrderEvent::Migrated {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(10.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::AlpacaTradingApi,
            status: MigratedOrderStatus::Failed {
                error: "Insufficient funds".to_string(),
            },
            executor_order_id: None,
            price_cents: None,
            executed_at: Some(executed_at),
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OffchainOrderView::default();
        view.update(&envelope);

        let OffchainOrderView::Execution {
            execution_id: view_execution_id,
            status,
            executor_order_id,
            price_cents,
            initiated_at,
            completed_at,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(status, ExecutionStatus::Failed);
        assert_eq!(executor_order_id, None);
        assert_eq!(price_cents, None);
        assert_eq!(initiated_at, executed_at);
        assert_eq!(completed_at, Some(executed_at));
    }

    #[test]
    fn test_view_update_from_placed_event() {
        let execution_id = ExecutionId(46);
        let placed_at = chrono::Utc::now();
        let symbol = Symbol::new("MSFT").unwrap();

        let event = OffchainOrderEvent::Placed {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(75.25))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OffchainOrderView::default();
        view.update(&envelope);

        let OffchainOrderView::Execution {
            execution_id: view_execution_id,
            symbol: view_symbol,
            shares,
            direction,
            executor,
            status,
            executor_order_id,
            price_cents,
            initiated_at,
            completed_at,
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(view_symbol, symbol);
        assert_eq!(
            shares,
            Positive::new(FractionalShares::new(dec!(75.25))).unwrap()
        );
        assert_eq!(direction, Direction::Buy);
        assert_eq!(executor, SupportedExecutor::Schwab);
        assert_eq!(status, ExecutionStatus::Pending);
        assert_eq!(executor_order_id, None);
        assert_eq!(price_cents, None);
        assert_eq!(initiated_at, placed_at);
        assert_eq!(completed_at, None);
    }

    #[test]
    fn test_view_update_from_submitted_event() {
        let execution_id = ExecutionId(47);
        let placed_at = chrono::Utc::now();
        let submitted_at = placed_at + chrono::Duration::seconds(2);
        let symbol = Symbol::new("GOOG").unwrap();

        let mut view = OffchainOrderView::Execution {
            execution_id,
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(50.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::AlpacaTradingApi,
            status: ExecutionStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD789"),
            submitted_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let OffchainOrderView::Execution {
            status,
            executor_order_id,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(status, ExecutionStatus::Submitted);
        assert_eq!(executor_order_id, Some(ExecutorOrderId::new("ORD789")));
    }

    #[test]
    fn test_view_update_from_partially_filled_event() {
        let execution_id = ExecutionId(48);
        let placed_at = chrono::Utc::now();
        let partially_filled_at = placed_at + chrono::Duration::seconds(5);
        let symbol = Symbol::new("META").unwrap();

        let mut view = OffchainOrderView::Execution {
            execution_id,
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(100.0))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: ExecutionStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD999")),
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::PartiallyFilled {
            shares_filled: FractionalShares::new(dec!(60.0)),
            avg_price_cents: PriceCents(32500),
            partially_filled_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 3,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let OffchainOrderView::Execution { status, .. } = view else {
            panic!("Expected Execution variant");
        };

        assert_eq!(status, ExecutionStatus::Submitted);
    }

    #[test]
    fn test_view_update_from_filled_event() {
        let execution_id = ExecutionId(49);
        let placed_at = chrono::Utc::now();
        let filled_at = placed_at + chrono::Duration::seconds(10);
        let symbol = Symbol::new("NFLX").unwrap();

        let mut view = OffchainOrderView::Execution {
            execution_id,
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(30.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::AlpacaTradingApi,
            status: ExecutionStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD111")),
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::Filled {
            price_cents: PriceCents(48500),
            filled_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 4,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let OffchainOrderView::Execution {
            status,
            price_cents,
            completed_at,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(price_cents, Some(PriceCents(48500)));
        assert_eq!(completed_at, Some(filled_at));
    }

    #[test]
    fn test_view_update_from_failed_event() {
        let execution_id = ExecutionId(50);
        let placed_at = chrono::Utc::now();
        let failed_at = placed_at + chrono::Duration::seconds(3);
        let symbol = Symbol::new("AMD").unwrap();

        let mut view = OffchainOrderView::Execution {
            execution_id,
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(200.0))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: ExecutionStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD222")),
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::Failed {
            error: "Order rejected".to_string(),
            failed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: execution_id.0.to_string(),
            sequence: 5,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let OffchainOrderView::Execution {
            status,
            completed_at,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(status, ExecutionStatus::Failed);
        assert_eq!(completed_at, Some(failed_at));
    }

    #[test]
    fn test_submitted_on_unavailable_does_not_change_state() {
        let mut view = OffchainOrderView::Unavailable;

        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD333"),
            submitted_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "51".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_partially_filled_on_unavailable_does_not_change_state() {
        let mut view = OffchainOrderView::Unavailable;

        let event = OffchainOrderEvent::PartiallyFilled {
            shares_filled: FractionalShares::new(dec!(50.0)),
            avg_price_cents: PriceCents(30000),
            partially_filled_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "52".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_filled_on_unavailable_does_not_change_state() {
        let mut view = OffchainOrderView::Unavailable;

        let event = OffchainOrderEvent::Filled {
            price_cents: PriceCents(35000),
            filled_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "53".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_failed_on_unavailable_does_not_change_state() {
        let mut view = OffchainOrderView::Unavailable;

        let event = OffchainOrderEvent::Failed {
            error: "Broker error".to_string(),
            failed_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "54".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_migrated_with_invalid_execution_id_remains_unavailable() {
        let mut view = OffchainOrderView::default();
        let symbol = Symbol::new("INTC").unwrap();

        let event = OffchainOrderEvent::Migrated {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(100.0))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
            migrated_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "not_a_number".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_placed_with_invalid_execution_id_remains_unavailable() {
        let mut view = OffchainOrderView::default();
        let symbol = Symbol::new("ORCL").unwrap();

        let event = OffchainOrderEvent::Placed {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(50.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::AlpacaTradingApi,
            placed_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "invalid".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OffchainOrderView::Unavailable));
    }

    #[test]
    fn test_transition_on_uninitialized_corrupts_state() {
        let mut order = Lifecycle::<OffchainOrder, Never>::default();

        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD123"),
            submitted_at: Utc::now(),
        };

        order.apply(event);

        assert!(matches!(order, Lifecycle::Failed { .. }));
    }

    #[tokio::test]
    async fn test_migrate_command_creates_migrated_event() {
        let order = Lifecycle::<OffchainOrder, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let command = OffchainOrderCommand::Migrate {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
        };

        let events = order.handle(command, &noop_order_placer()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            OffchainOrderEvent::Migrated {
                symbol: evt_symbol,
                shares,
                direction,
                executor,
                status,
                executor_order_id,
                price_cents,
                executed_at,
                ..
            } => {
                assert_eq!(evt_symbol, &symbol);
                assert_eq!(shares.inner(), dec!(100));
                assert_eq!(direction, &Direction::Buy);
                assert_eq!(executor, &SupportedExecutor::Schwab);
                assert!(matches!(status, MigratedOrderStatus::Pending));
                assert!(executor_order_id.is_none());
                assert!(price_cents.is_none());
                assert!(executed_at.is_none());
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_migrate_command_all_status_types() {
        let symbol = Symbol::new("TSLA").unwrap();

        // Test Pending status
        let order = Lifecycle::<OffchainOrder, Never>::default();
        let command = OffchainOrderCommand::Migrate {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
        };
        let events = order.handle(command, &noop_order_placer()).await.unwrap();
        assert!(matches!(
            events[0],
            OffchainOrderEvent::Migrated {
                status: MigratedOrderStatus::Pending,
                ..
            }
        ));

        // Test Submitted status
        let order = Lifecycle::<OffchainOrder, Never>::default();
        let command = OffchainOrderCommand::Migrate {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD123")),
            price_cents: None,
            executed_at: Some(Utc::now()),
        };
        let events = order.handle(command, &noop_order_placer()).await.unwrap();
        assert!(matches!(
            events[0],
            OffchainOrderEvent::Migrated {
                status: MigratedOrderStatus::Submitted,
                ..
            }
        ));

        // Test Filled status
        let order = Lifecycle::<OffchainOrder, Never>::default();
        let command = OffchainOrderCommand::Migrate {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Filled,
            executor_order_id: Some(ExecutorOrderId::new("ORD456")),
            price_cents: Some(PriceCents(20000)),
            executed_at: Some(Utc::now()),
        };
        let events = order.handle(command, &noop_order_placer()).await.unwrap();
        assert!(matches!(
            events[0],
            OffchainOrderEvent::Migrated {
                status: MigratedOrderStatus::Filled,
                ..
            }
        ));

        // Test Failed status
        let order = Lifecycle::<OffchainOrder, Never>::default();
        let command = OffchainOrderCommand::Migrate {
            symbol,
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Failed {
                error: "Insufficient funds".to_string(),
            },
            executor_order_id: None,
            price_cents: None,
            executed_at: Some(Utc::now()),
        };
        let events = order.handle(command, &noop_order_placer()).await.unwrap();
        assert!(matches!(
            events[0],
            OffchainOrderEvent::Migrated {
                status: MigratedOrderStatus::Failed { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_cannot_migrate_when_already_placed() {
        let order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Migrate {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(50))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: MigratedOrderStatus::Pending,
            executor_order_id: None,
            price_cents: None,
            executed_at: None,
        };

        assert!(matches!(
            order.handle(command, &noop_order_placer()).await,
            Err(OffchainOrderError::AlreadyPlaced)
        ));
    }

    /// Bug: ConfirmSubmission is not idempotent, blocking recovery after partial
    /// dual-write failures.
    ///
    /// If ES write succeeds but legacy write fails, retrying with the same
    /// executor_order_id fails with AlreadySubmitted. System stuck in inconsistent
    /// state with no programmatic recovery path.
    #[tokio::test]
    async fn test_confirm_submission_not_idempotent_blocks_retry_recovery() {
        let mut order = Lifecycle::Live(OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(dec!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            placed_at: Utc::now(),
        });

        let executor_order_id = ExecutorOrderId::new("ORD-SAME-123");

        let command = OffchainOrderCommand::ConfirmSubmission {
            executor_order_id: executor_order_id.clone(),
        };
        let events = order.handle(command, &noop_order_placer()).await.unwrap();
        assert_eq!(events.len(), 1);
        order.apply(events[0].clone());

        let Lifecycle::Live(OffchainOrder::Submitted {
            executor_order_id: stored_id,
            ..
        }) = &order
        else {
            panic!("Expected Submitted state");
        };
        assert_eq!(stored_id, &executor_order_id);

        let retry_command = OffchainOrderCommand::ConfirmSubmission {
            executor_order_id: executor_order_id.clone(),
        };

        let events = order
            .handle(retry_command, &noop_order_placer())
            .await
            .expect("Retry with same executor_order_id should succeed for idempotent behavior");

        assert!(
            events.is_empty(),
            "Idempotent retry should return empty events vec, got {events:?}"
        );
    }
}
