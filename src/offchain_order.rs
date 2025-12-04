//! OffchainOrder aggregate for tracking broker order lifecycle.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};
use tracing::error;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::position::{BrokerOrderId, ExecutionId, PriceCents};
use crate::shares::FractionalShares;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Order {
    Pending {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        placed_at: DateTime<Utc>,
    },
    Submitted {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        broker_order_id: BrokerOrderId,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        symbol: Symbol,
        shares: FractionalShares,
        shares_filled: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        broker_order_id: BrokerOrderId,
        avg_price_cents: PriceCents,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        broker_order_id: BrokerOrderId,
        price_cents: PriceCents,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        error: String,
        placed_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl Order {
    pub(crate) fn apply_transition(
        event: &OffchainOrderEvent,
        order: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            OffchainOrderEvent::Submitted {
                broker_order_id,
                submitted_at,
            } => Self::apply_submitted(order, broker_order_id, *submitted_at, event),

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

            OffchainOrderEvent::Migrated { .. } | OffchainOrderEvent::Placed { .. } => {
                Err(LifecycleError::Mismatch {
                    state: format!("{order:?}"),
                    event: event.event_type(),
                })
            }
        }
    }

    fn apply_submitted(
        order: &Self,
        broker_order_id: &BrokerOrderId,
        submitted_at: DateTime<Utc>,
        event: &OffchainOrderEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Pending {
            symbol,
            shares,
            direction,
            broker,
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
            broker: *broker,
            broker_order_id: broker_order_id.clone(),
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
                broker,
                broker_order_id,
                placed_at,
                submitted_at,
            }
            | Self::PartiallyFilled {
                symbol,
                shares,
                direction,
                broker,
                broker_order_id,
                placed_at,
                submitted_at,
                ..
            } => Ok(Self::PartiallyFilled {
                symbol: symbol.clone(),
                shares: *shares,
                shares_filled,
                direction: *direction,
                broker: *broker,
                broker_order_id: broker_order_id.clone(),
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
                broker,
                broker_order_id,
                placed_at,
                submitted_at,
            }
            | Self::PartiallyFilled {
                symbol,
                shares,
                direction,
                broker,
                broker_order_id,
                placed_at,
                submitted_at,
                ..
            } => Ok(Self::Filled {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                broker: *broker,
                broker_order_id: broker_order_id.clone(),
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
                broker,
                placed_at,
            }
            | Self::Submitted {
                symbol,
                shares,
                direction,
                broker,
                placed_at,
                ..
            }
            | Self::PartiallyFilled {
                symbol,
                shares,
                direction,
                broker,
                placed_at,
                ..
            } => Ok(Self::Failed {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                broker: *broker,
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
                broker,
                placed_at,
            } => Ok(Self::Pending {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                broker: *broker,
                placed_at: *placed_at,
            }),

            OffchainOrderEvent::Migrated {
                symbol,
                shares,
                direction,
                broker,
                status,
                broker_order_id,
                price_cents,
                executed_at,
                migrated_at,
            } => match status {
                MigratedOrderStatus::Pending => Ok(Self::Pending {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    placed_at: executed_at.unwrap_or(*migrated_at),
                }),
                MigratedOrderStatus::Submitted => Ok(Self::Submitted {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    broker_order_id: broker_order_id
                        .clone()
                        .unwrap_or_else(|| BrokerOrderId("unknown".to_string())),
                    placed_at: *migrated_at,
                    submitted_at: executed_at.unwrap_or(*migrated_at),
                }),
                MigratedOrderStatus::Filled => Ok(Self::Filled {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    broker_order_id: broker_order_id
                        .clone()
                        .unwrap_or_else(|| BrokerOrderId("unknown".to_string())),
                    price_cents: price_cents.unwrap_or(PriceCents(0)),
                    placed_at: *migrated_at,
                    submitted_at: *migrated_at,
                    filled_at: executed_at.unwrap_or(*migrated_at),
                }),
                MigratedOrderStatus::Failed { error } => Ok(Self::Failed {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    error: error.clone(),
                    placed_at: *migrated_at,
                    failed_at: executed_at.unwrap_or(*migrated_at),
                }),
            },

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
        }
    }
}

#[async_trait]
impl Aggregate for Lifecycle<Order, Never> {
    type Command = OffchainOrderCommand;
    type Event = OffchainOrderEvent;
    type Error = OffchainOrderError;
    type Services = ();

    fn aggregate_type() -> String {
        "OffchainOrder".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, Order::apply_transition)
            .or_initialize(&event, Order::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (
                Err(LifecycleError::Uninitialized),
                OffchainOrderCommand::Place {
                    symbol,
                    shares,
                    direction,
                    broker,
                },
            ) => Ok(vec![OffchainOrderEvent::Placed {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                broker: *broker,
                placed_at: Utc::now(),
            }]),

            (Ok(_), OffchainOrderCommand::Place { .. }) => Err(OffchainOrderError::AlreadyPlaced),

            (Err(e), _) => Err(e.into()),

            (Ok(order), OffchainOrderCommand::ConfirmSubmission { broker_order_id }) => match order
            {
                Order::Pending { .. } => Ok(vec![OffchainOrderEvent::Submitted {
                    broker_order_id: broker_order_id.clone(),
                    submitted_at: Utc::now(),
                }]),
                Order::Submitted { .. }
                | Order::PartiallyFilled { .. }
                | Order::Filled { .. }
                | Order::Failed { .. } => Err(OffchainOrderError::AlreadySubmitted),
            },

            (
                Ok(order),
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled,
                    avg_price_cents,
                },
            ) => match order {
                Order::Submitted { .. } | Order::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled: *shares_filled,
                        avg_price_cents: *avg_price_cents,
                        partially_filled_at: Utc::now(),
                    }])
                }
                Order::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Order::Filled { .. } | Order::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            (Ok(order), OffchainOrderCommand::CompleteFill { price_cents }) => match order {
                Order::Submitted { .. } | Order::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Filled {
                        price_cents: *price_cents,
                        filled_at: Utc::now(),
                    }])
                }
                Order::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Order::Filled { .. } | Order::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            (Ok(order), OffchainOrderCommand::MarkFailed { error }) => match order {
                Order::Pending { .. } | Order::Submitted { .. } | Order::PartiallyFilled { .. } => {
                    Ok(vec![OffchainOrderEvent::Failed {
                        error: error.clone(),
                        failed_at: Utc::now(),
                    }])
                }
                Order::Filled { .. } | Order::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ExecutionStatus {
    Pending,
    Submitted,
    Filled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum OffchainOrderView {
    Unavailable,
    Execution {
        execution_id: ExecutionId,
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        status: ExecutionStatus,
        broker_order_id: Option<BrokerOrderId>,
        price_cents: Option<PriceCents>,
        initiated_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
    },
}

impl Default for OffchainOrderView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl View<Lifecycle<Order, Never>> for OffchainOrderView {
    fn update(&mut self, event: &EventEnvelope<Lifecycle<Order, Never>>) {
        let Ok(execution_id) = event.aggregate_id.parse::<i64>() else {
            error!(
                aggregate_id = %event.aggregate_id,
                "CRITICAL: OffchainOrder aggregate_id is not a valid execution_id. View will remain Unavailable."
            );
            return;
        };

        let execution_id = ExecutionId(execution_id);

        match &event.payload {
            OffchainOrderEvent::Migrated {
                symbol,
                shares,
                direction,
                broker,
                status,
                broker_order_id,
                price_cents,
                executed_at,
                migrated_at,
            } => {
                let (status, completed_at) = match status {
                    MigratedOrderStatus::Pending => (ExecutionStatus::Pending, None),
                    MigratedOrderStatus::Submitted => (ExecutionStatus::Submitted, None),
                    MigratedOrderStatus::Filled => (ExecutionStatus::Filled, *executed_at),
                    MigratedOrderStatus::Failed { .. } => (ExecutionStatus::Failed, *executed_at),
                };

                *self = Self::Execution {
                    execution_id,
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    status,
                    broker_order_id: broker_order_id.clone(),
                    price_cents: *price_cents,
                    initiated_at: executed_at.unwrap_or(*migrated_at),
                    completed_at,
                };
            }
            OffchainOrderEvent::Placed {
                symbol,
                shares,
                direction,
                broker,
                placed_at,
            } => {
                self.handle_placed(
                    execution_id,
                    symbol.clone(),
                    *shares,
                    *direction,
                    *broker,
                    *placed_at,
                );
            }
            OffchainOrderEvent::Submitted {
                broker_order_id, ..
            } => {
                self.handle_submitted(broker_order_id.clone());
            }
            OffchainOrderEvent::PartiallyFilled { .. } => {
                self.handle_partially_filled();
            }
            OffchainOrderEvent::Filled {
                price_cents,
                filled_at,
            } => {
                self.handle_filled(*price_cents, *filled_at);
            }
            OffchainOrderEvent::Failed { failed_at, .. } => {
                self.handle_failed(*failed_at);
            }
        }
    }
}

impl OffchainOrderView {
    fn handle_placed(
        &mut self,
        execution_id: ExecutionId,
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        placed_at: DateTime<Utc>,
    ) {
        *self = Self::Execution {
            execution_id,
            symbol,
            shares,
            direction,
            broker,
            status: ExecutionStatus::Pending,
            broker_order_id: None,
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };
    }

    fn handle_submitted(&mut self, broker_order_id: BrokerOrderId) {
        let Self::Execution {
            status,
            broker_order_id: broker_order_id_ref,
            ..
        } = self
        else {
            error!("Submitted event received but OffchainOrderView is Unavailable. Event ignored.");
            return;
        };

        *status = ExecutionStatus::Submitted;
        *broker_order_id_ref = Some(broker_order_id);
    }

    fn handle_partially_filled(&mut self) {
        let Self::Execution { status, .. } = self else {
            error!(
                "PartiallyFilled event received but OffchainOrderView is Unavailable. Event ignored."
            );
            return;
        };

        *status = ExecutionStatus::Submitted;
    }

    fn handle_filled(&mut self, price_cents: PriceCents, filled_at: DateTime<Utc>) {
        let Self::Execution {
            status,
            price_cents: price_cents_ref,
            completed_at,
            ..
        } = self
        else {
            error!("Filled event received but OffchainOrderView is Unavailable. Event ignored.");
            return;
        };

        *status = ExecutionStatus::Filled;
        *price_cents_ref = Some(price_cents);
        *completed_at = Some(filled_at);
    }

    fn handle_failed(&mut self, failed_at: DateTime<Utc>) {
        let Self::Execution {
            status,
            completed_at,
            ..
        } = self
        else {
            error!("Failed event received but OffchainOrderView is Unavailable. Event ignored.");
            return;
        };

        *status = ExecutionStatus::Failed;
        *completed_at = Some(failed_at);
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
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
    },
    ConfirmSubmission {
        broker_order_id: BrokerOrderId,
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
pub(crate) enum MigratedOrderStatus {
    Pending,
    Submitted,
    Filled,
    Failed { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OffchainOrderEvent {
    Migrated {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        status: MigratedOrderStatus,
        broker_order_id: Option<BrokerOrderId>,
        price_cents: Option<PriceCents>,
        executed_at: Option<DateTime<Utc>>,
        migrated_at: DateTime<Utc>,
    },
    Placed {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        placed_at: DateTime<Utc>,
    },
    Submitted {
        broker_order_id: BrokerOrderId,
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
            Self::Migrated { .. } => "OffchainOrderEvent::Migrated".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_place_order() {
        let mut order = Lifecycle::<Order, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let command = OffchainOrderCommand::Place {
            symbol: symbol.clone(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Placed { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Pending { .. }));
    }

    #[tokio::test]
    async fn test_cannot_place_when_already_pending() {
        let order = Lifecycle::Live(Order::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(50)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyPlaced)));
    }

    #[tokio::test]
    async fn test_cannot_place_when_filled() {
        let order = Lifecycle::Live(Order::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(50)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyPlaced)));
    }

    #[tokio::test]
    async fn test_cannot_place_when_failed() {
        let order = Lifecycle::Live(Order::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            error: "Market closed".to_string(),
            placed_at: Utc::now(),
            failed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(50)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyPlaced)));
    }

    #[tokio::test]
    async fn test_confirm_submission_after_place() {
        let mut order = Lifecycle::Live(Order::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD123".to_string()),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Submitted { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_cannot_confirm_submission_if_not_placed() {
        let order = Lifecycle::<Order, Never>::default();

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD123".to_string()),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(
            result,
            Err(OffchainOrderError::State(LifecycleError::Uninitialized))
        ));
    }

    #[tokio::test]
    async fn test_cannot_submit_twice() {
        let order = Lifecycle::Live(Order::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD456".to_string()),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadySubmitted)));
    }

    #[tokio::test]
    async fn test_partial_fill_from_submitted() {
        let mut order = Lifecycle::Live(Order::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: FractionalShares(dec!(50)),
            avg_price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            OffchainOrderEvent::PartiallyFilled { .. }
        ));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::PartiallyFilled { .. }));
    }

    #[tokio::test]
    async fn test_partial_fill_updates_from_partially_filled() {
        let mut order = Lifecycle::Live(Order::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            shares_filled: FractionalShares(dec!(50)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: FractionalShares(dec!(75)),
            avg_price_cents: PriceCents(15050),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(Order::PartiallyFilled { shares_filled, .. }) = order else {
            panic!("Expected Live PartiallyFilled state");
        };

        assert_eq!(shares_filled, FractionalShares(dec!(75)));
    }

    #[tokio::test]
    async fn test_complete_fill_from_submitted() {
        let mut order = Lifecycle::Live(Order::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Filled { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Filled { .. }));
    }

    #[tokio::test]
    async fn test_complete_fill_from_partially_filled() {
        let mut order = Lifecycle::Live(Order::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            shares_filled: FractionalShares(dec!(75)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15025),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Filled { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fill_if_not_submitted() {
        let order = Lifecycle::Live(Order::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::NotSubmitted)));
    }

    #[tokio::test]
    async fn test_cannot_fill_already_filled() {
        let order = Lifecycle::Live(Order::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyCompleted)));
    }

    #[tokio::test]
    async fn test_mark_failed_from_pending() {
        let mut order = Lifecycle::Live(Order::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Market closed".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Failed { .. }));

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_submitted() {
        let mut order = Lifecycle::Live(Order::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Insufficient funds".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_partially_filled() {
        let mut order = Lifecycle::Live(Order::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            shares_filled: FractionalShares(dec!(50)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Order cancelled".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Failed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_filled() {
        let order = Lifecycle::Live(Order::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        });

        let command = OffchainOrderCommand::MarkFailed {
            error: "Test error".to_string(),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyCompleted)));
    }

    #[tokio::test]
    async fn test_migrated_event_pending_status() {
        let mut order = Lifecycle::<Order, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Pending,
            broker_order_id: None,
            price_cents: None,
            executed_at: None,
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Pending { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_submitted_status() {
        let mut order = Lifecycle::<Order, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD123".to_string())),
            price_cents: None,
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_filled_status() {
        let mut order = Lifecycle::<Order, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Filled,
            broker_order_id: Some(BrokerOrderId("ORD123".to_string())),
            price_cents: Some(PriceCents(15000)),
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(inner) = order else {
            panic!("Expected Live state");
        };
        assert!(matches!(inner, Order::Filled { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_failed_status() {
        let mut order = Lifecycle::<Order, Never>::default();

        let event = OffchainOrderEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Failed {
                error: "Insufficient funds".to_string(),
            },
            broker_order_id: None,
            price_cents: None,
            executed_at: Some(Utc::now()),
            migrated_at: Utc::now(),
        };

        order.apply(event);

        let Lifecycle::Live(Order::Failed { error, .. }) = order else {
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
            shares: FractionalShares(dec!(100.5)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Pending,
            broker_order_id: None,
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
            broker,
            status,
            broker_order_id,
            price_cents,
            initiated_at,
            completed_at,
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(view_symbol, symbol);
        assert_eq!(shares, FractionalShares(dec!(100.5)));
        assert_eq!(direction, Direction::Buy);
        assert_eq!(broker, SupportedBroker::Schwab);
        assert_eq!(status, ExecutionStatus::Pending);
        assert_eq!(broker_order_id, None);
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
            shares: FractionalShares(dec!(50.0)),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
            status: MigratedOrderStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD123".to_string())),
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
            broker_order_id,
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
        assert_eq!(broker_order_id, Some(BrokerOrderId("ORD123".to_string())));
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
            shares: FractionalShares(dec!(25.75)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Filled,
            broker_order_id: Some(BrokerOrderId("ORD456".to_string())),
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
            broker_order_id,
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
        assert_eq!(broker_order_id, Some(BrokerOrderId("ORD456".to_string())));
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
            shares: FractionalShares(dec!(10.0)),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
            status: MigratedOrderStatus::Failed {
                error: "Insufficient funds".to_string(),
            },
            broker_order_id: None,
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
            broker_order_id,
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
        assert_eq!(broker_order_id, None);
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
            shares: FractionalShares(dec!(75.25)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
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
            broker,
            status,
            broker_order_id,
            price_cents,
            initiated_at,
            completed_at,
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(view_execution_id, execution_id);
        assert_eq!(view_symbol, symbol);
        assert_eq!(shares, FractionalShares(dec!(75.25)));
        assert_eq!(direction, Direction::Buy);
        assert_eq!(broker, SupportedBroker::Schwab);
        assert_eq!(status, ExecutionStatus::Pending);
        assert_eq!(broker_order_id, None);
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
            shares: FractionalShares(dec!(50.0)),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
            status: ExecutionStatus::Pending,
            broker_order_id: None,
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::Submitted {
            broker_order_id: BrokerOrderId("ORD789".to_string()),
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
            broker_order_id,
            ..
        } = view
        else {
            panic!("Expected Execution variant");
        };

        assert_eq!(status, ExecutionStatus::Submitted);
        assert_eq!(broker_order_id, Some(BrokerOrderId("ORD789".to_string())));
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
            shares: FractionalShares(dec!(100.0)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: ExecutionStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD999".to_string())),
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::PartiallyFilled {
            shares_filled: FractionalShares(dec!(60.0)),
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
            shares: FractionalShares(dec!(30.0)),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
            status: ExecutionStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD111".to_string())),
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
            shares: FractionalShares(dec!(200.0)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: ExecutionStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD222".to_string())),
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
            broker_order_id: BrokerOrderId("ORD333".to_string()),
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
            shares_filled: FractionalShares(dec!(50.0)),
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
            shares: FractionalShares(dec!(100.0)),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: MigratedOrderStatus::Pending,
            broker_order_id: None,
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
            shares: FractionalShares(dec!(50.0)),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
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
        let mut order = Lifecycle::<Order, Never>::default();

        let event = OffchainOrderEvent::Submitted {
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            submitted_at: Utc::now(),
        };

        order.apply(event);

        assert!(matches!(order, Lifecycle::Failed { .. }));
    }
}
