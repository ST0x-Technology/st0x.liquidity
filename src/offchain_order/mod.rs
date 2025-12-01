use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};
use tracing::error;

mod cmd;
mod event;
mod view;

pub(crate) use cmd::OffchainOrderCommand;
pub(crate) use event::{MigratedOrderStatus, OffchainOrderEvent};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExecutionId(pub(crate) i64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BrokerOrderId(pub(crate) String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PriceCents(pub(crate) u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Usdc(Decimal);

impl Usdc {
    pub(crate) fn new(value: Decimal) -> Result<Self, InvalidThresholdError> {
        if value.is_sign_negative() {
            return Err(InvalidThresholdError::Negative(value));
        }

        if value.is_zero() {
            return Err(InvalidThresholdError::Zero);
        }

        Ok(Self(value))
    }

    #[cfg(test)]
    fn as_decimal(&self) -> Decimal {
        self.0
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum InvalidThresholdError {
    #[error("Threshold value cannot be negative: {0}")]
    Negative(Decimal),

    #[error("Threshold value cannot be zero")]
    Zero,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OffchainOrderError {
    #[error("Cannot confirm submission: order has not been placed")]
    NotPlaced,

    #[error("Cannot update order: order has already been completed (filled or failed)")]
    AlreadyCompleted,

    #[error("Cannot submit order: order has already been submitted")]
    AlreadySubmitted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum OffchainOrder {
    NotPlaced,
    Pending {
        symbol: Symbol,
        shares: Decimal,
        direction: Direction,
        broker: SupportedBroker,
        placed_at: DateTime<Utc>,
    },
    Submitted {
        symbol: Symbol,
        shares: Decimal,
        direction: Direction,
        broker: SupportedBroker,
        broker_order_id: BrokerOrderId,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        symbol: Symbol,
        shares: Decimal,
        shares_filled: Decimal,
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
        shares: Decimal,
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
        shares: Decimal,
        direction: Direction,
        broker: SupportedBroker,
        error: String,
        placed_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for OffchainOrder {
    fn default() -> Self {
        Self::NotPlaced
    }
}

#[async_trait]
impl Aggregate for OffchainOrder {
    type Command = OffchainOrderCommand;
    type Event = OffchainOrderEvent;
    type Error = OffchainOrderError;
    type Services = ();

    fn aggregate_type() -> String {
        "OffchainOrder".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OffchainOrderCommand::Place {
                symbol,
                shares,
                direction,
                broker,
            } => {
                let now = Utc::now();

                Ok(vec![OffchainOrderEvent::Placed {
                    symbol,
                    shares,
                    direction,
                    broker,
                    placed_at: now,
                }])
            }
            OffchainOrderCommand::ConfirmSubmission { broker_order_id } => match self {
                Self::NotPlaced => Err(OffchainOrderError::NotPlaced),
                Self::Pending { .. } => {
                    let now = Utc::now();

                    Ok(vec![OffchainOrderEvent::Submitted {
                        broker_order_id,
                        submitted_at: now,
                    }])
                }
                Self::Submitted { .. }
                | Self::PartiallyFilled { .. }
                | Self::Filled { .. }
                | Self::Failed { .. } => Err(OffchainOrderError::AlreadySubmitted),
            },
            OffchainOrderCommand::UpdatePartialFill {
                shares_filled,
                avg_price_cents,
            } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    let now = Utc::now();

                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price_cents,
                        partially_filled_at: now,
                    }])
                }
                Self::NotPlaced | Self::Pending { .. } => Err(OffchainOrderError::NotPlaced),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
            OffchainOrderCommand::CompleteFill { price_cents } => match self {
                Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    let now = Utc::now();

                    Ok(vec![OffchainOrderEvent::Filled {
                        price_cents,
                        filled_at: now,
                    }])
                }
                Self::NotPlaced | Self::Pending { .. } => Err(OffchainOrderError::NotPlaced),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
            OffchainOrderCommand::MarkFailed { error } => match self {
                Self::Pending { .. } | Self::Submitted { .. } | Self::PartiallyFilled { .. } => {
                    let now = Utc::now();

                    Ok(vec![OffchainOrderEvent::Failed {
                        error,
                        failed_at: now,
                    }])
                }
                Self::NotPlaced => Err(OffchainOrderError::NotPlaced),
                Self::Filled { .. } | Self::Failed { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
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
                MigratedOrderStatus::Pending => {
                    *self = Self::Pending {
                        symbol,
                        shares: shares.0,
                        direction,
                        broker,
                        placed_at: executed_at.unwrap_or(migrated_at),
                    };
                }
                MigratedOrderStatus::Submitted => {
                    *self = Self::Submitted {
                        symbol,
                        shares: shares.0,
                        direction,
                        broker,
                        broker_order_id: broker_order_id
                            .unwrap_or_else(|| BrokerOrderId("unknown".to_string())),
                        placed_at: migrated_at,
                        submitted_at: executed_at.unwrap_or(migrated_at),
                    };
                }
                MigratedOrderStatus::Filled => {
                    *self = Self::Filled {
                        symbol,
                        shares: shares.0,
                        direction,
                        broker,
                        broker_order_id: broker_order_id
                            .unwrap_or_else(|| BrokerOrderId("unknown".to_string())),
                        price_cents: price_cents.unwrap_or(PriceCents(0)),
                        placed_at: migrated_at,
                        submitted_at: migrated_at,
                        filled_at: executed_at.unwrap_or(migrated_at),
                    };
                }
                MigratedOrderStatus::Failed { error } => {
                    *self = Self::Failed {
                        symbol,
                        shares: shares.0,
                        direction,
                        broker,
                        error,
                        placed_at: migrated_at,
                        failed_at: executed_at.unwrap_or(migrated_at),
                    };
                }
            },
            OffchainOrderEvent::Placed {
                symbol,
                shares,
                direction,
                broker,
                placed_at,
            } => {
                *self = Self::Pending {
                    symbol,
                    shares,
                    direction,
                    broker,
                    placed_at,
                };
            }
            OffchainOrderEvent::Submitted {
                broker_order_id,
                submitted_at,
            } => {
                self.apply_submitted(broker_order_id, submitted_at);
            }
            OffchainOrderEvent::PartiallyFilled {
                shares_filled,
                avg_price_cents,
                partially_filled_at,
            } => {
                self.apply_partially_filled(shares_filled, avg_price_cents, partially_filled_at);
            }
            OffchainOrderEvent::Filled {
                price_cents,
                filled_at,
            } => {
                self.apply_filled(price_cents, filled_at);
            }
            OffchainOrderEvent::Failed { error, failed_at } => {
                self.apply_failed(error, failed_at);
            }
        }
    }
}

impl OffchainOrder {
    fn apply_submitted(&mut self, broker_order_id: BrokerOrderId, submitted_at: DateTime<Utc>) {
        if let Self::Pending {
            symbol,
            shares,
            direction,
            broker,
            placed_at,
        } = self
        {
            *self = Self::Submitted {
                symbol: symbol.clone(),
                shares: *shares,
                direction: *direction,
                broker: *broker,
                broker_order_id,
                placed_at: *placed_at,
                submitted_at,
            };
        } else {
            error!(
                current_state = ?self,
                "Submitted event applied to non-Pending state. Event ignored."
            );
        }
    }

    fn apply_partially_filled(
        &mut self,
        shares_filled: Decimal,
        avg_price_cents: PriceCents,
        partially_filled_at: DateTime<Utc>,
    ) {
        match self {
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
            } => {
                *self = Self::PartiallyFilled {
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
                };
            }
            Self::NotPlaced | Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => {
                error!(
                    current_state = ?self,
                    "PartiallyFilled event applied to invalid state. Event ignored."
                );
            }
        }
    }

    fn apply_filled(&mut self, price_cents: PriceCents, filled_at: DateTime<Utc>) {
        match self {
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
            } => {
                *self = Self::Filled {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    broker_order_id: broker_order_id.clone(),
                    price_cents,
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    filled_at,
                };
            }
            Self::NotPlaced | Self::Pending { .. } | Self::Filled { .. } | Self::Failed { .. } => {
                error!(
                    current_state = ?self,
                    "Filled event applied to invalid state. Event ignored."
                );
            }
        }
    }

    fn apply_failed(&mut self, error: String, failed_at: DateTime<Utc>) {
        match self {
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
            } => {
                *self = Self::Failed {
                    symbol: symbol.clone(),
                    shares: *shares,
                    direction: *direction,
                    broker: *broker,
                    error,
                    placed_at: *placed_at,
                    failed_at,
                };
            }
            Self::NotPlaced | Self::Filled { .. } | Self::Failed { .. } => {
                error!(
                    current_state = ?self,
                    "Failed event applied to invalid state. Event ignored."
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use st0x_broker::{Direction, SupportedBroker, Symbol};

    use crate::position::FractionalShares;

    #[tokio::test]
    async fn test_place_order() {
        let mut order = OffchainOrder::default();
        let symbol = Symbol::new("AAPL").unwrap();

        let command = OffchainOrderCommand::Place {
            symbol: symbol.clone(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Placed { .. }));

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Pending { .. }));
    }

    #[tokio::test]
    async fn test_confirm_submission_after_place() {
        let mut order = OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        };

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD123".to_string()),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Submitted { .. }));

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_cannot_confirm_submission_if_not_placed() {
        let order = OffchainOrder::default();

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD123".to_string()),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::NotPlaced)));
    }

    #[tokio::test]
    async fn test_cannot_submit_twice() {
        let order = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        let command = OffchainOrderCommand::ConfirmSubmission {
            broker_order_id: BrokerOrderId("ORD456".to_string()),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadySubmitted)));
    }

    #[tokio::test]
    async fn test_partial_fill_from_submitted() {
        let mut order = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: dec!(50),
            avg_price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            OffchainOrderEvent::PartiallyFilled { .. }
        ));

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::PartiallyFilled { .. }));
    }

    #[tokio::test]
    async fn test_partial_fill_updates_from_partially_filled() {
        let mut order = OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            shares_filled: dec!(50),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        };

        let command = OffchainOrderCommand::UpdatePartialFill {
            shares_filled: dec!(75),
            avg_price_cents: PriceCents(15050),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        if let OffchainOrder::PartiallyFilled { shares_filled, .. } = order {
            assert_eq!(shares_filled, dec!(75));
        } else {
            panic!("Expected PartiallyFilled state");
        }
    }

    #[tokio::test]
    async fn test_complete_fill_from_submitted() {
        let mut order = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Filled { .. }));

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_complete_fill_from_partially_filled() {
        let mut order = OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            shares_filled: dec!(75),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        };

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15025),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fill_if_not_submitted() {
        let order = OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        };

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::NotPlaced)));
    }

    #[tokio::test]
    async fn test_cannot_fill_already_filled() {
        let order = OffchainOrder::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        };

        let command = OffchainOrderCommand::CompleteFill {
            price_cents: PriceCents(15000),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyCompleted)));
    }

    #[tokio::test]
    async fn test_mark_failed_from_pending() {
        let mut order = OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            placed_at: Utc::now(),
        };

        let command = OffchainOrderCommand::MarkFailed {
            error: "Market closed".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OffchainOrderEvent::Failed { .. }));

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_submitted() {
        let mut order = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        let command = OffchainOrderCommand::MarkFailed {
            error: "Insufficient funds".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_mark_failed_from_partially_filled() {
        let mut order = OffchainOrder::PartiallyFilled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            shares_filled: dec!(50),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            avg_price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            partially_filled_at: Utc::now(),
        };

        let command = OffchainOrderCommand::MarkFailed {
            error: "Order cancelled".to_string(),
        };

        let events = order.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);

        order.apply(events[0].clone());

        assert!(matches!(order, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_filled() {
        let order = OffchainOrder::Filled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: dec!(100),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        };

        let command = OffchainOrderCommand::MarkFailed {
            error: "Test error".to_string(),
        };

        let result = order.handle(command, &()).await;

        assert!(matches!(result, Err(OffchainOrderError::AlreadyCompleted)));
    }

    #[tokio::test]
    async fn test_migrated_event_pending_status() {
        let mut order = OffchainOrder::default();

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

        assert!(matches!(order, OffchainOrder::Pending { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_submitted_status() {
        let mut order = OffchainOrder::default();

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

        assert!(matches!(order, OffchainOrder::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_filled_status() {
        let mut order = OffchainOrder::default();

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

        assert!(matches!(order, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_failed_status() {
        let mut order = OffchainOrder::default();

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

        if let OffchainOrder::Failed { error, .. } = order {
            assert_eq!(error, "Insufficient funds");
        } else {
            panic!("Expected Failed state");
        }
    }

    #[test]
    fn test_usdc_new_positive_value_succeeds() {
        let value = dec!(100.50);
        let usdc = Usdc::new(value).unwrap();
        assert_eq!(usdc.as_decimal(), value);
    }

    #[test]
    fn test_usdc_new_zero_fails() {
        let result = Usdc::new(Decimal::ZERO);
        assert_eq!(result.unwrap_err(), InvalidThresholdError::Zero);
    }

    #[test]
    fn test_usdc_new_negative_value_fails() {
        let negative = dec!(-50.25);
        let result = Usdc::new(negative);
        assert_eq!(
            result.unwrap_err(),
            InvalidThresholdError::Negative(negative)
        );
    }
}
