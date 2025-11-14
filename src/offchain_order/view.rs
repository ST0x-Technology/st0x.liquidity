use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};
use tracing::error;

use super::{
    BrokerOrderId, ExecutionId, MigratedOrderStatus, OffchainOrder, OffchainOrderEvent, PriceCents,
};

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
        shares: Decimal,
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

impl View<OffchainOrder> for OffchainOrderView {
    fn update(&mut self, event: &EventEnvelope<OffchainOrder>) {
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
                    shares: shares.0,
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
        shares: Decimal,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    use super::super::{MigratedOrderStatus, OffchainOrderEvent};
    use crate::position::FractionalShares;

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
        assert_eq!(shares, dec!(100.5));
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
            shares: dec!(75.25),
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
        assert_eq!(shares, dec!(75.25));
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
            shares: dec!(50.0),
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
            shares: dec!(100.0),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            status: ExecutionStatus::Submitted,
            broker_order_id: Some(BrokerOrderId("ORD999".to_string())),
            price_cents: None,
            initiated_at: placed_at,
            completed_at: None,
        };

        let event = OffchainOrderEvent::PartiallyFilled {
            shares_filled: dec!(60.0),
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
            shares: dec!(30.0),
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
            shares: dec!(200.0),
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
            shares_filled: dec!(50.0),
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
            shares: dec!(50.0),
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
}
