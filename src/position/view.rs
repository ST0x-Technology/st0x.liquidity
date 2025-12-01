use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, Symbol};
use tracing::error;

use super::event::{ExecutionId, FractionalShares};
use super::{Position, PositionEvent};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum PositionView {
    Unavailable,
    Position {
        symbol: Symbol,
        net: FractionalShares,
        accumulated_long: FractionalShares,
        accumulated_short: FractionalShares,
        pending_execution_id: Option<ExecutionId>,
        last_updated: DateTime<Utc>,
    },
}

impl Default for PositionView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl View<Position> for PositionView {
    fn update(&mut self, event: &EventEnvelope<Position>) {
        match &event.payload {
            PositionEvent::Initialized { initialized_at, .. } => {
                self.handle_initialized(&event.aggregate_id, *initialized_at);
            }
            PositionEvent::OnChainOrderFilled {
                direction,
                amount,
                seen_at,
                ..
            } => {
                self.handle_onchain_filled(*direction, *amount, *seen_at);
            }
            PositionEvent::OffChainOrderPlaced {
                execution_id,
                placed_at,
                ..
            } => {
                self.handle_offchain_placed(*execution_id, *placed_at);
            }
            PositionEvent::OffChainOrderFilled {
                shares_filled,
                direction,
                broker_timestamp,
                ..
            } => {
                self.handle_offchain_filled(*shares_filled, *direction, *broker_timestamp);
            }
            PositionEvent::OffChainOrderFailed { failed_at, .. } => {
                self.handle_offchain_failed(*failed_at);
            }
            PositionEvent::ThresholdUpdated { updated_at, .. } => {
                self.handle_threshold_updated(*updated_at);
            }
            PositionEvent::Migrated {
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                migrated_at,
                ..
            } => {
                self.handle_migrated(
                    symbol.clone(),
                    *net_position,
                    *accumulated_long,
                    *accumulated_short,
                    *migrated_at,
                );
            }
        }
    }
}

impl PositionView {
    fn handle_initialized(&mut self, aggregate_id: &str, initialized_at: DateTime<Utc>) {
        let Ok(symbol) = Symbol::new(aggregate_id) else {
            error!(
                aggregate_id = %aggregate_id,
                "CRITICAL: Position aggregate_id is not a valid symbol. View will remain Unavailable."
            );
            return;
        };

        *self = Self::Position {
            symbol,
            net: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            last_updated: initialized_at,
        };
    }

    fn handle_onchain_filled(
        &mut self,
        direction: Direction,
        amount: FractionalShares,
        seen_at: DateTime<Utc>,
    ) {
        let Self::Position {
            net,
            accumulated_long,
            accumulated_short,
            last_updated,
            ..
        } = self
        else {
            error!(
                "OnChainOrderFilled event received but PositionView is Unavailable. Event ignored."
            );
            return;
        };

        *net = match direction {
            Direction::Buy => *net + amount,
            Direction::Sell => *net - amount,
        };

        match direction {
            Direction::Buy => *accumulated_long += amount,
            Direction::Sell => *accumulated_short += amount,
        }

        *last_updated = seen_at;
    }

    fn handle_offchain_placed(&mut self, execution_id: ExecutionId, placed_at: DateTime<Utc>) {
        let Self::Position {
            pending_execution_id,
            last_updated,
            ..
        } = self
        else {
            error!(
                "OffChainOrderPlaced event received but PositionView is Unavailable. Event ignored."
            );
            return;
        };

        *pending_execution_id = Some(execution_id);
        *last_updated = placed_at;
    }

    fn handle_offchain_filled(
        &mut self,
        shares_filled: FractionalShares,
        direction: Direction,
        broker_timestamp: DateTime<Utc>,
    ) {
        let Self::Position {
            net,
            pending_execution_id,
            last_updated,
            ..
        } = self
        else {
            error!(
                "OffChainOrderFilled event received but PositionView is Unavailable. Event ignored."
            );
            return;
        };

        *net = match direction {
            Direction::Sell => *net - shares_filled,
            Direction::Buy => *net + shares_filled,
        };
        *pending_execution_id = None;
        *last_updated = broker_timestamp;
    }

    fn handle_offchain_failed(&mut self, failed_at: DateTime<Utc>) {
        let Self::Position {
            pending_execution_id,
            last_updated,
            ..
        } = self
        else {
            error!(
                "OffChainOrderFailed event received but PositionView is Unavailable. Event ignored."
            );
            return;
        };

        *pending_execution_id = None;
        *last_updated = failed_at;
    }

    fn handle_threshold_updated(&mut self, updated_at: DateTime<Utc>) {
        let Self::Position { last_updated, .. } = self else {
            error!(
                "ThresholdUpdated event received but PositionView is Unavailable. Event ignored."
            );
            return;
        };

        *last_updated = updated_at;
    }

    fn handle_migrated(
        &mut self,
        symbol: Symbol,
        net: FractionalShares,
        accumulated_long: FractionalShares,
        accumulated_short: FractionalShares,
        migrated_at: DateTime<Utc>,
    ) {
        *self = Self::Position {
            symbol,
            net,
            accumulated_long,
            accumulated_short,
            pending_execution_id: None,
            last_updated: migrated_at,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::TxHash;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use st0x_broker::{Direction, SupportedBroker};
    use std::collections::HashMap;
    use std::str::FromStr;

    use super::super::ExecutionThreshold;
    use super::super::event::{
        BrokerOrderId, ExecutionId, PriceCents, TradeId, TriggerReason, Usdc,
    };

    #[test]
    fn test_view_update_from_initialized_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();

        let event = PositionEvent::Initialized {
            initialized_at,
            threshold: ExecutionThreshold::Shares(FractionalShares::new(dec!(100)).unwrap()),
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = PositionView::default();

        assert!(matches!(view, PositionView::Unavailable));

        view.update(&envelope);

        let PositionView::Position {
            symbol: view_symbol,
            net,
            accumulated_long,
            accumulated_short,
            pending_execution_id,
            last_updated,
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(view_symbol, symbol);
        assert_eq!(net, FractionalShares::ZERO);
        assert_eq!(accumulated_long, FractionalShares::ZERO);
        assert_eq!(accumulated_short, FractionalShares::ZERO);
        assert_eq!(pending_execution_id, None);
        assert_eq!(last_updated, initialized_at);
    }

    #[test]
    fn test_view_update_from_onchain_filled_buy_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let seen_at = chrono::Utc::now();

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            last_updated: initialized_at,
        };

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x1234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares(dec!(10.5)),
            price_usdc: dec!(150.25),
            block_timestamp: seen_at,
            seen_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            net,
            accumulated_long,
            accumulated_short,
            last_updated,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(net, FractionalShares(dec!(10.5)));
        assert_eq!(accumulated_long, FractionalShares(dec!(10.5)));
        assert_eq!(accumulated_short, FractionalShares::ZERO);
        assert_eq!(last_updated, seen_at);
    }

    #[test]
    fn test_view_update_from_onchain_filled_sell_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let seen_at = chrono::Utc::now();

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(20.0)),
            accumulated_long: FractionalShares(dec!(20.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            last_updated: initialized_at,
        };

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x2234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 1,
            },
            direction: Direction::Sell,
            amount: FractionalShares(dec!(5.5)),
            price_usdc: dec!(149.75),
            block_timestamp: seen_at,
            seen_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 3,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            net,
            accumulated_long,
            accumulated_short,
            last_updated,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(net, FractionalShares(dec!(14.5)));
        assert_eq!(accumulated_long, FractionalShares(dec!(20.0)));
        assert_eq!(accumulated_short, FractionalShares(dec!(5.5)));
        assert_eq!(last_updated, seen_at);
    }

    #[test]
    fn test_view_update_from_offchain_placed_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let placed_at = chrono::Utc::now();

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            last_updated: initialized_at,
        };

        let execution_id = ExecutionId(42);

        let event = PositionEvent::OffChainOrderPlaced {
            execution_id,
            shares: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: dec!(100),
                threshold_shares: dec!(100),
            },
            placed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 4,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            pending_execution_id,
            last_updated,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(pending_execution_id, Some(execution_id));
        assert_eq!(last_updated, placed_at);
    }

    #[test]
    fn test_view_update_from_offchain_filled_sell_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let broker_timestamp = chrono::Utc::now();

        let execution_id = ExecutionId(42);

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            last_updated: initialized_at,
        };

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15025),
            broker_timestamp,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 5,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            net,
            pending_execution_id: pending,
            last_updated,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(net, FractionalShares::ZERO);
        assert_eq!(pending, None);
        assert_eq!(last_updated, broker_timestamp);
    }

    #[test]
    fn test_view_update_from_offchain_filled_buy_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let broker_timestamp = chrono::Utc::now();

        let execution_id = ExecutionId(43);

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(50.0)),
            accumulated_long: FractionalShares(dec!(50.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            last_updated: initialized_at,
        };

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(25)),
            direction: Direction::Buy,
            broker_order_id: BrokerOrderId("ORD456".to_string()),
            price_cents: PriceCents(14500),
            broker_timestamp,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 6,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            net,
            pending_execution_id: pending,
            last_updated,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(net, FractionalShares(dec!(75.0)));
        assert_eq!(pending, None);
        assert_eq!(last_updated, broker_timestamp);
    }

    #[test]
    fn test_view_update_from_offchain_failed_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let failed_at = chrono::Utc::now();

        let execution_id = ExecutionId(42);

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            last_updated: initialized_at,
        };

        let event = PositionEvent::OffChainOrderFailed {
            execution_id,
            error: "Market closed".to_string(),
            failed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 7,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position {
            pending_execution_id: pending,
            last_updated,
            net,
            ..
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(net, FractionalShares(dec!(100.0)));
        assert_eq!(pending, None);
        assert_eq!(last_updated, failed_at);
    }

    #[test]
    fn test_view_update_from_threshold_updated_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = chrono::Utc::now();
        let updated_at = chrono::Utc::now();

        let mut view = PositionView::Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            last_updated: initialized_at,
        };

        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::Shares(FractionalShares::new(dec!(100)).unwrap()),
            new_threshold: ExecutionThreshold::DollarValue(Usdc::new(dec!(10000)).unwrap()),
            updated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 8,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let PositionView::Position { last_updated, .. } = view else {
            panic!("Expected Position variant");
        };

        assert_eq!(last_updated, updated_at);
    }

    #[test]
    fn test_view_update_from_migrated_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let migrated_at = chrono::Utc::now();

        let event = PositionEvent::Migrated {
            symbol: symbol.clone(),
            net_position: FractionalShares(dec!(150.5)),
            accumulated_long: FractionalShares(dec!(200.0)),
            accumulated_short: FractionalShares(dec!(49.5)),
            threshold: ExecutionThreshold::Shares(FractionalShares::new(dec!(100)).unwrap()),
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = PositionView::default();

        assert!(matches!(view, PositionView::Unavailable));

        view.update(&envelope);

        let PositionView::Position {
            symbol: view_symbol,
            net,
            accumulated_long,
            accumulated_short,
            pending_execution_id,
            last_updated,
        } = view
        else {
            panic!("Expected Position variant");
        };

        assert_eq!(view_symbol, symbol);
        assert_eq!(net, FractionalShares(dec!(150.5)));
        assert_eq!(accumulated_long, FractionalShares(dec!(200.0)));
        assert_eq!(accumulated_short, FractionalShares(dec!(49.5)));
        assert_eq!(pending_execution_id, None);
        assert_eq!(last_updated, migrated_at);
    }

    #[test]
    fn test_onchain_filled_on_unavailable_does_not_change_state() {
        let mut view = PositionView::Unavailable;

        let seen_at = chrono::Utc::now();

        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::from_str(
                    "0x3234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap(),
                log_index: 0,
            },
            direction: Direction::Buy,
            amount: FractionalShares(dec!(10)),
            price_usdc: dec!(150.00),
            block_timestamp: seen_at,
            seen_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "AAPL".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }

    #[test]
    fn test_offchain_placed_on_unavailable_does_not_change_state() {
        let mut view = PositionView::Unavailable;

        let event = PositionEvent::OffChainOrderPlaced {
            execution_id: ExecutionId(42),
            shares: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: dec!(100),
                threshold_shares: dec!(100),
            },
            placed_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "AAPL".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }

    #[test]
    fn test_offchain_filled_on_unavailable_does_not_change_state() {
        let mut view = PositionView::Unavailable;

        let event = PositionEvent::OffChainOrderFilled {
            execution_id: ExecutionId(42),
            shares_filled: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15025),
            broker_timestamp: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "AAPL".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }

    #[test]
    fn test_offchain_failed_on_unavailable_does_not_change_state() {
        let mut view = PositionView::Unavailable;

        let event = PositionEvent::OffChainOrderFailed {
            execution_id: ExecutionId(42),
            error: "Market closed".to_string(),
            failed_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "AAPL".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }

    #[test]
    fn test_threshold_updated_on_unavailable_does_not_change_state() {
        let mut view = PositionView::Unavailable;

        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::Shares(FractionalShares::new(dec!(100)).unwrap()),
            new_threshold: ExecutionThreshold::DollarValue(Usdc::new(dec!(10000)).unwrap()),
            updated_at: chrono::Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "AAPL".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }

    #[test]
    fn test_initialized_with_invalid_symbol_remains_unavailable() {
        let mut view = PositionView::default();

        let event = PositionEvent::Initialized {
            initialized_at: chrono::Utc::now(),
            threshold: ExecutionThreshold::Shares(FractionalShares::new(dec!(100)).unwrap()),
        };

        let envelope = EventEnvelope {
            aggregate_id: String::new(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, PositionView::Unavailable));
    }
}
