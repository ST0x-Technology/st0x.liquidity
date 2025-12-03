use cqrs_es::{EventEnvelope, View};

use crate::state::State;

use super::Position;
use super::event::ArithmeticError;

impl View<Self> for State<Position, ArithmeticError> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, Position::apply_transition)
            .or_initialize(&event.payload, Position::from_event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::TxHash;
    use chrono::Utc;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use st0x_broker::{Direction, SupportedBroker, Symbol};
    use std::collections::HashMap;
    use std::str::FromStr;

    use super::super::event::PositionEvent;
    use super::super::event::{
        BrokerOrderId, ExecutionId, ExecutionThreshold, FractionalShares, PriceCents, TradeId,
        TriggerReason, Usdc,
    };

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: PositionEvent,
    ) -> EventEnvelope<State<Position, ArithmeticError>> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn initialized_creates_active_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();

        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            initialized_at,
        };

        let mut view = State::<Position, ArithmeticError>::default();
        assert!(matches!(view, State::Uninitialized));

        view.update(&make_envelope(&symbol.to_string(), 1, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(initialized_at));
    }

    #[test]
    fn onchain_buy_increases_net_and_accumulated_long() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let seen_at = Utc::now();

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares::ZERO,
            accumulated_long: FractionalShares::ZERO,
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

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

        view.update(&make_envelope(&symbol.to_string(), 2, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(10.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(10.5)));
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.last_updated, Some(seen_at));
    }

    #[test]
    fn onchain_sell_decreases_net_and_increases_accumulated_short() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let seen_at = Utc::now();

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(20.0)),
            accumulated_long: FractionalShares(dec!(20.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

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

        view.update(&make_envelope(&symbol.to_string(), 3, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(14.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(20.0)));
        assert_eq!(position.accumulated_short, FractionalShares(dec!(5.5)));
        assert_eq!(position.last_updated, Some(seen_at));
    }

    #[test]
    fn offchain_placed_sets_pending_execution() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let placed_at = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

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

        view.update(&make_envelope(&symbol.to_string(), 4, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.pending_execution_id, Some(execution_id));
        assert_eq!(position.last_updated, Some(placed_at));
    }

    #[test]
    fn offchain_filled_sell_reduces_net_and_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(100)),
            direction: Direction::Sell,
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15025),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 5, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_filled_buy_increases_net() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let broker_timestamp = Utc::now();
        let execution_id = ExecutionId(43);

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(50.0)),
            accumulated_long: FractionalShares(dec!(50.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFilled {
            execution_id,
            shares_filled: FractionalShares(dec!(25)),
            direction: Direction::Buy,
            broker_order_id: BrokerOrderId("ORD456".to_string()),
            price_cents: PriceCents(14500),
            broker_timestamp,
        };

        view.update(&make_envelope(&symbol.to_string(), 6, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(75.0)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(broker_timestamp));
    }

    #[test]
    fn offchain_failed_clears_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let failed_at = Utc::now();
        let execution_id = ExecutionId(42);

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: Some(execution_id),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::OffChainOrderFailed {
            execution_id,
            error: "Market closed".to_string(),
            failed_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 7, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.net, FractionalShares(dec!(100.0)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(failed_at));
    }

    #[test]
    fn threshold_updated_changes_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();
        let updated_at = Utc::now();

        let mut view = State::Active(Position {
            symbol: symbol.clone(),
            net: FractionalShares(dec!(100.0)),
            accumulated_long: FractionalShares(dec!(100.0)),
            accumulated_short: FractionalShares::ZERO,
            pending_execution_id: None,
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            last_updated: Some(initialized_at),
        });

        let event = PositionEvent::ThresholdUpdated {
            old_threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            new_threshold: ExecutionThreshold::dollar_value(Usdc(dec!(10000))).unwrap(),
            updated_at,
        };

        view.update(&make_envelope(&symbol.to_string(), 8, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.last_updated, Some(updated_at));
    }

    #[test]
    fn migrated_creates_active_state_with_values() {
        let symbol = Symbol::new("AAPL").unwrap();
        let migrated_at = Utc::now();

        let event = PositionEvent::Migrated {
            symbol: symbol.clone(),
            net_position: FractionalShares(dec!(150.5)),
            accumulated_long: FractionalShares(dec!(200.0)),
            accumulated_short: FractionalShares(dec!(49.5)),
            threshold: ExecutionThreshold::shares(FractionalShares(dec!(100))).unwrap(),
            migrated_at,
        };

        let mut view = State::<Position, ArithmeticError>::default();
        assert!(matches!(view, State::Uninitialized));

        view.update(&make_envelope(&symbol.to_string(), 1, event));

        let State::Active(position) = view else {
            panic!("Expected Active state");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares(dec!(150.5)));
        assert_eq!(position.accumulated_long, FractionalShares(dec!(200.0)));
        assert_eq!(position.accumulated_short, FractionalShares(dec!(49.5)));
        assert_eq!(position.pending_execution_id, None);
        assert_eq!(position.last_updated, Some(migrated_at));
    }

    #[test]
    fn transition_on_uninitialized_corrupts_state() {
        let mut view = State::<Position, ArithmeticError>::default();

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
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        };

        view.update(&make_envelope("AAPL", 1, event));

        assert!(matches!(view, State::Corrupted { .. }));
    }
}
