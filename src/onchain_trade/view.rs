use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::state::{Never, State};

use super::{OnChainTrade, OnChainTradeEvent, PythPrice, TradeAggregateId};
use st0x_broker::{Direction, Symbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainTradeViewError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("serde error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum OnChainTradeView {
    Unavailable,
    Trade {
        tx_hash: TxHash,
        log_index: u64,
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        gas_used: Option<u64>,
        pyth_price: Box<Option<PythPrice>>,
        recorded_at: DateTime<Utc>,
    },
}

impl Default for OnChainTradeView {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl OnChainTradeView {
    fn handle_filled(&mut self, event: &OnChainTradeEvent, tx_hash: TxHash, log_index: u64) {
        let OnChainTradeEvent::Filled {
            symbol,
            amount,
            direction,
            price_usdc,
            block_number,
            block_timestamp,
            filled_at,
        } = event
        else {
            return;
        };

        *self = Self::Trade {
            tx_hash,
            log_index,
            symbol: symbol.clone(),
            amount: *amount,
            direction: *direction,
            price_usdc: *price_usdc,
            block_number: *block_number,
            block_timestamp: *block_timestamp,
            gas_used: None,
            pyth_price: Box::new(None),
            recorded_at: *filled_at,
        };
    }

    fn handle_enriched(&mut self, gas_used: u64, pyth_price: PythPrice) {
        let Self::Trade {
            gas_used: gas_ref,
            pyth_price: pyth_ref,
            ..
        } = self
        else {
            error!("Enriched event received but OnChainTradeView is Unavailable. Event ignored.");
            return;
        };

        *gas_ref = Some(gas_used);
        *pyth_ref = Box::new(Some(pyth_price));
    }
}

impl View<State<OnChainTrade, Never>> for OnChainTradeView {
    fn update(&mut self, event: &EventEnvelope<State<OnChainTrade, Never>>) {
        let Ok(aggregate_id) = event.aggregate_id.parse::<TradeAggregateId>() else {
            error!(
                aggregate_id = %event.aggregate_id,
                "Failed to parse aggregate_id, cannot update view"
            );
            return;
        };

        match &event.payload {
            OnChainTradeEvent::Filled { .. } => {
                self.handle_filled(&event.payload, aggregate_id.tx_hash, aggregate_id.log_index);
            }
            OnChainTradeEvent::Enriched {
                gas_used,
                pyth_price,
                enriched_at: _,
            } => {
                self.handle_enriched(*gas_used, pyth_price.clone());
            }
            OnChainTradeEvent::Migrated {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                gas_used,
                pyth_price,
                migrated_at,
            } => {
                *self = Self::Trade {
                    tx_hash: aggregate_id.tx_hash,
                    log_index: aggregate_id.log_index,
                    symbol: symbol.clone(),
                    amount: *amount,
                    direction: *direction,
                    price_usdc: *price_usdc,
                    block_number: *block_number,
                    block_timestamp: *block_timestamp,
                    gas_used: *gas_used,
                    pyth_price: Box::new(pyth_price.clone()),
                    recorded_at: *migrated_at,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::fixed_bytes;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    #[test]
    fn test_view_update_from_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let filled_at = chrono::Utc::now();
        let expected_tx_hash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let event = OnChainTradeEvent::Filled {
            symbol: symbol.clone(),
            amount,
            direction,
            price_usdc,
            block_number,
            block_timestamp,
            filled_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: format!("{expected_tx_hash}:5"),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OnChainTradeView::default();

        assert!(matches!(view, OnChainTradeView::Unavailable));

        view.update(&envelope);

        let OnChainTradeView::Trade {
            tx_hash,
            log_index,
            symbol: view_symbol,
            amount: view_amount,
            direction: view_direction,
            price_usdc: view_price,
            block_number: view_block_number,
            block_timestamp: view_block_timestamp,
            gas_used,
            pyth_price,
            recorded_at,
        } = view
        else {
            panic!("Expected Trade variant");
        };

        assert_eq!(tx_hash, expected_tx_hash);
        assert_eq!(log_index, 5);
        assert_eq!(view_symbol, symbol);
        assert_eq!(view_amount, amount);
        assert_eq!(view_direction, direction);
        assert_eq!(view_price, price_usdc);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_block_timestamp, block_timestamp);
        assert_eq!(gas_used, None);
        assert_eq!(*pyth_price, None);
        assert_eq!(recorded_at, filled_at);
    }

    #[test]
    fn test_view_update_from_enriched_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let block_timestamp = chrono::Utc::now();
        let filled_at = chrono::Utc::now();
        let tx_hash =
            fixed_bytes!("0x0def123456789012def0123456789012def0123456789012def0123456789012");

        let mut view = OnChainTradeView::Trade {
            tx_hash,
            log_index: 3,
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp,
            gas_used: None,
            pyth_price: Box::new(None),
            recorded_at: filled_at,
        };

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: chrono::Utc::now(),
        };

        let enriched_at = chrono::Utc::now();

        let event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price: pyth_price.clone(),
            enriched_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: format!("{tx_hash}:3"),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let OnChainTradeView::Trade {
            gas_used: view_gas_used,
            pyth_price: view_pyth_price,
            recorded_at: view_recorded_at,
            ..
        } = view
        else {
            panic!("Expected Trade variant");
        };

        assert_eq!(view_gas_used, Some(50000));
        assert_eq!(*view_pyth_price, Some(pyth_price));
        assert_eq!(view_recorded_at, filled_at);
    }

    #[test]
    fn test_view_update_from_migrated_event_with_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let migrated_at = chrono::Utc::now();
        let expected_tx_hash =
            fixed_bytes!("0x09a1234567890123091234567890123091234567890123091234567890123091");

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: chrono::Utc::now(),
        };

        let event = OnChainTradeEvent::Migrated {
            symbol: symbol.clone(),
            amount,
            direction,
            price_usdc,
            block_number,
            block_timestamp,
            gas_used: Some(50000),
            pyth_price: Some(pyth_price.clone()),
            migrated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: format!("{expected_tx_hash}:1"),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OnChainTradeView::default();

        view.update(&envelope);

        let OnChainTradeView::Trade {
            tx_hash,
            log_index,
            symbol: view_symbol,
            gas_used,
            pyth_price: view_pyth_price,
            ..
        } = view
        else {
            panic!("Expected Trade variant");
        };

        assert_eq!(tx_hash, expected_tx_hash);
        assert_eq!(log_index, 1);
        assert_eq!(view_symbol, symbol);
        assert_eq!(gas_used, Some(50000));
        assert_eq!(*view_pyth_price, Some(pyth_price));
    }

    #[test]
    fn test_view_update_from_migrated_event_without_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let migrated_at = chrono::Utc::now();

        let event = OnChainTradeEvent::Migrated {
            symbol: symbol.clone(),
            amount,
            direction,
            price_usdc,
            block_number,
            block_timestamp,
            gas_used: None,
            pyth_price: None,
            migrated_at,
        };

        let tx_hash =
            fixed_bytes!("0x0bcd123456789012bcdbcd123456789012bcdbcd123456789012bcd1234567ab");

        let envelope = EventEnvelope {
            aggregate_id: format!("{tx_hash}:2"),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = OnChainTradeView::default();

        view.update(&envelope);

        let OnChainTradeView::Trade {
            symbol: view_symbol,
            gas_used,
            pyth_price,
            ..
        } = view
        else {
            panic!("Expected Trade variant");
        };

        assert_eq!(view_symbol, symbol);
        assert_eq!(gas_used, None);
        assert_eq!(*pyth_price, None);
    }

    #[test]
    fn test_enriched_event_on_unavailable_does_not_change_state() {
        let mut view = OnChainTradeView::Unavailable;

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: chrono::Utc::now(),
        };

        let event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price,
            enriched_at: chrono::Utc::now(),
        };

        let tx_hash =
            fixed_bytes!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let envelope = EventEnvelope {
            aggregate_id: format!("{tx_hash}:4"),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OnChainTradeView::Unavailable));
    }
}
