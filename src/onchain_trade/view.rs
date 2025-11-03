use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use st0x_broker::{Direction, Symbol};

use super::{OnChainTrade, OnChainTradeEvent, PythPrice};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainTradeViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum OnChainTradeView {
    Unavailable,
    Trade {
        tx_hash: String,
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
    fn handle_filled(&mut self, event: &OnChainTradeEvent, tx_hash: String, log_index: u64) {
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
        if let Self::Trade {
            gas_used: gas_ref,
            pyth_price: pyth_ref,
            ..
        } = self
        {
            *gas_ref = Some(gas_used);
            *pyth_ref = Box::new(Some(pyth_price));
        }
    }
}

impl View<OnChainTrade> for OnChainTradeView {
    fn update(&mut self, event: &EventEnvelope<OnChainTrade>) {
        let parts: Vec<&str> = event.aggregate_id.split(':').collect();
        let (tx_hash, log_index) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1].parse::<u64>().unwrap_or(0))
        } else {
            (event.aggregate_id.clone(), 0)
        };

        match &event.payload {
            OnChainTradeEvent::Filled { .. } => {
                self.handle_filled(&event.payload, tx_hash, log_index);
            }
            OnChainTradeEvent::Enriched {
                gas_used,
                pyth_price,
                enriched_at: _,
            } => {
                self.handle_enriched(*gas_used, pyth_price.clone());
            }
            OnChainTradeEvent::Genesis {
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
                    tx_hash,
                    log_index,
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

pub(crate) async fn find_by_tx_hash_and_log_index(
    pool: &Pool<Sqlite>,
    tx_hash: &str,
    log_index: u64,
) -> Result<Option<OnChainTradeView>, OnChainTradeViewError> {
    let view_id = format!("{tx_hash}:{log_index}");
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM onchain_trade_view
        WHERE view_id = ?
        "#,
        view_id
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: OnChainTradeView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

pub(crate) async fn find_by_symbol(
    pool: &Pool<Sqlite>,
    symbol: &str,
) -> Result<Vec<OnChainTradeView>, OnChainTradeViewError> {
    let symbol_pattern = format!("%\"symbol\":\"{symbol}\"%");
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM onchain_trade_view
        WHERE payload LIKE ?
        ORDER BY view_id
        "#,
        symbol_pattern
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| serde_json::from_str(&row.payload).map_err(OnChainTradeViewError::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;

    async fn setup_test_db() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    #[test]
    fn test_view_update_from_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let filled_at = chrono::Utc::now();

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
            aggregate_id: "0xabc:5".to_string(),
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

        assert_eq!(tx_hash, "0xabc");
        assert_eq!(log_index, 5);
        assert_eq!(view_symbol, symbol);
        assert_eq!(view_amount, amount);
        assert_eq!(view_direction, direction);
        assert_eq!(view_price, price_usdc);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_block_timestamp, block_timestamp);
        assert_eq!(gas_used, None);
        assert_eq!(pyth_price, Box::new(None));
        assert_eq!(recorded_at, filled_at);
    }

    #[test]
    fn test_view_update_from_enriched_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let block_timestamp = chrono::Utc::now();
        let filled_at = chrono::Utc::now();

        let mut view = OnChainTradeView::Trade {
            tx_hash: "0xdef".to_string(),
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
            aggregate_id: "0xdef:3".to_string(),
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
        assert_eq!(view_pyth_price, Box::new(Some(pyth_price)));
        assert_eq!(view_recorded_at, filled_at);
    }

    #[test]
    fn test_view_update_from_genesis_event_with_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let migrated_at = chrono::Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: chrono::Utc::now(),
        };

        let event = OnChainTradeEvent::Genesis {
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
            aggregate_id: "0xghi:1".to_string(),
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

        assert_eq!(tx_hash, "0xghi");
        assert_eq!(log_index, 1);
        assert_eq!(view_symbol, symbol);
        assert_eq!(gas_used, Some(50000));
        assert_eq!(view_pyth_price, Box::new(Some(pyth_price)));
    }

    #[test]
    fn test_view_update_from_genesis_event_without_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = dec!(10.5);
        let direction = Direction::Buy;
        let price_usdc = dec!(150.25);
        let block_number = 12345;
        let block_timestamp = chrono::Utc::now();
        let migrated_at = chrono::Utc::now();

        let event = OnChainTradeEvent::Genesis {
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

        let envelope = EventEnvelope {
            aggregate_id: "0xjkl:2".to_string(),
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
        assert_eq!(pyth_price, Box::new(None));
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

        let envelope = EventEnvelope {
            aggregate_id: "0xmno:4".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, OnChainTradeView::Unavailable));
    }

    #[tokio::test]
    async fn test_find_by_tx_hash_and_log_index_returns_view() {
        let pool = setup_test_db().await;

        let symbol = Symbol::new("TSLA").unwrap();
        let view = OnChainTradeView::Trade {
            tx_hash: "0xfind123".to_string(),
            log_index: 7,
            symbol: symbol.clone(),
            amount: dec!(50.0),
            direction: Direction::Sell,
            price_usdc: dec!(200.50),
            block_number: 54321,
            block_timestamp: chrono::Utc::now(),
            gas_used: Some(60000),
            pyth_price: Box::new(None),
            recorded_at: chrono::Utc::now(),
        };

        let view_id = "0xfind123:7";
        let payload = serde_json::to_string(&view).expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO onchain_trade_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            view_id,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let result = find_by_tx_hash_and_log_index(&pool, "0xfind123", 7)
            .await
            .expect("Query should succeed");

        assert!(result.is_some());

        let OnChainTradeView::Trade {
            tx_hash,
            log_index,
            symbol: found_symbol,
            ..
        } = result.unwrap()
        else {
            panic!("Expected Trade variant");
        };

        assert_eq!(tx_hash, "0xfind123");
        assert_eq!(log_index, 7);
        assert_eq!(found_symbol, symbol);
    }

    #[tokio::test]
    async fn test_find_by_tx_hash_and_log_index_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let result = find_by_tx_hash_and_log_index(&pool, "0xnonexistent", 999)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_find_by_symbol_returns_multiple_views() {
        let pool = setup_test_db().await;

        let symbol = Symbol::new("AAPL").unwrap();

        let view1 = OnChainTradeView::Trade {
            tx_hash: "0xsym1".to_string(),
            log_index: 1,
            symbol: symbol.clone(),
            amount: dec!(10.0),
            direction: Direction::Buy,
            price_usdc: dec!(150.0),
            block_number: 1000,
            block_timestamp: chrono::Utc::now(),
            gas_used: None,
            pyth_price: Box::new(None),
            recorded_at: chrono::Utc::now(),
        };

        let view2 = OnChainTradeView::Trade {
            tx_hash: "0xsym2".to_string(),
            log_index: 2,
            symbol: symbol.clone(),
            amount: dec!(20.0),
            direction: Direction::Sell,
            price_usdc: dec!(151.0),
            block_number: 1001,
            block_timestamp: chrono::Utc::now(),
            gas_used: Some(50000),
            pyth_price: Box::new(None),
            recorded_at: chrono::Utc::now(),
        };

        for (view_id, view) in [("0xsym1:1", &view1), ("0xsym2:2", &view2)] {
            let payload = serde_json::to_string(view).expect("Failed to serialize view");
            sqlx::query!(
                r"
                INSERT INTO onchain_trade_view (view_id, version, payload)
                VALUES (?, 1, ?)
                ",
                view_id,
                payload
            )
            .execute(&pool)
            .await
            .expect("Failed to insert view");
        }

        let results = find_by_symbol(&pool, "AAPL")
            .await
            .expect("Query should succeed");

        assert_eq!(results.len(), 2);

        let expected_symbol = Symbol::new("AAPL").unwrap();
        assert!(results.iter().all(|v| matches!(
            v,
            OnChainTradeView::Trade { symbol, .. } if symbol == &expected_symbol
        )));
    }
}
