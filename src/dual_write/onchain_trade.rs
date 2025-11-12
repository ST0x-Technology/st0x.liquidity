use chrono::Utc;
use cqrs_es::{Aggregate, DomainEvent};
use tracing::error;

use crate::dual_write::{DualWriteContext, DualWriteError};
use crate::onchain::trade::OnchainTrade as LegacyTrade;
use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};

pub(crate) async fn emit_trade_filled(
    trade: &LegacyTrade,
    context: &DualWriteContext,
) -> Result<(), DualWriteError> {
    let log_index_i64 = i64::try_from(trade.log_index)?;
    let aggregate_id = OnChainTrade::aggregate_id(trade.tx_hash, log_index_i64);

    let symbol = trade.symbol.base().clone();

    let amount = rust_decimal::Decimal::try_from(trade.amount)?;

    let price_usdc = rust_decimal::Decimal::try_from(trade.price_usdc)?;

    let block_timestamp = trade.block_timestamp.unwrap_or_else(Utc::now);

    let event = OnChainTradeEvent::Filled {
        symbol,
        amount,
        direction: trade.direction,
        price_usdc,
        block_number: 0,
        block_timestamp,
        filled_at: Utc::now(),
    };

    persist_event(context.pool(), &aggregate_id, event).await
}

async fn persist_event(
    pool: &sqlx::SqlitePool,
    aggregate_id: &str,
    event: OnChainTradeEvent,
) -> Result<(), DualWriteError> {
    let aggregate_type = OnChainTrade::aggregate_type();
    let event_type = event.event_type();
    let event_version = event.event_version();
    let payload = serde_json::to_string(&event)?;

    let sequence = get_next_sequence(pool, &aggregate_type, aggregate_id).await?;

    sqlx::query(
        "INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (?, ?, ?, ?, ?, ?, '{}')",
    )
    .bind(&aggregate_type)
    .bind(aggregate_id)
    .bind(sequence)
    .bind(&event_type)
    .bind(&event_version)
    .bind(&payload)
    .execute(pool)
    .await?;

    Ok(())
}

async fn get_next_sequence(
    pool: &sqlx::SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
) -> Result<i64, DualWriteError> {
    let max_sequence: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(sequence) FROM events WHERE aggregate_type = ? AND aggregate_id = ?",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_optional(pool)
    .await?
    .flatten();

    Ok(max_sequence.map_or(1, |s| s + 1))
}

pub(crate) fn log_event_error(trade: &LegacyTrade, error: &DualWriteError) {
    error!(
        tx_hash = %trade.tx_hash,
        log_index = trade.log_index,
        symbol = %trade.symbol,
        error = %error,
        "Failed to emit OnChainTradeEvent::Filled"
    );
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use st0x_broker::Direction;

    use super::*;
    use crate::onchain::io::TokenizedEquitySymbol;

    async fn create_test_pool() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn create_test_trade() -> LegacyTrade {
        LegacyTrade {
            id: None,
            tx_hash: b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
            log_index: 0,
            symbol: TokenizedEquitySymbol::parse("AAPL0x").unwrap(),
            amount: 10.0,
            direction: Direction::Buy,
            price_usdc: 150.50,
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        }
    }

    #[tokio::test]
    async fn test_emit_trade_filled_creates_event() {
        let pool = create_test_pool().await;
        let context = DualWriteContext::new(pool.clone());
        let trade = create_test_trade();

        emit_trade_filled(&trade, &context).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_emit_trade_filled_correct_aggregate_id() {
        let pool = create_test_pool().await;
        let context = DualWriteContext::new(pool.clone());
        let trade = create_test_trade();

        emit_trade_filled(&trade, &context).await.unwrap();

        let aggregate_id: String = sqlx::query_scalar(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let expected_id =
            OnChainTrade::aggregate_id(trade.tx_hash, i64::try_from(trade.log_index).unwrap());
        assert_eq!(aggregate_id, expected_id);
    }

    #[tokio::test]
    async fn test_emit_trade_filled_increments_sequence() {
        let pool = create_test_pool().await;
        let context = DualWriteContext::new(pool.clone());
        let mut trade = create_test_trade();

        emit_trade_filled(&trade, &context).await.unwrap();

        trade.log_index = 1;
        let aggregate_id =
            OnChainTrade::aggregate_id(trade.tx_hash, i64::try_from(trade.log_index).unwrap());

        emit_trade_filled(&trade, &context).await.unwrap();

        let sequence: i64 = sqlx::query_scalar(
            "SELECT sequence FROM events WHERE aggregate_type = 'OnChainTrade' AND aggregate_id = ?",
        )
        .bind(&aggregate_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(sequence, 1);
    }

    #[tokio::test]
    async fn test_emit_trade_filled_event_payload() {
        let pool = create_test_pool().await;
        let context = DualWriteContext::new(pool.clone());
        let trade = create_test_trade();

        emit_trade_filled(&trade, &context).await.unwrap();

        let payload: String =
            sqlx::query_scalar("SELECT payload FROM events WHERE aggregate_type = 'OnChainTrade'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let event: OnChainTradeEvent = serde_json::from_str(&payload).unwrap();

        match event {
            OnChainTradeEvent::Filled {
                symbol,
                amount,
                direction,
                ..
            } => {
                assert_eq!(symbol.to_string(), "AAPL");
                assert_eq!(amount, rust_decimal::Decimal::try_from(10.0).unwrap());
                assert_eq!(direction, Direction::Buy);
            }
            _ => panic!("Expected Filled event"),
        }
    }
}
