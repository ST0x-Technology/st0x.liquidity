use rust_decimal::Decimal;

use crate::onchain::OnchainTrade;
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand};

use super::{DualWriteContext, DualWriteError};

pub(crate) async fn witness_trade(
    context: &DualWriteContext,
    trade: &OnchainTrade,
    block_number: u64,
) -> Result<(), DualWriteError> {
    let aggregate_id = OnChainTrade::aggregate_id(trade.tx_hash, trade.log_index);

    let symbol = trade.symbol.base().clone();
    let amount = Decimal::try_from(trade.amount)?;
    let price_usdc = Decimal::try_from(trade.price.value())?;

    let block_timestamp =
        trade
            .block_timestamp
            .ok_or_else(|| DualWriteError::MissingBlockTimestamp {
                tx_hash: trade.tx_hash,
                log_index: trade.log_index,
            })?;

    let command = OnChainTradeCommand::Witness {
        symbol,
        amount,
        direction: trade.direction,
        price_usdc,
        block_number,
        block_timestamp,
    };

    context
        .onchain_trade_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use chrono::Utc;
    use st0x_broker::Direction;

    use super::*;
    use crate::onchain::io::{TokenizedEquitySymbol, Usdc};
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_witness_trade_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1234567890123456789012345678901234567890123456789012345678901234"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price: Usdc::new(150.25).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = witness_trade(&context, &trade, 12345).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let expected_id = OnChainTrade::aggregate_id(trade.tx_hash, 0);
        assert_eq!(aggregate_id, expected_id);

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "OnChainTradeEvent::Filled");
    }

    #[tokio::test]
    async fn test_witness_trade_sequence_increments() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let tx_hash =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let trade1 = OnchainTrade {
            id: None,
            tx_hash,
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price: Usdc::new(150.25).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        witness_trade(&context, &trade1, 12345).await.unwrap();

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 1);

        let sequence = sqlx::query_scalar!(
            "SELECT sequence FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(sequence, 1);
    }

    #[tokio::test]
    async fn test_witness_trade_missing_block_timestamp() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price: Usdc::new(150.25).unwrap(),
            block_timestamp: None,
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = witness_trade(&context, &trade, 12345).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::MissingBlockTimestamp { .. }
        ));
    }
}
