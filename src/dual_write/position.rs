use rust_decimal::Decimal;
use st0x_broker::{OrderState, SupportedBroker, Symbol};

use crate::offchain::execution::OffchainExecution;
use crate::onchain::OnchainTrade;
use crate::position::{
    BrokerOrderId, ExecutionId, FractionalShares, Position, PositionCommand, PriceCents, TradeId,
};

use super::{DualWriteContext, DualWriteError};

pub(crate) async fn acknowledge_onchain_fill(
    context: &DualWriteContext,
    trade: &OnchainTrade,
) -> Result<(), DualWriteError> {
    let aggregate_id = Position::aggregate_id(&trade.symbol.base().clone());

    let trade_id = TradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };
    let amount = FractionalShares(Decimal::try_from(trade.amount)?);
    let price_usdc = Decimal::try_from(trade.price_usdc)?;

    let block_timestamp =
        trade
            .block_timestamp
            .ok_or_else(|| DualWriteError::MissingBlockTimestamp {
                tx_hash: trade.tx_hash,
                log_index: trade.log_index,
            })?;

    let command = PositionCommand::AcknowledgeOnChainFill {
        trade_id,
        amount,
        direction: trade.direction,
        price_usdc,
        block_timestamp,
    };

    context
        .position_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

pub(crate) async fn place_offchain_order(
    context: &DualWriteContext,
    execution: &OffchainExecution,
    symbol: &Symbol,
) -> Result<(), DualWriteError> {
    let aggregate_id = Position::aggregate_id(symbol);

    let execution_id = ExecutionId(
        execution
            .id
            .ok_or_else(|| DualWriteError::MissingExecutionId)?,
    );
    let shares = FractionalShares(Decimal::from(execution.shares.value()));
    let direction = execution.direction;
    let broker = execution.broker;

    let command = PositionCommand::PlaceOffChainOrder {
        execution_id,
        shares,
        direction,
        broker,
    };

    context
        .position_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

pub(crate) async fn complete_offchain_order(
    context: &DualWriteContext,
    execution: &OffchainExecution,
    symbol: &Symbol,
) -> Result<(), DualWriteError> {
    let aggregate_id = Position::aggregate_id(symbol);

    let execution_id = ExecutionId(
        execution
            .id
            .ok_or_else(|| DualWriteError::MissingExecutionId)?,
    );
    let shares_filled = FractionalShares(Decimal::from(execution.shares.value()));
    let direction = execution.direction;

    let (broker_order_id, price_cents, broker_timestamp) = match &execution.state {
        OrderState::Filled {
            order_id,
            price_cents,
            executed_at,
        } => (
            BrokerOrderId(order_id.clone()),
            PriceCents(*price_cents),
            *executed_at,
        ),
        _ => {
            return Err(DualWriteError::InvalidOrderState {
                execution_id: execution_id.0,
                expected: "Filled".to_string(),
            });
        }
    };

    let command = PositionCommand::CompleteOffChainOrder {
        execution_id,
        shares_filled,
        direction,
        broker_order_id,
        price_cents,
        broker_timestamp,
    };

    context
        .position_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

pub(crate) async fn fail_offchain_order(
    context: &DualWriteContext,
    execution_id: i64,
    symbol: &Symbol,
    error: String,
) -> Result<(), DualWriteError> {
    let aggregate_id = Position::aggregate_id(symbol);

    let command = PositionCommand::FailOffChainOrder {
        execution_id: ExecutionId(execution_id),
        error,
    };

    context
        .position_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use chrono::Utc;
    use st0x_broker::Direction;

    use crate::onchain::io::TokenizedEquitySymbol;
    use crate::test_utils::setup_test_db;

    use super::*;

    #[tokio::test]
    async fn test_acknowledge_onchain_fill_success() {
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
            price_usdc: 150.25,
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = acknowledge_onchain_fill(&context, &trade).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 1);

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "PositionEvent::OnChainOrderFilled");
    }

    #[tokio::test]
    async fn test_acknowledge_onchain_fill_missing_block_timestamp() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price_usdc: 150.25,
            block_timestamp: None,
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = acknowledge_onchain_fill(&context, &trade).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::MissingBlockTimestamp { .. }
        ));
    }

    #[tokio::test]
    async fn test_place_offchain_order_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: st0x_broker::Shares::new(10).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price_usdc: 150.25,
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        let result = place_offchain_order(&context, &execution, &symbol).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 2);
    }

    #[tokio::test]
    async fn test_complete_offchain_order_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: st0x_broker::Shares::new(10).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Filled {
                order_id: "12345".to_string(),
                price_cents: 15025,
                executed_at: Utc::now(),
            },
        };

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price_usdc: 150.25,
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        place_offchain_order(&context, &execution, &symbol)
            .await
            .unwrap();

        let result = complete_offchain_order(&context, &execution, &symbol).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL' ORDER BY sequence DESC LIMIT 1"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "PositionEvent::OffChainOrderFilled");
    }

    #[tokio::test]
    async fn test_fail_offchain_order_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: st0x_broker::Shares::new(10).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.5,
            direction: Direction::Buy,
            price_usdc: 150.25,
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        place_offchain_order(&context, &execution, &symbol)
            .await
            .unwrap();

        let result =
            fail_offchain_order(&context, 1, &symbol, "Broker API timeout".to_string()).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL' ORDER BY sequence DESC LIMIT 1"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "PositionEvent::OffChainOrderFailed");
    }

    #[tokio::test]
    async fn test_complete_offchain_order_invalid_state() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: st0x_broker::Shares::new(10).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let result = complete_offchain_order(&context, &execution, &symbol).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::InvalidOrderState { .. }
        ));
    }

    #[tokio::test]
    async fn test_place_offchain_order_missing_execution_id() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: st0x_broker::Shares::new(10).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let result = place_offchain_order(&context, &execution, &symbol).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::MissingExecutionId
        ));
    }
}
