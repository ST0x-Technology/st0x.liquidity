use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{AggregateContext, EventStore};
use rust_decimal::Decimal;
use sqlite_es::SqliteEventRepository;
use st0x_execution::{OrderState, Symbol};

use crate::lifecycle::Lifecycle;
use crate::offchain::execution::OffchainExecution;
use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
use crate::onchain::OnchainTrade;
use crate::position::{Position, PositionCommand, TradeId};
use crate::shares::{ArithmeticError, FractionalShares};
use crate::threshold::ExecutionThreshold;

use super::{DualWriteContext, DualWriteError};

pub(crate) async fn initialize_position(
    context: &DualWriteContext,
    symbol: &Symbol,
    threshold: ExecutionThreshold,
) -> Result<(), DualWriteError> {
    let aggregate_id = Position::aggregate_id(symbol);

    let command = PositionCommand::Initialize {
        symbol: symbol.clone(),
        threshold,
    };

    context
        .position_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

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
    let price_usdc = Decimal::try_from(trade.price.value())?;

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
    let shares = FractionalShares(execution.shares.value());
    let direction = execution.direction;
    let executor = execution.executor;

    let command = PositionCommand::PlaceOffChainOrder {
        execution_id,
        shares,
        direction,
        executor,
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
    let shares_filled = FractionalShares(execution.shares.value());
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

/// Loads the current state of a Position aggregate from the event store.
///
/// Returns `Ok(Some(position))` if the position exists and is in Live state.
/// Returns `Ok(None)` if no position exists for this symbol (no events).
/// Returns `Err` if the aggregate is in a failed state or there's a database error.
pub(crate) async fn load_position(
    context: &DualWriteContext,
    symbol: &Symbol,
) -> Result<Option<Position>, DualWriteError> {
    let aggregate_id = Position::aggregate_id(symbol);

    let repo = SqliteEventRepository::new(context.pool().clone());
    let store = PersistedEventStore::<
        SqliteEventRepository,
        Lifecycle<Position, ArithmeticError<FractionalShares>>,
    >::new_event_store(repo);

    let aggregate_context = store.load_aggregate(&aggregate_id).await?;
    let aggregate = aggregate_context.aggregate();

    match aggregate {
        Lifecycle::Live(position) => Ok(Some(position.clone())),
        Lifecycle::Uninitialized => Ok(None),
        Lifecycle::Failed { error, .. } => Err(DualWriteError::PositionAggregateFailed {
            aggregate_id,
            error: error.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use st0x_execution::{Direction, FractionalShares as ExecutionShares, SupportedExecutor};

    use super::*;
    use crate::onchain::io::{TokenizedEquitySymbol, Usdc};
    use crate::shares::{FractionalShares, HasZero};
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_acknowledge_onchain_fill_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

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

        let result = acknowledge_onchain_fill(&context, &trade).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 2);

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events
            WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'
            ORDER BY sequence DESC LIMIT 1"
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
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: ExecutionShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
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

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        let result = place_offchain_order(&context, &execution, &symbol).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 3);
    }

    #[tokio::test]
    async fn test_complete_offchain_order_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: ExecutionShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
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

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        place_offchain_order(&context, &execution, &symbol)
            .await
            .unwrap();

        let result = complete_offchain_order(&context, &execution, &symbol).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events
            WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'
            ORDER BY sequence DESC LIMIT 1"
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
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: ExecutionShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
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

        acknowledge_onchain_fill(&context, &trade).await.unwrap();

        place_offchain_order(&context, &execution, &symbol)
            .await
            .unwrap();

        let result =
            fail_offchain_order(&context, 1, &symbol, "Broker API timeout".to_string()).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events
            WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL'
            ORDER BY sequence DESC LIMIT 1"
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
            shares: ExecutionShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
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
            shares: ExecutionShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let result = place_offchain_order(&context, &execution, &symbol).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::MissingExecutionId
        ));
    }

    #[tokio::test]
    async fn test_load_position_returns_none_for_uninitialized() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        let result = load_position(&context, &symbol).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_position_returns_some_for_initialized() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let result = load_position(&context, &symbol).await.unwrap();

        assert!(result.is_some());
        let position = result.unwrap();
        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.threshold, ExecutionThreshold::whole_share());
    }

    #[tokio::test]
    async fn test_load_position_reflects_accumulated_fills() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        initialize_position(&context, &symbol, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ),
            log_index: 0,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 2.5,
            direction: Direction::Buy,
            price: Usdc::new(150.0).unwrap(),
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

        let position = load_position(&context, &symbol).await.unwrap().unwrap();

        assert_eq!(position.net, FractionalShares(Decimal::new(25, 1)));
        assert_eq!(
            position.accumulated_long,
            FractionalShares(Decimal::new(25, 1))
        );
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.last_price_usdc, Some(Decimal::from(150)));
    }

    #[tokio::test]
    async fn test_load_position_different_symbols_isolated() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();

        initialize_position(&context, &aapl, ExecutionThreshold::whole_share())
            .await
            .unwrap();

        let aapl_position = load_position(&context, &aapl).await.unwrap();
        let msft_position = load_position(&context, &msft).await.unwrap();

        assert!(aapl_position.is_some());
        assert!(msft_position.is_none());
    }
}
