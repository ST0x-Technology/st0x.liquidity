use tracing::{debug, info};

use st0x_execution::{Direction, Executor, FractionalShares, Positive, SupportedExecutor, Symbol};

use st0x_event_sorcery::Projection;

use crate::onchain::OnChainError;
use crate::position::Position;
use crate::threshold::ExecutionThreshold;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionCtx {
    pub(crate) symbol: Symbol,
    pub(crate) direction: Direction,
    pub(crate) shares: Positive<FractionalShares>,
    pub(crate) executor: SupportedExecutor,
}

/// Checks whether a position is ready for offchain execution.
///
/// Loads the position from the CQRS view and checks if the net exposure
/// exceeds the configured threshold. Also verifies the market is open.
/// The Position aggregate already tracks pending executions —
/// `is_ready_for_execution` returns `None` if one is already in flight.
pub(crate) async fn check_execution_readiness<E: Executor>(
    executor: &E,
    position_projection: &Projection<Position>,
    symbol: &Symbol,
    executor_type: SupportedExecutor,
) -> Result<Option<ExecutionCtx>, OnChainError> {
    let Some(position) = position_projection.load(symbol).await? else {
        debug!(symbol = %symbol, "Position aggregate not found, skipping");
        return Ok(None);
    };

    let Some((direction, shares)) = position.is_ready_for_execution(executor_type)? else {
        debug!(
            symbol = %symbol,
            net = %position.net,
            "Position not ready for execution"
        );
        return Ok(None);
    };

    if !check_market_open(executor, symbol).await? {
        return Ok(None);
    }

    let shares = Positive::new(shares)?;

    info!(
        symbol = %symbol,
        shares = %shares,
        direction = ?direction,
        "Position ready for execution"
    );

    Ok(Some(ExecutionCtx {
        symbol: symbol.clone(),
        direction,
        shares,
        executor: executor_type,
    }))
}

async fn check_market_open<E: Executor>(
    executor: &E,
    symbol: &Symbol,
) -> Result<bool, OnChainError> {
    let is_open = executor
        .is_market_open()
        .await
        .map_err(|e| OnChainError::MarketHoursCheck(Box::new(e)))?;

    if !is_open {
        debug!(symbol = %symbol, "Market closed, deferring execution");
    }

    Ok(is_open)
}

/// Checks all positions for execution readiness.
///
/// Loads all active positions from the view, then checks each
/// against its configured threshold. Returns execution parameters for
/// positions that are ready.
#[tracing::instrument(
    skip(executor, position_projection),
    fields(executor_type = %executor_type),
    level = tracing::Level::DEBUG
)]
pub(crate) async fn check_all_positions<E: Executor>(
    executor: &E,
    position_projection: &Projection<Position>,
    executor_type: SupportedExecutor,
) -> Result<Vec<ExecutionCtx>, OnChainError> {
    let all_positions = position_projection.load_all().await?;

    let mut ready = Vec::new();

    for (symbol, position) in &all_positions {
        if let Some((direction, shares)) = position.is_ready_for_execution(executor_type)? {
            if !check_market_open(executor, symbol).await? {
                continue;
            }

            let shares = Positive::new(shares)?;

            info!(
                symbol = %symbol,
                shares = %shares,
                direction = ?direction,
                "Position ready for execution"
            );

            ready.push(ExecutionCtx {
                symbol: symbol.clone(),
                direction,
                shares,
                executor: executor_type,
            });
        }
    }

    if ready.is_empty() {
        debug!("No positions ready for execution");
    } else {
        info!("Found {} positions ready for execution", ready.len());
    }

    Ok(ready)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use rust_decimal_macros::dec;

    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};

    use sqlx::SqlitePool;

    use st0x_event_sorcery::{Projection, Store};
    use st0x_execution::MockExecutor;

    use super::*;
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    async fn create_test_position_infra(
        pool: &SqlitePool,
    ) -> (Store<Position>, Projection<Position>) {
        let projection = Projection::<Position>::sqlite(pool.clone()).unwrap();
        let position_store = st0x_event_sorcery::StoreBuilder::new(pool.clone())
            .with(projection.clone())
            .build(())
            .await
            .unwrap();
        (position_store, projection)
    }

    async fn initialize_position_with_fill(
        store: &Store<Position>,
        symbol: &Symbol,
        amount: FractionalShares,
        direction: Direction,
    ) {
        store
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount,
                    direction,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_none_when_no_position() {
        let pool = setup_test_db().await;
        let (_store, query) = create_test_position_infra(&pool).await;
        let executor = MockExecutor::new();

        let result = check_execution_readiness(
            &executor,
            &query,
            &Symbol::new("AAPL").unwrap(),
            SupportedExecutor::Schwab,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_none_below_threshold() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let executor = MockExecutor::new();

        initialize_position_with_fill(
            &store,
            &symbol,
            FractionalShares::new(dec!(0.5)),
            Direction::Buy,
        )
        .await;

        let result = check_execution_readiness(&executor, &query, &symbol, SupportedExecutor::Schwab)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_params_above_threshold() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let executor = MockExecutor::new();

        initialize_position_with_fill(
            &store,
            &symbol,
            FractionalShares::new(dec!(1.5)),
            Direction::Buy,
        )
        .await;

        let params = check_execution_readiness(&executor, &query, &symbol, SupportedExecutor::Schwab)
            .await
            .unwrap()
            .expect("should be ready for execution");

        assert_eq!(params.symbol, symbol);
        assert_eq!(
            params.shares,
            Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
            "DryRun supports fractional shares"
        );
        assert_eq!(
            params.direction,
            Direction::Sell,
            "Positive net (long) -> sell offchain to hedge"
        );
    }

    #[tokio::test]
    async fn check_all_positions_finds_ready_symbols() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let executor = MockExecutor::new();

        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();

        // AAPL: below threshold
        initialize_position_with_fill(
            &store,
            &aapl,
            FractionalShares::new(dec!(0.3)),
            Direction::Buy,
        )
        .await;

        // MSFT: above threshold
        initialize_position_with_fill(
            &store,
            &msft,
            FractionalShares::new(dec!(2.0)),
            Direction::Sell,
        )
        .await;

        let ready = check_all_positions(&executor, &query, SupportedExecutor::Schwab)
            .await
            .unwrap();

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].symbol, msft);
        assert_eq!(
            ready[0].shares,
            Positive::new(FractionalShares::new(dec!(2))).unwrap()
        );
        assert_eq!(
            ready[0].direction,
            Direction::Buy,
            "Negative net (short) -> buy offchain to hedge"
        );
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_none_when_market_closed() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Position above threshold
        initialize_position_with_fill(
            &store,
            &symbol,
            FractionalShares::new(dec!(2.0)),
            Direction::Buy,
        )
        .await;

        let executor = MockExecutor::new().with_market_open(false);

        let result = check_execution_readiness(&executor, &query, &symbol, SupportedExecutor::DryRun)
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Should return None when market is closed, even with position above threshold"
        );
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_params_when_market_open() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Position above threshold
        initialize_position_with_fill(
            &store,
            &symbol,
            FractionalShares::new(dec!(2.0)),
            Direction::Buy,
        )
        .await;

        let executor = MockExecutor::new().with_market_open(true);

        let params = check_execution_readiness(&executor, &query, &symbol, SupportedExecutor::DryRun)
            .await
            .unwrap()
            .expect("should be ready when market is open");

        assert_eq!(params.symbol, symbol);
        assert_eq!(
            params.shares,
            Positive::new(FractionalShares::new(dec!(2))).unwrap()
        );
    }

    #[tokio::test]
    async fn accumulated_position_during_market_close_triggers_on_market_open() {
        let pool = setup_test_db().await;
        let (store, query) = create_test_position_infra(&pool).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Simulate market closed - accumulate multiple trades
        let closed_executor = MockExecutor::new().with_market_open(false);

        // First trade while market closed
        initialize_position_with_fill(
            &store,
            &symbol,
            FractionalShares::new(dec!(0.5)),
            Direction::Buy,
        )
        .await;

        let result = check_execution_readiness(&closed_executor, &query, &symbol, SupportedExecutor::DryRun)
            .await
            .unwrap();
        assert!(result.is_none(), "Should not execute while market closed");

        // Second trade while market closed - now above threshold
        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: FractionalShares::new(dec!(1.0)),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let result = check_execution_readiness(&closed_executor, &query, &symbol, SupportedExecutor::DryRun)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "Should still not execute while market closed, even above threshold"
        );

        // Market opens - accumulated position should trigger
        let open_executor = MockExecutor::new().with_market_open(true);

        let params = check_execution_readiness(&open_executor, &query, &symbol, SupportedExecutor::DryRun)
            .await
            .unwrap()
            .expect("should execute when market opens with accumulated position");

        assert_eq!(params.symbol, symbol);
        assert_eq!(
            params.shares,
            Positive::new(FractionalShares::new(dec!(1.5))).unwrap(),
            "DryRun supports fractional shares, should return full 1.5"
        );
        assert_eq!(
            params.direction,
            Direction::Sell,
            "Positive net (long) -> sell offchain to hedge"
        );
    }
}
