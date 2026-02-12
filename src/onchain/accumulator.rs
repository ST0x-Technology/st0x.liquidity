use sqlx::SqlitePool;
use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};
use tracing::{debug, info};

use crate::onchain::OnChainError;
use crate::position::{PositionQuery, load_position};
use crate::threshold::ExecutionThreshold;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionParams {
    pub(crate) symbol: Symbol,
    pub(crate) direction: Direction,
    pub(crate) shares: Positive<FractionalShares>,
    pub(crate) executor: SupportedExecutor,
}

/// Checks whether a position is ready for offchain execution.
///
/// Loads the position from the CQRS view and checks if the net exposure
/// exceeds the configured threshold. The Position aggregate already tracks
/// pending executions — `is_ready_for_execution` returns `None` if one
/// is already in flight.
pub(crate) async fn check_execution_readiness(
    position_query: &PositionQuery,
    symbol: &Symbol,
    executor_type: SupportedExecutor,
    threshold: &ExecutionThreshold,
) -> Result<Option<ExecutionParams>, OnChainError> {
    let Some(position) = load_position(position_query, symbol).await? else {
        debug!(symbol = %symbol, "Position aggregate not found, skipping");
        return Ok(None);
    };

    let Some((direction, shares)) = position.is_ready_for_execution(executor_type, threshold)?
    else {
        debug!(
            symbol = %symbol,
            net = %position.net,
            "Position not ready for execution"
        );
        return Ok(None);
    };

    let shares = Positive::new(shares)?;

    info!(
        symbol = %symbol,
        shares = %shares,
        direction = ?direction,
        "Position ready for execution"
    );

    Ok(Some(ExecutionParams {
        symbol: symbol.clone(),
        direction,
        shares,
        executor: executor_type,
    }))
}

/// Checks all positions for execution readiness.
///
/// Queries the position_view for all active positions, then checks each
/// against its configured threshold. Returns execution parameters for
/// positions that are ready.
#[tracing::instrument(
    skip(pool, position_query),
    fields(executor_type = %executor_type),
    level = tracing::Level::DEBUG
)]
pub(crate) async fn check_all_positions(
    pool: &SqlitePool,
    position_query: &PositionQuery,
    executor_type: SupportedExecutor,
    threshold: &ExecutionThreshold,
) -> Result<Vec<ExecutionParams>, OnChainError> {
    let symbols = sqlx::query_scalar!("SELECT symbol FROM position_view WHERE symbol IS NOT NULL")
        .fetch_all(pool)
        .await?;

    let mut ready = Vec::new();

    for symbol_str in symbols.into_iter().flatten() {
        let symbol = Symbol::new(&symbol_str)?;

        if let Some(params) =
            check_execution_readiness(position_query, &symbol, executor_type, threshold).await?
        {
            ready.push(params);
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
    use std::sync::Arc;

    use cqrs_es::persist::GenericQuery;
    use rust_decimal_macros::dec;
    use sqlite_es::SqliteViewRepository;
    use sqlx::SqlitePool;
    use st0x_execution::{FractionalShares, Positive, SupportedExecutor, Symbol};

    use super::*;
    use crate::position::{Position, PositionCommand, PositionCqrs, PositionQuery};
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    fn create_test_position_infra(pool: &SqlitePool) -> (PositionCqrs, PositionQuery) {
        let view_repo = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "position_view".to_string(),
        ));
        let position_query = GenericQuery::new(view_repo.clone());
        let position_cqrs: PositionCqrs = sqlite_es::sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            (),
        );
        (position_cqrs, position_query)
    }

    async fn initialize_position_with_fill(
        cqrs: &PositionCqrs,
        symbol: &Symbol,
        amount: FractionalShares,
        direction: st0x_execution::Direction,
    ) {
        cqrs.execute(
            &Position::aggregate_id(symbol),
            PositionCommand::AcknowledgeOnChainFill {
                trade_id: crate::position::TradeId {
                    tx_hash: alloy::primitives::TxHash::random(),
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
        let (_cqrs, query) = create_test_position_infra(&pool);
        let threshold = ExecutionThreshold::whole_share();

        let result = check_execution_readiness(
            &query,
            &Symbol::new("AAPL").unwrap(),
            SupportedExecutor::Schwab,
            &threshold,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_none_below_threshold() {
        let pool = setup_test_db().await;
        let (cqrs, query) = create_test_position_infra(&pool);
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ExecutionThreshold::whole_share();

        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(0.5)),
            st0x_execution::Direction::Buy,
        )
        .await;

        let result =
            check_execution_readiness(&query, &symbol, SupportedExecutor::Schwab, &threshold)
                .await
                .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn check_execution_readiness_returns_params_above_threshold() {
        let pool = setup_test_db().await;
        let (cqrs, query) = create_test_position_infra(&pool);
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ExecutionThreshold::whole_share();

        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(1.5)),
            st0x_execution::Direction::Buy,
        )
        .await;

        let params =
            check_execution_readiness(&query, &symbol, SupportedExecutor::Schwab, &threshold)
                .await
                .unwrap()
                .expect("should be ready for execution");

        assert_eq!(params.symbol, symbol);
        assert_eq!(
            params.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap(),
            "Schwab floors to whole shares"
        );
        assert_eq!(
            params.direction,
            st0x_execution::Direction::Sell,
            "Positive net (long) -> sell offchain to hedge"
        );
    }

    #[tokio::test]
    async fn check_all_positions_finds_ready_symbols() {
        let pool = setup_test_db().await;
        let (cqrs, query) = create_test_position_infra(&pool);
        let threshold = ExecutionThreshold::whole_share();

        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();

        // AAPL: below threshold
        initialize_position_with_fill(
            &cqrs,
            &aapl,
            FractionalShares::new(dec!(0.3)),
            st0x_execution::Direction::Buy,
        )
        .await;

        // MSFT: above threshold
        initialize_position_with_fill(
            &cqrs,
            &msft,
            FractionalShares::new(dec!(2.0)),
            st0x_execution::Direction::Sell,
        )
        .await;

        let ready = check_all_positions(&pool, &query, SupportedExecutor::Schwab, &threshold)
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
            st0x_execution::Direction::Buy,
            "Negative net (short) -> buy offchain to hedge"
        );
    }
}
