use sqlx::SqlitePool;
use st0x_execution::{Direction, Executor, FractionalShares, Positive, SupportedExecutor, Symbol};
use tracing::{debug, info};

use crate::error::OnChainError;
use crate::position::{Position, PositionQuery, load_position};
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
/// exceeds the configured threshold. Also verifies the market is open.
/// The Position aggregate already tracks pending executions —
/// `is_ready_for_execution` returns `None` if one is already in flight.
pub(crate) async fn check_execution_readiness<E: Executor>(
    executor: &E,
    position_query: &PositionQuery,
    symbol: &Symbol,
    threshold: &ExecutionThreshold,
) -> Result<Option<ExecutionParams>, OnChainError> {
    let Some(position) = load_position(position_query, symbol).await? else {
        debug!(symbol = %symbol, "Position aggregate not found, skipping");
        return Ok(None);
    };

    let executor_type = executor.to_supported_executor();

    let execution_shares = Positive::new(shares)?;

    let execution_type = match direction {
        Direction::Sell => AccumulationBucket::LongExposure,
        Direction::Buy => AccumulationBucket::ShortExposure,
    };

    execute_position(
        &mut *sql_tx,
        base_symbol,
        calculator,
        execution_type,
        execution_shares,
        direction,
        executor_type,
    )
    .await
}

async fn execute_position(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
    calculator: &mut PositionCalculator,
    execution_type: AccumulationBucket,
    shares: Positive<FractionalShares>,
    direction: Direction,
    executor_type: SupportedExecutor,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let shares_f64 = shares.inner().inner().to_f64().ok_or_else(|| {
        OnChainError::Validation(TradeValidationError::ShareConversionFailed(shares))
    })?;

    let execution =
        create_execution_within_transaction(sql_tx, base_symbol, shares, direction, executor_type)
            .await?;

    let execution_id = execution.id.ok_or(PersistenceError::MissingExecutionId)?;

    // Find all trades that contributed to this execution and create linkages
    create_trade_execution_linkages(
        sql_tx,
        base_symbol,
        execution_id,
        execution_type,
        shares_f64,
    )
    .await?;

    calculator.reduce_accumulation(execution_type, shares_f64);

    info!(
        symbol = %base_symbol,
        shares = %shares,
        direction = ?direction,
        execution_type = ?execution_type,
        execution_id = ?execution.id,
        remaining_long = calculator.accumulated_long,
        remaining_short = calculator.accumulated_short,
        "Created offchain execution with trade linkages"
    );

    Ok(Some(execution))
}

/// Creates trade-execution linkages for an execution.
/// Links trades to executions based on chronological order and remaining available amounts.
async fn create_trade_execution_linkages(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
    execution_id: i64,
    execution_type: AccumulationBucket,
    execution_shares: f64,
) -> Result<(), OnChainError> {
    // Find all trades for this symbol that created this accumulated exposure
    // AccumulationBucket::ShortExposure comes from onchain SELL trades (sold stock, now short)
    // AccumulationBucket::LongExposure comes from onchain BUY trades (bought stock, now long)
    let trade_direction = match execution_type {
        AccumulationBucket::ShortExposure => Direction::Sell, // Short exposure from selling onchain
        AccumulationBucket::LongExposure => Direction::Buy,   // Long exposure from buying onchain
    };

    let direction_str = trade_direction.as_str();

    // Match all tokenized variants of this base symbol (prefix and suffix patterns)
    let base_str = base_symbol.to_string();
    let t_prefix = format!("t{base_str}");
    let zerox_suffix = format!("{base_str}0x");
    let s1_suffix = format!("{base_str}s1");

    // Use COALESCE(underlying_amount, amount) to get the hedge amount:
    // - For wrapped tokens: underlying_amount contains the underlying-equivalent
    // - For unwrapped tokens: underlying_amount is NULL, falls back to amount
    let trade_rows = sqlx::query!(
        r#"
        SELECT
            ot.id as trade_id,
            COALESCE(ot.underlying_amount, ot.amount) as "hedge_amount!: f64",
            COALESCE(SUM(tel.contributed_shares), 0.0) as "already_allocated: f64"
        FROM onchain_trades ot
        LEFT JOIN trade_execution_links tel ON ot.id = tel.trade_id
        WHERE (ot.symbol = ?1 OR ot.symbol = ?2 OR ot.symbol = ?3) AND ot.direction = ?4
        GROUP BY ot.id, COALESCE(ot.underlying_amount, ot.amount), ot.created_at
        HAVING (COALESCE(ot.underlying_amount, ot.amount) - COALESCE(SUM(tel.contributed_shares), 0.0)) > 0.001
        ORDER BY ot.created_at ASC
        "#,
        t_prefix,
        zerox_suffix,
        s1_suffix,
        direction_str
    )
    .fetch_all(&mut **sql_tx)
    .await?;

    let mut remaining_execution_shares = execution_shares;

    // Allocate trades to this execution in chronological order
    for row in trade_rows {
        if remaining_execution_shares <= 0.001 {
            break; // Execution fully allocated
        }

        let already_allocated = row.already_allocated.unwrap_or(0.0);
        let available_amount = row.hedge_amount - already_allocated;
        if available_amount <= 0.001 {
            continue; // Trade fully allocated to previous executions
        }

        // Allocate either the full remaining amount or up to execution remaining shares
        let contribution = available_amount.min(remaining_execution_shares);

        // Create the linkage
        let link = TradeExecutionLink::new(row.trade_id, execution_id, contribution);
        link.save_within_transaction(sql_tx).await?;

        remaining_execution_shares -= contribution;

        info!(
            trade_id = row.trade_id,
            execution_id = execution_id,
            contributed_shares = contribution,
            remaining_execution_shares = remaining_execution_shares,
            "Created trade-execution linkage"
        );
    }

    // Ensure we allocated the full execution (within floating point precision)
    if remaining_execution_shares > 0.001 {
        return Err(OnChainError::Validation(
            TradeValidationError::InsufficientTradeAllocation {
                symbol: base_symbol.to_string(),
                remaining_shares: remaining_execution_shares,
            },
        ));
    }

    Ok(())
}

async fn create_execution_within_transaction(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    executor: SupportedExecutor,
) -> Result<OffchainExecution, OnChainError> {
    let execution = OffchainExecution {
        id: None,
        symbol: symbol.clone(),
        shares,
        direction,
        executor,
        state: OrderState::Pending,
    };

    let execution_id = execution.save_within_transaction(sql_tx).await?;
    let mut execution_with_id = execution;
    execution_with_id.id = Some(execution_id);

    Ok(execution_with_id)
}

const STALE_EXECUTION_MINUTES: i32 = 10;

/// Clean up stale executions that have been in PENDING or SUBMITTED state for too long
/// Returns list of cleaned up executions for dual-write
async fn clean_up_stale_executions(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
) -> Result<Vec<CleanedUpExecution>, OnChainError> {
    let stale_execution_ids = find_stale_execution_ids(sql_tx, base_symbol).await?;

    let mut cleaned_up = Vec::new();

    for maybe_execution_id in stale_execution_ids {
        let Some(execution_id) = maybe_execution_id else {
            warn!(symbol = %base_symbol, "Stale execution has null ID, skipping cleanup");
            continue;
        };

        if let Some(cleaned) =
            mark_execution_as_timed_out(sql_tx, base_symbol, execution_id).await?
        {
            cleaned_up.push(cleaned);
        }
    }

    Ok(cleaned_up)
}

async fn find_stale_execution_ids(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
) -> Result<Vec<Option<i64>>, sqlx::Error> {
    let timeout_param = format!("-{STALE_EXECUTION_MINUTES} minutes");
    let base_symbol_str = base_symbol.to_string();

    sqlx::query_scalar!(
        r#"
        SELECT se.id
        FROM offchain_trades se
        JOIN trade_accumulators ta ON ta.pending_execution_id = se.id
        WHERE ta.symbol = ?1
          AND se.status IN ('PENDING', 'SUBMITTED')
          AND ta.last_updated < datetime('now', ?2)
        "#,
        base_symbol_str,
        timeout_param
    )
    .fetch_all(sql_tx.as_mut())
    .await
}

/// Checks if an execution is eligible for timeout cleanup.
/// Returns true if the execution exists, is in PENDING/SUBMITTED status, and pending_execution_id matches.
async fn is_execution_eligible_for_timeout(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
    execution_id: i64,
) -> Result<bool, OnChainError> {
    let base_symbol_str = base_symbol.to_string();
    let preflight = sqlx::query!(
        r#"
        SELECT se.status, ta.pending_execution_id
        FROM offchain_trades se
        LEFT JOIN trade_accumulators ta ON ta.symbol = ?1
        WHERE se.id = ?2
        "#,
        base_symbol_str,
        execution_id
    )
    .fetch_optional(sql_tx.as_mut())
    .await?;

    let Some(row) = preflight else {
        debug!(
            symbol = %base_symbol,
            execution_id = execution_id,
            "Execution not found, skipping cleanup"
        );
        return Ok(false);
    };

    let status = &row.status;
    if status != "PENDING" && status != "SUBMITTED" {
        debug!(
            symbol = %base_symbol,
            execution_id = execution_id,
            status = status,
            "Execution already progressed beyond PENDING/SUBMITTED, skipping cleanup"
        );
        return Ok(false);
    }

    if row.pending_execution_id != Some(execution_id) {
        debug!(
            symbol = %base_symbol,
            execution_id = execution_id,
            actual_pending_execution_id = ?row.pending_execution_id,
            "pending_execution_id mismatch, skipping cleanup"
        );
        return Ok(false);
    }

    Ok(true)
}

async fn mark_execution_as_timed_out(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
    execution_id: i64,
) -> Result<Option<CleanedUpExecution>, OnChainError> {
    if !is_execution_eligible_for_timeout(sql_tx, base_symbol, execution_id).await? {
        return Ok(None);
    }

    info!(
        symbol = %base_symbol,
        execution_id = execution_id,
        timeout_minutes = STALE_EXECUTION_MINUTES,
        "Cleaning up stale execution"
    );

    let error_reason = format!(
        "Execution timed out after {STALE_EXECUTION_MINUTES} minutes without status update"
    );

    let failed_state = OrderState::Failed {
        failed_at: chrono::Utc::now(),
        error_reason: Some(error_reason.clone()),
    };

    failed_state.store_update(sql_tx, execution_id).await?;

    let base_symbol_str = base_symbol.to_string();
    let result = sqlx::query!(
        "UPDATE trade_accumulators SET pending_execution_id = NULL WHERE symbol = ?1 AND pending_execution_id = ?2",
        base_symbol_str,
        execution_id
    )
    .execute(sql_tx.as_mut())
    .await?;

    if result.rows_affected() > 0 {
        clear_execution_lease(sql_tx, base_symbol).await?;

        info!(
            symbol = %base_symbol,
            execution_id = execution_id,
            "Cleared stale execution and released lock"
        );
    }

    Ok(Some(CleanedUpExecution {
        execution_id,
        symbol: base_symbol.clone(),
        error_reason,
    }))
}

/// Checks all accumulated positions and executes any that are ready for execution.
///
/// This function is designed to be called after processing batches of events
/// to ensure accumulated positions execute even when no new events arrive for those symbols.
/// It prevents positions from sitting idle indefinitely when they've accumulated
/// enough shares to execute but the triggering trade didn't push them over the threshold.
#[tracing::instrument(
    skip(pool, dual_write_context),
    fields(executor_type = %executor_type),
    level = tracing::Level::DEBUG
)]
pub(crate) async fn check_all_accumulated_positions(
    pool: &SqlitePool,
    dual_write_context: &DualWriteContext,
    executor_type: SupportedExecutor,
) -> Result<Vec<OffchainExecution>, OnChainError> {
    info!("Checking all accumulated positions for ready executions");

    let candidate_symbols = sqlx::query!(
        r#"
        SELECT symbol
        FROM trade_accumulators
        WHERE pending_execution_id IS NULL
        ORDER BY last_updated ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    if candidate_symbols.is_empty() {
        info!("No accumulated positions found to check");
        return Ok(vec![]);
    }

    info!(
        "Found {} symbols to check for execution readiness",
        candidate_symbols.len()
    );

    let mut executions = Vec::new();

    for row in candidate_symbols {
        let symbol = Symbol::new(&row.symbol)?;

        if let Some(execution) =
            check_symbol_for_execution(pool, dual_write_context, &symbol, executor_type).await?
        {
            executions.push(execution);
        }
    }

    if executions.is_empty() {
        info!("No new executions created from accumulated positions");
    } else {
        info!(
            "Created {} new executions from accumulated positions",
            executions.len()
        );
    }

    Ok(executions)
}

async fn check_symbol_for_execution(
    pool: &SqlitePool,
    dual_write_context: &DualWriteContext,
    symbol: &Symbol,
    executor_type: SupportedExecutor,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let Some((direction, shares)) =
        check_position_threshold(&position, symbol, executor_type, threshold)?
    else {
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

    Ok(Some(ExecutionParams {
        symbol: symbol.clone(),
        direction,
        shares,
        executor: executor_type,
    }))
}

fn check_position_threshold(
    position: &Position,
    symbol: &Symbol,
    executor_type: SupportedExecutor,
    threshold: &ExecutionThreshold,
) -> Result<Option<(Direction, FractionalShares)>, OnChainError> {
    let Some((direction, shares)) = position.is_ready_for_execution(executor_type, threshold)?
    else {
        debug!(
            symbol = %symbol,
            net = %position.net,
            "Position not ready for execution"
        );
        return Ok(None);
    };

    Ok(Some((direction, shares)))
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
/// Queries the position_view for all active positions, then checks each
/// against its configured threshold. Returns execution parameters for
/// positions that are ready.
#[tracing::instrument(
    skip(pool, executor, position_query),
    fields(executor_type = %executor.to_supported_executor()),
    level = tracing::Level::DEBUG
)]
pub(crate) async fn check_all_positions<E: Executor>(
    pool: &SqlitePool,
    executor: &E,
    position_query: &PositionQuery,
    threshold: &ExecutionThreshold,
) -> Result<Vec<ExecutionParams>, OnChainError> {
    let symbols = sqlx::query_scalar!("SELECT symbol FROM position_view WHERE symbol IS NOT NULL")
        .fetch_all(pool)
        .await?;

    let mut ready = Vec::new();

    for symbol_str in symbols.into_iter().flatten() {
        let symbol = Symbol::new(&symbol_str)?;

        if let Some(params) =
            check_execution_readiness(executor, position_query, &symbol, threshold).await?
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
    use alloy::primitives::TxHash;
    use cqrs_es::persist::GenericQuery;
    use rust_decimal_macros::dec;
    use sqlite_es::SqliteViewRepository;
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use st0x_execution::{Direction, FractionalShares, MockExecutor, Positive, Symbol};

    use super::*;
    use crate::conductor::wire::test_cqrs;
    use crate::position::{Position, PositionCommand, PositionCqrs, PositionQuery, TradeId};
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    fn create_test_position_infra(pool: &SqlitePool) -> (PositionCqrs, PositionQuery) {
        let view_repo = Arc::new(SqliteViewRepository::new(
            pool.clone(),
            "position_view".to_string(),
        ));
        let position_query = GenericQuery::new(view_repo.clone());
        let position_cqrs: PositionCqrs = test_cqrs(
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
        direction: Direction,
    ) {
        cqrs.execute(
            &Position::aggregate_id(symbol),
            PositionCommand::AcknowledgeOnChainFill {
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
        let (_cqrs, query) = create_test_position_infra(&pool);
        let threshold = ExecutionThreshold::whole_share();
        let executor = MockExecutor::new();

        let result =
            check_execution_readiness(&executor, &query, &Symbol::new("AAPL").unwrap(), &threshold)
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
        let executor = MockExecutor::new();

        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(0.5)),
            Direction::Buy,
        )
        .await;

        let result = check_execution_readiness(&executor, &query, &symbol, &threshold)
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
        let executor = MockExecutor::new();

        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(1.5)),
            Direction::Buy,
        )
        .await;

        let params = check_execution_readiness(&executor, &query, &symbol, &threshold)
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
        let (cqrs, query) = create_test_position_infra(&pool);
        let threshold = ExecutionThreshold::whole_share();
        let executor = MockExecutor::new();

        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();

        // AAPL: below threshold
        initialize_position_with_fill(
            &cqrs,
            &aapl,
            FractionalShares::new(dec!(0.3)),
            Direction::Buy,
        )
        .await;

        // MSFT: above threshold
        initialize_position_with_fill(
            &cqrs,
            &msft,
            FractionalShares::new(dec!(2.0)),
            Direction::Sell,
        )
        .await;

        let ready = check_all_positions(&pool, &executor, &query, &threshold)
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
        let (cqrs, query) = create_test_position_infra(&pool);
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ExecutionThreshold::whole_share();

        // Position above threshold
        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(2.0)),
            Direction::Buy,
        )
        .await;

        let executor = MockExecutor::new().with_market_open(false);

        let result = check_execution_readiness(&executor, &query, &symbol, &threshold)
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
        let (cqrs, query) = create_test_position_infra(&pool);
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ExecutionThreshold::whole_share();

        // Position above threshold
        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(2.0)),
            Direction::Buy,
        )
        .await;

        let executor = MockExecutor::new().with_market_open(true);

        let params = check_execution_readiness(&executor, &query, &symbol, &threshold)
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
        let (cqrs, query) = create_test_position_infra(&pool);
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ExecutionThreshold::whole_share();

        // Simulate market closed - accumulate multiple trades
        let closed_executor = MockExecutor::new().with_market_open(false);

        // First trade while market closed
        initialize_position_with_fill(
            &cqrs,
            &symbol,
            FractionalShares::new(dec!(0.5)),
            Direction::Buy,
        )
        .await;

        let result = check_execution_readiness(&closed_executor, &query, &symbol, &threshold)
            .await
            .unwrap();
        assert!(result.is_none(), "Should not execute while market closed");

        // Second trade while market closed - now above threshold
        cqrs.execute(
            &Position::aggregate_id(&symbol),
            PositionCommand::AcknowledgeOnChainFill {
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

        let result = check_execution_readiness(&closed_executor, &query, &symbol, &threshold)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "Should still not execute while market closed, even above threshold"
        );

        // Market opens - accumulated position should trigger
        let open_executor = MockExecutor::new().with_market_open(true);

        let params = check_execution_readiness(&open_executor, &query, &symbol, &threshold)
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
