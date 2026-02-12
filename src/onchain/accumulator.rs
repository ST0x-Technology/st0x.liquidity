//! Onchain trade accumulation and offchain order placement.
//!
//! Processes onchain trades through the position accumulator
//! with duplicate detection, then triggers offsetting offchain
//! orders when thresholds are met.

use num_traits::ToPrimitive;
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, FractionalShares, OrderState, PersistenceError, Positive, SupportedExecutor, Symbol,
};
use tracing::{debug, info, warn};

use super::OnChainError;
use super::OnchainTrade;
use crate::dual_write::{DualWriteContext, load_position};
use crate::lock::{clear_execution_lease, set_pending_execution_id, try_acquire_execution_lease};
use crate::offchain::execution::OffchainExecution;
use crate::onchain::position_calculator::{AccumulationBucket, PositionCalculator};
use crate::onchain::trade::TradeValidationError;
use crate::trade_execution_link::TradeExecutionLink;

#[derive(Debug, Clone)]
pub(crate) struct CleanedUpExecution {
    pub(crate) execution_id: i64,
    pub(crate) symbol: Symbol,
    pub(crate) error_reason: String,
}

pub(crate) struct TradeProcessingResult {
    pub(crate) execution: Option<OffchainExecution>,
    pub(crate) cleaned_up_executions: Vec<CleanedUpExecution>,
}

/// Processes an onchain trade through the accumulation system with duplicate detection.
///
/// This function handles the complete trade processing pipeline:
/// 1. Checks for duplicate trades (same tx_hash + log_index) and skips if already processed
/// 2. Saves the trade to the onchain_trades table
/// 3. Updates the position accumulator for the symbol
/// 4. Attempts to create an offchain execution if position thresholds are met
/// 5. Cleans up any stale pending executions for the symbol
///
/// Returns `TradeProcessingResult` containing the new execution (if created) and any
/// cleaned up stale executions. The transaction must be committed by the caller.
#[tracing::instrument(
    skip(sql_tx, dual_write_context, trade),
    fields(symbol = %trade.symbol, amount = %trade.amount, direction = ?trade.direction),
    level = tracing::Level::INFO
)]
pub(crate) async fn process_onchain_trade(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    dual_write_context: &DualWriteContext,
    trade: OnchainTrade,
    executor_type: SupportedExecutor,
) -> Result<TradeProcessingResult, OnChainError> {
    // Check if trade already exists to handle duplicates gracefully
    let tx_hash_str = trade.tx_hash.to_string();
    let log_index_i64 = i64::try_from(trade.log_index)?;

    let existing_trade = sqlx::query!(
        "
        SELECT id
        FROM onchain_trades
        WHERE tx_hash = ?1 AND log_index = ?2
        ",
        tx_hash_str,
        log_index_i64
    )
    .fetch_optional(&mut **sql_tx)
    .await?;

    if existing_trade.is_some() {
        info!(
            "Trade already exists (tx_hash={:?}, log_index={}), skipping duplicate processing",
            trade.tx_hash, trade.log_index
        );
        return Ok(TradeProcessingResult {
            execution: None,
            cleaned_up_executions: Vec::new(),
        });
    }

    let trade_id = trade.save_within_transaction(sql_tx).await?;
    info!(
        trade_id = trade_id,
        symbol = %trade.symbol,
        amount = trade.amount,
        direction = ?trade.direction,
        tx_hash = ?trade.tx_hash,
        log_index = trade.log_index,
        "Saved onchain trade"
    );

    let base_symbol = trade.symbol.base();

    let mut calculator = get_or_create_within_transaction(sql_tx, base_symbol).await?;

    // Map onchain direction to exposure state
    // Onchain SELL (gave away stock for USDC) -> we're now short the stock
    // Onchain BUY (gave away USDC for stock) -> we're now long the stock
    let exposure_bucket = match trade.direction {
        Direction::Sell => AccumulationBucket::ShortExposure, // Sold stock -> short exposure
        Direction::Buy => AccumulationBucket::LongExposure,   // Bought stock -> long exposure
    };
    calculator.add_trade(trade.amount, exposure_bucket);

    info!(
        symbol = %base_symbol,
        net_position = calculator.net_position(),
        accumulated_long = calculator.accumulated_long,
        accumulated_short = calculator.accumulated_short,
        exposure_bucket = ?exposure_bucket,
        trade_amount = trade.amount,
        "Updated calculator"
    );

    // Clean up any stale executions for this symbol before attempting new execution
    let cleaned_up_executions = clean_up_stale_executions(sql_tx, base_symbol).await?;

    let execution = if try_acquire_execution_lease(sql_tx, base_symbol).await? {
        let result = try_create_execution_if_ready(
            sql_tx,
            dual_write_context,
            base_symbol,
            &mut calculator,
            executor_type,
        )
        .await?;

        match &result {
            Some(execution) => {
                let execution_id = execution.id.ok_or(PersistenceError::MissingExecutionId)?;
                set_pending_execution_id(sql_tx, base_symbol, execution_id).await?;
            }
            None => {
                clear_execution_lease(sql_tx, base_symbol).await?;
            }
        }

        result
    } else {
        info!(
            symbol = %base_symbol,
            "Another worker holds execution lease, skipping execution creation"
        );
        None
    };

    let pending_execution_id = execution.as_ref().and_then(|e| e.id);
    save_within_transaction(&mut *sql_tx, base_symbol, &calculator, pending_execution_id).await?;

    Ok(TradeProcessingResult {
        execution,
        cleaned_up_executions,
    })
}

#[cfg(test)]
pub(crate) async fn find_by_symbol(
    pool: &SqlitePool,
    symbol: &str,
) -> Result<Option<(PositionCalculator, Option<i64>)>, OnChainError> {
    let row = sqlx::query!(
        "SELECT accumulated_long, accumulated_short, pending_execution_id FROM trade_accumulators WHERE symbol = ?1",
        symbol
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| {
        let calculator =
            PositionCalculator::with_positions(row.accumulated_long, row.accumulated_short);
        (calculator, row.pending_execution_id)
    }))
}

async fn get_or_create_within_transaction(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
) -> Result<PositionCalculator, OnChainError> {
    let symbol_str = symbol.to_string();
    let row = sqlx::query!(
        "SELECT accumulated_long, accumulated_short FROM trade_accumulators WHERE symbol = ?1",
        symbol_str
    )
    .fetch_optional(&mut **sql_tx)
    .await?;

    if let Some(row) = row {
        Ok(PositionCalculator::with_positions(
            row.accumulated_long,
            row.accumulated_short,
        ))
    } else {
        let new_calculator = PositionCalculator::new();
        save_within_transaction(sql_tx, symbol, &new_calculator, None).await?;
        Ok(new_calculator)
    }
}

pub(crate) async fn save_within_transaction(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
    calculator: &PositionCalculator,
    pending_execution_id: Option<i64>,
) -> Result<(), OnChainError> {
    let symbol_str = symbol.to_string();
    sqlx::query!(
        r#"
        INSERT INTO trade_accumulators (
            symbol,
            accumulated_long,
            accumulated_short,
            pending_execution_id,
            last_updated
        )
        VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
        ON CONFLICT(symbol) DO UPDATE SET
            accumulated_long = excluded.accumulated_long,
            accumulated_short = excluded.accumulated_short,
            pending_execution_id = COALESCE(
                excluded.pending_execution_id,
                pending_execution_id
            ),
            last_updated = CURRENT_TIMESTAMP
        "#,
        symbol_str,
        calculator.accumulated_long,
        calculator.accumulated_short,
        pending_execution_id
    )
    .execute(sql_tx.as_mut())
    .await?;

    Ok(())
}

async fn try_create_execution_if_ready(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    dual_write_context: &DualWriteContext,
    base_symbol: &Symbol,
    calculator: &mut PositionCalculator,
    executor_type: SupportedExecutor,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let Some(position) = load_position(dual_write_context, base_symbol).await? else {
        debug!(
            symbol = %base_symbol,
            "Position aggregate not found, cannot check threshold"
        );
        return Ok(None);
    };

    let Some((direction, shares)) = position.is_ready_for_execution(executor_type)? else {
        debug!(
            symbol = %base_symbol,
            net = %position.net,
            "Position threshold not met"
        );
        return Ok(None);
    };

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

    let trade_rows = sqlx::query!(
        r#"
        SELECT
            ot.id as trade_id,
            ot.amount as trade_amount,
            COALESCE(SUM(tel.contributed_shares), 0.0) as "already_allocated: f64"
        FROM onchain_trades ot
        LEFT JOIN trade_execution_links tel ON ot.id = tel.trade_id
        WHERE (ot.symbol = ?1 OR ot.symbol = ?2 OR ot.symbol = ?3) AND ot.direction = ?4
        GROUP BY ot.id, ot.amount, ot.created_at
        HAVING (ot.amount - COALESCE(SUM(tel.contributed_shares), 0.0)) > 0.001  -- Has remaining allocation
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
        let available_amount = row.trade_amount - already_allocated;
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
        get_ready_execution_params(dual_write_context, symbol, executor_type).await?
    else {
        return Ok(None);
    };

    info!(
        symbol = %symbol,
        shares = %shares,
        direction = ?direction,
        "Position ready for execution"
    );

    let mut sql_tx = pool.begin().await?;
    clean_up_stale_executions(&mut sql_tx, symbol).await?;

    let result = if try_acquire_execution_lease(&mut sql_tx, symbol).await? {
        process_symbol_execution(&mut sql_tx, symbol, direction, shares, executor_type).await?
    } else {
        info!(symbol = %symbol, "Another worker holds execution lease, skipping");
        None
    };

    sql_tx.commit().await?;
    Ok(result)
}

async fn get_ready_execution_params(
    dual_write_context: &DualWriteContext,
    symbol: &Symbol,
    executor: SupportedExecutor,
) -> Result<Option<(Direction, Positive<FractionalShares>)>, OnChainError> {
    let Some(position) = load_position(dual_write_context, symbol).await? else {
        debug!(symbol = %symbol, "Position aggregate not found, skipping");
        return Ok(None);
    };

    let Some((direction, shares)) = position.is_ready_for_execution(executor)? else {
        debug!(
            symbol = %symbol,
            net = %position.net,
            "Position threshold not met, skipping"
        );
        return Ok(None);
    };

    let execution_shares = Positive::new(shares)?;

    Ok(Some((direction, execution_shares)))
}

async fn process_symbol_execution(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
    direction: Direction,
    shares: Positive<FractionalShares>,
    executor_type: SupportedExecutor,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let mut calculator = get_or_create_within_transaction(sql_tx, symbol).await?;

    let execution_type = match direction {
        Direction::Sell => AccumulationBucket::LongExposure,
        Direction::Buy => AccumulationBucket::ShortExposure,
    };

    let result = execute_position(
        sql_tx,
        symbol,
        &mut calculator,
        execution_type,
        shares,
        direction,
        executor_type,
    )
    .await?;

    if let Some(execution) = &result {
        let execution_id = execution.id.ok_or(PersistenceError::MissingExecutionId)?;
        set_pending_execution_id(sql_tx, symbol, execution_id).await?;

        info!(
            symbol = %symbol,
            execution_id = ?execution.id,
            shares = ?execution.shares,
            direction = ?execution.direction,
            "Created execution for accumulated position"
        );
    } else {
        clear_execution_lease(sql_tx, symbol).await?;
        info!(symbol = %symbol, "No execution created for symbol");
    }

    let pending_execution_id = result.as_ref().and_then(|e| e.id);
    save_within_transaction(sql_tx, symbol, &calculator, pending_execution_id).await?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, FixedBytes, fixed_bytes};
    use backon::{ExponentialBuilder, Retryable};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use st0x_execution::{FractionalShares, OrderStatus, Positive, Symbol};

    use super::*;
    use crate::dual_write::DualWriteContext;
    use crate::offchain::execution::find_executions_by_symbol_status_and_broker;
    use crate::offchain_order::{BrokerOrderId, OffchainOrder, OffchainOrderCommand};
    use crate::onchain::io::Usdc;
    use crate::position::{Position, PositionCommand};
    use crate::symbol;
    use crate::symbol::lock::get_symbol_lock;
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;
    use crate::tokenized_symbol;
    use crate::trade_execution_link::TradeExecutionLink;

    fn create_test_onchain_trade(symbol: &str, tx_hash_byte: u8) -> OnchainTrade {
        OnchainTrade {
            id: None,
            tx_hash: FixedBytes([tx_hash_byte; 32]),
            log_index: 1,
            symbol: symbol.parse().unwrap(),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Buy,
            price: Usdc::new(250.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(48000),
            effective_gas_price: Some(1_400_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        }
    }

    async fn setup_stale_execution(
        pool: &SqlitePool,
        dual_write_context: &DualWriteContext,
        symbol: &Symbol,
    ) -> i64 {
        let stale_execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = stale_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, symbol, &calculator, Some(execution_id))
            .await
            .unwrap();

        let symbol_str = symbol.to_string();
        sqlx::query!(
            "UPDATE trade_accumulators \
            SET last_updated = datetime('now', '-15 minutes') WHERE symbol = ?1",
            symbol_str
        )
        .execute(sql_tx.as_mut())
        .await
        .unwrap();

        sql_tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(dual_write_context, &pending_execution)
            .await
            .unwrap();

        crate::dual_write::place_offchain_order(dual_write_context, &pending_execution, symbol)
            .await
            .unwrap();

        dual_write_context
            .offchain_order_framework()
            .execute(
                &OffchainOrder::aggregate_id(execution_id),
                OffchainOrderCommand::ConfirmSubmission {
                    broker_order_id: BrokerOrderId("ORDER123".to_string()),
                },
            )
            .await
            .unwrap();

        execution_id
    }

    async fn process_trade_with_tx(
        pool: &SqlitePool,
        trade: OnchainTrade,
    ) -> Result<Option<OffchainExecution>, OnChainError> {
        let dual_write_context = DualWriteContext::new(pool.clone());
        let base_symbol = trade.symbol.base();

        // Mirror production: acquire symbol lock before updating Position aggregate
        let symbol_lock = get_symbol_lock(base_symbol).await;
        let _guard = symbol_lock.lock().await;

        // Initialize Position aggregate and acknowledge fill BEFORE processing
        // so threshold check sees current state. Ignore AlreadyInitialized error
        // since this helper may be called multiple times for the same symbol.
        let _ = crate::dual_write::initialize_position(
            &dual_write_context,
            base_symbol,
            ExecutionThreshold::whole_share(),
        )
        .await;

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &trade)
            .await
            .unwrap();

        let mut sql_tx = pool.begin().await?;
        let TradeProcessingResult {
            execution,
            cleaned_up_executions: _,
        } = process_onchain_trade(
            &mut sql_tx,
            &dual_write_context,
            trade,
            SupportedExecutor::Schwab,
        )
        .await?;
        sql_tx.commit().await?;
        Ok(execution)
    }

    #[tokio::test]
    async fn test_add_trade_below_threshold() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.5,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(45000),
            effective_gas_price: Some(1_200_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, trade).await.unwrap();
        assert!(result.is_none());

        let (calculator, _) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        assert!((calculator.accumulated_short - 0.5).abs() < f64::EPSILON); // SELL creates short exposure
        assert!((calculator.net_position() - (-0.5)).abs() < f64::EPSILON); // Short position = negative net
        assert!((calculator.accumulated_long - 0.0).abs() < f64::EPSILON); // No long exposure
    }

    #[tokio::test]
    async fn test_add_trade_above_threshold() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("MSFT0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(300.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(52000),
            effective_gas_price: Some(1_800_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = process_trade_with_tx(&pool, trade).await.unwrap().unwrap();

        assert_eq!(execution.symbol, Symbol::new("MSFT").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL (short exposure)

        let (calculator, _) = find_by_symbol(&pool, "MSFT").await.unwrap().unwrap();
        // Residual 0.5 shares remain after floored execution (1.5 - 1.0 = 0.5)
        assert!((calculator.accumulated_short - 0.5).abs() < f64::EPSILON);
        assert!((calculator.net_position() - (-0.5)).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_accumulation_across_multiple_trades() {
        let pool = setup_test_db().await;

        let trade1 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x3333333333333333333333333333333333333333333333333333333333333333"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.3,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(43000),
            effective_gas_price: Some(1_100_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result1 = process_trade_with_tx(&pool, trade1).await.unwrap();
        assert!(result1.is_none());

        let trade2 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x4444444444444444444444444444444444444444444444444444444444444444"
            ),
            log_index: 2,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.4,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(44000),
            effective_gas_price: Some(1_250_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result2 = process_trade_with_tx(&pool, trade2).await.unwrap();
        assert!(result2.is_none());

        let trade3 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x5555555555555555555555555555555555555555555555555555555555555555"
            ),
            log_index: 3,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.4,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(46000),
            effective_gas_price: Some(1_300_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result3 = process_trade_with_tx(&pool, trade3).await.unwrap();
        let execution = result3.unwrap();

        assert_eq!(execution.symbol, Symbol::new("AAPL").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL (short exposure)

        let (calculator, _) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        // Residual 0.1 shares remain after floored execution (1.1 - 1.0 = 0.1)
        assert!((calculator.accumulated_short - 0.1).abs() < f64::EPSILON); // Remaining short exposure
        assert!((calculator.net_position() - (-0.1)).abs() < f64::EPSILON); // Net short position
    }

    #[tokio::test]
    async fn test_add_trade_with_valid_format_succeeds() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x6666666666666666666666666666666666666666666666666666666666666666"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("INVALID0x"),
            equity_token: Address::ZERO,
            amount: 1.0,
            direction: Direction::Buy,
            price: Usdc::new(100.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(48000),
            effective_gas_price: Some(1_400_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        process_trade_with_tx(&pool, trade).await.unwrap();
    }

    #[tokio::test]
    async fn test_direction_mapping_sell_instruction_preserved() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(47000),
            effective_gas_price: Some(1_350_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = process_trade_with_tx(&pool, trade).await.unwrap().unwrap();

        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL (short exposure)
        assert_eq!(execution.symbol, Symbol::new("AAPL").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
    }

    #[tokio::test]
    async fn test_direction_mapping_buy_instruction_preserved() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("MSFT0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Buy,
            price: Usdc::new(300.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(49000),
            effective_gas_price: Some(1_450_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = process_trade_with_tx(&pool, trade).await.unwrap().unwrap();

        assert_eq!(execution.direction, Direction::Sell); // Schwab SELL to offset onchain BUY (long exposure)
        assert_eq!(execution.symbol, Symbol::new("MSFT").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
    }

    #[tokio::test]
    async fn test_database_transaction_rollback_on_execution_save_failure() {
        let pool = setup_test_db().await;

        // First, create a pending execution for AAPL to trigger the unique constraint
        let blocking_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(50))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };
        let mut sql_tx = pool.begin().await.unwrap();
        blocking_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Create a trade that would trigger execution for the same symbol
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x8888888888888888888888888888888888888888888888888888888888888888"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(51000),
            effective_gas_price: Some(1_600_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Attempt to add trade - should fail when trying to save execution due to unique constraint
        let error_msg = process_trade_with_tx(&pool, trade)
            .await
            .unwrap_err()
            .to_string();
        assert!(error_msg.contains("UNIQUE constraint failed"));

        // Verify transaction was rolled back - no new trade should have been saved
        let trade_count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(trade_count, 0);

        // Verify accumulator was not created for this failed transaction
        let accumulator_result = find_by_symbol(&pool, "AAPL").await.unwrap();
        assert!(accumulator_result.is_none());

        // Verify only the original execution remains
        let executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Pending,
            None,
        )
        .await
        .unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(
            executions[0].shares,
            Positive::new(FractionalShares::new(Decimal::from(50))).unwrap()
        );
    }

    #[tokio::test]
    async fn test_accumulator_state_consistency_under_simulated_corruption() {
        let pool = setup_test_db().await;

        // Create multiple trades that would create inconsistent state if not properly handled
        let trade1 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x9999999999999999999999999999999999999999999999999999999999999999"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.8,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(42000),
            effective_gas_price: Some(1_050_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let trade2 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.3,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(43500),
            effective_gas_price: Some(1_150_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Add first trade (should not trigger execution)
        let result1 = process_trade_with_tx(&pool, trade1).await.unwrap();
        assert!(result1.is_none());

        // Add second trade (should trigger execution)
        let result2 = process_trade_with_tx(&pool, trade2).await.unwrap();
        let execution = result2.unwrap();

        // Verify execution created for exactly 1 share
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(Decimal::from(1))).unwrap()
        );
        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL

        // Verify accumulator shows correct remaining fractional amount
        let (calculator, _) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        assert!((calculator.accumulated_short - 0.1).abs() < f64::EPSILON); // SELL creates short exposure
        assert!((calculator.net_position() - (-0.1)).abs() < f64::EPSILON); // Short position = negative net

        // Verify both trades were saved
        let trade_count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(trade_count, 2);

        // Verify exactly one execution was created
        let execution_count = sqlx::query!("SELECT COUNT(*) as count FROM offchain_trades")
            .fetch_one(&pool)
            .await
            .unwrap()
            .count;
        assert_eq!(execution_count, 1);
    }

    async fn process_with_retry(
        pool: &SqlitePool,
        trade: OnchainTrade,
    ) -> Result<Option<OffchainExecution>, OnChainError> {
        let backoff = ExponentialBuilder::default()
            .with_min_delay(std::time::Duration::from_millis(10))
            .with_max_times(3);

        (|| async { process_trade_with_tx(pool, trade.clone()).await })
            .retry(backoff)
            .when(|e| e.to_string().contains("deadlocked"))
            .await
    }

    fn create_test_trade(tx_hash_byte: u8, symbol: &str, amount: f64) -> OnchainTrade {
        OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(tx_hash_byte),
            log_index: 1,
            symbol: tokenized_symbol!(symbol),
            equity_token: Address::ZERO,
            amount,
            direction: Direction::Sell,
            price: Usdc::new(15000.0).unwrap(),
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

    async fn verify_concurrent_execution_state(pool: &SqlitePool, expected_short: f64) {
        let trade_count = super::OnchainTrade::db_count(pool).await.unwrap();
        assert_eq!(trade_count, 2, "Expected 2 trades to be saved");

        let execution_count = sqlx::query!("SELECT COUNT(*) as count FROM offchain_trades")
            .fetch_one(pool)
            .await
            .unwrap()
            .count;
        assert_eq!(
            execution_count, 1,
            "Expected exactly 1 execution to prevent duplicate orders"
        );

        let (calculator, _) = find_by_symbol(pool, "AAPL")
            .await
            .unwrap()
            .expect("Accumulator should exist for AAPL");

        assert!(
            (calculator.accumulated_short - expected_short).abs() < f64::EPSILON,
            "Expected {expected_short} accumulated_short remaining, got {}",
            calculator.accumulated_short
        );
    }

    #[tokio::test]
    async fn test_concurrent_trade_processing_prevents_duplicate_executions() {
        let pool = setup_test_db().await;

        let trade1 = create_test_trade(0xaa, "AAPL0x", 0.8);
        let trade2 = create_test_trade(0xbb, "AAPL0x", 0.8);

        let (result1, result2) = tokio::join!(
            process_with_retry(&pool, trade1),
            process_with_retry(&pool, trade2)
        );

        let execution1 = result1.unwrap();
        let execution2 = result2.unwrap();

        let executions_created = match (execution1, execution2) {
            (Some(_), None) | (None, Some(_)) => 1,
            (Some(_), Some(_)) => 2,
            (None, None) => 0,
        };

        assert_eq!(
            executions_created, 1,
            "Per-symbol lease should prevent duplicate executions, but got {executions_created}"
        );

        verify_concurrent_execution_state(&pool, 0.6).await;
    }

    #[tokio::test]
    async fn test_trade_execution_linkage_single_trade() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(46500),
            effective_gas_price: Some(1_320_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = process_trade_with_tx(&pool, trade).await.unwrap().unwrap();
        let execution_id = execution.id.unwrap();

        // Verify trade-execution link was created
        let trade_count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(trade_count, 1);

        let links = TradeExecutionLink::find_by_execution_id(&pool, execution_id)
            .await
            .unwrap();
        assert_eq!(links.len(), 1);
        assert!((links[0].contributed_shares() - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_trade_execution_linkage_multiple_trades() {
        let pool = setup_test_db().await;

        let trades = vec![
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                log_index: 1,
                symbol: tokenized_symbol!("MSFT0x"),
                equity_token: Address::ZERO,
                amount: 0.3,
                direction: Direction::Buy,
                price: Usdc::new(300.0).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: Some(41000),
                effective_gas_price: Some(1_000_000_000),
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                ),
                log_index: 2,
                symbol: tokenized_symbol!("MSFT0x"),
                equity_token: Address::ZERO,
                amount: 0.4,
                direction: Direction::Buy,
                price: Usdc::new(305.0).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: Some(42500),
                effective_gas_price: Some(1_080_000_000),
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                ),
                log_index: 3,
                symbol: tokenized_symbol!("MSFT0x"),
                equity_token: Address::ZERO,
                amount: 0.5,
                direction: Direction::Buy,
                price: Usdc::new(310.0).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: Some(44000),
                effective_gas_price: Some(1_180_000_000),
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
        ];

        // Add first two trades - should not trigger execution
        let result1 = process_trade_with_tx(&pool, trades[0].clone())
            .await
            .unwrap();
        assert!(result1.is_none());

        let result2 = process_trade_with_tx(&pool, trades[1].clone())
            .await
            .unwrap();
        assert!(result2.is_none());

        // Third trade should trigger execution
        let result3 = process_trade_with_tx(&pool, trades[2].clone())
            .await
            .unwrap();
        let execution = result3.unwrap();
        let execution_id = execution.id.unwrap();

        // Verify all trades are linked to the execution
        let links = TradeExecutionLink::find_by_execution_id(&pool, execution_id)
            .await
            .unwrap();
        assert_eq!(links.len(), 3);

        // With Shares threshold, execution is floored to 1.0 share
        // Verify total contribution equals floored execution shares
        let total_contribution: f64 = links
            .iter()
            .map(TradeExecutionLink::contributed_shares)
            .sum();
        assert!((total_contribution - 1.0).abs() < f64::EPSILON);

        // Verify chronological allocation: 0.3 + 0.4 + 0.3 = 1.0
        // Trade 1: 0.3 (fully used), Trade 2: 0.4 (fully used), Trade 3: 0.3 (partially used)
        let mut contributions: Vec<_> = links
            .iter()
            .map(TradeExecutionLink::contributed_shares)
            .collect();
        contributions.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert!((contributions[0] - 0.3).abs() < f64::EPSILON);
        assert!((contributions[1] - 0.3).abs() < f64::EPSILON); // Only 0.3 of 0.5 needed
        assert!((contributions[2] - 0.4).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_audit_trail_completeness() {
        let pool = setup_test_db().await;

        // Create trades that will accumulate before triggering execution
        let trades = vec![
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                ),
                log_index: 1,
                symbol: tokenized_symbol!("AAPL0x"),
                equity_token: Address::ZERO,
                amount: 0.4, // Below threshold
                direction: Direction::Sell,
                price: Usdc::new(150.0).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: Some(40000),
                effective_gas_price: Some(950_000_000),
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                ),
                log_index: 2,
                symbol: tokenized_symbol!("AAPL0x"),
                equity_token: Address::ZERO,
                amount: 0.8, // Combined: 0.4 + 0.8 = 1.2, triggers execution of 1 share
                direction: Direction::Sell,
                price: Usdc::new(155.0).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: Some(41500),
                effective_gas_price: Some(1_020_000_000),
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
        ];

        // Add first trade - no execution
        let result1 = process_trade_with_tx(&pool, trades[0].clone())
            .await
            .unwrap();
        assert!(result1.is_none());

        // Add second trade - triggers execution
        let result2 = process_trade_with_tx(&pool, trades[1].clone())
            .await
            .unwrap();
        let execution = result2.unwrap();

        let execution_id = execution.id.unwrap();

        // Verify both trades are linked to the execution
        let links = TradeExecutionLink::find_by_execution_id(&pool, execution_id)
            .await
            .unwrap();
        assert_eq!(links.len(), 2);

        // With Shares threshold, execution is floored to 1.0 share
        // Contributions: 0.4 (fully used) + 0.6 (partial from 0.8) = 1.0
        let total: f64 = links
            .iter()
            .map(TradeExecutionLink::contributed_shares)
            .sum();
        assert!((total - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_linkage_with_fractional_shares() {
        let pool = setup_test_db().await;

        // Create a trade
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1010101010101010101010101010101010101010101010101010101010101010"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("TSLA0x"),
            equity_token: Address::ZERO,
            amount: 1.2,
            direction: Direction::Buy,
            price: Usdc::new(800.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(47500),
            effective_gas_price: Some(1_380_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Add trade and trigger execution
        let execution = process_trade_with_tx(&pool, trade).await.unwrap().unwrap();
        let execution_id = execution.id.unwrap();

        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );

        // Verify linkage shows floored contribution (1.0)
        let links = TradeExecutionLink::find_by_execution_id(&pool, execution_id)
            .await
            .unwrap();
        assert_eq!(links.len(), 1);
        assert!((links[0].contributed_shares() - 1.0).abs() < f64::EPSILON);

        // Verify residual 0.2 shares remain (1.2 - 1.0 = 0.2)
        let (calculator, _) = find_by_symbol(&pool, "TSLA").await.unwrap().unwrap();
        assert!((calculator.accumulated_long - 0.2).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_cross_direction_linkage_isolation() {
        let pool = setup_test_db().await;

        // Create trades in both directions for different symbols to avoid unique constraint
        let buy_trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2020202020202020202020202020202020202020202020202020202020202020"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Buy,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(48500),
            effective_gas_price: Some(1_420_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let sell_trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x3030303030303030303030303030303030303030303030303030303030303030"
            ),
            log_index: 2,
            symbol: tokenized_symbol!("MSFT0x"), // Different symbol
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(155.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(49500),
            effective_gas_price: Some(1_480_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Execute both trades
        let buy_result = process_trade_with_tx(&pool, buy_trade).await.unwrap();
        let sell_result = process_trade_with_tx(&pool, sell_trade).await.unwrap();

        let buy_execution = buy_result.unwrap();
        let sell_execution = sell_result.unwrap();

        // Verify each execution is linked to exactly one trade
        let buy_links = TradeExecutionLink::find_by_execution_id(&pool, buy_execution.id.unwrap())
            .await
            .unwrap();
        let sell_links =
            TradeExecutionLink::find_by_execution_id(&pool, sell_execution.id.unwrap())
                .await
                .unwrap();

        assert_eq!(buy_links.len(), 1);
        assert_eq!(sell_links.len(), 1);

        // Verify no cross-contamination (different trades linked to different executions)
        assert_ne!(buy_links[0].trade_id(), sell_links[0].trade_id());
    }

    #[tokio::test]
    async fn test_stale_execution_cleanup_clears_block() {
        let pool = setup_test_db().await;

        // Create a submitted execution that is stale
        let stale_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Submitted {
                order_id: "123456".to_string(),
            },
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = stale_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Set up accumulator with pending execution
        let calculator = PositionCalculator::new();
        save_within_transaction(
            &mut sql_tx,
            &symbol!("AAPL"),
            &calculator,
            Some(execution_id),
        )
        .await
        .unwrap();

        // Manually set last_updated to be stale (15 minutes ago)
        sqlx::query!(
            "UPDATE trade_accumulators SET last_updated = datetime('now', '-15 minutes') WHERE symbol = ?1",
            "AAPL"
        )
        .execute(sql_tx.as_mut())
        .await
        .unwrap();

        sql_tx.commit().await.unwrap();

        // Now process a new trade - it should clean up the stale execution
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1234567890123456789012345678901234567890123456789012345678901234"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(50000),
            effective_gas_price: Some(1_500_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Should succeed and create new execution (because stale one was cleaned up)
        let result = process_trade_with_tx(&pool, trade).await.unwrap();
        assert!(result.is_some());
        let new_execution = result.unwrap();
        assert_eq!(new_execution.symbol, Symbol::new("AAPL").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            new_execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );

        // Verify the stale execution was marked as failed
        let stale_executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Failed,
            None,
        )
        .await
        .unwrap();
        assert_eq!(stale_executions.len(), 1);
        assert_eq!(stale_executions[0].id.unwrap(), execution_id);

        // Verify the new execution was created and is pending
        let pending_executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Pending,
            None,
        )
        .await
        .unwrap();
        assert_eq!(pending_executions.len(), 1);
        assert_ne!(pending_executions[0].id.unwrap(), execution_id);
    }

    #[tokio::test]
    async fn test_stale_pending_execution_cleanup() {
        let pool = setup_test_db().await;

        // Create a pending execution that is stale (simulates deployment restart mid-execution)
        let stale_pending_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("NVDA").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = stale_pending_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Set up accumulator with pending execution
        let calculator = PositionCalculator::new();
        save_within_transaction(
            &mut sql_tx,
            &symbol!("NVDA"),
            &calculator,
            Some(execution_id),
        )
        .await
        .unwrap();

        // Manually set last_updated to be stale (15 minutes ago)
        sqlx::query!(
            "UPDATE trade_accumulators SET last_updated = datetime('now', '-15 minutes') WHERE symbol = ?1",
            "NVDA"
        )
        .execute(sql_tx.as_mut())
        .await
        .unwrap();

        sql_tx.commit().await.unwrap();

        // Now process a new trade - it should clean up the stale PENDING execution
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("NVDA0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Sell,
            price: Usdc::new(140.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(51000),
            effective_gas_price: Some(1_550_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Should succeed and create new execution (because stale PENDING one was cleaned up)
        let result = process_trade_with_tx(&pool, trade).await.unwrap();
        assert!(result.is_some());
        let new_execution = result.unwrap();
        assert_eq!(new_execution.symbol, Symbol::new("NVDA").unwrap());
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            new_execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );

        // Verify the stale PENDING execution was marked as failed
        let failed_executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("NVDA").unwrap()),
            OrderStatus::Failed,
            None,
        )
        .await
        .unwrap();
        assert_eq!(failed_executions.len(), 1);
        assert_eq!(failed_executions[0].id.unwrap(), execution_id);

        // Verify the new execution was created and is pending
        let pending_executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("NVDA").unwrap()),
            OrderStatus::Pending,
            None,
        )
        .await
        .unwrap();
        assert_eq!(pending_executions.len(), 1);
        assert_ne!(pending_executions[0].id.unwrap(), execution_id);
    }

    #[tokio::test]
    async fn test_stale_execution_cleanup_timeout_boundary() {
        let pool = setup_test_db().await;

        // Create executions at different ages
        let recent_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("MSFT").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Submitted {
                order_id: "recent123".to_string(),
            },
        };

        let stale_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Submitted {
                order_id: "stale456".to_string(),
            },
        };

        let mut sql_tx = pool.begin().await.unwrap();

        let recent_id = recent_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        let stale_id = stale_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Set up accumulators
        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol!("MSFT"), &calculator, Some(recent_id))
            .await
            .unwrap();
        save_within_transaction(&mut sql_tx, &symbol!("TSLA"), &calculator, Some(stale_id))
            .await
            .unwrap();

        // Make TSLA accumulator stale (15 minutes ago) but leave MSFT recent
        sqlx::query!(
            "UPDATE trade_accumulators SET last_updated = datetime('now', '-15 minutes') WHERE symbol = ?1",
            "TSLA"
        )
        .execute(sql_tx.as_mut())
        .await
        .unwrap();

        sql_tx.commit().await.unwrap();

        // Test cleanup only affects stale execution (TSLA)
        let mut test_tx = pool.begin().await.unwrap();
        clean_up_stale_executions(&mut test_tx, &symbol!("TSLA"))
            .await
            .unwrap();
        test_tx.commit().await.unwrap();

        // Verify recent execution (MSFT) is still submitted
        let msft_submitted = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("MSFT").unwrap()),
            OrderStatus::Submitted,
            None,
        )
        .await
        .unwrap();
        assert_eq!(msft_submitted.len(), 1);
        assert_eq!(msft_submitted[0].id.unwrap(), recent_id);

        // Verify stale execution (TSLA) was failed
        let tsla_failed = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("TSLA").unwrap()),
            OrderStatus::Failed,
            None,
        )
        .await
        .unwrap();
        assert_eq!(tsla_failed.len(), 1);
        assert_eq!(tsla_failed[0].id.unwrap(), stale_id);

        // Verify TSLA accumulator pending_execution_id was cleared
        let (_, pending_id) = find_by_symbol(&pool, "TSLA").await.unwrap().unwrap();
        assert!(pending_id.is_none());

        // Verify MSFT accumulator pending_execution_id is still set
        let (_, msft_pending_id) = find_by_symbol(&pool, "MSFT").await.unwrap().unwrap();
        assert_eq!(msft_pending_id, Some(recent_id));
    }

    #[tokio::test]
    async fn test_no_stale_executions_cleanup_is_noop() {
        let pool = setup_test_db().await;

        // Create only recent executions (not stale)
        let recent_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("NVDA").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(2))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Submitted {
                order_id: "recent789".to_string(),
            },
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = recent_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(
            &mut sql_tx,
            &symbol!("NVDA"),
            &calculator,
            Some(execution_id),
        )
        .await
        .unwrap();
        sql_tx.commit().await.unwrap();

        // Run cleanup - should be no-op
        let mut test_tx = pool.begin().await.unwrap();
        clean_up_stale_executions(&mut test_tx, &symbol!("NVDA"))
            .await
            .unwrap();
        test_tx.commit().await.unwrap();

        // Verify execution is still submitted (not failed)
        let submitted_executions = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("NVDA").unwrap()),
            OrderStatus::Submitted,
            None,
        )
        .await
        .unwrap();
        assert_eq!(submitted_executions.len(), 1);
        assert_eq!(submitted_executions[0].id.unwrap(), execution_id);

        // Verify accumulator pending_execution_id is still set
        let (_, pending_id) = find_by_symbol(&pool, "NVDA").await.unwrap().unwrap();
        assert_eq!(pending_id, Some(execution_id));
    }

    #[tokio::test]
    async fn test_check_all_accumulated_positions_finds_ready_symbols() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());

        // Create some accumulated positions using the normal flow
        let aapl_trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.8,
            direction: Direction::Sell,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(43200),
            effective_gas_price: Some(1_120_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, aapl_trade).await.unwrap();
        assert!(result.is_none()); // Should not execute yet (below 1.0)

        // Verify AAPL has accumulated position but no pending execution
        let (aapl_calc, aapl_pending) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        assert!((aapl_calc.accumulated_short - 0.8).abs() < f64::EPSILON); // SELL creates short exposure
        assert!(aapl_pending.is_none());

        // Run the function - should not create any executions since 0.8 < 1.0
        let executions =
            check_all_accumulated_positions(&pool, &dual_write_context, SupportedExecutor::Schwab)
                .await
                .unwrap();
        assert_eq!(executions.len(), 0);

        // Verify AAPL state unchanged
        let (aapl_calc, aapl_pending) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        assert!((aapl_calc.accumulated_short - 0.8).abs() < f64::EPSILON); // SELL creates short exposure
        assert!(aapl_pending.is_none());
    }

    #[tokio::test]
    async fn test_check_all_accumulated_positions_no_ready_positions() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());

        // Run the function on empty database
        let executions =
            check_all_accumulated_positions(&pool, &dual_write_context, SupportedExecutor::Schwab)
                .await
                .unwrap();

        // Should create no executions
        assert_eq!(executions.len(), 0);
    }

    #[tokio::test]
    async fn test_check_all_accumulated_positions_skips_pending_executions() {
        let pool = setup_test_db().await;

        // Create a pending execution first
        let pending_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = pending_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // AAPL: Has enough accumulated but already has pending execution (should skip)
        let aapl_calculator = PositionCalculator::with_positions(1.5, 0.0);
        save_within_transaction(
            &mut sql_tx,
            &symbol!("AAPL"),
            &aapl_calculator,
            Some(execution_id),
        )
        .await
        .unwrap();

        sql_tx.commit().await.unwrap();

        let dual_write_context = DualWriteContext::new(pool.clone());

        // Run the function
        let executions =
            check_all_accumulated_positions(&pool, &dual_write_context, SupportedExecutor::Schwab)
                .await
                .unwrap();

        // Should create no executions since AAPL has pending execution
        assert_eq!(executions.len(), 0);

        // Verify AAPL was unchanged (still has pending execution)
        let (aapl_calc, aapl_pending) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
        assert!((aapl_calc.accumulated_long - 1.5).abs() < f64::EPSILON); // Unchanged
        assert_eq!(aapl_pending, Some(execution_id)); // Still has same pending execution
    }

    #[tokio::test]
    async fn test_trades_with_different_markers_accumulate_together() {
        let pool = setup_test_db().await;

        // Create three trades with different markers but same base symbol (GME)
        let trade_0x = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xaaaa111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("GME0x"),
            equity_token: Address::ZERO,
            amount: 0.6,
            direction: Direction::Sell,
            price: Usdc::new(120.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(39000),
            effective_gas_price: Some(900_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let trade_s1 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xbbbb222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("GMEs1"),
            equity_token: Address::ZERO,
            amount: 0.3,
            direction: Direction::Sell,
            price: Usdc::new(100.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(40500),
            effective_gas_price: Some(980_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let trade_t = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xcccc333333333333333333333333333333333333333333333333333333333333"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("tGME"),
            equity_token: Address::ZERO,
            amount: 0.2,
            direction: Direction::Sell,
            price: Usdc::new(110.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(41000),
            effective_gas_price: Some(1_000_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Process first trade (GME0x) - should not trigger execution
        let result1 = process_trade_with_tx(&pool, trade_0x).await.unwrap();
        assert!(result1.is_none());

        // Verify accumulation for GME base symbol
        let (calculator, pending) = find_by_symbol(&pool, "GME").await.unwrap().unwrap();
        assert!((calculator.accumulated_short - 0.6).abs() < f64::EPSILON);
        assert!((calculator.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert_eq!(pending, None);

        // Process second trade (GMEs1) - should not trigger execution yet
        let result2 = process_trade_with_tx(&pool, trade_s1).await.unwrap();
        assert!(result2.is_none());

        // Verify accumulation increased
        let (calculator2, pending2) = find_by_symbol(&pool, "GME").await.unwrap().unwrap();
        assert!((calculator2.accumulated_short - 0.9).abs() < f64::EPSILON);
        assert!((calculator2.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert_eq!(pending2, None);

        // Process third trade (tGME) - should trigger execution since total is 1.1 shares
        let result3 = process_trade_with_tx(&pool, trade_t).await.unwrap();
        assert!(result3.is_some());

        let execution = result3.unwrap();
        assert_eq!(execution.symbol, Symbol::new("GME").unwrap()); // Base symbol used for execution
        // With Shares threshold, execution is floored to whole shares
        assert_eq!(
            execution.shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
        assert_eq!(execution.direction, Direction::Buy); // Buy to offset short exposure

        // Verify all three trades contributed to the execution (may be partial contributions)
        let links = TradeExecutionLink::find_by_execution_id(&pool, execution.id.unwrap())
            .await
            .unwrap();
        assert_eq!(links.len(), 3);

        // Verify total contribution equals 1.0 shares (floored)
        let total_contributed: f64 = links
            .iter()
            .map(TradeExecutionLink::contributed_shares)
            .sum();
        assert!((total_contributed - 1.0).abs() < f64::EPSILON);

        // Verify remaining accumulation (0.1 shares residual)
        let (final_calc, final_pending) = find_by_symbol(&pool, "GME").await.unwrap().unwrap();
        assert!((final_calc.accumulated_short - 0.1).abs() < f64::EPSILON); // 0.6 + 0.3 + 0.2 - 1.0 = 0.1 remaining
        assert!((final_calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert_eq!(final_pending, execution.id); // Has pending execution
    }

    #[tokio::test]
    async fn test_stale_execution_cleanup_executes_dual_write_commands() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("TSLA").unwrap();
        dual_write_context
            .position_framework()
            .execute(
                &Position::aggregate_id(&symbol),
                PositionCommand::Initialize {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let onchain_trade = create_test_onchain_trade("TSLA0x", 0x11);
        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &onchain_trade)
            .await
            .unwrap();

        let execution_id = setup_stale_execution(&pool, &dual_write_context, &symbol).await;

        let mut trade = create_test_onchain_trade("TSLA0x", 0x99);
        trade.block_timestamp = None;

        let mut sql_tx = pool.begin().await.unwrap();
        let TradeProcessingResult {
            execution: _,
            cleaned_up_executions,
        } = process_onchain_trade(
            &mut sql_tx,
            &dual_write_context,
            trade,
            SupportedExecutor::Schwab,
        )
        .await
        .unwrap();
        sql_tx.commit().await.unwrap();

        assert_eq!(cleaned_up_executions.len(), 1);
        assert_eq!(cleaned_up_executions[0].execution_id, execution_id);

        for cleaned_up in cleaned_up_executions {
            crate::dual_write::mark_failed(
                &dual_write_context,
                cleaned_up.execution_id,
                cleaned_up.error_reason.clone(),
            )
            .await
            .unwrap();

            crate::dual_write::fail_offchain_order(
                &dual_write_context,
                cleaned_up.execution_id,
                &cleaned_up.symbol,
                cleaned_up.error_reason,
            )
            .await
            .unwrap();
        }

        let aggregate_id = execution_id.to_string();
        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(offchain_order_events.len(), 3);
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
        assert_eq!(offchain_order_events[2], "OffchainOrderEvent::Failed");

        let position_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'Position' AND aggregate_id = ? ORDER BY sequence",
            "TSLA"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert!(
            !position_events.is_empty(),
            "Expected at least 1 Position event (OffChainOrderFailed)"
        );
        assert!(
            position_events
                .iter()
                .any(|e| e == "PositionEvent::OffChainOrderFailed"),
            "Expected PositionEvent::OffChainOrderFailed, got {position_events:?}"
        );
    }

    #[tokio::test]
    async fn test_check_all_accumulated_positions_uses_computed_net_not_stored_column() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO trade_accumulators (
                symbol,
                accumulated_long,
                accumulated_short,
                pending_execution_id,
                last_updated
            )
            VALUES ('SPLG', 6.0, 6.5, NULL, CURRENT_TIMESTAMP)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let row = sqlx::query!(
            "SELECT accumulated_long, accumulated_short
             FROM trade_accumulators
             WHERE symbol = 'SPLG'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let actual_net = row.accumulated_long - row.accumulated_short;
        assert!(
            (actual_net - (-0.5)).abs() < f64::EPSILON,
            "actual net (long - short) should be -0.5"
        );

        let dual_write_context = DualWriteContext::new(pool.clone());

        // Initialize Position for SPLG so the threshold check works
        crate::dual_write::initialize_position(
            &dual_write_context,
            &Symbol::new("SPLG").unwrap(),
            ExecutionThreshold::whole_share(),
        )
        .await
        .ok();

        let executions =
            check_all_accumulated_positions(&pool, &dual_write_context, SupportedExecutor::Schwab)
                .await
                .unwrap();

        assert_eq!(
            executions.len(),
            0,
            "No execution should be created since actual accumulated net is -0.5 < 1.0"
        );
    }

    #[tokio::test]
    async fn test_check_all_accumulated_positions_finds_position_above_threshold() {
        let pool = setup_test_db().await;

        // Use process_trade_with_tx to properly insert onchain trades and update
        // accumulators. We need trades that sum to net >= 1.0.
        let trade1 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xaaaa111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("NVDA0x"),
            equity_token: Address::ZERO,
            amount: 0.8,
            direction: Direction::Sell,
            price: Usdc::new(500.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(40000),
            effective_gas_price: Some(1_000_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, trade1).await.unwrap();
        assert!(result.is_none()); // 0.8 < 1.0, no execution yet

        let trade2 = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xbbbb222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("NVDA0x"),
            equity_token: Address::ZERO,
            amount: 0.7,
            direction: Direction::Sell,
            price: Usdc::new(510.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: Some(40000),
            effective_gas_price: Some(1_000_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // This trade should trigger an execution (0.8 + 0.7 = 1.5 >= 1.0)
        let result = process_trade_with_tx(&pool, trade2).await.unwrap();
        assert!(result.is_some());

        // With Shares threshold, execution is floored to 1.0 share.
        // Remaining accumulated_short = 1.5 - 1.0 = 0.5 shares.
        let (calc, pending_id) = find_by_symbol(&pool, "NVDA").await.unwrap().unwrap();
        assert!(
            (calc.accumulated_short - 0.5).abs() < f64::EPSILON,
            "accumulated_short={}, expected 0.5 (1.5 - 1.0 floored execution)",
            calc.accumulated_short
        );
        assert!(pending_id.is_some());

        let dual_write_context = DualWriteContext::new(pool.clone());

        // Now run check_all_accumulated_positions - should NOT create additional
        // executions since there's already a pending one (and net is now 0.5 < 1.0)
        let executions =
            check_all_accumulated_positions(&pool, &dual_write_context, SupportedExecutor::Schwab)
                .await
                .unwrap();

        assert_eq!(
            executions.len(),
            0,
            "No new execution since one is already pending and net is below threshold"
        );
    }

    #[tokio::test]
    async fn test_mark_execution_as_timed_out_returns_none_when_execution_already_filled() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Create execution and mark it as FILLED
        let execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id))
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Update execution to FILLED status
        let filled_state = OrderState::Filled {
            order_id: "ORD123".to_string(),
            price_cents: 15000,
            executed_at: Utc::now(),
        };
        let mut sql_tx = pool.begin().await.unwrap();
        filled_state
            .store_update(&mut sql_tx, execution_id)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Try to mark as timed out - should return None since already FILLED
        let mut sql_tx = pool.begin().await.unwrap();
        let result = mark_execution_as_timed_out(&mut sql_tx, &symbol, execution_id)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        assert!(
            result.is_none(),
            "Should return None when execution is already FILLED"
        );
    }

    #[tokio::test]
    async fn test_mark_execution_as_timed_out_returns_none_when_pending_execution_id_mismatch() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Create first execution (the one we'll try to mark as timed out)
        // Use a different symbol to avoid unique constraint on offchain_trades
        let symbol_other = Symbol::new("MSFT").unwrap();
        let execution1 = OffchainExecution {
            id: None,
            symbol: symbol_other,
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        // Create second execution (the one that will be in the accumulator for AAPL)
        let execution2 = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(2))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id_1 = execution1
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        let execution_id_2 = execution2
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Create accumulator pointing to execution_id_2 (not execution_id_1)
        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id_2))
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Try to mark execution_id_1 as timed out - should return None since
        // pending_execution_id points to execution_id_2
        let mut sql_tx = pool.begin().await.unwrap();
        let result = mark_execution_as_timed_out(&mut sql_tx, &symbol, execution_id_1)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        assert!(
            result.is_none(),
            "Should return None when pending_execution_id doesn't match"
        );
    }

    #[tokio::test]
    async fn test_mark_execution_as_timed_out_returns_some_when_conditions_met() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Create execution in PENDING state
        let execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id))
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Mark as timed out - should succeed
        let mut sql_tx = pool.begin().await.unwrap();
        let result = mark_execution_as_timed_out(&mut sql_tx, &symbol, execution_id)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        assert!(
            result.is_some(),
            "Should return Some when execution is PENDING and pending_execution_id matches"
        );
        let cleaned = result.unwrap();
        assert_eq!(cleaned.execution_id, execution_id);
        assert_eq!(cleaned.symbol, symbol);
    }

    #[tokio::test]
    async fn test_get_ready_execution_params_returns_none_when_position_not_found() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("NONEXISTENT").unwrap();

        let result =
            get_ready_execution_params(&dual_write_context, &symbol, SupportedExecutor::Schwab)
                .await
                .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_ready_execution_params_returns_none_when_below_threshold() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("AAPL").unwrap();

        crate::dual_write::initialize_position(
            &dual_write_context,
            &symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let trade = OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(0xaa),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.5,
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

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &trade)
            .await
            .unwrap();

        let result =
            get_ready_execution_params(&dual_write_context, &symbol, SupportedExecutor::Schwab)
                .await
                .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_ready_execution_params_returns_params_when_threshold_met() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("AAPL").unwrap();

        crate::dual_write::initialize_position(
            &dual_write_context,
            &symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let trade = OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(0xbb),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
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

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &trade)
            .await
            .unwrap();

        let result =
            get_ready_execution_params(&dual_write_context, &symbol, SupportedExecutor::Schwab)
                .await
                .unwrap();

        assert!(result.is_some());
        let (direction, shares) = result.unwrap();
        assert_eq!(direction, Direction::Sell);
        // Schwab doesn't support fractional shares, so execution is floored
        assert_eq!(
            shares,
            Positive::new(FractionalShares::new(dec!(1))).unwrap()
        );
    }

    #[tokio::test]
    async fn test_check_symbol_for_execution_returns_none_when_no_position() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("NONEXISTENT").unwrap();

        let result = check_symbol_for_execution(
            &pool,
            &dual_write_context,
            &symbol,
            SupportedExecutor::Schwab,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_check_symbol_for_execution_returns_none_when_below_threshold() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("AAPL").unwrap();

        crate::dual_write::initialize_position(
            &dual_write_context,
            &symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let trade = OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(0xcc),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            equity_token: Address::ZERO,
            amount: 0.3,
            direction: Direction::Sell,
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

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &trade)
            .await
            .unwrap();

        let mut sql_tx = pool.begin().await.unwrap();
        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, None)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        let result = check_symbol_for_execution(
            &pool,
            &dual_write_context,
            &symbol,
            SupportedExecutor::Schwab,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_check_symbol_for_execution_skips_when_lease_already_held() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let symbol = Symbol::new("INTC").unwrap();

        crate::dual_write::initialize_position(
            &dual_write_context,
            &symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let trade = OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(0xee),
            log_index: 1,
            symbol: tokenized_symbol!("INTC0x"),
            equity_token: Address::ZERO,
            amount: 1.5,
            direction: Direction::Buy,
            price: Usdc::new(50.0).unwrap(),
            block_timestamp: Some(Utc::now()),
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        crate::dual_write::acknowledge_onchain_fill(&dual_write_context, &trade)
            .await
            .unwrap();

        let existing_execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = existing_execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id))
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        crate::dual_write::place_offchain_order(
            &dual_write_context,
            &OffchainExecution {
                id: Some(execution_id),
                symbol: symbol.clone(),
                shares: Positive::new(FractionalShares::new(Decimal::from(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::Schwab,
                state: OrderState::Pending,
            },
            &symbol,
        )
        .await
        .unwrap();

        let result = check_symbol_for_execution(
            &pool,
            &dual_write_context,
            &symbol,
            SupportedExecutor::Schwab,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }
}
