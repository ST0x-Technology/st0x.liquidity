use num_traits::ToPrimitive;
use sqlx::SqlitePool;
use tracing::info;

use super::OnchainTrade;
use crate::error::{OnChainError, TradeValidationError};
use crate::lock::{clear_execution_lease, set_pending_execution_id, try_acquire_execution_lease};
use crate::offchain::execution::OffchainExecution;
use crate::onchain::position_calculator::{AccumulationBucket, PositionCalculator};
use crate::trade_execution_link::TradeExecutionLink;
use st0x_broker::{Direction, OrderState, Shares, SupportedBroker, Symbol};

/// Processes an onchain trade through the accumulation system with duplicate detection.
///
/// This function handles the complete trade processing pipeline:
/// 1. Checks for duplicate trades (same tx_hash + log_index) and skips if already processed
/// 2. Saves the trade to the onchain_trades table
/// 3. Updates the position accumulator for the symbol
/// 4. Attempts to create a Schwab execution if position thresholds are met
///
/// Returns `Some(OffchainExecution)` if a Schwab order was created, `None` if the trade
/// was accumulated but didn't trigger an execution (or was a duplicate).
///
/// The transaction must be committed by the caller.
#[tracing::instrument(skip(sql_tx, trade), fields(symbol = %trade.symbol, amount = %trade.amount, direction = ?trade.direction), level = tracing::Level::INFO)]
pub async fn process_onchain_trade(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trade: OnchainTrade,
    broker_type: st0x_broker::SupportedBroker,
) -> Result<Option<OffchainExecution>, OnChainError> {
    // Check if trade already exists to handle duplicates gracefully
    let tx_hash_str = trade.tx_hash.to_string();
    let log_index_i64 = i64::try_from(trade.log_index)
        .map_err(|_| OnChainError::Validation(crate::error::TradeValidationError::NoLogIndex))?;

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
        return Ok(None);
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
    clean_up_stale_executions(sql_tx, base_symbol).await?;

    let execution = if try_acquire_execution_lease(sql_tx, base_symbol).await? {
        let result =
            try_create_execution_if_ready(sql_tx, base_symbol, &mut calculator, broker_type)
                .await?;

        match &result {
            Some(execution) => {
                let execution_id = execution
                    .id
                    .ok_or(st0x_broker::PersistenceError::MissingExecutionId)?;
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

    Ok(execution)
}

#[cfg(test)]
pub async fn find_by_symbol(
    pool: &SqlitePool,
    symbol: &str,
) -> Result<Option<(PositionCalculator, Option<i64>)>, OnChainError> {
    let row = sqlx::query!("SELECT * FROM trade_accumulators WHERE symbol = ?1", symbol)
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
        "SELECT * FROM trade_accumulators WHERE symbol = ?1",
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

pub async fn save_within_transaction(
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
            pending_execution_id = COALESCE(excluded.pending_execution_id, pending_execution_id),
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
    base_symbol: &Symbol,
    calculator: &mut PositionCalculator,
    broker_type: st0x_broker::SupportedBroker,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let Some(execution_type) = calculator.determine_execution_type() else {
        return Ok(None);
    };

    execute_position(
        &mut *sql_tx,
        base_symbol,
        calculator,
        execution_type,
        broker_type,
    )
    .await
}

async fn execute_position(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
    calculator: &mut PositionCalculator,
    execution_type: AccumulationBucket,
    broker_type: st0x_broker::SupportedBroker,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let shares = calculator.calculate_executable_shares()?;

    if shares == 0 {
        return Ok(None);
    }

    let instruction = match execution_type {
        AccumulationBucket::LongExposure => Direction::Sell, // Long exposure -> Schwab SELL to offset
        AccumulationBucket::ShortExposure => Direction::Buy, // Short exposure -> Schwab BUY to offset
    };

    let execution =
        create_execution_within_transaction(sql_tx, base_symbol, shares, instruction, broker_type)
            .await?;

    let execution_id = execution
        .id
        .ok_or(st0x_broker::PersistenceError::MissingExecutionId)?;

    // Find all trades that contributed to this execution and create linkages
    create_trade_execution_linkages(sql_tx, base_symbol, execution_id, execution_type, shares)
        .await?;

    calculator.reduce_accumulation(execution_type, shares)?;

    info!(
        symbol = %base_symbol,
        shares = shares,
        direction = ?instruction,
        execution_type = ?execution_type,
        execution_id = ?execution.id,
        remaining_long = calculator.accumulated_long,
        remaining_short = calculator.accumulated_short,
        "Created Schwab execution with trade linkages"
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
    execution_shares: u64,
) -> Result<(), OnChainError> {
    // Find all trades for this symbol that created this accumulated exposure
    // AccumulationBucket::ShortExposure comes from onchain SELL trades (sold stock, now short)
    // AccumulationBucket::LongExposure comes from onchain BUY trades (bought stock, now long)
    let trade_direction = match execution_type {
        AccumulationBucket::ShortExposure => Direction::Sell, // Short exposure from selling onchain
        AccumulationBucket::LongExposure => Direction::Buy,   // Long exposure from buying onchain
    };

    // Get all trades for this base symbol/direction, regardless of tokenized marker
    let direction_str = match trade_direction {
        Direction::Sell => "SELL",
        Direction::Buy => "BUY",
    };

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

    let mut remaining_execution_shares =
        execution_shares.to_f64().ok_or(OnChainError::Conversion(
            crate::onchain::position_calculator::ConversionError::U64ToF64PrecisionLoss {
                value: execution_shares,
            },
        ))?;

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
    shares: u64,
    direction: Direction,
    broker: SupportedBroker,
) -> Result<OffchainExecution, OnChainError> {
    let execution = OffchainExecution {
        id: None,
        symbol: symbol.clone(),
        shares: Shares::new(shares)?,
        direction,
        broker,
        state: OrderState::Pending,
    };

    let execution_id = execution.save_within_transaction(sql_tx).await?;
    let mut execution_with_id = execution;
    execution_with_id.id = Some(execution_id);

    Ok(execution_with_id)
}

/// Clean up stale executions that have been in PENDING or SUBMITTED state for too long
async fn clean_up_stale_executions(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    base_symbol: &Symbol,
) -> Result<(), OnChainError> {
    const STALE_EXECUTION_MINUTES: i32 = 10;

    // Find executions that are PENDING or SUBMITTED but the accumulator was last updated more than timeout ago
    let timeout_param = format!("-{STALE_EXECUTION_MINUTES} minutes");
    let base_symbol_str = base_symbol.to_string();
    let stale_executions = sqlx::query!(
        r#"
        SELECT se.id, se.symbol
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
    .await?;

    for stale_execution in stale_executions {
        let Some(execution_id) = stale_execution.id else {
            tracing::warn!("Stale execution has null ID, skipping cleanup");
            continue;
        };

        info!(
            symbol = %base_symbol,
            execution_id = execution_id,
            timeout_minutes = STALE_EXECUTION_MINUTES,
            "Cleaning up stale execution"
        );

        // Mark execution as failed due to timeout
        let failed_state = OrderState::Failed {
            failed_at: chrono::Utc::now(),
            error_reason: Some(format!(
                "Execution timed out after {STALE_EXECUTION_MINUTES} minutes without status update"
            )),
        };

        failed_state.store_update(sql_tx, execution_id).await?;

        // Clear the pending execution ID from accumulator
        let base_symbol_str = base_symbol.to_string();
        sqlx::query!(
            "UPDATE trade_accumulators SET pending_execution_id = NULL WHERE symbol = ?1",
            base_symbol_str
        )
        .execute(sql_tx.as_mut())
        .await?;

        // Clear the symbol lock to allow new executions
        crate::lock::clear_execution_lease(sql_tx, base_symbol).await?;

        info!(
            symbol = %base_symbol,
            execution_id = execution_id,
            "Cleared stale execution and released lock"
        );
    }

    Ok(())
}

/// Checks all accumulated positions and executes any that are ready for execution.
///
/// This function is designed to be called after processing batches of events
/// to ensure accumulated positions execute even when no new events arrive for those symbols.
/// It prevents positions from sitting idle indefinitely when they've accumulated
/// enough shares to execute but the triggering trade didn't push them over the threshold.
#[tracing::instrument(skip(pool), fields(broker_type = %broker_type), level = tracing::Level::DEBUG)]
pub async fn check_all_accumulated_positions(
    pool: &SqlitePool,
    broker_type: st0x_broker::SupportedBroker,
) -> Result<Vec<OffchainExecution>, OnChainError> {
    info!("Checking all accumulated positions for ready executions");

    // Query all symbols with net position >= 1.0 shares absolute value
    // and no pending execution
    let ready_symbols = sqlx::query!(
        r#"
        SELECT
            symbol,
            net_position,
            accumulated_long,
            accumulated_short,
            pending_execution_id
        FROM trade_accumulators
        WHERE pending_execution_id IS NULL
          AND ABS(net_position) >= 1.0
        ORDER BY last_updated ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    if ready_symbols.is_empty() {
        info!("No accumulated positions found ready for execution");
        return Ok(vec![]);
    }

    info!(
        "Found {} symbols with positions ready for execution",
        ready_symbols.len()
    );

    let mut executions = Vec::new();

    // Process each symbol individually to respect locking
    for row in ready_symbols {
        let symbol = Symbol::new(&row.symbol)?;
        info!(
            symbol = %symbol,
            accumulated_long = row.accumulated_long,
            accumulated_short = row.accumulated_short,
            net_position = row.net_position,
            "Checking symbol for execution"
        );

        let mut sql_tx = pool.begin().await?;

        // Clean up any stale executions for this symbol
        clean_up_stale_executions(&mut sql_tx, &symbol).await?;

        // Try to acquire execution lease for this symbol
        if try_acquire_execution_lease(&mut sql_tx, &symbol).await? {
            // Re-fetch calculator to get current state
            let mut calculator = get_or_create_within_transaction(&mut sql_tx, &symbol).await?;

            // Check if still ready after potentially concurrent processing
            if let Some(execution_type) = calculator.determine_execution_type() {
                // The linkage system will handle allocating the oldest available trades
                let result = execute_position(
                    &mut sql_tx,
                    &symbol,
                    &mut calculator,
                    execution_type,
                    broker_type,
                )
                .await?;

                if let Some(execution) = &result {
                    let execution_id = execution
                        .id
                        .ok_or(st0x_broker::PersistenceError::MissingExecutionId)?;
                    set_pending_execution_id(&mut sql_tx, &symbol, execution_id).await?;

                    info!(
                        symbol = %symbol,
                        execution_id = ?execution.id,
                        shares = ?execution.shares,
                        direction = ?execution.direction,
                        "Created execution for accumulated position"
                    );

                    executions.push(execution.clone());
                } else {
                    clear_execution_lease(&mut sql_tx, &symbol).await?;
                    info!(
                        symbol = %symbol,
                        "No execution created for symbol (insufficient shares after re-check)"
                    );
                }

                // Save updated calculator state
                let pending_execution_id = result.as_ref().and_then(|e| e.id);
                save_within_transaction(&mut sql_tx, &symbol, &calculator, pending_execution_id)
                    .await?;
            } else {
                clear_execution_lease(&mut sql_tx, &symbol).await?;
                info!(
                    symbol = %symbol,
                    "No execution needed for symbol (insufficient shares after cleanup)"
                );
            }
        } else {
            info!(
                symbol = %symbol,
                "Another worker holds execution lease, skipping"
            );
        }

        sql_tx.commit().await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offchain::execution::find_executions_by_symbol_status_and_broker;
    use crate::symbol;
    use crate::test_utils::setup_test_db;
    use crate::tokenized_symbol;
    use crate::trade_execution_link::TradeExecutionLink;
    use alloy::primitives::fixed_bytes;
    use st0x_broker::{OrderStatus, Symbol};

    // Helper function for tests to handle transaction management
    async fn process_trade_with_tx(
        pool: &SqlitePool,
        trade: OnchainTrade,
    ) -> Result<Option<OffchainExecution>, OnChainError> {
        let mut sql_tx = pool.begin().await?;
        let result =
            process_onchain_trade(&mut sql_tx, trade, st0x_broker::SupportedBroker::Schwab).await?;
        sql_tx.commit().await?;
        Ok(result)
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
            amount: 0.5,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 300.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap());
        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL (short exposure)

        let (calculator, _) = find_by_symbol(&pool, "MSFT").await.unwrap().unwrap();
        assert!((calculator.accumulated_short - 0.5).abs() < f64::EPSILON); // SELL creates short exposure
        assert!((calculator.net_position() - (-0.5)).abs() < f64::EPSILON); // Short position = negative net
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
            amount: 0.3,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
            amount: 0.4,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
            amount: 0.4,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap());
        assert_eq!(execution.direction, Direction::Buy); // Schwab BUY to offset onchain SELL (short exposure)

        let (calculator, _) = find_by_symbol(&pool, "AAPL").await.unwrap().unwrap();
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
            amount: 1.0,
            direction: Direction::Buy,
            price_usdc: 100.0,
            block_timestamp: None,
            created_at: None,
            gas_used: Some(48000),
            effective_gas_price: Some(1_400_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, trade).await;
        // Should succeed because INVALID0x has valid format, even if INVALID isn't a real ticker
        assert!(result.is_ok());
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap());
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
            amount: 1.5,
            direction: Direction::Buy,
            price_usdc: 300.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap());
    }

    #[tokio::test]
    async fn test_database_transaction_rollback_on_execution_save_failure() {
        let pool = setup_test_db().await;

        // First, create a pending execution for AAPL to trigger the unique constraint
        let blocking_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(50).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
            created_at: None,
            gas_used: Some(51000),
            effective_gas_price: Some(1_600_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Attempt to add trade - should fail when trying to save execution due to unique constraint
        let result = process_trade_with_tx(&pool, trade).await;

        // Verify the operation failed due to execution save failure (unique constraint violation)
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
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
        assert_eq!(executions[0].shares, Shares::new(50).unwrap());
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
            amount: 0.8,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
            amount: 0.3,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap());
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
        for attempt in 0..3 {
            match process_trade_with_tx(pool, trade.clone()).await {
                Ok(result) => return Ok(result),
                Err(OnChainError::Persistence(st0x_broker::PersistenceError::Database(
                    sqlx::Error::Database(db_err),
                ))) if db_err.message().contains("database is deadlocked") => {
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(10 * (1 << attempt)))
                            .await;
                        continue;
                    }
                    return Err(OnChainError::Persistence(
                        st0x_broker::PersistenceError::Database(sqlx::Error::Database(db_err)),
                    ));
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    fn create_test_trade(tx_hash_byte: u8, symbol: &str, amount: f64) -> OnchainTrade {
        OnchainTrade {
            id: None,
            tx_hash: alloy::primitives::B256::repeat_byte(tx_hash_byte),
            log_index: 1,
            symbol: tokenized_symbol!(symbol),
            amount,
            direction: Direction::Sell,
            price_usdc: 15000.0,
            block_timestamp: None,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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

        let link_count = TradeExecutionLink::db_count(&pool).await.unwrap();
        assert_eq!(link_count, 1);

        // Find the trade ID to verify linkage
        let trades_for_execution =
            TradeExecutionLink::find_trades_for_execution(&pool, execution_id)
                .await
                .unwrap();
        assert_eq!(trades_for_execution.len(), 1);
        assert!((trades_for_execution[0].contributed_shares - 1.0).abs() < f64::EPSILON);
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
                amount: 0.3,
                direction: Direction::Buy,
                price_usdc: 300.0,
                block_timestamp: None,
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
                amount: 0.4,
                direction: Direction::Buy,
                price_usdc: 305.0,
                block_timestamp: None,
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
                amount: 0.5,
                direction: Direction::Buy,
                price_usdc: 310.0,
                block_timestamp: None,
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
        let contributing_trades =
            TradeExecutionLink::find_trades_for_execution(&pool, execution_id)
                .await
                .unwrap();
        assert_eq!(contributing_trades.len(), 3);

        // Verify total contribution equals execution shares
        let total_contribution: f64 = contributing_trades
            .iter()
            .map(|t| t.contributed_shares)
            .sum();
        assert!((total_contribution - 1.0).abs() < f64::EPSILON);

        // Verify individual contributions match chronological allocation
        let mut contributions = contributing_trades;
        contributions.sort_by(|a, b| a.trade_id.cmp(&b.trade_id));

        assert!((contributions[0].contributed_shares - 0.3).abs() < f64::EPSILON);
        assert!((contributions[1].contributed_shares - 0.4).abs() < f64::EPSILON);
        assert!((contributions[2].contributed_shares - 0.3).abs() < f64::EPSILON); // Only 0.3 of 0.5 needed
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
                amount: 0.4, // Below threshold
                direction: Direction::Sell,
                price_usdc: 150.0,
                block_timestamp: None,
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
                amount: 0.8, // Combined: 0.4 + 0.8 = 1.2, triggers execution of 1 share
                direction: Direction::Sell,
                price_usdc: 155.0,
                block_timestamp: None,
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

        // Test audit trail completeness
        let tokenized_symbol = tokenized_symbol!("AAPL0x");
        let audit_trail = TradeExecutionLink::get_symbol_audit_trail(&pool, &tokenized_symbol)
            .await
            .unwrap();

        assert_eq!(audit_trail.len(), 2); // Both trades should appear

        // Verify audit trail contains complete information
        for entry in &audit_trail {
            assert!(!entry.trade_tx_hash.is_empty());
            assert!(entry.trade_id > 0);
            assert!(entry.execution_id > 0);
            assert!(entry.contributed_shares > 0.0);
            assert_eq!(entry.execution_shares, 1); // Should be 1 whole share
        }

        // Verify total contributions in audit trail
        let total_audit_contribution: f64 = audit_trail.iter().map(|e| e.contributed_shares).sum();
        assert!((total_audit_contribution - 1.0).abs() < f64::EPSILON);

        // Test reverse lookups work
        let execution_id = execution.id.unwrap();
        let executions_for_first_trade = TradeExecutionLink::find_executions_for_trade(&pool, 1) // Assuming first trade has ID 1
            .await
            .unwrap();
        assert_eq!(executions_for_first_trade.len(), 1);
        assert_eq!(executions_for_first_trade[0].execution_id, execution_id);
    }

    #[tokio::test]
    async fn test_linkage_prevents_over_allocation() {
        let pool = setup_test_db().await;

        // Create a trade
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1010101010101010101010101010101010101010101010101010101010101010"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("TSLA0x"),
            amount: 1.2,
            direction: Direction::Buy,
            price_usdc: 800.0,
            block_timestamp: None,
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

        // Verify only 1 share executed, not 1.2
        assert_eq!(execution.shares, Shares::new(1).unwrap());

        // Verify linkage shows correct contribution
        let execution_id = execution.id.unwrap();
        let trades_for_execution =
            TradeExecutionLink::find_trades_for_execution(&pool, execution_id)
                .await
                .unwrap();

        assert_eq!(trades_for_execution.len(), 1);
        assert!((trades_for_execution[0].contributed_shares - 1.0).abs() < f64::EPSILON);

        // Verify the remaining 0.2 is still available for future executions
        let (calculator, _) = find_by_symbol(&pool, "TSLA").await.unwrap().unwrap();
        assert!((calculator.accumulated_long - 0.2).abs() < f64::EPSILON); // BUY creates long exposure
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
            amount: 1.5,
            direction: Direction::Buy,
            price_usdc: 150.0,
            block_timestamp: None,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 155.0,
            block_timestamp: None,
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

        // Verify each execution is only linked to trades of matching direction
        let buy_execution_trades =
            TradeExecutionLink::find_trades_for_execution(&pool, buy_execution.id.unwrap())
                .await
                .unwrap();
        let sell_execution_trades =
            TradeExecutionLink::find_trades_for_execution(&pool, sell_execution.id.unwrap())
                .await
                .unwrap();

        assert_eq!(buy_execution_trades.len(), 1);
        assert_eq!(sell_execution_trades.len(), 1);
        assert_eq!(buy_execution_trades[0].trade_direction, "BUY");
        assert_eq!(sell_execution_trades[0].trade_direction, "SELL");

        // Verify no cross-contamination
        assert_ne!(
            buy_execution_trades[0].trade_id,
            sell_execution_trades[0].trade_id
        );
    }

    #[tokio::test]
    async fn test_stale_execution_cleanup_clears_block() {
        let pool = setup_test_db().await;

        // Create a submitted execution that is stale
        let stale_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
            created_at: None,
            gas_used: Some(50000),
            effective_gas_price: Some(1_500_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, trade).await.unwrap();

        // Should succeed and create new execution (because stale one was cleaned up)
        assert!(result.is_some());
        let new_execution = result.unwrap();
        assert_eq!(new_execution.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(new_execution.shares, Shares::new(1).unwrap());

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
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
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
            amount: 1.5,
            direction: Direction::Sell,
            price_usdc: 140.0,
            block_timestamp: None,
            created_at: None,
            gas_used: Some(51000),
            effective_gas_price: Some(1_550_000_000),
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let result = process_trade_with_tx(&pool, trade).await.unwrap();

        // Should succeed and create new execution (because stale PENDING one was cleaned up)
        assert!(result.is_some());
        let new_execution = result.unwrap();
        assert_eq!(new_execution.symbol, Symbol::new("NVDA").unwrap());
        assert_eq!(new_execution.shares, Shares::new(1).unwrap());

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
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: st0x_broker::SupportedBroker::Schwab,
            state: OrderState::Submitted {
                order_id: "recent123".to_string(),
            },
        };

        let stale_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Sell,
            broker: st0x_broker::SupportedBroker::Schwab,
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
            shares: Shares::new(2).unwrap(),
            direction: Direction::Buy,
            broker: st0x_broker::SupportedBroker::Schwab,
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

        // Create some accumulated positions using the normal flow
        let aapl_trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            amount: 0.8,
            direction: Direction::Sell,
            price_usdc: 150.0,
            block_timestamp: None,
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
            check_all_accumulated_positions(&pool, st0x_broker::SupportedBroker::Schwab)
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

        // Run the function on empty database
        let executions =
            check_all_accumulated_positions(&pool, st0x_broker::SupportedBroker::Schwab)
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
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: st0x_broker::SupportedBroker::Schwab,
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

        // Run the function
        let executions =
            check_all_accumulated_positions(&pool, st0x_broker::SupportedBroker::Schwab)
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
            amount: 0.6,
            direction: Direction::Sell,
            price_usdc: 120.0,
            block_timestamp: None,
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
            amount: 0.3,
            direction: Direction::Sell,
            price_usdc: 100.0,
            block_timestamp: None,
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
            amount: 0.2,
            direction: Direction::Sell,
            price_usdc: 110.0,
            block_timestamp: None,
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
        assert_eq!(execution.shares, Shares::new(1).unwrap()); // 1 whole share executed
        assert_eq!(execution.direction, Direction::Buy); // Buy to offset short exposure

        // Verify all three trades contributed to the same execution
        let links = TradeExecutionLink::find_trades_for_execution(&pool, execution.id.unwrap())
            .await
            .unwrap();
        assert_eq!(links.len(), 3);

        // Verify the allocation amounts
        let total_contributed: f64 = links.iter().map(|l| l.contributed_shares).sum();
        assert!((total_contributed - 1.0).abs() < f64::EPSILON);

        // Verify remaining accumulation
        let (final_calc, final_pending) = find_by_symbol(&pool, "GME").await.unwrap().unwrap();
        assert!((final_calc.accumulated_short - 0.1).abs() < f64::EPSILON); // 0.6 + 0.3 + 0.2 - 1.0 = 0.1 remaining
        assert!((final_calc.accumulated_long - 0.0).abs() < f64::EPSILON);
        assert_eq!(final_pending, execution.id); // Has pending execution

        // Verify audit trail shows all three marker types
        let tokenized_gme_0x = tokenized_symbol!("GME0x");
        let audit_trail = TradeExecutionLink::get_symbol_audit_trail(&pool, &tokenized_gme_0x)
            .await
            .unwrap();

        // Should include all three trades in the audit trail
        assert_eq!(audit_trail.len(), 3);
    }
}
