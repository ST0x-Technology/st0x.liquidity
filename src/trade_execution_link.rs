use chrono::{DateTime, Utc};
use st0x_broker::{Direction, OrderState};

#[cfg(test)]
use crate::error::OnChainError;
#[cfg(test)]
use crate::onchain::io::TokenizedEquitySymbol;
#[cfg(test)]
use sqlx::SqlitePool;
#[cfg(test)]
use st0x_broker::PersistenceError;
#[cfg(test)]
use st0x_broker::{OrderStatus, Shares, SupportedBroker, Symbol};

/// Links individual onchain trades to their contributing Schwab executions.
///
/// Provides complete audit trail for trade batching and execution attribution.
/// Supports many-to-many relationships as multiple trades can contribute to one execution.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeExecutionLink {
    pub id: Option<i64>,
    pub trade_id: i64,
    pub execution_id: i64,
    pub contributed_shares: f64,
    pub created_at: Option<DateTime<Utc>>,
}

impl TradeExecutionLink {
    pub const fn new(trade_id: i64, execution_id: i64, contributed_shares: f64) -> Self {
        Self {
            id: None,
            trade_id,
            execution_id,
            contributed_shares,
            created_at: None,
        }
    }

    /// Save link within an existing transaction
    pub async fn save_within_transaction(
        &self,
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<i64, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            INSERT INTO trade_execution_links (trade_id, execution_id, contributed_shares)
            VALUES (?1, ?2, ?3)
            "#,
            self.trade_id,
            self.execution_id,
            self.contributed_shares
        )
        .execute(&mut **sql_tx)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Find all executions that a specific trade contributed to
    #[cfg(test)]
    pub async fn find_executions_for_trade(
        pool: &SqlitePool,
        trade_id: i64,
    ) -> Result<Vec<ExecutionContribution>, OnChainError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                tel.id,
                tel.execution_id,
                tel.contributed_shares,
                tel.created_at,
                se.symbol,
                se.shares,
                se.direction,
                se.status,
                se.order_id,
                se.price_cents,
                se.executed_at
            FROM trade_execution_links tel
            JOIN offchain_trades se ON tel.execution_id = se.id
            WHERE tel.trade_id = ?1
            ORDER BY tel.created_at ASC
            "#,
            trade_id
        )
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let execution_direction = row.direction.parse::<Direction>().map_err(|e| {
                    OnChainError::Persistence(st0x_broker::PersistenceError::InvalidDirection(e))
                })?;

                let execution_status_enum = row.status.parse::<OrderStatus>().map_err(|e| {
                    OnChainError::Persistence(st0x_broker::PersistenceError::InvalidTradeStatus(
                        e.to_string(),
                    ))
                })?;

                let execution_status = OrderState::from_db_row(
                    execution_status_enum,
                    row.order_id,
                    row.price_cents,
                    row.executed_at,
                )?;

                Ok(ExecutionContribution {
                    link_id: row.id.unwrap(),
                    execution_id: row.execution_id,
                    contributed_shares: row.contributed_shares,
                    execution_symbol: row.symbol,
                    execution_total_shares: row.shares.try_into().map_err(|_| {
                        OnChainError::Persistence(PersistenceError::InvalidShareQuantity(
                            row.shares,
                        ))
                    })?,
                    execution_direction,
                    execution_status,
                    created_at: Some(DateTime::from_naive_utc_and_offset(row.created_at, Utc)),
                })
            })
            .collect()
    }

    /// Find all trades that contributed to a specific execution
    #[cfg(test)]
    pub async fn find_trades_for_execution(
        pool: &SqlitePool,
        execution_id: i64,
    ) -> Result<Vec<TradeContribution>, OnChainError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                tel.id,
                tel.trade_id,
                tel.contributed_shares,
                tel.created_at,
                ot.tx_hash,
                ot.log_index,
                ot.symbol,
                ot.amount,
                ot.direction,
                ot.price_usdc
            FROM trade_execution_links tel
            JOIN onchain_trades ot ON tel.trade_id = ot.id
            WHERE tel.execution_id = ?1
            ORDER BY tel.created_at ASC
            "#,
            execution_id
        )
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(TradeContribution {
                    link_id: row.id.unwrap(),
                    trade_id: row.trade_id,
                    contributed_shares: row.contributed_shares,
                    trade_tx_hash: row.tx_hash,
                    #[allow(clippy::cast_sign_loss)]
                    trade_log_index: row.log_index as u64,
                    trade_symbol: row.symbol,
                    trade_total_amount: row.amount,
                    trade_direction: row.direction,
                    trade_price_usdc: row.price_usdc,
                    created_at: Some(DateTime::from_naive_utc_and_offset(row.created_at, Utc)),
                })
            })
            .collect()
    }

    /// Get complete audit trail for a tokenized equity symbol showing all trades and their executions
    #[cfg(test)]
    pub async fn get_symbol_audit_trail(
        pool: &SqlitePool,
        tokenized_symbol: &TokenizedEquitySymbol,
    ) -> Result<Vec<AuditTrailEntry>, OnChainError> {
        let symbol = tokenized_symbol.to_string();
        let base_symbol = tokenized_symbol.base().to_string();

        let rows = sqlx::query!(
            r#"
            SELECT
                tel.id as link_id,
                tel.contributed_shares,
                tel.created_at as link_created_at,
                ot.id as trade_id,
                ot.tx_hash,
                ot.log_index,
                ot.amount as trade_amount,
                ot.direction as trade_direction,
                ot.price_usdc,
                ot.created_at as trade_created_at,
                se.id as execution_id,
                se.shares as execution_shares,
                se.direction as execution_direction,
                se.status,
                se.order_id,
                se.executed_at
            FROM trade_execution_links tel
            JOIN onchain_trades ot ON tel.trade_id = ot.id
            JOIN offchain_trades se ON tel.execution_id = se.id
            WHERE ot.symbol = ?1 OR se.symbol = ?2
            ORDER BY tel.created_at ASC
            "#,
            symbol,
            base_symbol
        )
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(AuditTrailEntry {
                    link_id: row.link_id,
                    contributed_shares: row.contributed_shares,
                    link_created_at: Some(DateTime::from_naive_utc_and_offset(
                        row.link_created_at,
                        Utc,
                    )),
                    trade_id: row.trade_id,
                    trade_tx_hash: row.tx_hash,
                    #[allow(clippy::cast_sign_loss)]
                    trade_log_index: row.log_index as u64,
                    trade_amount: row.trade_amount,
                    trade_direction: row.trade_direction,
                    trade_price_usdc: row.price_usdc,
                    trade_created_at: row
                        .trade_created_at
                        .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
                    execution_id: row.execution_id,
                    execution_shares: row.execution_shares.try_into().map_err(|_| {
                        OnChainError::Persistence(PersistenceError::InvalidShareQuantity(
                            row.execution_shares,
                        ))
                    })?,
                    execution_direction: row.execution_direction,
                    execution_status: row.status,
                    execution_order_id: row.order_id,
                    execution_executed_at: row
                        .executed_at
                        .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub async fn db_count(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
        let row = sqlx::query!("SELECT COUNT(*) as count FROM trade_execution_links")
            .fetch_one(pool)
            .await?;
        Ok(row.count)
    }
}

/// Represents an execution that a trade contributed to, with execution details
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionContribution {
    pub link_id: i64,
    pub execution_id: i64,
    pub contributed_shares: f64,
    pub execution_symbol: String,
    pub execution_total_shares: u64,
    pub execution_direction: Direction,
    pub execution_status: OrderState,
    pub created_at: Option<DateTime<Utc>>,
}

/// Represents a trade that contributed to an execution, with trade details
#[derive(Debug, Clone, PartialEq)]
pub struct TradeContribution {
    pub link_id: i64,
    pub trade_id: i64,
    pub contributed_shares: f64,
    pub trade_tx_hash: String,
    pub trade_log_index: u64,
    pub trade_symbol: String,
    pub trade_total_amount: f64,
    pub trade_direction: String,
    pub trade_price_usdc: f64,
    pub created_at: Option<DateTime<Utc>>,
}

/// Complete audit trail entry linking trade and execution with all relevant details
#[derive(Debug, Clone, PartialEq)]
pub struct AuditTrailEntry {
    pub link_id: i64,
    pub contributed_shares: f64,
    pub link_created_at: Option<DateTime<Utc>>,

    // Trade details
    pub trade_id: i64,
    pub trade_tx_hash: String,
    pub trade_log_index: u64,
    pub trade_amount: f64,
    pub trade_direction: String,
    pub trade_price_usdc: f64,
    pub trade_created_at: Option<DateTime<Utc>>,

    // Execution details
    pub execution_id: i64,
    pub execution_shares: u64,
    pub execution_direction: String,
    pub execution_status: String,
    pub execution_order_id: Option<String>,
    pub execution_executed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offchain::execution::OffchainExecution;
    use crate::onchain::OnchainTrade;
    use crate::test_utils::setup_test_db;
    use crate::tokenized_symbol;
    use alloy::primitives::fixed_bytes;
    use chrono::Utc;

    #[tokio::test]
    async fn test_trade_execution_link_save_and_find() {
        let pool = setup_test_db().await;

        // Create a trade and execution first
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
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let trade_id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Create and save the link
        let link = TradeExecutionLink::new(trade_id, execution_id, 1.0);
        let link_id = link.save_within_transaction(&mut sql_tx).await.unwrap();
        sql_tx.commit().await.unwrap();

        assert!(link_id > 0);

        // Test finding executions for trade
        let executions = TradeExecutionLink::find_executions_for_trade(&pool, trade_id)
            .await
            .unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].execution_id, execution_id);
        assert!((executions[0].contributed_shares - 1.0).abs() < f64::EPSILON);

        // Test finding trades for execution
        let trades = TradeExecutionLink::find_trades_for_execution(&pool, execution_id)
            .await
            .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, trade_id);
        assert!((trades[0].contributed_shares - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_symbol_audit_trail() {
        let pool = setup_test_db().await;

        // Create multiple trades and executions for the same symbol
        let trades = vec![
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                log_index: 1,
                symbol: tokenized_symbol!("MSFT0x"),
                amount: 0.5,
                direction: Direction::Buy,
                price_usdc: 300.0,
                block_timestamp: None,
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
            OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0x3333333333333333333333333333333333333333333333333333333333333333"
                ),
                log_index: 2,
                symbol: tokenized_symbol!("MSFT0x"),
                amount: 0.8,
                direction: Direction::Buy,
                price_usdc: 305.0,
                block_timestamp: None,
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
        ];

        let execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("MSFT").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Filled {
                executed_at: Utc::now(),
                order_id: "1004055538123".to_string(),
                price_cents: 30250,
            },
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let mut trade_ids = Vec::new();
        for trade in trades {
            let trade_id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
            trade_ids.push(trade_id);
        }
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Create links
        let link1 = TradeExecutionLink::new(trade_ids[0], execution_id, 0.5);
        let link2 = TradeExecutionLink::new(trade_ids[1], execution_id, 0.5); // Only 0.5 of the 0.8 trade contributed

        link1.save_within_transaction(&mut sql_tx).await.unwrap();
        link2.save_within_transaction(&mut sql_tx).await.unwrap();
        sql_tx.commit().await.unwrap();

        // Test audit trail
        let tokenized_symbol = tokenized_symbol!("MSFT0x");
        let audit_trail = TradeExecutionLink::get_symbol_audit_trail(&pool, &tokenized_symbol)
            .await
            .unwrap();
        assert_eq!(audit_trail.len(), 2);

        // Verify total contributed shares add up correctly
        let total_contributed: f64 = audit_trail.iter().map(|e| e.contributed_shares).sum();
        assert!((total_contributed - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_multiple_trades_single_execution() {
        let pool = setup_test_db().await;

        // Simulate multiple small trades that together trigger one execution
        let trades = vec![(0.3, 1u64), (0.4, 2u64), (0.5, 3u64)];

        let execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let mut trade_ids = Vec::new();

        for (amount, log_index) in trades {
            let trade = OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0x4444444444444444444444444444444444444444444444444444444444444444"
                ),
                log_index,
                symbol: tokenized_symbol!("AAPL0x"),
                amount,
                direction: Direction::Sell,
                price_usdc: 150.0,
                block_timestamp: None,
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            };
            let trade_id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
            trade_ids.push(trade_id);
        }

        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Create links showing how each trade contributed
        let links = vec![
            TradeExecutionLink::new(trade_ids[0], execution_id, 0.3),
            TradeExecutionLink::new(trade_ids[1], execution_id, 0.4),
            TradeExecutionLink::new(trade_ids[2], execution_id, 0.3), // Only 0.3 of the 0.5 contributed to this execution
        ];

        for link in links {
            link.save_within_transaction(&mut sql_tx).await.unwrap();
        }
        sql_tx.commit().await.unwrap();

        // Verify all trades contributed to the execution
        let contributing_trades =
            TradeExecutionLink::find_trades_for_execution(&pool, execution_id)
                .await
                .unwrap();
        assert_eq!(contributing_trades.len(), 3);

        // Verify total contributions equal exactly 1 share
        let total_contributions: f64 = contributing_trades
            .iter()
            .map(|t| t.contributed_shares)
            .sum();
        assert!((total_contributions - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_unique_constraint_prevents_duplicate_links() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x5555555555555555555555555555555555555555555555555555555555555555"
            ),
            log_index: 1,
            symbol: tokenized_symbol!("AAPL0x"),
            amount: 1.0,
            direction: Direction::Buy,
            price_usdc: 150.0,
            block_timestamp: None,
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(1).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let trade_id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        // Create first link
        let link1 = TradeExecutionLink::new(trade_id, execution_id, 0.5);
        link1.save_within_transaction(&mut sql_tx).await.unwrap();

        // Try to create duplicate link - should fail
        let link2 = TradeExecutionLink::new(trade_id, execution_id, 0.5);
        let result = link2.save_within_transaction(&mut sql_tx).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("UNIQUE constraint failed"));
    }
}
