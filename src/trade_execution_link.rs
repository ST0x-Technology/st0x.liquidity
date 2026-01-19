use chrono::{DateTime, Utc};

#[cfg(test)]
use sqlx::SqlitePool;

/// Links individual onchain trades to their contributing Schwab executions.
///
/// Provides complete audit trail for trade batching and execution attribution.
/// Supports many-to-many relationships as multiple trades can contribute to one execution.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TradeExecutionLink {
    id: Option<i64>,
    trade_id: i64,
    execution_id: i64,
    contributed_shares: f64,
    created_at: Option<DateTime<Utc>>,
}

impl TradeExecutionLink {
    pub(crate) const fn new(trade_id: i64, execution_id: i64, contributed_shares: f64) -> Self {
        Self {
            id: None,
            trade_id,
            execution_id,
            contributed_shares,
            created_at: None,
        }
    }

    /// Save link within an existing transaction
    pub(crate) async fn save_within_transaction(
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

    #[cfg(test)]
    pub(crate) async fn db_count(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
        let row = sqlx::query!("SELECT COUNT(*) as count FROM trade_execution_links")
            .fetch_one(pool)
            .await?;
        Ok(row.count)
    }

    #[cfg(test)]
    pub(crate) async fn find_by_execution_id(
        pool: &SqlitePool,
        execution_id: i64,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            SELECT id, trade_id, execution_id, contributed_shares, created_at
            FROM trade_execution_links
            WHERE execution_id = ?1
            ORDER BY created_at ASC
            "#,
            execution_id
        )
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| Self {
                id: row.id,
                trade_id: row.trade_id,
                execution_id: row.execution_id,
                contributed_shares: row.contributed_shares,
                created_at: Some(DateTime::from_naive_utc_and_offset(row.created_at, Utc)),
            })
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn trade_id(&self) -> i64 {
        self.trade_id
    }

    #[cfg(test)]
    pub(crate) fn contributed_shares(&self) -> f64 {
        self.contributed_shares
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use chrono::Utc;

    use super::*;
    use alloy::primitives::fixed_bytes;
    use st0x_execution::{Direction, OrderState, Shares, SupportedExecutor, Symbol};

    use crate::offchain::execution::OffchainExecution;
    use crate::onchain::OnchainTrade;
    use crate::onchain::io::Usdc;
    use crate::test_utils::setup_test_db;
    use crate::tokenized_symbol;

    #[tokio::test]
    async fn test_trade_execution_link_save() {
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
            price: Usdc::new(150.0).unwrap(),
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
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let trade_id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let link = TradeExecutionLink::new(trade_id, execution_id, 1.0);
        let link_id = link.save_within_transaction(&mut sql_tx).await.unwrap();
        sql_tx.commit().await.unwrap();

        assert!(link_id > 0);
        assert_eq!(TradeExecutionLink::db_count(&pool).await.unwrap(), 1);

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
                price: Usdc::new(300.0).unwrap(),
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
                price: Usdc::new(305.0).unwrap(),
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
            executor: SupportedExecutor::Schwab,
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
            executor: SupportedExecutor::Schwab,
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
                price: Usdc::new(150.0).unwrap(),
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
            price: Usdc::new(150.0).unwrap(),
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
            executor: SupportedExecutor::Schwab,
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
