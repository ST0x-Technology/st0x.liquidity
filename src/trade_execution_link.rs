use chrono::{DateTime, Utc};

#[cfg(test)]
use chrono::TimeZone;

#[cfg(test)]
use sqlx::SqlitePool;

#[cfg(test)]
use crate::onchain::io::TokenizedEquitySymbol;

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

    #[cfg(test)]
    pub(crate) async fn db_count(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
        let row = sqlx::query!("SELECT COUNT(*) as count FROM trade_execution_links")
            .fetch_one(pool)
            .await?;
        Ok(row.count)
    }

    /// Find all executions that a specific trade contributed to
    #[cfg(test)]
    pub(crate) async fn find_executions_for_trade(
        pool: &SqlitePool,
        trade_id: i64,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            SELECT id, trade_id, execution_id, contributed_shares, created_at
            FROM trade_execution_links
            WHERE trade_id = ?1
            ORDER BY created_at ASC
            "#,
            trade_id
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
                created_at: Some(Utc.from_utc_datetime(&row.created_at)),
            })
            .collect())
    }

    /// Find all trades that contributed to a specific execution
    #[cfg(test)]
    pub(crate) async fn find_trades_for_execution(
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
                created_at: Some(Utc.from_utc_datetime(&row.created_at)),
            })
            .collect())
    }

    /// Get the complete audit trail for a base symbol, showing all trade-execution links
    /// for all tokenized variants (e.g., GME0x, GMEs1, tGME all map to base GME)
    #[cfg(test)]
    pub(crate) async fn get_symbol_audit_trail(
        pool: &SqlitePool,
        symbol: &TokenizedEquitySymbol,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let base = symbol.extract_base();
        let suffix_0x = format!("{base}0x");
        let suffix_s1 = format!("{base}s1");
        let prefix_t = format!("t{base}");

        let rows = sqlx::query!(
            r#"
            SELECT tel.id, tel.trade_id, tel.execution_id, tel.contributed_shares, tel.created_at
            FROM trade_execution_links tel
            JOIN onchain_trades ot ON tel.trade_id = ot.id
            WHERE ot.symbol = ?1 OR ot.symbol = ?2 OR ot.symbol = ?3
            ORDER BY tel.created_at ASC
            "#,
            suffix_0x,
            suffix_s1,
            prefix_t
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
                created_at: Some(Utc.from_utc_datetime(&row.created_at)),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offchain::execution::OffchainExecution;
    use crate::onchain::OnchainTrade;
    use crate::test_utils::setup_test_db;
    use crate::tokenized_symbol;
    use alloy::primitives::fixed_bytes;
    use st0x_broker::{Direction, OrderState, Shares, SupportedBroker, Symbol};

    #[tokio::test]
    async fn unique_constraint_prevents_duplicate_links() {
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

        let link1 = TradeExecutionLink::new(trade_id, execution_id, 0.5);
        link1.save_within_transaction(&mut sql_tx).await.unwrap();

        let link2 = TradeExecutionLink::new(trade_id, execution_id, 0.5);
        let result = link2.save_within_transaction(&mut sql_tx).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("UNIQUE constraint failed")
        );
    }
}
