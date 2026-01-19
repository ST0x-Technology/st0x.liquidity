use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use st0x_execution::Symbol;
use tracing::info;

use super::{ExecutionMode, MigrationError};
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand};

#[derive(sqlx::FromRow)]
struct OnchainTradeRow {
    tx_hash: String,
    log_index: i64,
    symbol: String,
    amount: f64,
    direction: String,
    price_usdc: f64,
    created_at: DateTime<Utc>,
}

pub async fn migrate_onchain_trades(
    pool: &SqlitePool,
    cqrs: &SqliteCqrs<Lifecycle<OnChainTrade, Never>>,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = sqlx::query_as::<_, OnchainTradeRow>(
        "SELECT tx_hash, log_index, symbol, amount, direction, price_usdc, created_at
         FROM onchain_trades
         ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?;

    let total = rows.len();
    info!("Found {total} onchain trades to migrate");

    for (idx, row) in rows.into_iter().enumerate() {
        log_progress("onchain trades", idx + 1, total);
        migrate_single_onchain_trade(cqrs, row, execution).await?;
    }

    info!("Migrated {total} onchain trades");
    Ok(total)
}

fn log_progress(entity: &str, progress: usize, total: usize) {
    if progress % 100 == 0 {
        info!("Migrating {entity}: {progress}/{total}");
    }
}

async fn migrate_single_onchain_trade(
    cqrs: &SqliteCqrs<Lifecycle<OnChainTrade, Never>>,
    row: OnchainTradeRow,
    execution: ExecutionMode,
) -> Result<(), MigrationError> {
    let tx_hash: TxHash = row.tx_hash.parse()?;
    let log_index: u64 = row
        .log_index
        .try_into()
        .map_err(|_| MigrationError::NegativeValue {
            field: "log_index".to_string(),
            value: row.log_index,
        })?;

    let aggregate_id = OnChainTrade::aggregate_id(tx_hash, log_index);
    let command = build_onchain_trade_command(&row)?;

    if matches!(execution, ExecutionMode::Commit) {
        cqrs.execute(&aggregate_id, command).await?;
    }

    Ok(())
}

fn build_onchain_trade_command(
    row: &OnchainTradeRow,
) -> Result<OnChainTradeCommand, MigrationError> {
    let symbol = Symbol::new(&row.symbol)?;
    let amount = Decimal::try_from(row.amount)?;
    let direction = row.direction.parse()?;
    let price_usdc = Decimal::try_from(row.price_usdc)?;

    Ok(OnChainTradeCommand::Migrate {
        symbol,
        amount,
        direction,
        price_usdc,
        block_number: 0,
        block_timestamp: row.created_at,
        gas_used: None,
        pyth_price: None,
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, b256};
    use sqlite_es::sqlite_cqrs;
    use sqlx::SqlitePool;

    use super::{ExecutionMode, migrate_onchain_trades};
    use crate::onchain_trade::OnChainTrade;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_test_trade(
        pool: &SqlitePool,
        tx_hash: TxHash,
        log_index: i64,
        symbol: &str,
        amount: f64,
        direction: &str,
        price_usdc: f64,
    ) {
        let tx_hash_str = tx_hash.to_string();
        sqlx::query!(
            "
            INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ",
            tx_hash_str,
            log_index,
            symbol,
            amount,
            direction,
            price_usdc
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_empty_database() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_single_trade() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(result, 1);

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let expected_id = OnChainTrade::aggregate_id(tx_hash, 0);
        assert_eq!(aggregate_id, expected_id);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_multiple_trades() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let tx_hash1 = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let tx_hash2 = b256!("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321");

        insert_test_trade(&pool, tx_hash1, 0, "AAPL", 10.0, "BUY", 150.50).await;
        insert_test_trade(&pool, tx_hash1, 1, "TSLA", 5.0, "SELL", 200.75).await;
        insert_test_trade(&pool, tx_hash2, 0, "GOOGL", 3.0, "BUY", 2800.00).await;

        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(result, 3);

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_dry_run() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert_eq!(result, 1);

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_invalid_symbol() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::query(
            "CREATE TABLE onchain_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                direction TEXT NOT NULL,
                price_usdc REAL NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

        sqlx::query(
            "
            INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(tx_hash)
        .bind(0_i64)
        .bind("")
        .bind(10.0)
        .bind("BUY")
        .bind(150.50)
        .execute(&pool)
        .await
        .unwrap();

        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            super::MigrationError::EmptySymbol(_)
        ));
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_invalid_decimal_infinity() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::query(
            "CREATE TABLE onchain_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                direction TEXT NOT NULL,
                price_usdc REAL NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

        sqlx::query(
            "
            INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(tx_hash)
        .bind(0_i64)
        .bind("AAPL")
        .bind(10.0)
        .bind("BUY")
        .bind(f64::INFINITY)
        .execute(&pool)
        .await
        .unwrap();

        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            super::MigrationError::InvalidDecimal(_)
        ));
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_invalid_direction() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::query(
            "CREATE TABLE onchain_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                direction TEXT NOT NULL,
                price_usdc REAL NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

        sqlx::query(
            "
            INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(tx_hash)
        .bind(0_i64)
        .bind("AAPL")
        .bind(10.0)
        .bind("INVALID")
        .bind(150.50)
        .execute(&pool)
        .await
        .unwrap();

        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
        let result = migrate_onchain_trades(&pool, &cqrs, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            super::MigrationError::InvalidDirection(_)
        ));
    }
}
