use alloy::primitives::TxHash;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use st0x_broker::{Direction, Symbol};
use std::io;
use tracing::{info, warn};

use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};

#[derive(Debug, Parser)]
#[command(name = "migrate_to_events")]
pub struct MigrationEnv {
    #[clap(long, env = "DATABASE_URL")]
    pub database_url: String,

    #[clap(long, value_enum, default_value = "interactive")]
    pub confirmation: ConfirmationMode,

    #[clap(long, value_enum, default_value = "preserve")]
    pub clean: CleanMode,

    #[clap(long, value_enum, default_value = "commit")]
    pub execution: ExecutionMode,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Invalid symbol: {0}")]
    InvalidSymbol(#[from] st0x_broker::BrokerError),
    #[error("Invalid decimal conversion: {0}")]
    InvalidDecimal(#[from] rust_decimal::Error),
    #[error("Invalid direction: {0}")]
    InvalidDirection(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("User cancelled migration")]
    UserCancelled,
    #[error("Hex parsing error: {0}")]
    FromHex(#[from] alloy::hex::FromHexError),
}

#[derive(Debug, Default)]
pub struct MigrationSummary {
    pub onchain_trades: usize,
    pub positions: usize,
    pub offchain_orders: usize,
    pub schwab_auth: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConfirmationMode {
    Interactive,
    Force,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CleanMode {
    Preserve,
    Delete,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExecutionMode {
    DryRun,
    Commit,
}

pub async fn check_existing_events(
    pool: &SqlitePool,
    aggregate_type: &str,
    confirmation: ConfirmationMode,
) -> Result<(), MigrationError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = ?")
        .bind(aggregate_type)
        .fetch_one(pool)
        .await?;

    if count > 0 && matches!(confirmation, ConfirmationMode::Interactive) {
        warn!("Events detected for {aggregate_type} (count: {count}). Continue? [y/N]");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            return Err(MigrationError::UserCancelled);
        }
    }

    Ok(())
}

pub fn safety_prompt(confirmation: ConfirmationMode) -> Result<(), MigrationError> {
    if matches!(confirmation, ConfirmationMode::Force) {
        return Ok(());
    }

    warn!("⚠️  Create database backup before proceeding! Continue? [y/N]");
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if !input.trim().eq_ignore_ascii_case("y") {
        return Err(MigrationError::UserCancelled);
    }

    Ok(())
}

pub async fn clean_events(
    pool: &SqlitePool,
    confirmation: ConfirmationMode,
) -> Result<(), MigrationError> {
    if matches!(confirmation, ConfirmationMode::Interactive) {
        warn!("⚠️  This will DELETE all events! Type 'DELETE' to confirm:");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.trim() != "DELETE" {
            return Err(MigrationError::UserCancelled);
        }
    }

    let deleted_events = sqlx::query("DELETE FROM events")
        .execute(pool)
        .await?
        .rows_affected();

    let deleted_snapshots = sqlx::query("DELETE FROM snapshots")
        .execute(pool)
        .await?
        .rows_affected();

    info!("Deleted {deleted_events} events and {deleted_snapshots} snapshots from event store");

    Ok(())
}

pub async fn run_migration(
    pool: &SqlitePool,
    env: &MigrationEnv,
) -> Result<MigrationSummary, MigrationError> {
    match env.execution {
        ExecutionMode::DryRun => {
            info!("Starting migration in DRY-RUN mode - no events will be persisted");
        }
        ExecutionMode::Commit => {
            info!("Starting migration...");
        }
    }

    check_existing_events(pool, "OnChainTrade", env.confirmation).await?;
    check_existing_events(pool, "Position", env.confirmation).await?;
    check_existing_events(pool, "OffchainOrder", env.confirmation).await?;
    check_existing_events(pool, "SchwabAuth", env.confirmation).await?;

    safety_prompt(env.confirmation)?;

    if matches!(env.clean, CleanMode::Delete) {
        clean_events(pool, env.confirmation).await?;
    }

    let dry_run = matches!(env.execution, ExecutionMode::DryRun);
    let onchain_trades = migrate_onchain_trades(pool, dry_run).await?;

    Ok(MigrationSummary {
        onchain_trades,
        positions: 0,
        offchain_orders: 0,
        schwab_auth: false,
    })
}

pub async fn migrate_onchain_trades(
    pool: &SqlitePool,
    dry_run: bool,
) -> Result<usize, MigrationError> {
    let rows: Vec<(String, i64, String, f64, String, f64)> = sqlx::query_as(
        "SELECT tx_hash, log_index, symbol, amount, direction, price_usdc
         FROM onchain_trades
         ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?;

    let total = rows.len();
    info!("Found {total} onchain trades to migrate");

    for (idx, (tx_hash, log_index, symbol, amount, direction, price_usdc)) in
        rows.into_iter().enumerate()
    {
        let progress = idx + 1;
        if progress % 100 == 0 {
            info!("Migrating onchain trades: {progress}/{total}");
        }

        let tx_hash: TxHash = tx_hash.parse()?;
        let aggregate_id = OnChainTrade::aggregate_id(tx_hash, log_index);
        let symbol = Symbol::new(&symbol)?;
        let amount = Decimal::try_from(amount)?;
        let direction = match direction.as_str() {
            "BUY" => Direction::Buy,
            "SELL" => Direction::Sell,
            _ => return Err(MigrationError::InvalidDirection(direction)),
        };
        let price_usdc = Decimal::try_from(price_usdc)?;

        let event = OnChainTradeEvent::Migrated {
            symbol,
            amount,
            direction,
            price_usdc,
            block_number: 0,
            block_timestamp: Utc::now(),
            gas_used: None,
            pyth_price: None,
            migrated_at: Utc::now(),
        };

        if !dry_run {
            persist_event(pool, &aggregate_id, event).await?;
        }
    }

    info!("Migrated {total} onchain trades");
    Ok(total)
}

async fn persist_event(
    pool: &SqlitePool,
    aggregate_id: &str,
    event: OnChainTradeEvent,
) -> Result<(), MigrationError> {
    let aggregate_type = OnChainTrade::aggregate_type();
    let event_type = event.event_type();
    let event_version = event.event_version();
    let payload = serde_json::to_string(&event)?;

    sqlx::query(
        "INSERT INTO events (
            aggregate_type,
            aggregate_id,
            sequence,
            event_type,
            event_version,
            payload,
            metadata
        )
        VALUES (?, ?, 1, ?, ?, ?, '{}')",
    )
    .bind(&aggregate_type)
    .bind(aggregate_id)
    .bind(&event_type)
    .bind(&event_version)
    .bind(&payload)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;

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
        .bind(tx_hash.to_string())
        .bind(log_index)
        .bind(symbol)
        .bind(amount)
        .bind(direction)
        .bind(price_usdc)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_check_existing_events_empty_database() {
        let pool = create_test_pool().await;

        check_existing_events(&pool, "OnChainTrade", ConfirmationMode::Force)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_existing_events_with_force_mode() {
        let pool = create_test_pool().await;

        sqlx::query(
            "INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('OnChainTrade', 'test:0', 1, 'Migrated', '1.0', '{}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        check_existing_events(&pool, "OnChainTrade", ConfirmationMode::Force)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_clean_events_with_force_mode() {
        let pool = create_test_pool().await;

        sqlx::query(
            "INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('OnChainTrade', 'test:0', 1, 'Migrated', '1.0', '{}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        clean_events(&pool, ConfirmationMode::Force).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_empty_database() {
        let pool = create_test_pool().await;

        let result = migrate_onchain_trades(&pool, false).await.unwrap();

        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_single_trade() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let result = migrate_onchain_trades(&pool, false).await.unwrap();

        assert_eq!(result, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id: String = sqlx::query_scalar(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'",
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

        let tx_hash1 = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let tx_hash2 = b256!("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321");

        insert_test_trade(&pool, tx_hash1, 0, "AAPL", 10.0, "BUY", 150.50).await;
        insert_test_trade(&pool, tx_hash1, 1, "TSLA", 5.0, "SELL", 200.75).await;
        insert_test_trade(&pool, tx_hash2, 0, "GOOGL", 3.0, "BUY", 2800.00).await;

        let result = migrate_onchain_trades(&pool, false).await.unwrap();

        assert_eq!(result, 3);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_dry_run() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let result = migrate_onchain_trades(&pool, true).await.unwrap();

        assert_eq!(result, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_persist_event() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let aggregate_id = OnChainTrade::aggregate_id(tx_hash, 0);

        let event = OnChainTradeEvent::Migrated {
            symbol: Symbol::new("AAPL").unwrap(),
            amount: Decimal::try_from(10.0).unwrap(),
            direction: Direction::Buy,
            price_usdc: Decimal::try_from(150.50).unwrap(),
            block_number: 12345,
            block_timestamp: Utc::now(),
            gas_used: Some(21000),
            pyth_price: None,
            migrated_at: Utc::now(),
        };

        persist_event(&pool, &aggregate_id, event).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let fetched_id: String = sqlx::query_scalar(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(fetched_id, aggregate_id);
    }

    #[tokio::test]
    async fn test_run_migration_dry_run() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let env = MigrationEnv {
            database_url: ":memory:".to_string(),
            confirmation: ConfirmationMode::Force,
            clean: CleanMode::Preserve,
            execution: ExecutionMode::DryRun,
        };

        let result = run_migration(&pool, &env).await.unwrap();

        assert_eq!(result.onchain_trades, 1);
        assert_eq!(result.positions, 0);
        assert_eq!(result.offchain_orders, 0);
        assert!(!result.schwab_auth);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_run_migration_commit() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let env = MigrationEnv {
            database_url: ":memory:".to_string(),
            confirmation: ConfirmationMode::Force,
            clean: CleanMode::Preserve,
            execution: ExecutionMode::Commit,
        };

        let result = run_migration(&pool, &env).await.unwrap();

        assert_eq!(result.onchain_trades, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);
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
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, ?, ?, ?, ?, ?)",
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

        let result = migrate_onchain_trades(&pool, false).await;

        assert!(matches!(
            result.unwrap_err(),
            MigrationError::InvalidSymbol(_)
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
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, ?, ?, ?, ?, ?)",
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

        let result = migrate_onchain_trades(&pool, false).await;

        assert!(matches!(
            result.unwrap_err(),
            MigrationError::InvalidDecimal(_)
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
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, ?, ?, ?, ?, ?)",
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

        let result = migrate_onchain_trades(&pool, false).await;

        assert!(matches!(
            result.unwrap_err(),
            MigrationError::InvalidDirection(_)
        ));
    }

    #[tokio::test]
    async fn test_run_migration_with_clean() {
        let pool = create_test_pool().await;

        let old_hash = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_id = OnChainTrade::aggregate_id(old_hash, 0);

        sqlx::query(
            "INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('OnChainTrade', ?, 1, 'Migrated', '1.0', '{}', '{}')",
        )
        .bind(&old_id)
        .execute(&pool)
        .await
        .unwrap();

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let env = MigrationEnv {
            database_url: ":memory:".to_string(),
            confirmation: ConfirmationMode::Force,
            clean: CleanMode::Delete,
            execution: ExecutionMode::Commit,
        };

        let result = run_migration(&pool, &env).await.unwrap();

        assert_eq!(result.onchain_trades, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id: String = sqlx::query_scalar(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let expected_id = OnChainTrade::aggregate_id(tx_hash, 0);
        assert_eq!(aggregate_id, expected_id);
    }
}
