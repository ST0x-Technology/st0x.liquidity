use alloy::primitives::TxHash;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use st0x_broker::{SupportedBroker, Symbol};
use std::io;
use tracing::{info, warn};

use crate::offchain_order::{InvalidMigratedOrderStatus, OffchainOrder, OffchainOrderEvent};
use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
use crate::position::{
    BrokerOrderId, ExecutionThreshold, FractionalShares, NegativePriceCents, Position,
    PositionEvent, PriceCents,
};

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
    InvalidDirection(#[from] st0x_broker::InvalidDirectionError),
    #[error("Invalid order status: {0}")]
    InvalidOrderStatus(#[from] InvalidMigratedOrderStatus),
    #[error("Negative price in cents: {0}")]
    NegativePriceCents(#[from] NegativePriceCents),
    #[error("Negative value in field that must be non-negative: {field} = {value}")]
    NegativeValue { field: String, value: i64 },
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

    let onchain_trades = migrate_onchain_trades(pool, env.execution).await?;
    let positions = migrate_positions(pool, env.execution).await?;
    let offchain_orders = migrate_offchain_orders(pool, env.execution).await?;

    Ok(MigrationSummary {
        onchain_trades,
        positions,
        offchain_orders,
        schwab_auth: false,
    })
}

#[derive(sqlx::FromRow)]
struct OnchainTradeRow {
    tx_hash: String,
    log_index: i64,
    symbol: String,
    amount: f64,
    direction: String,
    price_usdc: f64,
}

pub async fn migrate_onchain_trades(
    pool: &SqlitePool,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = sqlx::query_as::<_, OnchainTradeRow>(
        "SELECT tx_hash, log_index, symbol, amount, direction, price_usdc
         FROM onchain_trades
         ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?;

    let total = rows.len();
    info!("Found {total} onchain trades to migrate");

    for (idx, row) in rows.into_iter().enumerate() {
        let progress = idx + 1;
        if progress % 100 == 0 {
            info!("Migrating onchain trades: {progress}/{total}");
        }

        let tx_hash: TxHash = row.tx_hash.parse()?;
        let aggregate_id = OnChainTrade::aggregate_id(tx_hash, row.log_index);
        let symbol = Symbol::new(&row.symbol)?;
        let amount = Decimal::try_from(row.amount)?;
        let direction = row.direction.parse()?;
        let price_usdc = Decimal::try_from(row.price_usdc)?;

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

        match execution {
            ExecutionMode::Commit => {
                persist_event(pool, &aggregate_id, event).await?;
            }
            ExecutionMode::DryRun => {}
        }
    }

    info!("Migrated {total} onchain trades");
    Ok(total)
}

#[derive(sqlx::FromRow)]
struct PositionRow {
    symbol: String,
    net_position: f64,
    accumulated_long: f64,
    accumulated_short: f64,
    pending_execution_id: Option<i64>,
}

pub async fn migrate_positions(
    pool: &SqlitePool,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = sqlx::query_as::<_, PositionRow>(
        "SELECT symbol, net_position, accumulated_long, accumulated_short, pending_execution_id
         FROM trade_accumulators
         ORDER BY symbol ASC",
    )
    .fetch_all(pool)
    .await?;

    let total = rows.len();
    info!("Found {total} positions to migrate");

    let pending_count = rows
        .iter()
        .filter(|row| row.pending_execution_id.is_some())
        .count();

    for (idx, row) in rows.into_iter().enumerate() {
        let processed = idx + 1;
        if processed % 100 == 0 {
            info!("Migrating positions: {processed}/{total}");
        }

        if let Some(exec_id) = row.pending_execution_id {
            warn!(
                "Position {} has pending execution {exec_id} - will be reconciled in dual-write phase",
                row.symbol
            );
        }

        let symbol = Symbol::new(&row.symbol)?;
        let aggregate_id = Position::aggregate_id(&symbol);

        let net_position = Decimal::try_from(row.net_position)?;
        let accumulated_long = Decimal::try_from(row.accumulated_long)?;
        let accumulated_short = Decimal::try_from(row.accumulated_short)?;

        let event = PositionEvent::Migrated {
            symbol,
            net_position: FractionalShares(net_position),
            accumulated_long: FractionalShares(accumulated_long),
            accumulated_short: FractionalShares(accumulated_short),
            threshold: ExecutionThreshold::Shares(Decimal::ONE),
            migrated_at: Utc::now(),
        };

        match execution {
            ExecutionMode::Commit => {
                persist_position_event(pool, &aggregate_id, event).await?;
            }
            ExecutionMode::DryRun => {}
        }
    }

    info!("Migrated {total} positions, {pending_count} with pending executions");
    Ok(total)
}

#[derive(sqlx::FromRow)]
struct OffchainOrderRow {
    id: i64,
    symbol: String,
    shares: i64,
    direction: String,
    order_id: Option<String>,
    price_cents: Option<i64>,
    status: String,
}

pub async fn migrate_offchain_orders(
    pool: &SqlitePool,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = sqlx::query_as::<_, OffchainOrderRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker_order_id as order_id,
            price_cents,
            status
        FROM offchain_trades
        ORDER BY id ASC
        ",
    )
    .fetch_all(pool)
    .await?;

    let total = rows.len();
    info!("Found {total} offchain orders to migrate");

    let status_counts = rows.iter().fold(
        (0usize, 0usize, 0usize, 0usize),
        |(pending, submitted, filled, failed), row| match row.status.as_str() {
            "PENDING" => (pending + 1, submitted, filled, failed),
            "SUBMITTED" => (pending, submitted + 1, filled, failed),
            "FILLED" => (pending, submitted, filled + 1, failed),
            "FAILED" => (pending, submitted, filled, failed + 1),
            _ => (pending, submitted, filled, failed),
        },
    );

    for (idx, row) in rows.into_iter().enumerate() {
        let processed = idx + 1;
        if processed % 100 == 0 {
            info!("Migrating offchain orders: {processed}/{total}");
        }

        let aggregate_id = OffchainOrder::aggregate_id(row.id);
        let symbol = Symbol::new(&row.symbol)?;

        if row.shares < 0 {
            return Err(MigrationError::NegativeValue {
                field: "shares".to_string(),
                value: row.shares,
            });
        }
        let shares = Decimal::from(row.shares);

        let direction = row.direction.parse()?;
        let migrated_status = row.status.parse()?;

        let broker_order_id = row.order_id.map(BrokerOrderId);
        let price_cents = row.price_cents.map(PriceCents::try_from).transpose()?;

        let event = OffchainOrderEvent::Migrated {
            symbol,
            shares: FractionalShares(shares),
            direction,
            broker: SupportedBroker::Schwab,
            status: migrated_status,
            broker_order_id,
            price_cents,
            executed_at: None,
            migrated_at: Utc::now(),
        };

        match execution {
            ExecutionMode::Commit => {
                persist_offchain_order_event(pool, &aggregate_id, event).await?;
            }
            ExecutionMode::DryRun => {}
        }
    }

    let (pending, submitted, filled, failed) = status_counts;
    info!(
        "Migrated {total} offchain orders. Status breakdown: {pending} PENDING, {submitted} SUBMITTED, {filled} FILLED, {failed} FAILED"
    );
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

async fn persist_position_event(
    pool: &SqlitePool,
    aggregate_id: &str,
    event: PositionEvent,
) -> Result<(), MigrationError> {
    let aggregate_type = Position::aggregate_type();
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

async fn persist_offchain_order_event(
    pool: &SqlitePool,
    aggregate_id: &str,
    event: OffchainOrderEvent,
) -> Result<(), MigrationError> {
    let aggregate_type = OffchainOrder::aggregate_type();
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
    use st0x_broker::Direction;

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_migrate_onchain_trades_single_trade() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        insert_test_trade(&pool, tx_hash, 0, "AAPL", 10.0, "BUY", 150.50).await;

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::DryRun)
            .await
            .unwrap();

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit).await;

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit).await;

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

        let result = migrate_onchain_trades(&pool, ExecutionMode::Commit).await;

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

    async fn insert_test_position(
        pool: &SqlitePool,
        symbol: &str,
        net_position: f64,
        accumulated_long: f64,
        accumulated_short: f64,
        pending_execution_id: Option<i64>,
    ) {
        sqlx::query(
            "
            INSERT INTO trade_accumulators (
                symbol,
                net_position,
                accumulated_long,
                accumulated_short,
                pending_execution_id
            )
            VALUES (?, ?, ?, ?, ?)
            ",
        )
        .bind(symbol)
        .bind(net_position)
        .bind(accumulated_long)
        .bind(accumulated_short)
        .bind(pending_execution_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_positions_empty() {
        let pool = create_test_pool().await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_positions_single_position() {
        let pool = create_test_pool().await;

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id: String =
            sqlx::query_scalar("SELECT aggregate_id FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let expected_id = Position::aggregate_id(&symbol);
        assert_eq!(aggregate_id, expected_id);
    }

    #[tokio::test]
    async fn test_migrate_positions_multiple_positions() {
        let pool = create_test_pool().await;

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;
        insert_test_position(&pool, "TSLA", -2.0, 3.0, 5.0, None).await;
        insert_test_position(&pool, "MSFT", 0.0, 0.0, 0.0, None).await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 3);

        let event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(event_count, 3);
    }

    #[tokio::test]
    async fn test_migrate_positions_with_pending_execution() {
        let pool = create_test_pool().await;

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id: String =
            sqlx::query_scalar("SELECT aggregate_id FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let expected_id = Position::aggregate_id(&symbol);
        assert_eq!(aggregate_id, expected_id);
    }

    #[tokio::test]
    async fn test_migrate_positions_dry_run() {
        let pool = create_test_pool().await;

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn test_migrate_positions_invalid_symbol() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        sqlx::query(
            "CREATE TABLE trade_accumulators (
                symbol TEXT PRIMARY KEY NOT NULL,
                net_position REAL NOT NULL DEFAULT 0.0,
                accumulated_long REAL NOT NULL DEFAULT 0.0,
                accumulated_short REAL NOT NULL DEFAULT 0.0,
                pending_execution_id INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO trade_accumulators (
                symbol,
                net_position,
                accumulated_long,
                accumulated_short
            )
            VALUES (?, ?, ?, ?)
            ",
        )
        .bind("")
        .bind(5.5)
        .bind(10.0)
        .bind(4.5)
        .execute(&pool)
        .await
        .unwrap();

        let result = migrate_positions(&pool, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            MigrationError::InvalidSymbol(_)
        ));
    }

    async fn insert_test_order(
        pool: &SqlitePool,
        symbol: &str,
        shares: i64,
        direction: &str,
        order_id: Option<&str>,
        price_cents: Option<i64>,
        status: &str,
    ) {
        let executed_at = match status {
            "FILLED" | "FAILED" => Some(chrono::Utc::now()),
            _ => None,
        };

        sqlx::query(
            "
            INSERT INTO offchain_trades (
                symbol,
                shares,
                direction,
                broker_order_id,
                price_cents,
                status,
                executed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(symbol)
        .bind(shares)
        .bind(direction)
        .bind(order_id)
        .bind(price_cents)
        .bind(status)
        .bind(executed_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_empty() {
        let pool = create_test_pool().await;

        let count = migrate_offchain_orders(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_single_order() {
        let pool = create_test_pool().await;

        insert_test_order(
            &pool,
            "AAPL",
            10,
            "BUY",
            Some("ORDER123"),
            Some(15050),
            "FILLED",
        )
        .await;

        let count = migrate_offchain_orders(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id: String = sqlx::query_scalar(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OffchainOrder'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let expected_id = OffchainOrder::aggregate_id(1);
        assert_eq!(aggregate_id, expected_id);
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_all_status_types() {
        let pool = create_test_pool().await;

        insert_test_order(&pool, "AAPL", 10, "BUY", None, None, "PENDING").await;
        insert_test_order(&pool, "TSLA", 5, "SELL", None, None, "PENDING").await;
        insert_test_order(
            &pool,
            "MSFT",
            20,
            "BUY",
            Some("ORDER123"),
            None,
            "SUBMITTED",
        )
        .await;
        insert_test_order(
            &pool,
            "GOOGL",
            15,
            "SELL",
            Some("ORDER456"),
            None,
            "SUBMITTED",
        )
        .await;
        insert_test_order(
            &pool,
            "NVDA",
            8,
            "BUY",
            Some("ORDER789"),
            Some(50025),
            "FILLED",
        )
        .await;
        insert_test_order(
            &pool,
            "AMD",
            12,
            "SELL",
            Some("ORDER999"),
            Some(14050),
            "FILLED",
        )
        .await;
        insert_test_order(&pool, "META", 3, "BUY", None, None, "FAILED").await;

        let count = migrate_offchain_orders(&pool, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 7);

        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 7);
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_dry_run() {
        let pool = create_test_pool().await;

        insert_test_order(
            &pool,
            "AAPL",
            10,
            "BUY",
            Some("ORDER123"),
            Some(15050),
            "FILLED",
        )
        .await;

        let count = migrate_offchain_orders(&pool, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }
}
