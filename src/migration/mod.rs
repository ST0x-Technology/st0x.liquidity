mod offchain_order;
mod onchain_trade;
mod position;
mod schwab_auth;

use clap::{Parser, ValueEnum};
use cqrs_es::AggregateError;
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use std::io;
use tracing::{info, warn};

use crate::offchain_order::{InvalidMigratedOrderStatus, OffchainOrderError};
use crate::onchain_trade::OnChainTradeError;
use crate::position::{NegativePriceCents, PositionError};
use offchain_order::migrate_offchain_orders;
use onchain_trade::migrate_onchain_trades;
use position::migrate_positions;
use schwab_auth::migrate_schwab_auth;

#[derive(Debug, Parser)]
#[command(
    name = "migrate_to_events",
    about = "Migrate legacy CRUD data to event-sourced aggregates",
    long_about = "One-time migration tool that converts existing data from legacy tables \
                  (onchain_trades, trade_accumulators, offchain_trades, schwab_auth) into \
                  event-sourced aggregates using cqrs-es framework.\n\n\
                  ⚠️  IMPORTANT: Create a database backup before running!\n\n\
                  Example usage:\n  \
                    # Dry run (preview without persisting)\n  \
                    migrate_to_events --execution dry-run\n\n  \
                    # Run migration with interactive confirmation\n  \
                    migrate_to_events\n\n  \
                    # Run without prompts (for automation)\n  \
                    migrate_to_events --confirmation force\n\n  \
                    # Clean existing events and re-migrate\n  \
                    migrate_to_events --clean delete --confirmation force"
)]
pub struct MigrationEnv {
    #[clap(
        long,
        env = "DATABASE_URL",
        help = "SQLite database path (or set DATABASE_URL env var)"
    )]
    pub database_url: String,

    #[clap(
        long,
        value_enum,
        default_value = "interactive",
        help = "Confirmation mode: interactive (prompt for confirmations) or force (skip prompts)"
    )]
    pub confirmation: ConfirmationMode,

    #[clap(
        long,
        value_enum,
        default_value = "preserve",
        help = "Clean mode: preserve (keep existing events) or delete (remove all events before migrating)"
    )]
    pub clean: CleanMode,

    #[clap(
        long,
        value_enum,
        default_value = "commit",
        help = "Execution mode: commit (persist events) or dry-run (preview only)"
    )]
    pub execution: ExecutionMode,
}

#[derive(Debug, thiserror::Error)]
enum MigrationError {
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
    #[error("OnChainTrade aggregate error: {0}")]
    OnChainTradeAggregate(#[from] AggregateError<OnChainTradeError>),
    #[error("Position aggregate error: {0}")]
    PositionAggregate(#[from] AggregateError<PositionError>),
    #[error("OffchainOrder aggregate error: {0}")]
    OffchainOrderAggregate(#[from] AggregateError<OffchainOrderError>),
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

pub async fn run_migration(
    pool: &SqlitePool,
    env: &MigrationEnv,
) -> anyhow::Result<MigrationSummary> {
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

    let onchain_trade_cqrs = sqlite_cqrs(pool.clone(), vec![], ());
    let position_cqrs = sqlite_cqrs(pool.clone(), vec![], ());
    let offchain_order_cqrs = sqlite_cqrs(pool.clone(), vec![], ());

    let onchain_trades = migrate_onchain_trades(pool, &onchain_trade_cqrs, env.execution).await?;
    let positions = migrate_positions(pool, &position_cqrs, env.execution).await?;
    let offchain_orders =
        migrate_offchain_orders(pool, &offchain_order_cqrs, env.execution).await?;
    let schwab_auth = migrate_schwab_auth(pool, env.execution).await?;

    Ok(MigrationSummary {
        onchain_trades,
        positions,
        offchain_orders,
        schwab_auth,
    })
}

async fn check_existing_events(
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

fn safety_prompt(confirmation: ConfirmationMode) -> Result<(), MigrationError> {
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

async fn clean_events(
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

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, b256};
    use chrono;
    use sqlx::SqlitePool;

    use super::{
        CleanMode, ConfirmationMode, ExecutionMode, MigrationEnv, check_existing_events,
        clean_events, run_migration,
    };
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
    async fn test_check_existing_events_empty_database() {
        let pool = create_test_pool().await;

        check_existing_events(&pool, "OnChainTrade", ConfirmationMode::Force)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_existing_events_with_force_mode() {
        let pool = create_test_pool().await;

        sqlx::query!(
            "INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('OnChainTrade', 'test:0', 1, 'Migrated', '1.0', '{}', '{}')"
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

        sqlx::query!(
            "INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ('OnChainTrade', 'test:0', 1, 'Migrated', '1.0', '{}', '{}')"
        )
        .execute(&pool)
        .await
        .unwrap();

        clean_events(&pool, ConfirmationMode::Force).await.unwrap();

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 0);
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

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
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

        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_run_migration_with_clean() {
        let pool = create_test_pool().await;

        let old_hash = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let old_id = OnChainTrade::aggregate_id(old_hash, 0);

        sqlx::query!(
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
            old_id
        )
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
    async fn test_comprehensive_migration_all_aggregates() {
        let pool = create_test_pool().await;

        let tx1 = b256!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx2 = b256!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let tx3 = b256!("0x3333333333333333333333333333333333333333333333333333333333333333");

        insert_test_trade(&pool, tx1, 0, "AAPL", 10.0, "BUY", 150.50).await;
        insert_test_trade(&pool, tx2, 0, "TSLA", 5.0, "SELL", 200.75).await;
        insert_test_trade(&pool, tx3, 0, "GOOGL", 3.0, "BUY", 2800.00).await;

        sqlx::query!(
            "INSERT INTO trade_accumulators (symbol, net_position, accumulated_long, accumulated_short)
             VALUES (?, ?, ?, ?)",
            "AAPL",
            10.0,
            10.0,
            0.0
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            "INSERT INTO trade_accumulators (symbol, net_position, accumulated_long, accumulated_short)
             VALUES (?, ?, ?, ?)",
            "TSLA",
            -5.0,
            0.0,
            5.0
        )
        .execute(&pool)
        .await
        .unwrap();

        let executed_at = chrono::Utc::now();
        sqlx::query!(
            "INSERT INTO offchain_trades (symbol, shares, direction, broker_order_id, price_cents, status, executed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            "AAPL",
            10,
            "BUY",
            "ORD123",
            15050,
            "FILLED",
            executed_at
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            "INSERT INTO offchain_trades (symbol, shares, direction, status)
             VALUES (?, ?, ?, ?)",
            "TSLA",
            5,
            "SELL",
            "PENDING"
        )
        .execute(&pool)
        .await
        .unwrap();

        let env = MigrationEnv {
            database_url: ":memory:".to_string(),
            confirmation: ConfirmationMode::Force,
            clean: CleanMode::Preserve,
            execution: ExecutionMode::Commit,
        };

        let result = run_migration(&pool, &env).await.unwrap();

        assert_eq!(result.onchain_trades, 3);
        assert_eq!(result.positions, 2);
        assert_eq!(result.offchain_orders, 2);
        assert!(!result.schwab_auth);

        let total_events = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(total_events, 7);

        let onchain_trade_events = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OnChainTrade'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(onchain_trade_events, 3);

        let position_events =
            sqlx::query_scalar!("SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(position_events, 2);

        let offchain_order_events = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(offchain_order_events, 2);
    }
}
