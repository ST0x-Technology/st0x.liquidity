use rust_decimal::{Decimal, prelude::One};
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use st0x_broker::Symbol;
use tracing::{info, warn};

use super::{ExecutionMode, MigrationError};
use crate::lifecycle::Lifecycle;
use crate::position::{Position, PositionCommand};
use crate::shares::{ArithmeticError, FractionalShares};
use crate::threshold::ExecutionThreshold;

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
    cqrs: &SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = fetch_position_rows(pool).await?;

    let total = rows.len();
    info!("Found {total} positions to migrate");

    let pending_count = rows
        .iter()
        .filter(|row| row.pending_execution_id.is_some())
        .count();

    for (idx, row) in rows.into_iter().enumerate() {
        log_progress("positions", idx + 1, total);
        migrate_single_position(cqrs, row, execution).await?;
    }

    info!("Migrated {total} positions, {pending_count} with pending executions");
    Ok(total)
}

async fn fetch_position_rows(pool: &SqlitePool) -> Result<Vec<PositionRow>, sqlx::Error> {
    sqlx::query_as::<_, PositionRow>(
        "SELECT symbol, net_position, accumulated_long, accumulated_short, pending_execution_id
         FROM trade_accumulators
         ORDER BY symbol ASC",
    )
    .fetch_all(pool)
    .await
}

fn log_progress(entity: &str, progress: usize, total: usize) {
    if progress % 100 == 0 {
        info!("Migrating {entity}: {progress}/{total}");
    }
}

async fn migrate_single_position(
    cqrs: &SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>,
    row: PositionRow,
    execution: ExecutionMode,
) -> Result<(), MigrationError> {
    log_pending_execution_warning(&row);

    let symbol = Symbol::new(&row.symbol)?;
    let aggregate_id = Position::aggregate_id(&symbol);
    let command = build_position_command(&row, symbol)?;

    if matches!(execution, ExecutionMode::Commit) {
        cqrs.execute(&aggregate_id, command).await?;
    }

    Ok(())
}

fn log_pending_execution_warning(row: &PositionRow) {
    if let Some(exec_id) = row.pending_execution_id {
        warn!(
            "Position {} has pending execution {exec_id} - will be reconciled in dual-write phase",
            row.symbol
        );
    }
}

fn build_position_command(
    row: &PositionRow,
    symbol: Symbol,
) -> Result<PositionCommand, MigrationError> {
    let net_position = Decimal::try_from(row.net_position)?;
    let accumulated_long = Decimal::try_from(row.accumulated_long)?;
    let accumulated_short = Decimal::try_from(row.accumulated_short)?;

    Ok(PositionCommand::Migrate {
        symbol,
        net_position: FractionalShares(net_position),
        accumulated_long: FractionalShares(accumulated_long),
        accumulated_short: FractionalShares(accumulated_short),
        threshold: ExecutionThreshold::Shares(FractionalShares(Decimal::one())),
    })
}

#[cfg(test)]
mod tests {
    use sqlite_es::sqlite_cqrs;
    use sqlx::SqlitePool;
    use st0x_broker::Symbol;

    use super::{ExecutionMode, migrate_positions};
    use crate::position::Position;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_test_position(
        pool: &SqlitePool,
        symbol: &str,
        net_position: f64,
        accumulated_long: f64,
        accumulated_short: f64,
        pending_execution_id: Option<i64>,
    ) {
        sqlx::query!(
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
            symbol,
            net_position,
            accumulated_long,
            accumulated_short,
            pending_execution_id
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_positions_empty() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let count = migrate_positions(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_positions_single_position() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'Position'"
        )
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
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;
        insert_test_position(&pool, "TSLA", -2.0, 3.0, 5.0, None).await;
        insert_test_position(&pool, "MSFT", 0.0, 0.0, 0.0, None).await;

        let count = migrate_positions(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 3);

        let event_count =
            sqlx::query_scalar!("SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(event_count, 3);
    }

    #[tokio::test]
    async fn test_migrate_positions_with_pending_execution() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        sqlx::query!(
            "INSERT INTO offchain_trades (symbol, shares, direction, status)
             VALUES ('AAPL', 5, 'SELL', 'PENDING')"
        )
        .execute(&pool)
        .await
        .unwrap();

        let execution_id: i64 = sqlx::query_scalar!("SELECT id FROM offchain_trades LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap()
            .unwrap();

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, Some(execution_id)).await;

        let count = migrate_positions(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'Position'"
        )
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
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, &cqrs, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
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

        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
        let result = migrate_positions(&pool, &cqrs, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            super::MigrationError::InvalidSymbol(_)
        ));
    }
}
