use chrono::Utc;
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use st0x_broker::Symbol;
use tracing::{info, warn};

use super::{ExecutionMode, MigrationError};
use crate::position::{ExecutionThreshold, FractionalShares, Position, PositionEvent};

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
            threshold: ExecutionThreshold::whole_share(),
            migrated_at: Utc::now(),
        };

        match execution {
            ExecutionMode::Commit => {
                persist_event(pool, &aggregate_id, event).await?;
            }
            ExecutionMode::DryRun => {}
        }
    }

    info!("Migrated {total} positions, {pending_count} with pending executions");
    Ok(total)
}

async fn persist_event(
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

#[cfg(test)]
mod tests {
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

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;
        insert_test_position(&pool, "TSLA", -2.0, 3.0, 5.0, None).await;
        insert_test_position(&pool, "MSFT", 0.0, 0.0, 0.0, None).await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
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

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, ExecutionMode::Commit)
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

        insert_test_position(&pool, "AAPL", 5.5, 10.0, 4.5, None).await;

        let count = migrate_positions(&pool, ExecutionMode::DryRun)
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

        let result = migrate_positions(&pool, ExecutionMode::Commit).await;

        assert!(matches!(
            result.unwrap_err(),
            super::MigrationError::InvalidSymbol(_)
        ));
    }
}
