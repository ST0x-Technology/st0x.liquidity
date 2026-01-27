use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use st0x_execution::{SupportedExecutor, Symbol};
use tracing::info;

use super::{ExecutionMode, MigrationError};
use crate::lifecycle::{Lifecycle, Never};
use crate::offchain_order::{BrokerOrderId, PriceCents};
use crate::offchain_order::{OffchainOrder, OffchainOrderCommand};
use crate::shares::FractionalShares;

#[derive(sqlx::FromRow)]
struct OffchainOrderRow {
    id: i64,
    symbol: String,
    shares: f64,
    direction: String,
    order_id: Option<String>,
    price_cents: Option<i64>,
    status: String,
    executed_at: Option<DateTime<Utc>>,
}

pub async fn migrate_offchain_orders(
    pool: &SqlitePool,
    cqrs: &SqliteCqrs<Lifecycle<OffchainOrder, Never>>,
    execution: ExecutionMode,
) -> Result<usize, MigrationError> {
    let rows = fetch_offchain_order_rows(pool).await?;

    let total = rows.len();
    info!("Found {total} offchain orders to migrate");

    let status_counts = count_statuses(&rows);

    for (idx, row) in rows.into_iter().enumerate() {
        log_progress("offchain orders", idx + 1, total);
        migrate_single_offchain_order(cqrs, row, execution).await?;
    }

    log_status_breakdown(total, status_counts);
    Ok(total)
}

async fn fetch_offchain_order_rows(
    pool: &SqlitePool,
) -> Result<Vec<OffchainOrderRow>, sqlx::Error> {
    sqlx::query_as::<_, OffchainOrderRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker_order_id as order_id,
            price_cents,
            status,
            executed_at
        FROM offchain_trades
        ORDER BY id ASC
        ",
    )
    .fetch_all(pool)
    .await
}

fn count_statuses(rows: &[OffchainOrderRow]) -> (usize, usize, usize, usize) {
    rows.iter().fold(
        (0usize, 0usize, 0usize, 0usize),
        |(pending, submitted, filled, failed), row| match row.status.as_str() {
            "PENDING" => (pending + 1, submitted, filled, failed),
            "SUBMITTED" => (pending, submitted + 1, filled, failed),
            "FILLED" => (pending, submitted, filled + 1, failed),
            "FAILED" => (pending, submitted, filled, failed + 1),
            _ => (pending, submitted, filled, failed),
        },
    )
}

fn log_progress(entity: &str, progress: usize, total: usize) {
    if progress % 100 == 0 {
        info!("Migrating {entity}: {progress}/{total}");
    }
}

fn log_status_breakdown(total: usize, status_counts: (usize, usize, usize, usize)) {
    let (pending, submitted, filled, failed) = status_counts;
    info!(
        "Migrated {total} offchain orders. Status breakdown: {pending} PENDING, {submitted} SUBMITTED, {filled} FILLED, {failed} FAILED"
    );
}

async fn migrate_single_offchain_order(
    cqrs: &SqliteCqrs<Lifecycle<OffchainOrder, Never>>,
    row: OffchainOrderRow,
    execution: ExecutionMode,
) -> Result<(), MigrationError> {
    let aggregate_id = OffchainOrder::aggregate_id(row.id);
    let command = build_offchain_order_command(&row)?;

    if matches!(execution, ExecutionMode::Commit) {
        cqrs.execute(&aggregate_id, command).await?;
    }

    Ok(())
}

fn build_offchain_order_command(
    row: &OffchainOrderRow,
) -> Result<OffchainOrderCommand, MigrationError> {
    let symbol = Symbol::new(&row.symbol)?;

    if row.shares < 0.0 {
        return Err(MigrationError::NegativeValue {
            field: "shares".to_string(),
            value: row.shares.to_string(),
        });
    }
    let shares = Decimal::try_from(row.shares)?;

    let direction = row.direction.parse()?;
    let migrated_status = row.status.parse()?;
    let broker_order_id = row.order_id.clone().map(BrokerOrderId);
    let price_cents = row.price_cents.map(PriceCents::try_from).transpose()?;

    Ok(OffchainOrderCommand::Migrate {
        symbol,
        shares: FractionalShares(shares),
        direction,
        executor: SupportedExecutor::Schwab,
        status: migrated_status,
        broker_order_id,
        price_cents,
        executed_at: row.executed_at,
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sqlite_es::sqlite_cqrs;
    use sqlx::SqlitePool;

    use super::{ExecutionMode, migrate_offchain_orders};
    use crate::offchain_order::OffchainOrder;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_test_order(
        pool: &SqlitePool,
        symbol: &str,
        shares: f64,
        direction: &str,
        order_id: Option<&str>,
        price_cents: Option<i64>,
        status: &str,
    ) {
        let executed_at = match status {
            "FILLED" | "FAILED" => Some(Utc::now()),
            _ => None,
        };

        sqlx::query!(
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
            symbol,
            shares,
            direction,
            order_id,
            price_cents,
            status,
            executed_at
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_empty() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        let count = migrate_offchain_orders(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_single_order() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_order(
            &pool,
            "AAPL",
            10.0,
            "BUY",
            Some("ORDER123"),
            Some(15050),
            "FILLED",
        )
        .await;

        let count = migrate_offchain_orders(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'OffchainOrder'"
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
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_order(&pool, "AAPL", 10.0, "BUY", None, None, "PENDING").await;
        insert_test_order(&pool, "TSLA", 5.0, "SELL", None, None, "PENDING").await;
        insert_test_order(
            &pool,
            "MSFT",
            20.0,
            "BUY",
            Some("ORDER123"),
            None,
            "SUBMITTED",
        )
        .await;
        insert_test_order(
            &pool,
            "GOOGL",
            15.0,
            "SELL",
            Some("ORDER456"),
            None,
            "SUBMITTED",
        )
        .await;
        insert_test_order(
            &pool,
            "NVDA",
            8.0,
            "BUY",
            Some("ORDER789"),
            Some(50025),
            "FILLED",
        )
        .await;
        insert_test_order(
            &pool,
            "AMD",
            12.0,
            "SELL",
            Some("ORDER999"),
            Some(14050),
            "FILLED",
        )
        .await;
        insert_test_order(&pool, "META", 3.0, "BUY", None, None, "FAILED").await;

        let count = migrate_offchain_orders(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert_eq!(count, 7);

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 7);
    }

    #[tokio::test]
    async fn test_migrate_offchain_orders_dry_run() {
        let pool = create_test_pool().await;
        let cqrs = sqlite_cqrs(pool.clone(), vec![], ());

        insert_test_order(
            &pool,
            "AAPL",
            10.0,
            "BUY",
            Some("ORDER123"),
            Some(15050),
            "FILLED",
        )
        .await;

        let count = migrate_offchain_orders(&pool, &cqrs, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert_eq!(count, 1);

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }
}
