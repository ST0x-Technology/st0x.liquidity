use chrono::Utc;
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use st0x_broker::{SupportedBroker, Symbol};
use tracing::info;

use super::{ExecutionMode, MigrationError};
use crate::offchain_order::{BrokerOrderId, OffchainOrder, OffchainOrderEvent, PriceCents};
use crate::position::FractionalShares;

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
                persist_event(pool, &aggregate_id, event).await?;
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
    use chrono::Utc;
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
        shares: i64,
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

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }
}
