use serde_json::json;
use sqlx::SqlitePool;

const REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS: &str =
    include_str!("../migrations/20260701223808_repair_legacy_usdc_conversion_confirmed_events.sql");

#[tokio::test]
async fn repairs_legacy_usdc_conversion_confirmed_events() -> anyhow::Result<()> {
    let pool = SqlitePool::connect(":memory:").await?;
    create_event_store_tables(&pool).await?;

    insert_event(
        &pool,
        "UsdcRebalance",
        "legacy-usdc-rebalance",
        "UsdcRebalanceEvent::ConversionConfirmed",
        "1.0",
        json!({
            "ConversionConfirmed": {
                "direction": "BaseToAlpaca",
                "filled_amount": "1082.711862",
                "converted_at": "2026-07-01T19:58:41.907Z"
            }
        }),
    )
    .await?;
    insert_event(
        &pool,
        "UsdcRebalance",
        "current-usdc-rebalance",
        "UsdcRebalanceEvent::ConversionConfirmed",
        "2.0",
        json!({
            "ConversionConfirmed": {
                "direction": "AlpacaToBase",
                "source_amount": "100.25",
                "received_amount": "99.95",
                "converted_at": "2026-07-01T20:00:00Z"
            }
        }),
    )
    .await?;
    insert_event(
        &pool,
        "InventorySnapshot",
        "inventory",
        "InventorySnapshotEvent::OffchainUsd",
        "1.0",
        json!({
            "OffchainUsd": {
                "usd_balance_cents": 100,
                "fetched_at": "2026-07-01T20:00:00Z"
            }
        }),
    )
    .await?;
    insert_snapshot(&pool, "UsdcRebalance", "legacy-usdc-rebalance").await?;
    insert_snapshot(&pool, "Position", "SPYM").await?;

    sqlx::raw_sql(REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS)
        .execute(&pool)
        .await?;

    assert_conversion_field(
        &pool,
        "legacy-usdc-rebalance",
        "source_amount",
        "1082.711862",
    )
    .await?;
    assert_conversion_field(
        &pool,
        "legacy-usdc-rebalance",
        "received_amount",
        "1082.711862",
    )
    .await?;
    assert_json_field_missing(
        &pool,
        "legacy-usdc-rebalance",
        "$.ConversionConfirmed.filled_amount",
    )
    .await?;
    assert_event_version(&pool, "legacy-usdc-rebalance", "2.0").await?;

    assert_conversion_field(&pool, "current-usdc-rebalance", "source_amount", "100.25").await?;
    assert_conversion_field(&pool, "current-usdc-rebalance", "received_amount", "99.95").await?;
    assert_json_field_missing(
        &pool,
        "current-usdc-rebalance",
        "$.ConversionConfirmed.filled_amount",
    )
    .await?;
    assert_event_version(&pool, "current-usdc-rebalance", "2.0").await?;

    let unrelated_payload: String =
        sqlx::query_scalar("SELECT payload FROM events WHERE aggregate_type = 'InventorySnapshot'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&unrelated_payload)?,
        json!({
            "OffchainUsd": {
                "usd_balance_cents": 100,
                "fetched_at": "2026-07-01T20:00:00Z"
            }
        })
    );

    let usdc_snapshot_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE aggregate_type = 'UsdcRebalance'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(usdc_snapshot_count, 0);

    let position_snapshot_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE aggregate_type = 'Position'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(position_snapshot_count, 1);

    Ok(())
}

async fn create_event_store_tables(pool: &SqlitePool) -> sqlx::Result<()> {
    sqlx::raw_sql(
        r"
        CREATE TABLE events (
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            sequence BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            event_version TEXT NOT NULL,
            payload JSON NOT NULL,
            metadata JSON NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id, sequence)
        );

        CREATE TABLE snapshots (
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            last_sequence BIGINT NOT NULL,
            payload JSON NOT NULL,
            timestamp TEXT NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id)
        );
        ",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_event(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    event_version: &str,
    payload: serde_json::Value,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO events \
         (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
         VALUES (?, ?, 1, ?, ?, ?, '{}')",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .bind(event_type)
    .bind(event_version)
    .bind(payload.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_snapshot(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO snapshots \
         (aggregate_type, aggregate_id, last_sequence, payload, timestamp) \
         VALUES (?, ?, 1, '{}', '2026-07-01T20:00:00Z')",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn assert_conversion_field(
    pool: &SqlitePool,
    aggregate_id: &str,
    field: &str,
    expected: &str,
) -> sqlx::Result<()> {
    let path = format!("$.ConversionConfirmed.{field}");
    let actual: String =
        sqlx::query_scalar("SELECT json_extract(payload, ?) FROM events WHERE aggregate_id = ?")
            .bind(path)
            .bind(aggregate_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(actual, expected);

    Ok(())
}

async fn assert_json_field_missing(
    pool: &SqlitePool,
    aggregate_id: &str,
    path: &str,
) -> sqlx::Result<()> {
    let actual: Option<String> =
        sqlx::query_scalar("SELECT json_type(payload, ?) FROM events WHERE aggregate_id = ?")
            .bind(path)
            .bind(aggregate_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(actual, None);

    Ok(())
}

async fn assert_event_version(
    pool: &SqlitePool,
    aggregate_id: &str,
    expected: &str,
) -> sqlx::Result<()> {
    let actual: String =
        sqlx::query_scalar("SELECT event_version FROM events WHERE aggregate_id = ?")
            .bind(aggregate_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(actual, expected);

    Ok(())
}
