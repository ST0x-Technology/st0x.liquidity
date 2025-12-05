use chrono::Utc;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use st0x_broker::schwab::{EncryptedToken, SchwabAuth, SchwabAuthCommand};
use tracing::info;

use super::{ExecutionMode, MigrationError};

#[derive(sqlx::FromRow)]
struct SchwabAuthRow {
    access_token: String,
    access_token_fetched_at: chrono::DateTime<Utc>,
    refresh_token: String,
    refresh_token_fetched_at: chrono::DateTime<Utc>,
}

pub async fn migrate_schwab_auth(
    pool: &SqlitePool,
    cqrs: &SqliteCqrs<SchwabAuth>,
    execution: ExecutionMode,
) -> Result<bool, MigrationError> {
    let row = sqlx::query_as::<_, SchwabAuthRow>(
        "SELECT access_token, access_token_fetched_at, refresh_token, refresh_token_fetched_at
         FROM schwab_auth
         WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        info!("No schwab_auth row found - skipping migration");
        return Ok(false);
    };

    info!("Found schwab_auth tokens to migrate");

    let access_token: EncryptedToken = serde_json::from_str(&format!("\"{}\"", row.access_token))?;
    let refresh_token: EncryptedToken =
        serde_json::from_str(&format!("\"{}\"", row.refresh_token))?;

    let command = SchwabAuthCommand::Migrate {
        access_token,
        access_token_fetched_at: row.access_token_fetched_at,
        refresh_token,
        refresh_token_fetched_at: row.refresh_token_fetched_at,
    };

    let aggregate_id = "schwab";

    match execution {
        ExecutionMode::Commit => {
            cqrs.execute(aggregate_id, command).await?;
        }
        ExecutionMode::DryRun => {}
    }

    info!("Migrated schwab_auth tokens");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use chrono::Utc;
    use sqlite_es::sqlite_cqrs;
    use sqlx::SqlitePool;

    use st0x_broker::schwab::EncryptionKey;

    use super::{ExecutionMode, migrate_schwab_auth};

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn create_test_key() -> EncryptionKey {
        b256!("0x0000000000000000000000000000000000000000000000000000000000000000")
    }

    async fn insert_test_schwab_auth(pool: &SqlitePool, access_token: &str, refresh_token: &str) {
        let now = Utc::now();

        sqlx::query!(
            "
            INSERT INTO schwab_auth (
                id,
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
                encryption_version
            )
            VALUES (1, ?, ?, ?, ?, 1)
            ",
            access_token,
            now,
            refresh_token,
            now
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_schwab_auth_empty() {
        let pool = create_test_pool().await;
        let encryption_key = create_test_key();
        let cqrs = sqlite_cqrs(pool.clone(), vec![], encryption_key);

        let migrated = migrate_schwab_auth(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert!(!migrated);

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn test_migrate_schwab_auth_with_tokens() {
        let pool = create_test_pool().await;
        let encryption_key = create_test_key();
        let cqrs = sqlite_cqrs(pool.clone(), vec![], encryption_key);

        let access_hex = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        let refresh_hex = "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40";

        insert_test_schwab_auth(&pool, access_hex, refresh_hex).await;

        let migrated = migrate_schwab_auth(&pool, &cqrs, ExecutionMode::Commit)
            .await
            .unwrap();

        assert!(migrated);

        let event_count =
            sqlx::query_scalar!("SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchwabAuth'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(event_count, 1);

        let aggregate_id = sqlx::query_scalar!(
            "SELECT aggregate_id FROM events WHERE aggregate_type = 'SchwabAuth'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(aggregate_id, "schwab");

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'SchwabAuth'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "TokensStored");
    }

    #[tokio::test]
    async fn test_migrate_schwab_auth_dry_run() {
        let pool = create_test_pool().await;
        let encryption_key = create_test_key();
        let cqrs = sqlite_cqrs(pool.clone(), vec![], encryption_key);

        let access_hex = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        let refresh_hex = "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40";

        insert_test_schwab_auth(&pool, access_hex, refresh_hex).await;

        let migrated = migrate_schwab_auth(&pool, &cqrs, ExecutionMode::DryRun)
            .await
            .unwrap();

        assert!(migrated);

        let event_count = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(event_count, 0);
    }
}
