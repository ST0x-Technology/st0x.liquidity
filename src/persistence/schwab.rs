use alloy::primitives::FixedBytes;
use async_trait::async_trait;
use sqlx::SqlitePool;

use st0x_execution::schwab::{
    EncryptionKey, SchwabPersistence, SchwabTokens, decrypt_token, encrypt_token,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum SchwabPersistenceError {
    #[error("No tokens stored in database")]
    NoTokens,
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Encryption error: {0}")]
    Encryption(#[from] st0x_execution::schwab::EncryptionError),
    #[error("Hex decoding error: {0}")]
    Hex(#[from] alloy::hex::FromHexError),
}

/// SQLite-backed implementation of SchwabPersistence for auth token storage.
#[derive(Debug, Clone)]
pub(crate) struct SqliteSchwabPersistence {
    pool: SqlitePool,
    encryption_key: EncryptionKey,
}

impl SqliteSchwabPersistence {
    pub(crate) fn new(pool: SqlitePool, encryption_key: FixedBytes<32>) -> Self {
        Self {
            pool,
            encryption_key,
        }
    }
}

#[async_trait]
impl SchwabPersistence for SqliteSchwabPersistence {
    type Error = SchwabPersistenceError;

    async fn store_tokens(&self, tokens: &SchwabTokens) -> Result<(), Self::Error> {
        let encrypted_access = encrypt_token(&self.encryption_key, &tokens.access_token)?;
        let encrypted_refresh = encrypt_token(&self.encryption_key, &tokens.refresh_token)?;

        let access_hex = alloy::hex::encode(encrypted_access.as_ref());
        let refresh_hex = alloy::hex::encode(encrypted_refresh.as_ref());

        sqlx::query!(
            "INSERT INTO schwab_auth (
                id,
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
                encryption_version
            ) VALUES (1, ?1, ?2, ?3, ?4, 1)
            ON CONFLICT(id) DO UPDATE SET
                access_token = excluded.access_token,
                access_token_fetched_at = excluded.access_token_fetched_at,
                refresh_token = excluded.refresh_token,
                refresh_token_fetched_at = excluded.refresh_token_fetched_at",
            access_hex,
            tokens.access_token_fetched_at,
            refresh_hex,
            tokens.refresh_token_fetched_at,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load_tokens(&self) -> Result<SchwabTokens, Self::Error> {
        let row = sqlx::query!(
            "SELECT access_token, access_token_fetched_at, refresh_token, refresh_token_fetched_at
             FROM schwab_auth WHERE id = 1"
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SchwabPersistenceError::NoTokens)?;

        let access_bytes = alloy::hex::decode(&row.access_token)?;
        let refresh_bytes = alloy::hex::decode(&row.refresh_token)?;

        let access_token = decrypt_token(&self.encryption_key, &access_bytes.into())?;
        let refresh_token = decrypt_token(&self.encryption_key, &refresh_bytes.into())?;

        Ok(SchwabTokens {
            access_token,
            access_token_fetched_at: row.access_token_fetched_at.and_utc(),
            refresh_token,
            refresh_token_fetched_at: row.refresh_token_fetched_at.and_utc(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    async fn setup_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    fn create_test_tokens() -> SchwabTokens {
        SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: Utc::now(),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_store_and_load_tokens() {
        let pool = setup_test_pool().await;
        let persistence = SqliteSchwabPersistence::new(pool, TEST_ENCRYPTION_KEY);

        let tokens = create_test_tokens();
        persistence.store_tokens(&tokens).await.unwrap();

        let loaded = persistence.load_tokens().await.unwrap();
        assert_eq!(loaded.access_token, tokens.access_token);
        assert_eq!(loaded.refresh_token, tokens.refresh_token);
    }

    #[tokio::test]
    async fn test_store_tokens_upsert() {
        let pool = setup_test_pool().await;
        let persistence = SqliteSchwabPersistence::new(pool, TEST_ENCRYPTION_KEY);

        let tokens1 = create_test_tokens();
        persistence.store_tokens(&tokens1).await.unwrap();

        let tokens2 = SchwabTokens {
            access_token: "updated_access".to_string(),
            access_token_fetched_at: Utc::now(),
            refresh_token: "updated_refresh".to_string(),
            refresh_token_fetched_at: Utc::now(),
        };
        persistence.store_tokens(&tokens2).await.unwrap();

        let loaded = persistence.load_tokens().await.unwrap();
        assert_eq!(loaded.access_token, "updated_access");
        assert_eq!(loaded.refresh_token, "updated_refresh");
    }

    #[tokio::test]
    async fn test_load_tokens_no_tokens() {
        let pool = setup_test_pool().await;
        let persistence = SqliteSchwabPersistence::new(pool, TEST_ENCRYPTION_KEY);

        let result = persistence.load_tokens().await;
        assert!(matches!(
            result.unwrap_err(),
            SchwabPersistenceError::NoTokens
        ));
    }
}
