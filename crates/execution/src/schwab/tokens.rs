use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, interval};
use tracing::{error, info, warn};

use alloy::primitives::FixedBytes;

use super::SchwabError;
use super::auth::SchwabAuthCtx;
use super::encryption::{EncryptedToken, EncryptionError, decrypt_token, encrypt_token};

const ACCESS_TOKEN_DURATION_MINUTES: i64 = 30;
const REFRESH_TOKEN_DURATION_DAYS: i64 = 7;
const ENCRYPTION_VERSION: i64 = 1;

#[derive(Debug)]
pub struct SchwabTokens {
    /// Expires every 30 minutes
    pub access_token: String,
    pub access_token_fetched_at: DateTime<Utc>,
    /// Expires every 7 days
    pub refresh_token: String,
    pub refresh_token_fetched_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for SchwabTokens {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SchwabTokensHelper {
            access_token: String,
            access_token_fetched_at: DateTime<Utc>,
            refresh_token: String,
            refresh_token_fetched_at: DateTime<Utc>,
        }

        let helper = SchwabTokensHelper::deserialize(deserializer)?;
        Ok(Self {
            access_token: helper.access_token,
            access_token_fetched_at: helper.access_token_fetched_at,
            refresh_token: helper.refresh_token,
            refresh_token_fetched_at: helper.refresh_token_fetched_at,
        })
    }
}

impl SchwabTokens {
    pub async fn store(
        &self,
        pool: &SqlitePool,
        encryption_key: &FixedBytes<32>,
    ) -> Result<(), SchwabError> {
        let encrypted_access = encrypt_token(encryption_key, &self.access_token)?;
        let encrypted_refresh = encrypt_token(encryption_key, &self.refresh_token)?;

        let encrypted_access_hex = alloy::hex::encode(&encrypted_access);
        let encrypted_refresh_hex = alloy::hex::encode(&encrypted_refresh);

        sqlx::query!(
            r#"
            INSERT INTO schwab_auth (
                id,
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
                encryption_version
            )
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                access_token = excluded.access_token,
                access_token_fetched_at = excluded.access_token_fetched_at,
                refresh_token = excluded.refresh_token,
                refresh_token_fetched_at = excluded.refresh_token_fetched_at,
                encryption_version = excluded.encryption_version
            "#,
            encrypted_access_hex,
            self.access_token_fetched_at,
            encrypted_refresh_hex,
            self.refresh_token_fetched_at,
            ENCRYPTION_VERSION,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn load(
        pool: &SqlitePool,
        encryption_key: &FixedBytes<32>,
    ) -> Result<Self, SchwabError> {
        let row = sqlx::query!(
            r#"
            SELECT
                id,
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
                encryption_version
            FROM schwab_auth
            "#
        )
        .fetch_one(pool)
        .await?;

        let encrypted_access_bytes: Vec<u8> =
            alloy::hex::decode(&row.access_token).map_err(EncryptionError::Hex)?;
        let encrypted_refresh_bytes: Vec<u8> =
            alloy::hex::decode(&row.refresh_token).map_err(EncryptionError::Hex)?;

        let encrypted_access = EncryptedToken::from(encrypted_access_bytes);
        let encrypted_refresh = EncryptedToken::from(encrypted_refresh_bytes);

        let access_token = decrypt_token(encryption_key, &encrypted_access)?;
        let refresh_token = decrypt_token(encryption_key, &encrypted_refresh)?;

        Ok(Self {
            access_token,
            access_token_fetched_at: DateTime::from_naive_utc_and_offset(
                row.access_token_fetched_at,
                Utc,
            ),
            refresh_token,
            refresh_token_fetched_at: DateTime::from_naive_utc_and_offset(
                row.refresh_token_fetched_at,
                Utc,
            ),
        })
    }

    pub fn is_access_token_expired(&self) -> bool {
        let now = Utc::now();
        let expires_at =
            self.access_token_fetched_at + Duration::minutes(ACCESS_TOKEN_DURATION_MINUTES);
        now >= expires_at
    }

    pub fn is_refresh_token_expired(&self) -> bool {
        let now = Utc::now();
        let expires_at =
            self.refresh_token_fetched_at + Duration::days(REFRESH_TOKEN_DURATION_DAYS);
        now >= expires_at
    }

    pub fn access_token_expires_in(&self) -> Duration {
        let now = Utc::now();
        let expires_at =
            self.access_token_fetched_at + Duration::minutes(ACCESS_TOKEN_DURATION_MINUTES);
        expires_at - now
    }

    pub async fn get_valid_access_token(
        pool: &SqlitePool,
        config: &SchwabAuthCtx,
    ) -> Result<String, SchwabError> {
        let tokens = Self::load(pool, &config.encryption_key).await?;

        if !tokens.is_access_token_expired() {
            return Ok(tokens.access_token);
        }

        if tokens.is_refresh_token_expired() {
            return Err(SchwabError::RefreshTokenExpired);
        }

        let new_tokens = config.refresh_tokens(&tokens.refresh_token).await?;
        new_tokens.store(pool, &config.encryption_key).await?;
        Ok(new_tokens.access_token)
    }

    #[cfg(test)]
    pub(crate) async fn db_count(pool: &SqlitePool) -> Result<i64, SchwabError> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM schwab_auth")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    pub async fn refresh_if_needed(
        pool: &SqlitePool,
        config: &SchwabAuthCtx,
    ) -> Result<bool, SchwabError> {
        let tokens = Self::load(pool, &config.encryption_key).await?;

        if tokens.is_refresh_token_expired() {
            return Err(SchwabError::RefreshTokenExpired);
        }

        if tokens.is_access_token_expired()
            || tokens.access_token_expires_in() <= Duration::minutes(1)
        {
            let new_tokens = config.refresh_tokens(&tokens.refresh_token).await?;
            new_tokens.store(pool, &config.encryption_key).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// Moved out of impl block to avoid clippy false positive with unsafe_derive_deserialize
pub(crate) fn spawn_automatic_token_refresh(
    pool: SqlitePool,
    config: SchwabAuthCtx,
) -> JoinHandle<()> {
    info!("Starting token refresh service");
    tokio::spawn(async move {
        if let Err(e) = start_automatic_token_refresh_loop(pool, config).await {
            error!("Token refresh task failed: {e:?}");
        }
    })
}

async fn start_automatic_token_refresh_loop(
    pool: SqlitePool,
    config: SchwabAuthCtx,
) -> Result<(), SchwabError> {
    let refresh_interval_secs = (ACCESS_TOKEN_DURATION_MINUTES - 1) * 60;
    let refresh_interval_u64 = refresh_interval_secs.try_into().map_err(|_| {
        SchwabError::InvalidConfiguration("Refresh interval out of range".to_string())
    })?;
    let mut interval_timer = interval(TokioDuration::from_secs(refresh_interval_u64));

    loop {
        interval_timer.tick().await;
        handle_token_refresh(&pool, &config).await?;
    }
}

async fn handle_token_refresh(
    pool: &SqlitePool,
    config: &SchwabAuthCtx,
) -> Result<(), SchwabError> {
    match SchwabTokens::refresh_if_needed(pool, config).await {
        Ok(refreshed) if refreshed => {
            info!("Access token refreshed successfully");
            Ok(())
        }
        Ok(_) => Ok(()),
        Err(SchwabError::RefreshTokenExpired) => {
            error!("Refresh token expired, manual re-authentication required");
            Err(SchwabError::RefreshTokenExpired)
        }
        Err(e) => {
            warn!("Failed to refresh token: {e}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::Utc;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::thread;
    use tokio::time::{Duration as TokioDuration, sleep};

    fn create_test_config_with_mock_server(mock_server: &MockServer) -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_app_key".to_string(),
            app_secret: "test_app_secret".to_string(),
            redirect_uri: None,
            base_url: Some(url::Url::parse(&mock_server.base_url()).expect("mock server base_url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_config() -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_app_key".to_string(),
            app_secret: "test_app_secret".to_string(),
            redirect_uri: None,
            base_url: None,
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_schwab_tokens_store_success() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let stored_token = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_token.access_token, "test_access_token");
        assert_eq!(stored_token.refresh_token, "test_refresh_token");
        assert_eq!(stored_token.access_token_fetched_at, now);
        assert_eq!(stored_token.refresh_token_fetched_at, now);
    }

    #[tokio::test]
    async fn test_schwab_tokens_store_upsert() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let updated_tokens = SchwabTokens {
            access_token: "updated_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "updated_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        updated_tokens
            .store(&pool, &config.encryption_key)
            .await
            .unwrap();

        let count = SchwabTokens::db_count(&pool).await.unwrap();
        assert_eq!(count, 1);

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_tokens.access_token, "updated_access_token");
        assert_eq!(stored_tokens.refresh_token, "updated_refresh_token");
    }

    #[test]
    fn test_is_access_token_expired_not_expired() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(15),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        assert!(!tokens.is_access_token_expired());
    }

    #[test]
    fn test_is_access_token_expired_expired() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(31),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        assert!(tokens.is_access_token_expired());
    }

    #[test]
    fn test_is_access_token_expired_exactly_30_minutes() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(30),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        assert!(tokens.is_access_token_expired());
    }

    #[test]
    fn test_is_refresh_token_expired_not_expired() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(3),
        };

        assert!(!tokens.is_refresh_token_expired());
    }

    #[test]
    fn test_is_refresh_token_expired_expired() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(8),
        };

        assert!(tokens.is_refresh_token_expired());
    }

    #[test]
    fn test_is_refresh_token_expired_exactly_7_days() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(7),
        };

        assert!(tokens.is_refresh_token_expired());
    }

    #[test]
    fn test_access_token_expires_in_positive() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(10),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        let expires_in = tokens.access_token_expires_in();
        assert!(expires_in > Duration::minutes(19));
        assert!(expires_in <= Duration::minutes(20));
    }

    #[test]
    fn test_access_token_expires_in_negative() {
        let now = Utc::now();
        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        let expires_in = tokens.access_token_expires_in();
        assert!(expires_in < Duration::zero());
    }

    #[tokio::test]
    async fn test_get_valid_access_token_valid_token() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        assert_eq!(
            SchwabTokens::get_valid_access_token(&pool, &config)
                .await
                .unwrap(),
            "valid_access_token"
        );
    }

    #[tokio::test]
    async fn test_get_valid_access_token_refresh_token_expired() {
        let pool = setup_test_db().await;
        let config = create_test_config();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(8),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let error = SchwabTokens::get_valid_access_token(&pool, &config)
            .await
            .unwrap_err();

        assert!(matches!(error, SchwabError::RefreshTokenExpired));
    }

    #[tokio::test]
    async fn test_get_valid_access_token_needs_refresh() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let mock_response = json!({
            "access_token": "refreshed_access_token",
            "refresh_token": "new_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let access_token = SchwabTokens::get_valid_access_token(&pool, &config)
            .await
            .unwrap();
        mock.assert();

        assert_eq!(access_token, "refreshed_access_token");

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_get_valid_access_token_refresh_fails() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "invalid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let error = SchwabTokens::get_valid_access_token(&pool, &config)
            .await
            .unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::RequestFailed { .. }));
    }

    #[tokio::test]
    async fn test_get_valid_access_token_no_tokens_in_db() {
        let pool = setup_test_db().await;
        let config = create_test_config();

        let error = SchwabTokens::get_valid_access_token(&pool, &config)
            .await
            .unwrap_err();

        assert!(matches!(error, SchwabError::Sqlx(_)));
    }

    #[tokio::test]
    async fn test_refresh_if_needed_success() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(31),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let mock_response = json!({
            "access_token": "refreshed_access_token",
            "refresh_token": "new_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .header(
                    "authorization",
                    "Basic dGVzdF9hcHBfa2V5OnRlc3RfYXBwX3NlY3JldA==",
                )
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let refreshed = SchwabTokens::refresh_if_needed(&pool, &config)
            .await
            .unwrap();
        mock.assert();

        assert!(refreshed);

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_refresh_if_needed_with_expired_refresh_token() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(31),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(8),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let error = SchwabTokens::refresh_if_needed(&pool, &config)
            .await
            .unwrap_err();

        assert!(matches!(error, SchwabError::RefreshTokenExpired));
    }

    #[tokio::test]
    async fn test_refresh_if_needed_no_refresh_needed() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let refreshed = SchwabTokens::refresh_if_needed(&pool, &config)
            .await
            .unwrap();

        assert!(!refreshed);

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_tokens.access_token, "valid_access_token");
    }

    #[tokio::test]
    async fn test_refresh_if_needed_near_expiration() {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "near_expiry_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(29),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await.unwrap();

        let mock_response = json!({
            "access_token": "refreshed_access_token",
            "refresh_token": "new_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .header(
                    "authorization",
                    "Basic dGVzdF9hcHBfa2V5OnRlc3RfYXBwX3NlY3JldA==",
                )
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let refreshed = SchwabTokens::refresh_if_needed(&pool, &config)
            .await
            .unwrap();
        mock.assert();

        assert!(refreshed);

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key)
            .await
            .unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_automatic_token_refresh_before_expiration() -> Result<(), SchwabError> {
        let server = MockServer::start();
        let config = create_test_config_with_mock_server(&server);
        let pool = setup_test_db().await;
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "near_expiration_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(29),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        tokens.store(&pool, &config.encryption_key).await?;

        let mock_response = json!({
            "access_token": "refreshed_access_token",
            "refresh_token": "new_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/oauth/token")
                .header(
                    "authorization",
                    "Basic dGVzdF9hcHBfa2V5OnRlc3RfYXBwX3NlY3JldA==",
                )
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let pool_clone = pool.clone();
        let config_clone = config.clone();

        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                tokio::time::timeout(
                    TokioDuration::from_secs(5),
                    start_automatic_token_refresh_loop(pool_clone, config_clone),
                )
                .await
            })
        });

        sleep(TokioDuration::from_millis(2000)).await;

        handle.join().unwrap().unwrap_err();

        mock.assert();

        let stored_tokens = SchwabTokens::load(&pool, &config.encryption_key).await?;
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");

        Ok(())
    }
}
