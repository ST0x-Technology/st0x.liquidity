use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, interval};
use tracing::{error, info, warn};

use super::SchwabError;
use super::auth::SchwabAuthEnv;
use super::persistence::SchwabPersistence;

const ACCESS_TOKEN_DURATION_MINUTES: i64 = 30;
const REFRESH_TOKEN_DURATION_DAYS: i64 = 7;

#[derive(Debug, Clone)]
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

    pub async fn get_valid_access_token<P: SchwabPersistence>(
        persistence: &P,
        env: &SchwabAuthEnv,
    ) -> Result<String, SchwabError> {
        let tokens = persistence
            .load_tokens()
            .await
            .map_err(|e| SchwabError::Persistence(Box::new(e)))?;

        if !tokens.is_access_token_expired() {
            return Ok(tokens.access_token);
        }

        if tokens.is_refresh_token_expired() {
            return Err(SchwabError::RefreshTokenExpired);
        }

        let new_tokens = env.refresh_tokens(&tokens.refresh_token).await?;
        persistence
            .store_tokens(&new_tokens)
            .await
            .map_err(|e| SchwabError::Persistence(Box::new(e)))?;
        Ok(new_tokens.access_token)
    }

    pub async fn refresh_if_needed<P: SchwabPersistence>(
        persistence: &P,
        env: &SchwabAuthEnv,
    ) -> Result<bool, SchwabError> {
        let tokens = persistence
            .load_tokens()
            .await
            .map_err(|e| SchwabError::Persistence(Box::new(e)))?;

        if tokens.is_refresh_token_expired() {
            return Err(SchwabError::RefreshTokenExpired);
        }

        if tokens.is_access_token_expired()
            || tokens.access_token_expires_in() <= Duration::minutes(1)
        {
            let new_tokens = env.refresh_tokens(&tokens.refresh_token).await?;
            persistence
                .store_tokens(&new_tokens)
                .await
                .map_err(|e| SchwabError::Persistence(Box::new(e)))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Spawns automatic token refresh background task.
pub(crate) fn spawn_automatic_token_refresh<P: SchwabPersistence>(
    persistence: P,
    env: SchwabAuthEnv,
) -> JoinHandle<()> {
    info!("Starting token refresh service");
    tokio::spawn(async move {
        if let Err(e) = start_automatic_token_refresh_loop(persistence, env).await {
            error!("Token refresh task failed: {e:?}");
        }
    })
}

async fn start_automatic_token_refresh_loop<P: SchwabPersistence>(
    persistence: P,
    env: SchwabAuthEnv,
) -> Result<(), SchwabError> {
    let refresh_interval_secs = (ACCESS_TOKEN_DURATION_MINUTES - 1) * 60;
    let refresh_interval_u64 = refresh_interval_secs.try_into().map_err(|_| {
        SchwabError::InvalidConfiguration("Refresh interval out of range".to_string())
    })?;
    let mut interval_timer = interval(TokioDuration::from_secs(refresh_interval_u64));

    loop {
        interval_timer.tick().await;
        handle_token_refresh(&persistence, &env).await?;
    }
}

async fn handle_token_refresh<P: SchwabPersistence>(
    persistence: &P,
    env: &SchwabAuthEnv,
) -> Result<(), SchwabError> {
    match SchwabTokens::refresh_if_needed(persistence, env).await {
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
    use crate::schwab::persistence::MockSchwabPersistence;
    use crate::test_utils::TEST_ENCRYPTION_KEY;
    use chrono::Utc;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::thread;
    use tokio::time::{Duration as TokioDuration, sleep};

    fn create_test_env_with_mock_server(mock_server: &MockServer) -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_app_key".to_string(),
            schwab_app_secret: "test_app_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: mock_server.base_url(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_env() -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_app_key".to_string(),
            schwab_app_secret: "test_app_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: "https://api.schwabapi.com".to_string(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_schwab_tokens_store_and_load() {
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        let persistence = MockSchwabPersistence::new();
        persistence.store_tokens(&tokens).await.unwrap();

        let stored_token = persistence.load_tokens().await.unwrap();
        assert_eq!(stored_token.access_token, "test_access_token");
        assert_eq!(stored_token.refresh_token, "test_refresh_token");
        assert_eq!(stored_token.access_token_fetched_at, now);
        assert_eq!(stored_token.refresh_token_fetched_at, now);
    }

    #[tokio::test]
    async fn test_schwab_tokens_store_upsert() {
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        let persistence = MockSchwabPersistence::new();
        persistence.store_tokens(&tokens).await.unwrap();

        let updated_tokens = SchwabTokens {
            access_token: "updated_access_token".to_string(),
            access_token_fetched_at: now,
            refresh_token: "updated_refresh_token".to_string(),
            refresh_token_fetched_at: now,
        };

        persistence.store_tokens(&updated_tokens).await.unwrap();

        assert!(persistence.tokens_stored().await);

        let stored_tokens = persistence.load_tokens().await.unwrap();
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
        let env = create_test_env();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

        assert_eq!(
            SchwabTokens::get_valid_access_token(&persistence, &env)
                .await
                .unwrap(),
            "valid_access_token"
        );
    }

    #[tokio::test]
    async fn test_get_valid_access_token_refresh_token_expired() {
        let env = create_test_env();
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(8),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

        let result = SchwabTokens::get_valid_access_token(&persistence, &env).await;

        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RefreshTokenExpired
        ));
    }

    #[tokio::test]
    async fn test_get_valid_access_token_needs_refresh() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

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

        let result = SchwabTokens::get_valid_access_token(&persistence, &env).await;

        mock.assert();
        assert_eq!(result.unwrap(), "refreshed_access_token");

        let stored_tokens = persistence.get_tokens().await.unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_get_valid_access_token_refresh_fails() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(35),
            refresh_token: "invalid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let result = SchwabTokens::get_valid_access_token(&persistence, &env).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_get_valid_access_token_no_tokens_in_db() {
        let env = create_test_env();
        let persistence = MockSchwabPersistence::new();

        let result = SchwabTokens::get_valid_access_token(&persistence, &env).await;

        assert!(matches!(result.unwrap_err(), SchwabError::Persistence(_)));
    }

    #[tokio::test]
    async fn test_refresh_if_needed_success() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(31),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

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

        let result = SchwabTokens::refresh_if_needed(&persistence, &env).await;

        mock.assert();
        assert!(result.unwrap());

        let stored_tokens = persistence.get_tokens().await.unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_refresh_if_needed_with_expired_refresh_token() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(31),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(8),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

        let result = SchwabTokens::refresh_if_needed(&persistence, &env).await;

        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RefreshTokenExpired
        ));
    }

    #[tokio::test]
    async fn test_refresh_if_needed_no_refresh_needed() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

        let result = SchwabTokens::refresh_if_needed(&persistence, &env).await;

        assert!(!result.unwrap());

        let stored_tokens = persistence.get_tokens().await.unwrap();
        assert_eq!(stored_tokens.access_token, "valid_access_token");
    }

    #[tokio::test]
    async fn test_refresh_if_needed_near_expiration() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "near_expiry_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(29),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

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

        let result = SchwabTokens::refresh_if_needed(&persistence, &env).await;

        mock.assert();
        assert!(result.unwrap());

        let stored_tokens = persistence.get_tokens().await.unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_automatic_token_refresh_before_expiration() -> Result<(), SchwabError> {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let now = Utc::now();

        let tokens = SchwabTokens {
            access_token: "near_expiration_access_token".to_string(),
            access_token_fetched_at: now - Duration::minutes(29),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: now - Duration::days(1),
        };

        let persistence = MockSchwabPersistence::with_tokens(tokens);

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

        let persistence_clone = persistence.clone();
        let env_clone = env.clone();

        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                tokio::time::timeout(
                    TokioDuration::from_secs(5),
                    start_automatic_token_refresh_loop(persistence_clone, env_clone),
                )
                .await
            })
        });

        sleep(TokioDuration::from_millis(2000)).await;

        handle.join().unwrap().unwrap_err();

        mock.assert();

        let stored_tokens = persistence.get_tokens().await.unwrap();
        assert_eq!(stored_tokens.access_token, "refreshed_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");

        Ok(())
    }
}
