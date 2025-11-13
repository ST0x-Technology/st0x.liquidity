use alloy::primitives::FixedBytes;
use backon::{ExponentialBuilder, Retryable};
use base64::prelude::*;
use chrono::Utc;
use clap::Parser;
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::Deserialize;
use sqlx::SqlitePool;
use tracing::{debug, info};

use super::{SchwabError, tokens::SchwabTokens};

#[derive(Parser, Debug, Clone)]
pub struct SchwabAuthEnv {
    #[clap(long, env)]
    pub schwab_app_key: String,
    #[clap(long, env)]
    pub schwab_app_secret: String,
    #[clap(long, env, default_value = "https://127.0.0.1")]
    pub schwab_redirect_uri: String,
    #[clap(long, env, default_value = "https://api.schwabapi.com")]
    pub schwab_base_url: String,
    #[clap(long, env, default_value = "0")]
    pub schwab_account_index: usize,
    #[clap(long, env)]
    pub encryption_key: FixedBytes<32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SchwabAuthResponse {
    /// Expires every 30 minutes
    pub access_token: String,
    /// Expires every 7 days
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountNumbers {
    // Field exists in API response but isn't currently used
    #[allow(dead_code)]
    pub account_number: String,
    pub hash_value: String,
}

impl SchwabAuthEnv {
    pub async fn get_account_hash(&self, pool: &SqlitePool) -> Result<String, SchwabError> {
        let access_token = SchwabTokens::get_valid_access_token(pool, self).await?;

        let headers = [
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {access_token}"))?,
            ),
            (header::ACCEPT, HeaderValue::from_str("application/json")?),
        ]
        .into_iter()
        .collect::<HeaderMap>();

        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .get(format!(
                    "{}/trader/v1/accounts/accountNumbers",
                    self.schwab_base_url
                ))
                .headers(headers.clone())
                .send()
                .await
        })
        .retry(ExponentialBuilder::default())
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(SchwabError::RequestFailed {
                action: "get account hash".to_string(),
                status,
                body,
            });
        }

        let account_numbers: Vec<AccountNumbers> = response.json().await?;

        if account_numbers.is_empty() {
            return Err(SchwabError::NoAccountsFound);
        }

        if self.schwab_account_index >= account_numbers.len() {
            return Err(SchwabError::AccountIndexOutOfBounds {
                index: self.schwab_account_index,
                count: account_numbers.len(),
            });
        }

        Ok(account_numbers[self.schwab_account_index]
            .hash_value
            .clone())
    }

    pub fn get_auth_url(&self) -> String {
        format!(
            "{}/v1/oauth/authorize?client_id={}&redirect_uri={}",
            self.schwab_base_url,
            urlencoding::encode(&self.schwab_app_key),
            urlencoding::encode(&self.schwab_redirect_uri)
        )
    }

    pub async fn get_tokens_from_code(&self, code: &str) -> Result<SchwabTokens, SchwabError> {
        info!("Getting tokens for code: {code}");
        let credentials = format!("{}:{}", self.schwab_app_key, self.schwab_app_secret);
        let credentials = BASE64_STANDARD.encode(credentials);

        let payload = format!(
            "grant_type=authorization_code&code={}&redirect_uri={}",
            urlencoding::encode(code),
            urlencoding::encode(&self.schwab_redirect_uri)
        );

        let headers = [
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {credentials}"))?,
            ),
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str("application/x-www-form-urlencoded")?,
            ),
        ]
        .into_iter()
        .collect::<HeaderMap>();

        debug!("Sending request to Schwab API with headers: {headers:?}\nAnd payload: {payload}");
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/oauth/token", self.schwab_base_url))
            .headers(headers)
            .body(payload)
            .send()
            .await?;

        debug!("Received response from Schwab API: {response:#?}");

        if !response.status().is_success() {
            let status = response.status();
            let body = if response
                .headers()
                .get("content-encoding")
                .map(reqwest::header::HeaderValue::as_bytes)
                == Some(b"gzip")
            {
                use std::io::Read;
                let bytes = response.bytes().await.unwrap_or_default();
                let mut decoder = flate2::read::GzDecoder::new(bytes.as_ref());
                let mut decompressed = String::new();
                match decoder.read_to_string(&mut decompressed) {
                    Ok(_) => decompressed,
                    Err(_) => "Failed to decode gzipped response".to_string(),
                }
            } else {
                response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read response body".to_string())
            };
            return Err(SchwabError::RequestFailed {
                action: "get tokens".to_string(),
                status,
                body,
            });
        }

        let response: SchwabAuthResponse = response.json().await?;

        Ok(SchwabTokens {
            access_token: response.access_token,
            access_token_fetched_at: Utc::now(),
            refresh_token: response.refresh_token,
            refresh_token_fetched_at: Utc::now(),
        })
    }

    pub async fn refresh_tokens(&self, refresh_token: &str) -> Result<SchwabTokens, SchwabError> {
        let credentials = format!("{}:{}", self.schwab_app_key, self.schwab_app_secret);
        let credentials = BASE64_STANDARD.encode(credentials);

        let payload = format!(
            "grant_type=refresh_token&refresh_token={}",
            urlencoding::encode(refresh_token)
        );

        let headers = [
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {credentials}"))?,
            ),
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str("application/x-www-form-urlencoded")?,
            ),
        ]
        .into_iter()
        .collect::<HeaderMap>();

        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .post(format!("{}/v1/oauth/token", self.schwab_base_url))
                .headers(headers.clone())
                .body(payload.clone())
                .send()
                .await
        })
        .retry(ExponentialBuilder::default())
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(SchwabError::RequestFailed {
                action: "token request".to_string(),
                status,
                body,
            });
        }

        let response: SchwabAuthResponse = response.json().await?;

        Ok(SchwabTokens {
            access_token: response.access_token,
            access_token_fetched_at: Utc::now(),
            refresh_token: response.refresh_token,
            refresh_token_fetched_at: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::{Duration, Utc};
    use httpmock::prelude::*;
    use serde_json::json;

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

    #[test]
    fn test_schwab_auth_env_get_auth_url() {
        let env = create_test_env();
        let expected_url = "https://api.schwabapi.com/v1/oauth/authorize?client_id=test_app_key&redirect_uri=https%3A%2F%2F127.0.0.1";
        assert_eq!(env.get_auth_url(), expected_url);
    }

    #[test]
    fn test_schwab_auth_env_get_auth_url_custom_base_url() {
        let env = SchwabAuthEnv {
            schwab_app_key: "custom_key".to_string(),
            schwab_app_secret: "custom_secret".to_string(),
            schwab_redirect_uri: "https://custom.redirect.com".to_string(),
            schwab_base_url: "https://custom.api.com".to_string(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        };
        let expected_url = "https://custom.api.com/v1/oauth/authorize?client_id=custom_key&redirect_uri=https%3A%2F%2Fcustom.redirect.com";
        assert_eq!(env.get_auth_url(), expected_url);
    }

    #[test]
    fn test_schwab_auth_env_get_auth_url_with_special_characters() {
        let env = SchwabAuthEnv {
            schwab_app_key: "test key with spaces & symbols!".to_string(),
            schwab_app_secret: "test_secret".to_string(),
            schwab_redirect_uri: "https://example.com/callback?param=value&other=test".to_string(),
            schwab_base_url: "https://api.schwabapi.com".to_string(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        };
        let expected_url = "https://api.schwabapi.com/v1/oauth/authorize?client_id=test%20key%20with%20spaces%20%26%20symbols%21&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback%3Fparam%3Dvalue%26other%3Dtest";
        assert_eq!(env.get_auth_url(), expected_url);
    }

    #[tokio::test]
    async fn test_get_tokens_success() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .header(
                    "authorization",
                    "Basic dGVzdF9hcHBfa2V5OnRlc3RfYXBwX3NlY3JldA==",
                )
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=authorization_code")
                .body_contains("code=test_code")
                .body_contains("redirect_uri=https%3A%2F%2F127.0.0.1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.get_tokens_from_code("test_code").await;

        mock.assert();

        let tokens = result.unwrap();
        assert_eq!(tokens.access_token, "test_access_token");
        assert_eq!(tokens.refresh_token, "test_refresh_token");

        let now = Utc::now();
        assert!(now.signed_duration_since(tokens.access_token_fetched_at) < Duration::seconds(5));
        assert!(now.signed_duration_since(tokens.refresh_token_fetched_at) < Duration::seconds(5));
    }

    #[tokio::test]
    async fn test_get_tokens_http_error() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_request"}));
        });

        let result = env.get_tokens_from_code("invalid_code").await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { action, status, .. }
            if action == "get tokens" && status.as_u16() == 400
        ));
    }

    #[tokio::test]
    async fn test_get_tokens_json_parse_error() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        let result = env.get_tokens_from_code("test_code").await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_get_tokens_missing_fields() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "test_access_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.get_tokens_from_code("test_code").await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_get_tokens_with_special_characters() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "access_token_with_special_chars_!@#$%^&*()",
            "refresh_token": "refresh_token_with_special_chars_!@#$%^&*()"
        });

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env
            .get_tokens_from_code("code_with_special_chars_!@#$%^&*()")
            .await;

        mock.assert();
        let tokens = result.unwrap();
        assert_eq!(
            tokens.access_token,
            "access_token_with_special_chars_!@#$%^&*()"
        );
        assert_eq!(
            tokens.refresh_token,
            "refresh_token_with_special_chars_!@#$%^&*()"
        );
    }

    #[tokio::test]
    async fn test_refresh_tokens_success() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "new_access_token",
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
                .body_contains("refresh_token=old_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.refresh_tokens("old_refresh_token").await;

        mock.assert();

        let tokens = result.unwrap();
        assert_eq!(tokens.access_token, "new_access_token");
        assert_eq!(tokens.refresh_token, "new_refresh_token");

        let now = Utc::now();
        assert!(now.signed_duration_since(tokens.access_token_fetched_at) < Duration::seconds(5));
        assert!(now.signed_duration_since(tokens.refresh_token_fetched_at) < Duration::seconds(5));
    }

    #[tokio::test]
    async fn test_refresh_tokens_http_error() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let result = env.refresh_tokens("invalid_refresh_token").await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_refresh_tokens_json_parse_error() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        let result = env.refresh_tokens("test_refresh_token").await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_refresh_tokens_missing_fields() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "new_access_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.refresh_tokens("test_refresh_token").await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));
    }

    #[test]
    fn test_schwab_auth_response_deserialization() {
        let json_str = r#"{"access_token": "test_access", "refresh_token": "test_refresh"}"#;
        let response: SchwabAuthResponse = serde_json::from_str(json_str).unwrap();

        assert_eq!(response.access_token, "test_access");
        assert_eq!(response.refresh_token, "test_refresh");
    }

    #[test]
    fn test_schwab_auth_response_deserialization_missing_field() {
        let json_str = r#"{"access_token": "test_access"}"#;
        let result: Result<SchwabAuthResponse, _> = serde_json::from_str(json_str);
        assert!(matches!(result.unwrap_err(), serde_json::Error { .. }));
    }

    #[test]
    fn test_schwab_auth_error_display() {
        let invalid_header_err =
            SchwabError::InvalidHeader(HeaderValue::from_str("test\x00").unwrap_err());
        assert!(
            invalid_header_err
                .to_string()
                .contains("Failed to create header value")
        );
    }

    #[test]
    fn test_schwab_auth_env_default_values() {
        let env = SchwabAuthEnv {
            schwab_app_key: "test_key".to_string(),
            schwab_app_secret: "test_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: "https://api.schwabapi.com".to_string(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        };

        assert_eq!(env.schwab_redirect_uri, "https://127.0.0.1");
        assert_eq!(env.schwab_base_url, "https://api.schwabapi.com");
    }

    #[tokio::test]
    async fn test_get_account_hash_success() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool).await;

        let mock_response = json!([
            {
                "accountNumber": "123456789",
                "hashValue": "ABC123DEF456"
            },
            {
                "accountNumber": "987654321",
                "hashValue": "XYZ789GHI012"
            }
        ]);

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/trader/v1/accounts/accountNumbers")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.get_account_hash(&pool).await;

        mock.assert();
        assert_eq!(result.unwrap(), "ABC123DEF456");
    }

    #[tokio::test]
    async fn test_get_account_hash_with_custom_index() {
        let server = MockServer::start();
        let mut env = create_test_env_with_mock_server(&server);
        env.schwab_account_index = 1;
        let pool = setup_test_db().await;
        setup_test_tokens(&pool).await;

        let mock_response = json!([
            {
                "accountNumber": "123456789",
                "hashValue": "ABC123DEF456"
            },
            {
                "accountNumber": "987654321",
                "hashValue": "XYZ789GHI012"
            }
        ]);

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/trader/v1/accounts/accountNumbers")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.get_account_hash(&pool).await;

        mock.assert();
        assert_eq!(result.unwrap(), "XYZ789GHI012");
    }

    #[tokio::test]
    async fn test_get_account_hash_index_out_of_bounds() {
        let server = MockServer::start();
        let mut env = create_test_env_with_mock_server(&server);
        env.schwab_account_index = 2;
        let pool = setup_test_db().await;
        setup_test_tokens(&pool).await;

        let mock_response = json!([
            {
                "accountNumber": "123456789",
                "hashValue": "ABC123DEF456"
            }
        ]);

        let mock = server.mock(|when, then| {
            when.method(GET).path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = env.get_account_hash(&pool).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::AccountIndexOutOfBounds { index: 2, count: 1 }
        ));
    }

    #[tokio::test]
    async fn test_get_account_hash_no_accounts() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool).await;

        let mock = server.mock(|when, then| {
            when.method(GET).path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let result = env.get_account_hash(&pool).await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::NoAccountsFound));
    }

    #[tokio::test]
    async fn test_get_tokens_from_code_no_retries_on_failure() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(500);
        });

        let result = env.get_tokens_from_code("test_code").await;

        assert_eq!(mock.hits(), 1);
        match result {
            Err(SchwabError::RequestFailed { action, status, .. }) => {
                assert_eq!(action, "get tokens");
                assert_eq!(status.as_u16(), 500);
            }
            other => panic!("Expected RequestFailed error, got: {other:?}"),
        }
    }

    async fn setup_test_tokens(pool: &SqlitePool) {
        let tokens = crate::schwab::tokens::SchwabTokens {
            access_token: "test_access_token".to_string(),
            access_token_fetched_at: Utc::now(),
            refresh_token: "test_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now(),
        };
        tokens.store(pool, &TEST_ENCRYPTION_KEY).await.unwrap();
    }
}
