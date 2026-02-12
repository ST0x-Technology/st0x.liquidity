use alloy::primitives::FixedBytes;
use backon::{ExponentialBuilder, Retryable};
use base64::prelude::*;
use chrono::Utc;
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::Deserialize;
use sqlx::SqlitePool;
use std::io::Read;
use std::sync::LazyLock;
use tracing::{debug, info};
use url::Url;

use super::super::{SchwabError, tokens::SchwabTokens};

static DEFAULT_REDIRECT_URI: LazyLock<Result<Url, url::ParseError>> =
    LazyLock::new(|| Url::parse("https://127.0.0.1"));

static DEFAULT_BASE_URL: LazyLock<Result<Url, url::ParseError>> =
    LazyLock::new(|| Url::parse("https://api.schwabapi.com"));

#[derive(Debug, Clone, Deserialize)]
pub struct SchwabAuthCtx {
    pub app_key: String,
    pub app_secret: String,
    pub redirect_uri: Option<Url>,
    pub base_url: Option<Url>,
    pub account_index: Option<usize>,
    pub encryption_key: FixedBytes<32>,
}

impl SchwabAuthCtx {
    pub fn redirect_uri(&self) -> Result<&Url, url::ParseError> {
        self.redirect_uri.as_ref().map_or_else(
            || DEFAULT_REDIRECT_URI.as_ref().map_err(ToOwned::to_owned),
            Ok,
        )
    }

    pub fn base_url(&self) -> Result<&Url, url::ParseError> {
        self.base_url
            .as_ref()
            .map_or_else(|| DEFAULT_BASE_URL.as_ref().map_err(ToOwned::to_owned), Ok)
    }

    pub fn account_index(&self) -> usize {
        self.account_index.unwrap_or(0)
    }
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
    pub hash_value: String,
}

impl SchwabAuthCtx {
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

        let base_url = self.base_url()?;
        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .get(format!("{base_url}trader/v1/accounts/accountNumbers",))
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

        if self.account_index() >= account_numbers.len() {
            return Err(SchwabError::AccountIndexOutOfBounds {
                index: self.account_index(),
                count: account_numbers.len(),
            });
        }

        Ok(account_numbers[self.account_index()].hash_value.clone())
    }

    pub fn get_auth_url(&self) -> Result<String, SchwabError> {
        Ok(format!(
            "{}v1/oauth/authorize?client_id={}&redirect_uri={}",
            self.base_url()?,
            urlencoding::encode(&self.app_key),
            urlencoding::encode(self.redirect_uri()?.as_str())
        ))
    }

    pub async fn get_tokens_from_code(&self, code: &str) -> Result<SchwabTokens, SchwabError> {
        info!("Getting tokens for code: {code}");
        let credentials = format!("{}:{}", self.app_key, self.app_secret);
        let credentials = BASE64_STANDARD.encode(credentials);

        let payload = format!(
            "grant_type=authorization_code&code={}&redirect_uri={}",
            urlencoding::encode(code),
            urlencoding::encode(self.redirect_uri()?.as_str())
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
            .post(format!("{}v1/oauth/token", self.base_url()?))
            .headers(headers)
            .body(payload)
            .send()
            .await?;

        debug!("Received response from Schwab API: {response:#?}");

        if !response.status().is_success() {
            let status = response.status();
            let body = extract_error_body(response).await;
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
        let credentials = format!("{}:{}", self.app_key, self.app_secret);
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

        let base_url = self.base_url()?;
        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .post(format!("{base_url}v1/oauth/token"))
                .headers(headers.clone())
                .body(payload.clone())
                .send()
                .await
        })
        .retry(ExponentialBuilder::default())
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = extract_error_body(response).await;
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

async fn extract_error_body(response: reqwest::Response) -> String {
    let is_gzipped = response
        .headers()
        .get("content-encoding")
        .map(HeaderValue::as_bytes)
        == Some(b"gzip");

    if is_gzipped {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::{Duration, Utc};
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_ctx() -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_app_key".to_string(),
            app_secret: "test_app_secret".to_string(),
            redirect_uri: None,
            base_url: None,
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_ctx_with_mock_server(mock_server: &MockServer) -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_app_key".to_string(),
            app_secret: "test_app_secret".to_string(),
            redirect_uri: None,
            base_url: Some(Url::parse(&mock_server.base_url()).expect("mock server base_url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[test]
    fn test_schwab_auth_env_get_auth_url() {
        let ctx = create_test_ctx();
        let expected_url = "https://api.schwabapi.com/v1/oauth/authorize?client_id=test_app_key&redirect_uri=https%3A%2F%2F127.0.0.1%2F";
        assert_eq!(ctx.get_auth_url().unwrap(), expected_url);
    }

    #[test]
    fn test_schwab_auth_env_get_auth_url_custom_base_url() {
        let ctx = SchwabAuthCtx {
            app_key: "custom_key".to_string(),
            app_secret: "custom_secret".to_string(),
            redirect_uri: Some(Url::parse("https://custom.redirect.com").expect("test url")),
            base_url: Some(Url::parse("https://custom.api.com").expect("test url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        };
        let expected_url = "https://custom.api.com/v1/oauth/authorize?client_id=custom_key&redirect_uri=https%3A%2F%2Fcustom.redirect.com%2F";
        assert_eq!(ctx.get_auth_url().unwrap(), expected_url);
    }

    #[test]
    fn test_schwab_auth_env_get_auth_url_with_special_characters() {
        let ctx = SchwabAuthCtx {
            app_key: "test key with spaces & symbols!".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: Some(
                Url::parse("https://example.com/callback?param=value&other=test")
                    .expect("test url"),
            ),
            base_url: None,
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        };
        let expected_url = "https://api.schwabapi.com/v1/oauth/authorize?client_id=test%20key%20with%20spaces%20%26%20symbols%21&redirect_uri=https%3A%2F%2Fexample.com%2Fcallback%3Fparam%3Dvalue%26other%3Dtest";
        assert_eq!(ctx.get_auth_url().unwrap(), expected_url);
    }

    #[tokio::test]
    async fn test_get_tokens_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

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
                .body_contains("redirect_uri=https%3A%2F%2F127.0.0.1%2F");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let tokens = ctx.get_tokens_from_code("test_code").await.unwrap();
        mock.assert();

        assert_eq!(tokens.access_token, "test_access_token");
        assert_eq!(tokens.refresh_token, "test_refresh_token");

        let now = Utc::now();
        assert!(now.signed_duration_since(tokens.access_token_fetched_at) < Duration::seconds(5));
        assert!(now.signed_duration_since(tokens.refresh_token_fetched_at) < Duration::seconds(5));
    }

    #[tokio::test]
    async fn test_get_tokens_http_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_request"}));
        });

        let error = ctx.get_tokens_from_code("invalid_code").await.unwrap_err();
        mock.assert();

        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "get tokens" && status.as_u16() == 400
        ));
    }

    #[tokio::test]
    async fn test_get_tokens_json_parse_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        let error = ctx.get_tokens_from_code("test_code").await.unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_get_tokens_missing_fields() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "test_access_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let error = ctx.get_tokens_from_code("test_code").await.unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_get_tokens_with_special_characters() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

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

        let tokens = ctx
            .get_tokens_from_code("code_with_special_chars_!@#$%^&*()")
            .await
            .unwrap();
        mock.assert();

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
        let ctx = create_test_ctx_with_mock_server(&server);

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

        let tokens = ctx.refresh_tokens("old_refresh_token").await.unwrap();
        mock.assert();

        assert_eq!(tokens.access_token, "new_access_token");
        assert_eq!(tokens.refresh_token, "new_refresh_token");

        let now = Utc::now();
        assert!(now.signed_duration_since(tokens.access_token_fetched_at) < Duration::seconds(5));
        assert!(now.signed_duration_since(tokens.refresh_token_fetched_at) < Duration::seconds(5));
    }

    #[tokio::test]
    async fn test_refresh_tokens_http_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let error = ctx
            .refresh_tokens("invalid_refresh_token")
            .await
            .unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::RequestFailed { .. }));
    }

    #[tokio::test]
    async fn test_refresh_tokens_json_parse_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        let error = ctx.refresh_tokens("test_refresh_token").await.unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_refresh_tokens_missing_fields() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock_response = json!({
            "access_token": "new_access_token"
        });

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let error = ctx.refresh_tokens("test_refresh_token").await.unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::Reqwest(_)));
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
    fn test_schwab_auth_ctx_defaults() {
        let ctx = SchwabAuthCtx {
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: None,
            base_url: None,
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        };

        assert_eq!(ctx.redirect_uri().unwrap().as_str(), "https://127.0.0.1/");
        assert_eq!(
            ctx.base_url().unwrap().as_str(),
            "https://api.schwabapi.com/"
        );
        assert_eq!(ctx.account_index(), 0);
    }

    #[tokio::test]
    async fn test_get_account_hash_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
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

        let hash = ctx.get_account_hash(&pool).await.unwrap();
        mock.assert();

        assert_eq!(hash, "ABC123DEF456");
    }

    #[tokio::test]
    async fn test_get_account_hash_with_custom_index() {
        let server = MockServer::start();
        let mut ctx = create_test_ctx_with_mock_server(&server);
        ctx.account_index = Some(1);
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

        let hash = ctx.get_account_hash(&pool).await.unwrap();
        mock.assert();

        assert_eq!(hash, "XYZ789GHI012");
    }

    #[tokio::test]
    async fn test_get_account_hash_index_out_of_bounds() {
        let server = MockServer::start();
        let mut ctx = create_test_ctx_with_mock_server(&server);
        ctx.account_index = Some(2);
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

        let error = ctx.get_account_hash(&pool).await.unwrap_err();
        mock.assert();

        assert!(matches!(
            error,
            SchwabError::AccountIndexOutOfBounds { index: 2, count: 1 }
        ));
    }

    #[tokio::test]
    async fn test_get_account_hash_no_accounts() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool).await;

        let mock = server.mock(|when, then| {
            when.method(GET).path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let error = ctx.get_account_hash(&pool).await.unwrap_err();
        mock.assert();

        assert!(matches!(error, SchwabError::NoAccountsFound));
    }

    #[tokio::test]
    async fn test_get_tokens_from_code_no_retries_on_failure() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(500);
        });

        let error = ctx.get_tokens_from_code("test_code").await.unwrap_err();

        assert_eq!(mock.hits(), 1);
        match error {
            SchwabError::RequestFailed { action, status, .. } => {
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
