use reqwest::header::InvalidHeaderValue;
use thiserror::Error;

mod auth;
mod broker;
mod encryption;
mod market_hours;
mod order;
mod order_status;
mod tokens;

// Re-export only what's needed for broker construction
pub use auth::SchwabAuthEnv;
pub use broker::{SchwabBroker, SchwabConfig};

// Re-export for auth CLI command (Schwab-specific, not part of generic broker API)
pub use tokens::SchwabTokens;

/// Errors that can occur during Schwab broker operations including API calls,
/// authentication, database operations, and order processing.
#[derive(Error, Debug)]
pub enum SchwabError {
    /// HTTP header creation failed, wraps [`reqwest::header::InvalidHeaderValue`].
    #[error("Failed to create header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),

    /// HTTP request execution failed, wraps [`reqwest::Error`].
    #[error("Request failed: {0}")]
    Reqwest(#[from] reqwest::Error),

    /// Database query or migration failed, wraps [`sqlx::Error`].
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// File system operation failed, wraps [`std::io::Error`].
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// URL string parsing failed, wraps [`url::ParseError`].
    #[error("URL parsing failed: {0}")]
    Url(#[from] url::ParseError),

    /// OAuth redirect URL missing required authorization code parameter.
    /// `url`: The full redirect URL received.
    #[error("Missing authorization code parameter in URL: {url}")]
    MissingAuthCode { url: String },

    /// JSON serialization or deserialization failed, wraps [`serde_json::Error`].
    #[error("JSON serialization failed: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    /// Refresh token expired and manual re-authentication required.
    #[error("Refresh token has expired")]
    RefreshTokenExpired,

    /// Schwab API returned no account numbers for the authenticated user.
    #[error("No accounts found")]
    NoAccountsFound,

    /// Requested account index exceeds available accounts.
    /// `index`: The requested account index.
    /// `count`: Total number of accounts available.
    #[error("Account index {index} out of bounds (found {count} accounts)")]
    AccountIndexOutOfBounds { index: usize, count: usize },

    /// Schwab API request completed with non-success HTTP status.
    /// `action`: Description of the attempted operation.
    /// `status`: HTTP status code returned.
    /// `body`: Response body text.
    #[error("{action} failed with status: {status}, body: {body}")]
    RequestFailed {
        action: String,
        status: reqwest::StatusCode,
        body: String,
    },

    /// Broker configuration validation failed during initialization.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Order execution database persistence failed, wraps [`crate::error::PersistenceError`].
    #[error("Execution persistence error: {0}")]
    ExecutionPersistence(#[from] crate::error::PersistenceError),

    /// Schwab API response body parsing failed.
    /// `action`: Description of the attempted operation.
    /// `response_text`: Raw API response body.
    /// `parse_error`: Error message from parser.
    #[error(
        "Failed to parse API response: {action}, response: {response_text}, error: {parse_error}"
    )]
    ApiResponseParse {
        action: String,
        response_text: String,
        parse_error: String,
    },

    /// Token encryption or decryption failed, wraps [`encryption::EncryptionError`].
    #[error("Encryption error: {0}")]
    Encryption(#[from] encryption::EncryptionError),
}

pub fn extract_code_from_url(url: &str) -> Result<String, SchwabError> {
    let parsed_url = url::Url::parse(url)?;

    parsed_url
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| SchwabError::MissingAuthCode {
            url: url.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TEST_ENCRYPTION_KEY;
    use httpmock::prelude::*;
    use serde_json::json;

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
    fn test_extract_code_from_url_success() {
        let url = "https://127.0.0.1/?code=test_auth_code&state=xyz";
        assert_eq!(extract_code_from_url(url).unwrap(), "test_auth_code");
    }

    #[test]
    fn test_extract_code_from_url_missing_code() {
        let url = "https://127.0.0.1/?state=xyz&other=param";
        let result = extract_code_from_url(url);
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::MissingAuthCode { url: ref u } if u == "https://127.0.0.1/?state=xyz&other=param"
        ));
    }

    #[test]
    fn test_extract_code_from_url_invalid_url() {
        let url = "not_a_valid_url";
        assert!(matches!(
            extract_code_from_url(url).unwrap_err(),
            SchwabError::Url(_)
        ));
    }

    #[test]
    fn test_extract_code_from_url_no_query_params() {
        let url = "https://127.0.0.1/";
        let result = extract_code_from_url(url);
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::MissingAuthCode { url: ref u } if u == "https://127.0.0.1/"
        ));
    }

    #[tokio::test]
    async fn test_get_tokens_from_code_http_401() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .header(
                    "authorization",
                    "Basic dGVzdF9hcHBfa2V5OnRlc3RfYXBwX3NlY3JldA==",
                )
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=authorization_code")
                .body_contains("code=invalid_code");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let result = env.get_tokens_from_code("invalid_code").await;
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { action, status, .. }
            if action == "get tokens" && status.as_u16() == 401
        ));

        mock.assert();
    }

    #[tokio::test]
    async fn test_get_tokens_from_code_http_500() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(500);
        });

        let result = env.get_tokens_from_code("test_code").await;
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { action, status, .. }
            if action == "get tokens" && status.as_u16() == 500
        ));

        mock.assert();
    }

    #[tokio::test]
    async fn test_get_tokens_from_code_invalid_json_response() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        assert!(matches!(
            env.get_tokens_from_code("test_code").await.unwrap_err(),
            SchwabError::Reqwest(_)
        ));

        mock.assert();
    }
}
