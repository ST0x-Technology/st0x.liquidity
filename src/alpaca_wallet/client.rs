//! Authenticated HTTP transport for Alpaca Broker API wallet modules.

use std::borrow::Cow;

use alloy::primitives::{Address, TxHash, hex::FromHexError};
use reqwest::{Client, Method, Response, StatusCode};
use thiserror::Error;
use tracing::{trace, warn};

use st0x_execution::AlpacaAccountId;

use super::transfer::{AlpacaTransferId, Network, TokenSymbol, TransferStatus};
use super::whitelist::{TravelRuleInfo, WhitelistEntry, WhitelistStatus};

#[derive(Debug, Error)]
pub enum AlpacaWalletError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },
    #[error("Failed to parse response")]
    ParseError(#[from] serde_json::Error),
    #[error("response body was not valid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    FromHex(#[from] FromHexError),
    #[error("Transfer not found: {transfer_id}")]
    TransferNotFound { transfer_id: AlpacaTransferId },
    #[error("Transfer {transfer_id} timed out after {elapsed:?}")]
    TransferTimeout {
        transfer_id: AlpacaTransferId,
        elapsed: std::time::Duration,
    },
    #[error(
        "Invalid status transition for transfer {transfer_id}: \
         {previous:?} -> {next:?}"
    )]
    InvalidStatusTransition {
        transfer_id: AlpacaTransferId,
        previous: TransferStatus,
        next: TransferStatus,
    },
    #[error("Address {address} is not whitelisted for {asset} on {network}")]
    AddressNotWhitelisted {
        address: Address,
        asset: TokenSymbol,
        network: Network,
    },
    #[error("No whitelist entries found for address {address}")]
    NoWhitelistEntries { address: Address },
    #[error("Deposit with tx hash {tx_hash} not detected after {elapsed:?}")]
    DepositTimeout {
        tx_hash: TxHash,
        elapsed: std::time::Duration,
    },
    #[error("Invalid status transition for deposit {tx_hash}: {previous:?} -> {next:?}")]
    InvalidDepositTransition {
        tx_hash: TxHash,
        previous: TransferStatus,
        next: TransferStatus,
    },
}

pub struct AlpacaWalletClient {
    client: Client,
    account_id: AlpacaAccountId,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaWalletClient {
    pub(crate) fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
    ) -> Self {
        Self {
            client: Client::new(),
            account_id,
            base_url,
            api_key,
            api_secret,
        }
    }

    pub(super) async fn get(&self, path: &str) -> Result<String, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);
        trace!(target: "wallet", "GET {url}");

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        read_response_body(Method::GET, response).await
    }

    pub(super) async fn post<T: serde::Serialize + Sync>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<String, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);
        trace!(target: "wallet", "POST {url}");

        // Alpaca API requires both Basic auth AND APCA headers for authentication
        let response = self
            .client
            .post(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(body)
            .send()
            .await?;

        read_response_body(Method::POST, response).await
    }

    pub(super) async fn delete(&self, path: &str) -> Result<String, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);
        trace!(target: "wallet", "DELETE {url}");

        let response = self
            .client
            .delete(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        read_response_body(Method::DELETE, response).await
    }

    pub(super) async fn patch<T: serde::Serialize + Sync>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<String, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);
        trace!(target: "wallet", "PATCH {url}");

        let response = self
            .client
            .patch(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(body)
            .send()
            .await?;

        read_response_body(Method::PATCH, response).await
    }

    pub(super) fn account_id(&self) -> &AlpacaAccountId {
        &self.account_id
    }

    pub(super) async fn get_whitelisted_addresses(
        &self,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        let path = format!("/v1/accounts/{}/wallets/whitelists", self.account_id);

        let body = self.get(&path).await?;
        let entries: Vec<WhitelistEntry> = serde_json::from_str(&body)?;

        Ok(entries)
    }

    pub(super) async fn is_address_whitelisted_and_approved(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        _network: &Network,
    ) -> Result<bool, AlpacaWalletError> {
        let entries = self.get_whitelisted_addresses().await?;

        // NOTE: Chain comparison disabled due to Alpaca API inconsistency.
        // Request uses "ethereum" but response returns "ETH".
        // TODO: Re-enable once Alpaca fixes the chain field or we normalize values.
        Ok(entries.iter().any(|entry| {
            entry.address == *address
                && entry.asset == *asset
                && entry.status == WhitelistStatus::Approved
        }))
    }

    /// Creates a whitelist entry for a withdrawal address.
    ///
    /// The address will be in PENDING status initially and must be approved
    /// before withdrawals can be made (typically within 24 hours).
    /// Alpaca requires `travel_rule_info` on all whitelist creation requests,
    /// effective 2026-03-27.
    pub(super) async fn create_whitelist_entry(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        _network: &Network,
        travel_rule_info: &TravelRuleInfo,
    ) -> Result<WhitelistEntry, AlpacaWalletError> {
        #[derive(serde::Serialize)]
        struct Request<'a> {
            address: String,
            asset: &'a str,
            travel_rule_info: &'a TravelRuleInfo,
        }

        let path = format!("/v1/accounts/{}/wallets/whitelists", self.account_id);

        let request = Request {
            // None = standard EIP-55 checksum (no chain-specific EIP-1191 encoding).
            // Fine for now since this system only handles Ethereum mainnet.
            address: address.to_checksum(None),
            asset: asset.as_ref(),
            travel_rule_info,
        };

        let body = self.post(&path, &request).await?;

        Ok(serde_json::from_str::<WhitelistEntry>(&body)?)
    }

    pub(super) async fn delete_whitelist_entry(
        &self,
        whitelist_id: &str,
    ) -> Result<(), AlpacaWalletError> {
        let path = format!(
            "/v1/accounts/{}/wallets/whitelists/{}",
            self.account_id, whitelist_id
        );

        self.delete(&path).await?;
        Ok(())
    }

    /// Updates travel rule info on an existing whitelisted address.
    ///
    /// Uses the PATCH endpoint added by Alpaca for the March 2026 travel
    /// rule requirement. Existing whitelists that were created without
    /// travel rule info must be patched before they can be used for
    /// withdrawals.
    pub(super) async fn patch_whitelist_travel_rule(
        &self,
        whitelist_id: &str,
        travel_rule_info: &TravelRuleInfo,
    ) -> Result<(), AlpacaWalletError> {
        #[derive(serde::Serialize)]
        struct Request<'a> {
            travel_rule_info: &'a TravelRuleInfo,
        }

        let path = format!(
            "/v1/accounts/{}/wallets/whitelists/{}/travel-rule-info",
            self.account_id, whitelist_id
        );

        self.patch(&path, &Request { travel_rule_info }).await?;
        Ok(())
    }
}

async fn read_response_body(
    method: Method,
    response: Response,
) -> Result<String, AlpacaWalletError> {
    let status = response.status();
    let url = response.url().clone();
    // Read raw bytes and convert the success body with `String::from_utf8` so
    // invalid UTF-8 fails fast (matching the prior `response.json()` at the call
    // sites) instead of being silently replaced by `response.text()`'s lossy
    // decoding. Lossy decoding is used only for the trace line and error body.
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        // Preserve the HTTP status on a non-success response even if the body
        // stream fails to read, so the poll retry predicate (which only retries
        // `ApiError { status }` with `status.is_server_error()`) still fires on
        // a transient 5xx. Mirrors the pre-refactor `.text().unwrap_or_else(..)`.
        Err(_) if !status.is_success() => {
            return Err(AlpacaWalletError::ApiError {
                status,
                message: "Unknown error".to_string(),
            });
        }
        Err(error) => return Err(error.into()),
    };

    trace!(
        target: "wallet",
        %method,
        status = %status,
        url = %url,
        body = %redact_beneficiary_for_logging(&String::from_utf8_lossy(&bytes)),
        "Alpaca wallet API response body received"
    );

    if !status.is_success() {
        // Redact the stored body too: `ApiError.message` is surfaced via
        // Display/Debug and propagated to other log sites, so it must honour the
        // same beneficiary-redaction boundary as the trace line above.
        return Err(AlpacaWalletError::ApiError {
            status,
            message: redact_beneficiary_for_logging(&String::from_utf8_lossy(&bytes)).into_owned(),
        });
    }

    // `bytes` is no longer borrowed here, so consume it without copying.
    Ok(String::from_utf8(bytes.into())?)
}

/// Redacts `beneficiary_entity_name` values from a JSON response body for
/// logging, mirroring the redaction `TravelRuleConfig`'s `Debug` impl applies.
/// Whitelist responses echo back the Travel Rule beneficiary identity, which
/// the project deliberately keeps out of logs. The full body is still returned
/// to callers; only the logged representation is scrubbed.
fn redact_beneficiary_for_logging(body: &str) -> Cow<'_, str> {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(mut value) => {
            redact_beneficiary_in_place(&mut value);
            match serde_json::to_string(&value) {
                Ok(redacted) => Cow::Owned(redacted),
                // Fail closed: never fall back to the raw (unredacted) body, or
                // a re-serialization failure would leak the beneficiary name.
                Err(error) => {
                    warn!(
                        ?error,
                        "failed to re-serialize redacted wallet body; omitting body from log"
                    );
                    Cow::Owned(format!("<redaction failed, body {} bytes>", body.len()))
                }
            }
        }
        // Non-JSON bodies (e.g. plain-text gateway errors) carry no structured
        // travel-rule data, so they are normally safe to log verbatim. Fail
        // closed if such a body nonetheless references the sensitive field by
        // name (e.g. a proxy echoing the request), rather than leaking it.
        Err(_) if body.contains("beneficiary_entity_name") => {
            warn!("non-JSON wallet body references beneficiary identity; omitting body from log");
            Cow::Owned(format!(
                "<redaction failed, non-JSON body {} bytes>",
                body.len()
            ))
        }
        Err(_) => Cow::Borrowed(body),
    }
}

fn redact_beneficiary_in_place(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "beneficiary_entity_name" {
                    *child = serde_json::Value::String("<redacted>".to_string());
                } else {
                    redact_beneficiary_in_place(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                redact_beneficiary_in_place(item);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use uuid::uuid;

    use st0x_config::TravelRuleConfig;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[test]
    fn test_client_construction() {
        let client = AlpacaWalletClient::new(
            "https://broker-api.sandbox.alpaca.markets".to_string(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        assert_eq!(*client.account_id(), TEST_ACCOUNT_ID);
        assert_eq!(client.api_key, "test_key_id");
        assert_eq!(client.api_secret, "test_secret_key");
    }

    #[tokio::test]
    async fn test_get_with_auth_headers() {
        let server = MockServer::start();

        // Basic auth header: base64("test_key_id:test_secret_key") = "dGVzdF9rZXlfaWQ6dGVzdF9zZWNyZXRfa2V5"
        let test_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/test")
                .header(
                    "authorization",
                    "Basic dGVzdF9rZXlfaWQ6dGVzdF9zZWNyZXRfa2V5",
                )
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let response = client.get("/v1/test").await.unwrap();

        assert_eq!(response, r#"{"success":true}"#);
        test_mock.assert();
    }

    #[tokio::test]
    async fn test_get_api_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/error");
            then.status(401).json_body(json!({
                "message": "Invalid credentials"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        assert!(matches!(
            client.get("/v1/error").await.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::UNAUTHORIZED
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_get_server_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/server_error");
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        assert!(matches!(
            client.get("/v1/server_error").await.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::INTERNAL_SERVER_ERROR
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_post_with_json_body() {
        let server = MockServer::start();

        let test_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/test")
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key")
                .json_body(json!({"amount": "10.5", "asset": "USDC"}));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let body = json!({"amount": "10.5", "asset": "USDC"});
        let response = client.post("/v1/test", &body).await.unwrap();

        assert_eq!(response, r#"{"success":true}"#);
        test_mock.assert();
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn get_logs_success_response_body_at_trace_level() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v1/test");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"wallet_marker": "success-body"}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        client.get("/v1/test").await.unwrap();

        assert!(logs_contain("Alpaca wallet API response body received"));
        assert!(logs_contain("wallet_marker"));
        assert!(logs_contain("success-body"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn get_logs_error_response_body_at_trace_level() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v1/error");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"wallet_marker": "error-body"}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = client.get("/v1/error").await.unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::BAD_REQUEST
        ));
        assert!(logs_contain("Alpaca wallet API response body received"));
        assert!(logs_contain("wallet_marker"));
        assert!(logs_contain("error-body"));
    }

    #[test]
    fn redact_for_logging_passes_through_non_json_bodies() {
        // Gateway/plain-text error bodies (common during incidents) carry no
        // structured travel-rule data and are returned verbatim, borrowed.
        let body = "502 Bad Gateway";

        let redacted = redact_beneficiary_for_logging(body);

        assert!(matches!(redacted, Cow::Borrowed(_)));
        assert_eq!(redacted, "502 Bad Gateway");
    }

    #[test]
    fn redact_for_logging_scrubs_beneficiary_name_nested_in_arrays() {
        let body = json!({
            "entries": [
                {
                    "travel_rule_info": {
                        "beneficiary_entity_name": "T0 TRADE (BVI) LTD",
                        "beneficiary_is_self_hosted": true
                    }
                }
            ]
        })
        .to_string();

        let redacted = redact_beneficiary_for_logging(&body);

        assert!(matches!(redacted, Cow::Owned(_)));
        let parsed: serde_json::Value = serde_json::from_str(&redacted).unwrap();
        // The key must be present with the redacted value (not deleted), the
        // non-sensitive sibling preserved, and the original value gone.
        assert_eq!(
            parsed["entries"][0]["travel_rule_info"]["beneficiary_entity_name"],
            "<redacted>"
        );
        assert_eq!(
            parsed["entries"][0]["travel_rule_info"]["beneficiary_is_self_hosted"],
            true
        );
        assert!(!redacted.contains("T0 TRADE (BVI) LTD"));
    }

    #[test]
    fn redact_for_logging_fails_closed_on_non_json_body_referencing_beneficiary() {
        // A non-JSON body that echoes the sensitive field (e.g. a proxy
        // reflecting the request) must not be logged verbatim.
        let body = "error: beneficiary_entity_name 'T0 TRADE (BVI) LTD' rejected";

        let redacted = redact_beneficiary_for_logging(body);

        assert!(matches!(redacted, Cow::Owned(_)));
        assert!(!redacted.contains("T0 TRADE (BVI) LTD"));
    }

    #[test]
    fn redact_for_logging_leaves_json_without_beneficiary_field_unchanged() {
        let body = json!({ "id": "wl-1", "status": "approved" }).to_string();

        let redacted = redact_beneficiary_for_logging(&body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&redacted).unwrap(),
            serde_json::from_str::<serde_json::Value>(&body).unwrap()
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn trace_log_redacts_beneficiary_entity_name_but_caller_keeps_full_body() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v1/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "wl-1",
                    "travel_rule_info": {
                        "beneficiary_is_self_hosted": true,
                        "beneficiary_entity_name": "T0 TRADE (BVI) LTD"
                    }
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let body = client.get("/v1/whitelists").await.unwrap();

        // The caller still receives the full, unredacted body for parsing.
        assert!(body.contains("T0 TRADE (BVI) LTD"));

        // The trace log must scrub the beneficiary identity while keeping the
        // non-sensitive travel-rule fields for debugging.
        assert!(logs_contain("Alpaca wallet API response body received"));
        assert!(logs_contain("<redacted>"));
        assert!(logs_contain("beneficiary_is_self_hosted"));
        assert!(!logs_contain("T0 TRADE (BVI) LTD"));
    }

    #[tokio::test]
    async fn api_error_message_redacts_beneficiary_entity_name() {
        let server = MockServer::start();

        // A non-2xx body echoing the Travel Rule beneficiary identity must have
        // that identity scrubbed from `ApiError.message`, which is surfaced via
        // Display/Debug and re-logged downstream.
        server.mock(|when, then| {
            when.method(POST).path("/v1/whitelists");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "invalid travel rule info",
                    "travel_rule_info": {
                        "beneficiary_is_self_hosted": true,
                        "beneficiary_entity_name": "T0 TRADE (BVI) LTD"
                    }
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let AlpacaWalletError::ApiError { status, message } = client
            .post("/v1/whitelists", &json!({"any": "body"}))
            .await
            .unwrap_err()
        else {
            panic!("expected AlpacaWalletError::ApiError");
        };

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!message.contains("T0 TRADE (BVI) LTD"));
        let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(
            parsed["travel_rule_info"]["beneficiary_entity_name"],
            "<redacted>"
        );
        // Non-sensitive fields must survive redaction for debuggability.
        assert_eq!(parsed["message"], "invalid travel rule info");
        assert_eq!(
            parsed["travel_rule_info"]["beneficiary_is_self_hosted"],
            true
        );
    }

    #[tokio::test]
    async fn get_returns_utf8_error_for_non_utf8_success_body() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v1/test");
            then.status(200)
                .header("content-type", "application/octet-stream")
                .body(b"\xFF\xFE not valid utf-8");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        assert!(matches!(
            client.get("/v1/test").await.unwrap_err(),
            AlpacaWalletError::Utf8(_)
        ));
    }

    #[tokio::test]
    async fn test_post_api_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/error");
            then.status(400).json_body(json!({
                "message": "Invalid request"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let body = json!({"test": "data"});

        assert!(matches!(
            client.post("/v1/error", &body).await.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::BAD_REQUEST
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_patch_whitelist_travel_rule_sends_expected_body() {
        let server = MockServer::start();

        let travel_rule = TravelRuleInfo::from_config(&TravelRuleConfig {
            beneficiary_entity_name: "Acme Corp".to_string(),
        });

        let whitelist_id = "wl-abc-123";

        let patch_mock = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!(
                    "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists/{whitelist_id}/travel-rule-info"
                ))
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key")
                .json_body(json!({
                    "travel_rule_info": {
                        "beneficiary_is_self_hosted": true,
                        "beneficiary_entity_name": "Acme Corp"
                    }
                }));
            then.status(204);
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        client
            .patch_whitelist_travel_rule(whitelist_id, &travel_rule)
            .await
            .unwrap();

        patch_mock.assert();
    }
}
