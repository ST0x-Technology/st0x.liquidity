//! Relay API client for cross-chain settlement-stable transfers.
//!
//! Relay moves a stable between chains through a depository and a solver: we
//! approve and deposit on the origin chain, and Relay's solver pays the
//! recipient on the destination chain. This module speaks the HTTP API only:
//! [`RelayClient::quote`] fetches and checks a quote, and
//! [`RelayClient::status`] reads where an order is. See `docs/relay.md`.

mod acceptance;
mod quote;
mod status;

pub use acceptance::{
    BasisPoints, BasisPointsOutOfRange, QuoteAcceptanceError, QuoteAmounts, QuoteBounds,
};
pub use quote::{
    QuoteFees, QuoteMismatch, QuoteRequest, QuoteStep, QuotedCurrency, RelayOrderId, RelayQuote,
    RelayRequestId, StepTransaction,
};
pub use status::{FailReason, InFlightStage, IntentStatus, IntentStatusReport};

use std::time::Duration;

use backon::Retryable;
use reqwest::header::RETRY_AFTER;
use reqwest::{Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tracing::{debug, warn};

use quote::{QuoteRequestBody, QuoteResponse};
use status::StatusResponse;

const RELAY_API_BASE: &str = "https://api.relay.link";

/// Retries of a transient failure (transport, 5xx, a transient quote code)
/// within one call. Rate limits are never retried here.
const TRANSIENT_RETRIES: usize = 2;

const TRANSIENT_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Relay API key, sent as `x-api-key`. Debug output never shows it.
#[derive(Clone)]
pub struct RelayApiKey(String);

impl From<String> for RelayApiKey {
    fn from(key: String) -> Self {
        Self(key)
    }
}

impl std::fmt::Debug for RelayApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RelayApiKey(..)")
    }
}

/// HTTP client for Relay's quote and status endpoints.
#[derive(Debug, Clone)]
pub struct RelayClient {
    http_client: reqwest::Client,
    api_base: String,
    api_key: Option<RelayApiKey>,
    retry_delay: Duration,
}

impl RelayClient {
    /// Builds the client. Without a key Relay applies its unauthenticated
    /// rate limits, which it does not document.
    pub fn new(api_key: Option<RelayApiKey>) -> Result<Self, RelayError> {
        if api_key.is_none() {
            warn!(target: "bridge", "No Relay API key: requests use unauthenticated rate limits");
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(RelayError::Transport)?;

        Ok(Self {
            http_client,
            api_base: RELAY_API_BASE.to_owned(),
            api_key,
            retry_delay: TRANSIENT_RETRY_DELAY,
        })
    }

    /// Points the client at a mock server.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_api_base(mut self, api_base: String) -> Self {
        self.api_base = api_base;
        self
    }

    /// Fetches an exact-input quote and checks its steps against `request`.
    /// Bound checks on the amounts are the caller's ([`QuoteAmounts::accept`]).
    /// An origin with no pinned depository is refused before any request.
    pub async fn quote(&self, request: &QuoteRequest) -> Result<RelayQuote, RelayError> {
        let depository = request
            .origin
            .relay_depository()
            .ok_or(QuoteMismatch::NoDepository {
                chain: request.origin,
            })?;

        let url = format!("{}/quote/v2", self.api_base);
        let body = QuoteRequestBody::from(request);

        let send = || async {
            let response = self
                .authenticated(self.http_client.post(&url))
                .json(&body)
                .send()
                .await
                .map_err(RelayError::Transport)?;

            read_json::<QuoteResponse>(response, RelayEndpoint::Quote).await
        };

        let response = self.with_retries(send).await?;
        let quote = response.validate(request, depository)?;

        debug!(
            target: "bridge",
            request_id = %quote.request_id,
            order_id = %quote.order_id,
            amounts = ?quote.amounts,
            "Relay quote checked"
        );

        Ok(quote)
    }

    /// Reads the status of `request_id` once.
    pub async fn status(
        &self,
        request_id: RelayRequestId,
    ) -> Result<IntentStatusReport, RelayError> {
        let url = format!("{}/intents/status/v3", self.api_base);
        let request_id = request_id.to_string();

        let send = || async {
            let response = self
                .authenticated(self.http_client.get(&url))
                .query(&[("requestId", &request_id)])
                .send()
                .await
                .map_err(RelayError::Transport)?;

            read_json::<StatusResponse>(response, RelayEndpoint::Status).await
        };

        let report = IntentStatusReport::from(self.with_retries(send).await?);

        if let IntentStatus::Unknown(status) = &report.status {
            warn!(target: "bridge", %request_id, %status, "Unknown Relay status, not terminal");
        }

        Ok(report)
    }

    fn authenticated(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(RelayApiKey(key)) => request.header("x-api-key", key),
            None => request,
        }
    }

    async fn with_retries<Output, Send, Attempt>(&self, send: Send) -> Result<Output, RelayError>
    where
        Send: FnMut() -> Attempt,
        Attempt: Future<Output = Result<Output, RelayError>>,
    {
        let backoff = backon::ConstantBuilder::default()
            .with_delay(self.retry_delay)
            .with_max_times(TRANSIENT_RETRIES);

        send.retry(backoff)
            .when(RelayError::is_transient)
            .notify(|error, delay| {
                warn!(target: "bridge", ?error, ?delay, "Transient Relay API error, retrying");
            })
            .await
    }
}

/// The endpoint a decode failure came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayEndpoint {
    Quote,
    Status,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("Relay API transport error: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("Relay {endpoint:?} response does not decode: {source}")]
    Decode {
        endpoint: RelayEndpoint,
        #[source]
        source: serde_json::Error,
    },
    #[error("Relay rate limit hit, retry after {retry_after:?}")]
    RateLimited { retry_after: Option<Duration> },
    #[error("Relay refused the quote: {code:?}")]
    QuoteRefused { code: QuoteErrorCode },
    #[error("Relay {endpoint:?} answered HTTP {status}")]
    HttpStatus {
        endpoint: RelayEndpoint,
        status: u16,
    },
    #[error("Relay quote does not match the request: {0}")]
    QuoteMismatch(#[from] QuoteMismatch),
}

impl RelayError {
    /// Worth retrying within the same call: nothing moved, and the same
    /// request may succeed a moment later.
    fn is_transient(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::HttpStatus { status, .. } => *status >= 500,
            Self::QuoteRefused { code } => code.is_transient(),
            Self::Decode { .. } | Self::RateLimited { .. } | Self::QuoteMismatch(_) => false,
        }
    }
}

/// Relay's quote `errorCode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteErrorCode {
    AmountTooLow,
    AmountTooHigh,
    InsufficientLiquidity,
    ChainDisabled,
    RouteTemporarilyRestricted,
    NoQuotes,
    NoSwapRoutesFound,
    SwapImpactTooHigh,
    PriceFetchFailed,
    ServiceUnavailable,
    RequestTimedOut,
    RpcHttpError,
    /// A code this build does not know, kept verbatim.
    Unknown(String),
}

impl QuoteErrorCode {
    fn parse(code: &str) -> Self {
        match code {
            "AMOUNT_TOO_LOW" => Self::AmountTooLow,
            "AMOUNT_TOO_HIGH" => Self::AmountTooHigh,
            "INSUFFICIENT_LIQUIDITY" => Self::InsufficientLiquidity,
            "CHAIN_DISABLED" => Self::ChainDisabled,
            "ROUTE_TEMPORARILY_RESTRICTED" => Self::RouteTemporarilyRestricted,
            "NO_QUOTES" => Self::NoQuotes,
            "NO_SWAP_ROUTES_FOUND" => Self::NoSwapRoutesFound,
            "SWAP_IMPACT_TOO_HIGH" => Self::SwapImpactTooHigh,
            "PRICE_FETCH_FAILED" => Self::PriceFetchFailed,
            "SERVICE_UNAVAILABLE" => Self::ServiceUnavailable,
            "REQUEST_TIMED_OUT" => Self::RequestTimedOut,
            "RPC_HTTP_ERROR" => Self::RpcHttpError,
            other => Self::Unknown(other.to_owned()),
        }
    }

    /// The codes Relay's docs call transient.
    const fn is_transient(&self) -> bool {
        match self {
            Self::PriceFetchFailed
            | Self::ServiceUnavailable
            | Self::RequestTimedOut
            | Self::RpcHttpError => true,
            Self::AmountTooLow
            | Self::AmountTooHigh
            | Self::InsufficientLiquidity
            | Self::ChainDisabled
            | Self::RouteTemporarilyRestricted
            | Self::NoQuotes
            | Self::NoSwapRoutesFound
            | Self::SwapImpactTooHigh
            | Self::Unknown(_) => false,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    error_code: Option<String>,
}

/// Maps a response to its body or to the error it carries: 429 to
/// [`RelayError::RateLimited`], a quote `errorCode` to
/// [`RelayError::QuoteRefused`], any other failure to its HTTP status.
async fn read_json<Body: DeserializeOwned>(
    response: Response,
    endpoint: RelayEndpoint,
) -> Result<Body, RelayError> {
    let status = response.status();

    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);

        return Err(RelayError::RateLimited { retry_after });
    }

    let bytes = response.bytes().await.map_err(RelayError::Transport)?;

    if status.is_success() {
        return serde_json::from_slice(&bytes)
            .map_err(|source| RelayError::Decode { endpoint, source });
    }

    let code = serde_json::from_slice::<ErrorBody>(&bytes)
        .ok()
        .and_then(|body| body.error_code);

    match (endpoint, code) {
        (RelayEndpoint::Quote, Some(code)) => Err(RelayError::QuoteRefused {
            code: QuoteErrorCode::parse(&code),
        }),
        (RelayEndpoint::Quote | RelayEndpoint::Status, _) => Err(RelayError::HttpStatus {
            endpoint,
            status: status.as_u16(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, b256};
    use httpmock::prelude::*;
    use serde_json::json;

    use st0x_evm::Chain;

    use super::quote::tests::{FUNDED_QUOTE, FUNDED_WALLET, funded_request};
    use super::*;

    const REQUEST_ID: RelayRequestId = RelayRequestId(b256!(
        "0x1790875063e29a43c3a2201049fa8f6e6542e940fe2060ed03ea9107630b5a49"
    ));

    fn client(server: &MockServer, api_key: Option<RelayApiKey>) -> RelayClient {
        let mut client = RelayClient::new(api_key)
            .unwrap()
            .with_api_base(server.base_url());
        client.retry_delay = Duration::from_millis(1);
        client
    }

    #[tokio::test]
    async fn quote_posts_the_request_and_checks_the_response() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/quote/v2").json_body_includes(
                json!({
                    "user": FUNDED_WALLET,
                    "amount": "5000000",
                    "tradeType": "EXACT_INPUT",
                    "slippageTolerance": "30",
                })
                .to_string(),
            );
            then.status(200).body(FUNDED_QUOTE);
        });

        let quote = client(&server, None)
            .quote(&funded_request())
            .await
            .unwrap();

        assert_eq!(quote.request_id, REQUEST_ID);
        assert_eq!(quote.amounts.expected_out, U256::from(4_763_755));
        mock.assert();
    }

    #[tokio::test]
    async fn origin_without_a_depository_is_refused_before_any_request() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(200).body(FUNDED_QUOTE);
        });

        let request = QuoteRequest {
            origin: Chain::Base,
            ..funded_request()
        };

        let error = client(&server, None).quote(&request).await.unwrap_err();

        assert!(
            matches!(
                error,
                RelayError::QuoteMismatch(QuoteMismatch::NoDepository { chain: Chain::Base })
            ),
            "{error:?}"
        );
        assert_eq!(mock.calls(), 0);
    }

    #[tokio::test]
    async fn api_key_is_sent_when_configured() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/intents/status/v3")
                .query_param("requestId", REQUEST_ID.to_string())
                .header("x-api-key", "secret-key");
            then.status(200)
                .body(include_str!("../../relay-fixtures/status_waiting.json"));
        });

        let report = client(&server, Some(RelayApiKey::from("secret-key".to_owned())))
            .status(REQUEST_ID)
            .await
            .unwrap();

        assert_eq!(report.status, IntentStatus::Waiting);
        mock.assert();
    }

    #[tokio::test]
    async fn status_reads_the_refund_fixture() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(200)
                .body(include_str!("../../relay-fixtures/status_refund.json"));
        });

        let report = client(&server, None).status(REQUEST_ID).await.unwrap();

        assert_eq!(
            report.status,
            IntentStatus::Refund {
                refund_txs: vec![b256!(
                    "0xf27f49b3e941788a37775b874e1a91a711c26578c041921a24efd96cd14cea8d"
                )],
                reason: Some(FailReason::DepositedAmountTooLowToFill),
            }
        );
    }

    #[tokio::test]
    async fn rate_limit_carries_retry_after() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(429).header("Retry-After", "17");
        });

        let error = client(&server, None).status(REQUEST_ID).await.unwrap_err();

        assert!(
            matches!(
                error,
                RelayError::RateLimited { retry_after: Some(retry_after) }
                    if retry_after == Duration::from_secs(17)
            ),
            "{error:?}"
        );
        assert_eq!(mock.calls(), 1, "a rate limit is not retried in the call");
    }

    #[tokio::test]
    async fn rate_limit_without_retry_after_has_none() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(429);
        });

        let error = client(&server, None)
            .quote(&funded_request())
            .await
            .unwrap_err();

        assert!(
            matches!(error, RelayError::RateLimited { retry_after: None }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn quote_refusals_map_to_their_codes() {
        for (fixture, expected) in [
            (
                include_str!("../../relay-fixtures/quote_error_amount_too_high.json"),
                QuoteErrorCode::AmountTooHigh,
            ),
            (
                include_str!("../../relay-fixtures/quote_error_amount_too_low.json"),
                QuoteErrorCode::AmountTooLow,
            ),
            (
                include_str!("../../relay-fixtures/quote_error_no_swap_routes_found.json"),
                QuoteErrorCode::NoSwapRoutesFound,
            ),
        ] {
            let server = MockServer::start();
            let mock = server.mock(|when, then| {
                when.method(POST).path("/quote/v2");
                then.status(400).body(fixture);
            });

            let error = client(&server, None)
                .quote(&funded_request())
                .await
                .unwrap_err();

            assert!(
                matches!(&error, RelayError::QuoteRefused { code } if *code == expected),
                "{error:?}"
            );
            assert_eq!(mock.calls(), 1, "a refusal is not retried");
        }
    }

    #[tokio::test]
    async fn unknown_quote_code_is_kept_verbatim() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400)
                .json_body(json!({"message": "new", "errorCode": "BRAND_NEW_CODE"}));
        });

        let error = client(&server, None)
            .quote(&funded_request())
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                RelayError::QuoteRefused { code: QuoteErrorCode::Unknown(code) }
                    if code == "BRAND_NEW_CODE"
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn transient_quote_code_is_retried() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(503)
                .json_body(json!({"message": "down", "errorCode": "SERVICE_UNAVAILABLE"}));
        });

        let error = client(&server, None)
            .quote(&funded_request())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayError::QuoteRefused {
                    code: QuoteErrorCode::ServiceUnavailable
                }
            ),
            "{error:?}"
        );
        assert_eq!(mock.calls(), 1 + TRANSIENT_RETRIES);
    }

    #[tokio::test]
    async fn status_server_error_is_retried_then_surfaced() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(502);
        });

        let error = client(&server, None).status(REQUEST_ID).await.unwrap_err();

        assert!(
            matches!(
                error,
                RelayError::HttpStatus {
                    endpoint: RelayEndpoint::Status,
                    status: 502
                }
            ),
            "{error:?}"
        );
        assert_eq!(mock.calls(), 1 + TRANSIENT_RETRIES);
    }

    #[tokio::test]
    async fn malformed_status_body_is_a_decode_error() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(200).body("{\"txHashes\": []}");
        });

        let error = client(&server, None).status(REQUEST_ID).await.unwrap_err();

        assert!(
            matches!(
                error,
                RelayError::Decode {
                    endpoint: RelayEndpoint::Status,
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[test]
    fn api_key_is_redacted_in_debug_output() {
        let key = RelayApiKey::from("secret-key".to_owned());

        assert_eq!(format!("{key:?}"), "RelayApiKey(..)");
    }
}
