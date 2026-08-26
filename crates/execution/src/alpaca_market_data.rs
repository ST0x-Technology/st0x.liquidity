//! Shared Alpaca market-data lookups used for hedge preflight checks.

use rain_math_float::Float;
use reqwest::{RequestBuilder, StatusCode};
use serde::Deserialize;
use std::time::Duration;
use tracing::trace;

use crate::alpaca_broker_api::AlpacaBrokerApiClient;
use crate::alpaca_broker_api::kms_jwt::KmsJwtError;

use crate::rate_limit::retry_after_from_response_headers;
use crate::{
    Backpressure, LatestQuote, LatestQuoteError, Permanence, Positive, Symbol, Usd,
    deserialize_float_from_number_or_string, deserialize_option_float_from_number_or_string,
    status_permanence,
};

/// The only market-data feed this account can price a quote against.
///
/// `sip` is rejected outright without a real-time SIP entitlement, which Broker
/// API partners must arrange separately. `iex` answers, but it is a single
/// venue that stops quoting around 16:00 ET and publishes stub books afterwards
/// -- exactly when extended-hours hedging needs it. `delayed_sip` returns a real
/// consolidated NBBO fifteen minutes old, which is the best quote available
/// here. Not configurable: there is one correct value, and offering the others
/// only creates a way to misconfigure the bot into losing money (ADR 0019).
const QUOTE_FEED: &str = "delayed_sip";

#[derive(Debug, thiserror::Error)]
pub enum AlpacaMarketDataError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("market data auth failed: {0}")]
    Auth(#[from] KmsJwtError),
    #[error("API error (status {status}): {body}")]
    ApiError {
        status: StatusCode,
        body: String,
        retry_after: Option<Duration>,
    },
    #[error("failed to parse latest trade response: {0}")]
    JsonParse(#[from] serde_json::Error),
    #[error("failed to parse latest quote response: {0}")]
    LatestQuoteJsonParse(#[source] serde_json::Error),
    #[error(
        "latest quote endpoint returned {returned} when {requested} was requested; refusing to \
         price from another symbol"
    )]
    LatestQuoteSymbolMismatch { requested: Symbol, returned: Symbol },
    #[error("latest trade response for {symbol} did not include a price")]
    MissingPrice { symbol: Symbol },
    #[error(
        "latest trade response for {symbol} returned non-positive price {}",
        st0x_float_serde::format_float_with_fallback(.price)
    )]
    NonPositivePrice { symbol: Symbol, price: Float },
    #[error("latest quote response for {symbol} did not include a quote")]
    MissingQuote { symbol: Symbol },
    #[error("latest quote response for {symbol} did not include a bid")]
    MissingBid { symbol: Symbol },
    #[error("latest quote response for {symbol} did not include an ask")]
    MissingAsk { symbol: Symbol },
    #[error("latest quote response for {symbol} returned non-positive bid {bid}")]
    NonPositiveBid { symbol: Symbol, bid: Usd },
    #[error("latest quote response for {symbol} returned non-positive ask {ask}")]
    NonPositiveAsk { symbol: Symbol, ask: Usd },
    #[error("latest quote response for {symbol} is invalid")]
    InvalidQuote {
        symbol: Symbol,
        #[source]
        source: LatestQuoteError,
    },
}

impl AlpacaMarketDataError {
    /// Classifies this error as broker rate-limiting (HTTP 429), returning
    /// its `Retry-After` hint when the broker sent one. Every other variant
    /// returns `None` -- an exhaustive match so a new variant added later
    /// forces a conscious decision here rather than silently classifying as
    /// "not backpressure".
    pub fn backpressure(&self) -> Option<Backpressure> {
        match self {
            Self::ApiError {
                status,
                retry_after,
                ..
            } if *status == StatusCode::TOO_MANY_REQUESTS => Some(Backpressure {
                retry_after: *retry_after,
            }),

            // A rate-limited token mint throttles every keyless call at
            // once; surface it with its Retry-After hint.
            Self::Auth(error) if error.is_rate_limited() => Some(Backpressure {
                retry_after: error.retry_after(),
            }),

            Self::ApiError { .. }
            | Self::Http(_)
            | Self::Auth(_)
            | Self::JsonParse(_)
            | Self::LatestQuoteJsonParse(_)
            | Self::LatestQuoteSymbolMismatch { .. }
            | Self::MissingPrice { .. }
            | Self::NonPositivePrice { .. }
            | Self::MissingQuote { .. }
            | Self::MissingBid { .. }
            | Self::MissingAsk { .. }
            | Self::NonPositiveBid { .. }
            | Self::NonPositiveAsk { .. }
            | Self::InvalidQuote { .. } => None,
        }
    }

    /// Classifies whether an immediate retry of the same lookup can plausibly
    /// succeed.
    pub(crate) fn permanence(&self) -> Permanence {
        match self {
            Self::ApiError { status, .. } => status_permanence(*status),

            // Deterministic mint failures (revoked IAM grant, disabled
            // credential) fail identically on every retry; the rest of a
            // mint is network-shaped.
            Self::Auth(error) if error.is_deterministic() => Permanence::Permanent,

            // A malformed response is deterministic for the response that
            // arrived and does not become usable by immediately parsing it
            // again.
            Self::JsonParse(_)
            | Self::LatestQuoteJsonParse(_)
            | Self::MissingPrice { .. }
            | Self::NonPositivePrice { .. } => Permanence::Permanent,

            // Transport failures can clear, and syntactically valid latest
            // quotes are dynamic snapshots: a later request can carry a
            // complete, positive, uncrossed book even when this one did not.
            Self::Auth(_)
            | Self::Http(_)
            | Self::LatestQuoteSymbolMismatch { .. }
            | Self::MissingQuote { .. }
            | Self::MissingBid { .. }
            | Self::MissingAsk { .. }
            | Self::NonPositiveBid { .. }
            | Self::NonPositiveAsk { .. }
            | Self::InvalidQuote { .. } => Permanence::Transient,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LatestTradeEnvelope {
    trade: Option<LatestTrade>,
}

#[derive(Debug, Deserialize)]
struct LatestTrade {
    #[serde(
        rename = "p",
        deserialize_with = "deserialize_float_from_number_or_string"
    )]
    price: Float,
}

#[derive(Debug, Deserialize)]
struct LatestQuoteEnvelope {
    symbol: Symbol,
    quote: Option<LatestQuotePayload>,
}

/// Payload for Alpaca's stock latest-quote endpoint
/// (https://docs.alpaca.markets/reference/stocklatestquotesingle):
/// `GET /v2/stocks/{symbol}/quotes/latest` returns
/// `{"symbol": ..., "quote": {"t", "ax", "ap", "as", "bx", "bp", "bs", "c", "z"}}`.
/// `bp`/`ap` are the best bid/ask price in dollars -- the only fields this
/// type consumes. The sibling fields (exchange codes, sizes, timestamp,
/// condition flags, tape) are covered by `fetch_latest_quote_deserializes_full_alpaca_response`
/// below and ignored here via serde's default unknown-field behavior.
#[derive(Debug, Deserialize)]
struct LatestQuotePayload {
    #[serde(
        rename = "bp",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    bid: Option<Float>,
    #[serde(
        rename = "ap",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    ask: Option<Float>,
}

/// Sends a market-data request and returns the raw response body, handling
/// the send/trace/non-success-status handling shared by every Alpaca market
/// data endpoint. Callers own only their own deserialization target.
///
/// Bytes (not `response.json()`/`response.text()`) so invalid UTF-8 fails
/// fast instead of being lossily replaced; lossy decoding is used only for
/// the trace line and the error-body display.
async fn get_market_data_bytes(request: RequestBuilder) -> Result<Vec<u8>, AlpacaMarketDataError> {
    let response = request.send().await?;
    let status = response.status();
    let url = response.url().clone();
    let retry_after = retry_after_from_response_headers(response.headers());
    let bytes = response.bytes().await?;

    trace!(
        target: "market_data",
        status = %status,
        url = %url,
        body = %String::from_utf8_lossy(&bytes),
        "Alpaca market data response body received"
    );

    if !status.is_success() {
        return Err(AlpacaMarketDataError::ApiError {
            status,
            body: String::from_utf8_lossy(&bytes).into_owned(),
            retry_after,
        });
    }

    Ok(bytes.into())
}

pub(crate) async fn fetch_latest_trade_price(
    client: &AlpacaBrokerApiClient,
    symbol: &Symbol,
) -> Result<Positive<Usd>, AlpacaMarketDataError> {
    let request = client
        .market_data_get(&format!("/v2/stocks/{symbol}/trades/latest"))
        .await?;
    let bytes = get_market_data_bytes(request).await?;

    let response: LatestTradeEnvelope = serde_json::from_slice(&bytes)?;

    response
        .trade
        .map(|trade| {
            Positive::new(Usd::new(trade.price)).map_err(|error| {
                AlpacaMarketDataError::NonPositivePrice {
                    symbol: symbol.clone(),
                    price: error.value.inner(),
                }
            })
        })
        .transpose()?
        .ok_or_else(|| AlpacaMarketDataError::MissingPrice {
            symbol: symbol.clone(),
        })
}

pub(crate) async fn fetch_latest_quote(
    client: &AlpacaBrokerApiClient,
    symbol: &Symbol,
) -> Result<LatestQuote, AlpacaMarketDataError> {
    let request = client
        .market_data_get(&format!("/v2/stocks/{symbol}/quotes/latest"))
        .await?;
    let bytes = get_market_data_bytes(request.query(&[("feed", QUOTE_FEED)])).await?;

    let response: LatestQuoteEnvelope =
        serde_json::from_slice(&bytes).map_err(AlpacaMarketDataError::LatestQuoteJsonParse)?;
    if response.symbol != *symbol {
        return Err(AlpacaMarketDataError::LatestQuoteSymbolMismatch {
            requested: symbol.clone(),
            returned: response.symbol,
        });
    }
    let quote = response
        .quote
        .ok_or_else(|| AlpacaMarketDataError::MissingQuote {
            symbol: symbol.clone(),
        })?;
    let bid = quote.bid.ok_or_else(|| AlpacaMarketDataError::MissingBid {
        symbol: symbol.clone(),
    })?;
    let ask = quote.ask.ok_or_else(|| AlpacaMarketDataError::MissingAsk {
        symbol: symbol.clone(),
    })?;
    let bid =
        Positive::new(Usd::new(bid)).map_err(|error| AlpacaMarketDataError::NonPositiveBid {
            symbol: symbol.clone(),
            bid: error.value,
        })?;
    let ask =
        Positive::new(Usd::new(ask)).map_err(|error| AlpacaMarketDataError::NonPositiveAsk {
            symbol: symbol.clone(),
            ask: error.value,
        })?;

    LatestQuote::new(bid, ask).map_err(|source| AlpacaMarketDataError::InvalidQuote {
        symbol: symbol.clone(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaBrokerAuth, TimeInForce,
    };

    fn mock_client(server: &MockServer) -> AlpacaBrokerApiClient {
        AlpacaBrokerApiClient::new(&AlpacaBrokerApiCtx {
            auth: AlpacaBrokerAuth::Basic {
                api_key: "test_key_id".to_string(),
                api_secret: "test_secret_key".to_string(),
            },
            account_id: "904837e3-3b76-47ec-b432-046db621571b"
                .parse::<AlpacaAccountId>()
                .unwrap(),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        })
        .unwrap()
    }

    async fn latest_quote_result(
        quote: serde_json::Value,
    ) -> Result<LatestQuote, AlpacaMarketDataError> {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "delayed_sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({ "symbol": "AAPL", "quote": quote }));
        });

        fetch_latest_quote(&client, &symbol).await
    }

    #[tokio::test]
    async fn fetch_latest_quote_returns_valid_bid_and_ask() {
        let quote = latest_quote_result(json!({ "bp": "99.50", "ap": "100.25" }))
            .await
            .unwrap();

        assert_eq!(
            quote.bid().inner(),
            Usd::new(Float::parse("99.50".to_string()).unwrap())
        );
        assert_eq!(
            quote.ask().inner(),
            Usd::new(Float::parse("100.25".to_string()).unwrap())
        );
    }

    #[tokio::test]
    async fn fetch_latest_quote_deserializes_full_alpaca_response() {
        // Pins the parser against the full response shape documented at
        // https://docs.alpaca.markets/reference/stocklatestquotesingle,
        // including the sibling fields (`t`, `ax`, `as`, `bx`, `bs`, `c`,
        // `z`) a hand-trimmed `{"bp", "ap"}`-only fixture would not catch a
        // regression against -- e.g. an envelope or field-name change that
        // happens to still leave a minimal fixture parseable.
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "delayed_sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "symbol": "AAPL",
                    "quote": {
                        "t": "2021-04-20T12:40:34.484136Z",
                        "ax": "PN",
                        "ap": 133.55,
                        "as": 100,
                        "bx": "K",
                        "bp": 133.50,
                        "bs": 200,
                        "c": ["R"],
                        "z": "C"
                    }
                }));
        });

        let quote = fetch_latest_quote(&client, &symbol).await.unwrap();

        assert_eq!(
            quote.bid().inner(),
            Usd::new(Float::parse("133.50".to_string()).unwrap())
        );
        assert_eq!(
            quote.ask().inner(),
            Usd::new(Float::parse("133.55".to_string()).unwrap())
        );
    }

    #[tokio::test]
    async fn fetch_latest_quote_rejects_a_response_for_another_symbol() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "delayed_sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "symbol": "TSLA",
                    "quote": { "bp": "99.50", "ap": "100.25" }
                }));
        });

        let error = fetch_latest_quote(&client, &symbol).await.unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::LatestQuoteSymbolMismatch {
                requested,
                returned,
            } if requested == symbol && returned == Symbol::new("TSLA").unwrap()
        ));
    }

    #[tokio::test]
    async fn fetch_latest_quote_rejects_missing_side() {
        let missing_bid = latest_quote_result(json!({ "ap": "100.25" }))
            .await
            .unwrap_err();
        let missing_ask = latest_quote_result(json!({ "bp": "99.50" }))
            .await
            .unwrap_err();

        assert!(matches!(
            missing_bid,
            AlpacaMarketDataError::MissingBid { .. }
        ));
        assert!(matches!(
            missing_ask,
            AlpacaMarketDataError::MissingAsk { .. }
        ));
    }

    #[tokio::test]
    async fn fetch_latest_quote_rejects_missing_quote() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "delayed_sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({ "symbol": "AAPL" }));
        });

        let error = fetch_latest_quote(&client, &symbol).await.unwrap_err();

        assert!(matches!(error, AlpacaMarketDataError::MissingQuote { .. }));
    }

    #[tokio::test]
    async fn fetch_latest_quote_rejects_non_positive_sides() {
        let zero_bid = latest_quote_result(json!({ "bp": "0", "ap": "100.25" }))
            .await
            .unwrap_err();
        let negative_ask = latest_quote_result(json!({ "bp": "99.50", "ap": "-1" }))
            .await
            .unwrap_err();

        assert!(matches!(
            zero_bid,
            AlpacaMarketDataError::NonPositiveBid { .. }
        ));
        assert!(matches!(
            negative_ask,
            AlpacaMarketDataError::NonPositiveAsk { .. }
        ));
    }

    #[tokio::test]
    async fn fetch_latest_quote_rejects_crossed_market() {
        let error = latest_quote_result(json!({ "bp": "100.26", "ap": "100.25" }))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::InvalidQuote {
                source: LatestQuoteError::Crossed { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn fetch_latest_trade_price_rejects_zero_price() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "trade": {
                        "p": "0"
                    }
                }));
        });

        let error = fetch_latest_trade_price(&client, &symbol)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::NonPositivePrice {
                symbol: error_symbol,
                price
            } if error_symbol == symbol
                && price.eq(Float::parse("0".to_string()).unwrap()).unwrap()
        ));
    }

    #[tokio::test]
    async fn fetch_latest_trade_price_returns_positive_price() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "trade": {
                        "p": "123.45"
                    }
                }));
        });

        let price = fetch_latest_trade_price(&client, &symbol).await.unwrap();

        assert_eq!(
            price.inner(),
            Usd::new(Float::parse("123.45".to_string()).unwrap())
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn fetch_latest_trade_price_logs_success_response_body() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "trade": {
                        "p": "123.45",
                        "market_data_marker": "success-body"
                    }
                }));
        });

        fetch_latest_trade_price(&client, &symbol).await.unwrap();

        assert!(logs_contain("Alpaca market data response body received"));
        assert!(logs_contain("market_data_marker"));
        assert!(logs_contain("success-body"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn fetch_latest_trade_price_logs_error_response_body() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(429)
                .header("content-type", "application/json")
                .header("Retry-After", "30")
                .json_body(json!({
                    "message": "rate limited",
                    "market_data_marker": "error-body"
                }));
        });

        let error = fetch_latest_trade_price(&client, &symbol)
            .await
            .unwrap_err();

        let AlpacaMarketDataError::ApiError {
            status,
            retry_after,
            ..
        } = error
        else {
            panic!("expected ApiError");
        };
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(retry_after, Some(Duration::from_secs(30)));
        assert!(logs_contain("Alpaca market data response body received"));
        assert!(logs_contain("market_data_marker"));
        assert!(logs_contain("error-body"));
    }

    #[tokio::test]
    async fn fetch_latest_quote_returns_api_error_on_non_success_status() {
        // Mirrors `fetch_latest_trade_price_logs_error_response_body`'s status
        // check, but for `fetch_latest_quote`'s own (separately implemented)
        // non-success branch -- e.g. Alpaca returning 403 for a missing SIP
        // subscription, which SPEC.md calls out as a case the close-flatten
        // window must treat as retryable rather than falling back to a worse
        // price.
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "delayed_sip");
            then.status(403)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "subscription does not permit SIP feed" }));
        });

        let error = fetch_latest_quote(&client, &symbol).await.unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::ApiError { status, .. }
                if status == StatusCode::FORBIDDEN
        ));
    }

    #[tokio::test]
    async fn fetch_latest_trade_price_backpressure_some_for_429_with_header() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(429)
                .header("content-type", "application/json")
                .header("Retry-After", "12")
                .json_body(json!({ "message": "rate limited" }));
        });

        let error = fetch_latest_trade_price(&client, &symbol)
            .await
            .unwrap_err();

        assert_eq!(
            error.backpressure(),
            Some(Backpressure {
                retry_after: Some(Duration::from_secs(12))
            })
        );
    }

    #[tokio::test]
    async fn fetch_latest_trade_price_backpressure_some_with_none_without_header() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(429)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "rate limited" }));
        });

        let error = fetch_latest_trade_price(&client, &symbol)
            .await
            .unwrap_err();

        assert_eq!(
            error.backpressure(),
            Some(Backpressure { retry_after: None })
        );
    }

    #[tokio::test]
    async fn fetch_latest_trade_price_backpressure_none_for_non_429() {
        let server = MockServer::start();
        let client = mock_client(&server);
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "boom" }));
        });

        let error = fetch_latest_trade_price(&client, &symbol)
            .await
            .unwrap_err();

        assert_eq!(error.backpressure(), None);
    }

    #[test]
    fn api_error_permanence_splits_on_status() {
        let api_error = |status| AlpacaMarketDataError::ApiError {
            status,
            body: "rejected".to_string(),
            retry_after: None,
        };

        assert_eq!(
            api_error(StatusCode::FORBIDDEN).permanence(),
            Permanence::Permanent
        );
        assert_eq!(
            api_error(StatusCode::INTERNAL_SERVER_ERROR).permanence(),
            Permanence::Transient
        );
        assert_eq!(
            api_error(StatusCode::TOO_MANY_REQUESTS).permanence(),
            Permanence::Transient
        );
    }

    #[tokio::test]
    async fn dynamic_quote_shape_errors_are_transient() {
        let payloads = [
            json!(null),
            json!({ "ap": "100.25" }),
            json!({ "bp": "100.00" }),
            json!({ "bp": "0", "ap": "100.25" }),
            json!({ "bp": "100.00", "ap": "0" }),
            json!({ "bp": "100.26", "ap": "100.25" }),
        ];

        for payload in payloads {
            let error = latest_quote_result(payload).await.unwrap_err();
            assert_eq!(error.permanence(), Permanence::Transient, "{error:?}");
        }
    }

    #[test]
    fn quote_parse_errors_are_permanent_but_dynamic_failures_are_transient() {
        let json_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let requested = Symbol::new("AAPL").unwrap();
        let returned = Symbol::new("TSLA").unwrap();
        let http_error = reqwest::Client::new()
            .get("://invalid")
            .build()
            .unwrap_err();

        assert_eq!(
            AlpacaMarketDataError::LatestQuoteJsonParse(json_error).permanence(),
            Permanence::Permanent,
            "a syntactically malformed response must match latest-trade classification"
        );

        for error in [
            AlpacaMarketDataError::LatestQuoteSymbolMismatch {
                requested,
                returned,
            },
            AlpacaMarketDataError::Http(http_error),
        ] {
            assert_eq!(error.permanence(), Permanence::Transient, "{error:?}");
        }
    }
}
