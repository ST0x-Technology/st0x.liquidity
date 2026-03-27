//! HTTP client for the Alpaca Market Data API.

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;
use tracing::debug;

use crate::Symbol;

use super::{MarketDataProvider, MarketQuote};

/// Configuration for the Alpaca Market Data API client.
#[derive(Clone)]
pub struct AlpacaMarketDataCtx {
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl AlpacaMarketDataCtx {
    /// Creates a context with an explicit base URL.
    #[must_use]
    pub fn new(api_key: String, api_secret: String, base_url: String) -> Self {
        Self {
            api_key,
            api_secret,
            base_url,
        }
    }

    /// Creates a context for the Alpaca sandbox environment.
    #[must_use]
    pub fn sandbox(api_key: String, api_secret: String) -> Self {
        Self::new(
            api_key,
            api_secret,
            "https://data.sandbox.alpaca.markets".to_owned(),
        )
    }

    /// Creates a context for the Alpaca production environment.
    #[must_use]
    pub fn production(api_key: String, api_secret: String) -> Self {
        Self::new(
            api_key,
            api_secret,
            "https://data.alpaca.markets".to_owned(),
        )
    }
}

impl std::fmt::Debug for AlpacaMarketDataCtx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaMarketDataCtx")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

/// Alpaca Market Data API HTTP client.
pub struct AlpacaMarketData {
    http_client: reqwest::Client,
    base_url: String,
}

impl std::fmt::Debug for AlpacaMarketData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaMarketData")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl AlpacaMarketData {
    /// Creates a new market data client from the given configuration.
    pub fn new(ctx: &AlpacaMarketDataCtx) -> Result<Self, AlpacaMarketDataError> {
        let credentials = format!("{}:{}", ctx.api_key, ctx.api_secret);
        let encoded = BASE64_STANDARD.encode(credentials.as_bytes());
        let auth_value = format!("Basic {encoded}");

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_value)?);

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(AlpacaMarketDataError::Http)?;

        Ok(Self {
            http_client,
            base_url: ctx.base_url.clone(),
        })
    }
}

#[async_trait]
impl MarketDataProvider for AlpacaMarketData {
    type Error = AlpacaMarketDataError;

    async fn get_latest_quote(&self, symbol: &Symbol) -> Result<MarketQuote, Self::Error> {
        let url = format!("{}/v2/stocks/{symbol}/quotes/latest", self.base_url);
        debug!(%symbol, %url, "Fetching latest quote");

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(AlpacaMarketDataError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .inspect_err(|error| {
                    tracing::warn!("Failed to read error response body: {error}");
                })
                .unwrap_or_default();
            return Err(AlpacaMarketDataError::ApiError {
                status: status.as_u16(),
                body,
            });
        }

        let api_response: LatestQuoteResponse =
            response.json().await.map_err(AlpacaMarketDataError::Http)?;

        // Parse directly from the JSON number's string representation
        // to avoid f64 precision loss on financial data.
        let bid_price = Float::parse(api_response.quote.bid_price.to_string())
            .map_err(AlpacaMarketDataError::Float)?;
        let ask_price = Float::parse(api_response.quote.ask_price.to_string())
            .map_err(AlpacaMarketDataError::Float)?;

        Ok(MarketQuote {
            symbol: symbol.clone(),
            bid_price,
            ask_price,
            timestamp: api_response.quote.timestamp,
        })
    }
}

/// Errors from the Alpaca Market Data API.
#[derive(Debug, thiserror::Error)]
pub enum AlpacaMarketDataError {
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),

    #[error("authentication error: {0}")]
    Auth(#[from] reqwest::header::InvalidHeaderValue),

    #[error("API error (status {status}): {body}")]
    ApiError { status: u16, body: String },

    #[error("float conversion error: {0}")]
    Float(rain_math_float::FloatError),
}

/// Alpaca API response for `/v2/stocks/{symbol}/quotes/latest`.
#[derive(Debug, Deserialize)]
struct LatestQuoteResponse {
    quote: QuoteData,
}

/// Individual quote data from the Alpaca API.
///
/// Prices are deserialized via `serde_json::Number` to avoid `f64`
/// precision loss — financial data must not silently truncate.
#[derive(Debug, Deserialize)]
struct QuoteData {
    /// Bid price
    #[serde(rename = "bp")]
    bid_price: serde_json::Number,

    /// Ask price
    #[serde(rename = "ap")]
    ask_price: serde_json::Number,

    /// Timestamp
    #[serde(rename = "t")]
    timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;

    use super::*;

    fn test_ctx(server: &MockServer) -> AlpacaMarketDataCtx {
        AlpacaMarketDataCtx::new(
            "test_key".to_owned(),
            "test_secret".to_owned(),
            server.base_url(),
        )
    }

    #[tokio::test]
    async fn fetches_latest_quote() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .header("Authorization", "Basic dGVzdF9rZXk6dGVzdF9zZWNyZXQ=");
            then.status(200).json_body_obj(&serde_json::json!({
                "quote": {
                    "bp": 189.50,
                    "ap": 190.10,
                    "bs": 100,
                    "as": 200,
                    "t": "2026-03-27T12:00:00Z"
                }
            }));
        });

        let ctx = test_ctx(&server);
        let client = AlpacaMarketData::new(&ctx).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();

        let quote = client.get_latest_quote(&symbol).await.unwrap();

        assert_eq!(quote.symbol, symbol);

        let mid = quote.mid_price().unwrap();
        let mid_formatted = mid.format().unwrap();
        // Mid-price of 189.50 and 190.10 = 189.80
        assert!(
            mid_formatted.starts_with("189.8"),
            "Expected mid ~189.80, got {mid_formatted}"
        );
    }

    #[tokio::test]
    async fn handles_api_error() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/INVALID/quotes/latest");
            then.status(404).body(r#"{"message": "symbol not found"}"#);
        });

        let ctx = test_ctx(&server);
        let client = AlpacaMarketData::new(&ctx).unwrap();
        let symbol = Symbol::new("INVALID").unwrap();

        let result = client.get_latest_quote(&symbol).await;
        let error = result.unwrap_err();
        assert!(
            matches!(error, AlpacaMarketDataError::ApiError { status: 404, .. }),
            "Expected 404 ApiError, got: {error:?}"
        );
    }
}
