//! Shared Alpaca market-data lookups used for hedge preflight checks.

use rain_math_float::Float;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tracing::trace;

use crate::{
    LatestQuote, LatestQuoteError, Positive, Symbol, Usd, deserialize_float_from_number_or_string,
    deserialize_option_float_from_number_or_string,
};

#[derive(Debug, thiserror::Error)]
pub enum AlpacaMarketDataError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error (status {status}): {body}")]
    ApiError { status: StatusCode, body: String },
    #[error("failed to parse latest trade response: {0}")]
    JsonParse(#[from] serde_json::Error),
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

pub(crate) async fn fetch_latest_trade_price(
    client: &Client,
    market_data_base_url: &str,
    symbol: &Symbol,
) -> Result<Positive<Usd>, AlpacaMarketDataError> {
    let response = client
        .get(format!(
            "{market_data_base_url}/v2/stocks/{symbol}/trades/latest"
        ))
        .send()
        .await?;
    let status = response.status();
    let url = response.url().clone();
    // Parse successful responses from raw bytes with `from_slice` so invalid
    // UTF-8 fails fast (matching the prior `response.json()`) instead of being
    // lossily replaced by `response.text()`. Lossy decoding is used only for
    // the trace line and the error-body display.
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
        });
    }

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
    client: &Client,
    market_data_base_url: &str,
    symbol: &Symbol,
) -> Result<LatestQuote, AlpacaMarketDataError> {
    let response = client
        .get(format!(
            "{market_data_base_url}/v2/stocks/{symbol}/quotes/latest"
        ))
        // Explicit SIP prevents Alpaca's subscription-dependent default from
        // silently returning the single-exchange IEX quote instead of NBBO.
        .query(&[("feed", "sip")])
        .send()
        .await?;
    let status = response.status();
    let url = response.url().clone();
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
        });
    }

    let response: LatestQuoteEnvelope = serde_json::from_slice(&bytes)?;
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

    async fn latest_quote_result(
        quote: serde_json::Value,
    ) -> Result<LatestQuote, AlpacaMarketDataError> {
        let server = MockServer::start();
        let client = Client::new();
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({ "quote": quote }));
        });

        fetch_latest_quote(&client, &server.base_url(), &symbol).await
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
        let client = Client::new();
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "sip");
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

        let quote = fetch_latest_quote(&client, &server.base_url(), &symbol)
            .await
            .unwrap();

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
        let client = Client::new();
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "sip");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({}));
        });

        let error = fetch_latest_quote(&client, &server.base_url(), &symbol)
            .await
            .unwrap_err();

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
        let client = Client::new();
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

        let error = fetch_latest_trade_price(&client, &server.base_url(), &symbol)
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
        let client = Client::new();
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

        let price = fetch_latest_trade_price(&client, &server.base_url(), &symbol)
            .await
            .unwrap();

        assert_eq!(
            price.inner(),
            Usd::new(Float::parse("123.45".to_string()).unwrap())
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn fetch_latest_trade_price_logs_success_response_body() {
        let server = MockServer::start();
        let client = Client::new();
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

        fetch_latest_trade_price(&client, &server.base_url(), &symbol)
            .await
            .unwrap();

        assert!(logs_contain("Alpaca market data response body received"));
        assert!(logs_contain("market_data_marker"));
        assert!(logs_contain("success-body"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn fetch_latest_trade_price_logs_error_response_body() {
        let server = MockServer::start();
        let client = Client::new();
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(429)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "rate limited",
                    "market_data_marker": "error-body"
                }));
        });

        let error = fetch_latest_trade_price(&client, &server.base_url(), &symbol)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::ApiError { status, .. }
                if status == StatusCode::TOO_MANY_REQUESTS
        ));
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
        let client = Client::new();
        let symbol = Symbol::new("AAPL").unwrap();

        server.mock(|when, then| {
            when.method(GET)
                .path("/v2/stocks/AAPL/quotes/latest")
                .query_param("feed", "sip");
            then.status(403)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "subscription does not permit SIP feed" }));
        });

        let error = fetch_latest_quote(&client, &server.base_url(), &symbol)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaMarketDataError::ApiError { status, .. }
                if status == StatusCode::FORBIDDEN
        ));
    }
}
