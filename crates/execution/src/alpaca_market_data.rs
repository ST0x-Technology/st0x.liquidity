//! Shared Alpaca market-data lookups used for hedge preflight checks.

use rain_math_float::Float;
use reqwest::Client;
use serde::Deserialize;

use crate::{Positive, Symbol, deserialize_float_from_number_or_string};

#[derive(Debug, thiserror::Error)]
pub enum AlpacaMarketDataError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("latest trade response for {symbol} did not include a price")]
    MissingPrice { symbol: Symbol },
    #[error(
        "latest trade response for {symbol} returned non-positive price {}",
        st0x_float_serde::format_float_with_fallback(.price)
    )]
    NonPositivePrice { symbol: Symbol, price: Float },
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

pub(crate) async fn fetch_latest_trade_price(
    client: &Client,
    market_data_base_url: &str,
    symbol: &Symbol,
) -> Result<Positive<Float>, AlpacaMarketDataError> {
    let response: LatestTradeEnvelope = client
        .get(format!(
            "{market_data_base_url}/v2/stocks/{symbol}/trades/latest"
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    response
        .trade
        .map(|trade| {
            Positive::new(trade.price).map_err(|error| AlpacaMarketDataError::NonPositivePrice {
                symbol: symbol.clone(),
                price: error.value,
            })
        })
        .transpose()?
        .ok_or_else(|| AlpacaMarketDataError::MissingPrice {
            symbol: symbol.clone(),
        })
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;

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

        assert!(
            price
                .inner()
                .eq(Float::parse("123.45".to_string()).unwrap())
                .unwrap()
        );
    }
}
