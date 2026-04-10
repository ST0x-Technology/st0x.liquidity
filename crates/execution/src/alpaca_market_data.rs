//! Shared Alpaca market-data lookups used for hedge preflight checks.

use rain_math_float::Float;
use reqwest::Client;
use serde::Deserialize;

use crate::{Symbol, deserialize_float_from_number_or_string};

#[derive(Debug, thiserror::Error)]
pub enum AlpacaMarketDataError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("latest trade response for {symbol} did not include a price")]
    MissingPrice { symbol: Symbol },
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
) -> Result<Float, AlpacaMarketDataError> {
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
        .map(|trade| trade.price)
        .ok_or_else(|| AlpacaMarketDataError::MissingPrice {
            symbol: symbol.clone(),
        })
}
