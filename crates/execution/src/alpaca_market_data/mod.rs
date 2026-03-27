//! Alpaca Market Data API client for real-time stock quotes.
//!
//! Provides current bid/ask prices for equities via Alpaca's Market Data
//! API (`data.alpaca.markets`). Uses the same API credentials as the
//! Broker API but with a different base URL.

mod client;
#[cfg(any(test, feature = "mock"))]
mod mock;

pub use client::{AlpacaMarketData, AlpacaMarketDataCtx, AlpacaMarketDataError};
#[cfg(any(test, feature = "mock"))]
pub use mock::AlpacaMarketDataMock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;

use crate::Symbol;

/// A real-time stock quote with bid and ask prices.
#[derive(Debug, Clone)]
pub struct MarketQuote {
    pub symbol: Symbol,
    pub bid_price: Float,
    pub ask_price: Float,
    pub timestamp: DateTime<Utc>,
}

impl MarketQuote {
    /// Computes the mid-price as `(bid + ask) / 2`.
    pub fn mid_price(&self) -> Result<Float, rain_math_float::FloatError> {
        let sum = (self.bid_price + self.ask_price)?;
        let two = Float::from_fixed_decimal_lossy(alloy::primitives::U256::from(2), 0)?.0;
        sum / two
    }
}

/// Trait for fetching real-time market quotes.
///
/// Implemented by [`AlpacaMarketData`] for production and
/// [`AlpacaMarketDataMock`] for testing.
#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetches the latest quote for a symbol.
    async fn get_latest_quote(&self, symbol: &Symbol) -> Result<MarketQuote, Self::Error>;
}
