//! Mock market data provider for testing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use chrono::Utc;
use rain_math_float::Float;

use crate::Symbol;

use super::{AlpacaMarketDataError, MarketDataProvider, MarketQuote};

fn lock(
    quotes: &Mutex<HashMap<Symbol, (Float, Float)>>,
) -> MutexGuard<'_, HashMap<Symbol, (Float, Float)>> {
    quotes.lock().unwrap_or_else(PoisonError::into_inner)
}

/// In-memory mock market data provider.
///
/// Stores quotes per symbol, returns them when queried.
/// Thread-safe for use in async test environments.
pub struct AlpacaMarketDataMock {
    quotes: Arc<Mutex<HashMap<Symbol, (Float, Float)>>>,
}

impl AlpacaMarketDataMock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            quotes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Sets the bid/ask quote for a symbol.
    pub fn set_quote(&self, symbol: Symbol, bid: Float, ask: Float) {
        lock(&self.quotes).insert(symbol, (bid, ask));
    }
}

impl Default for AlpacaMarketDataMock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataProvider for AlpacaMarketDataMock {
    type Error = AlpacaMarketDataError;

    async fn get_latest_quote(&self, symbol: &Symbol) -> Result<MarketQuote, Self::Error> {
        let (bid, ask) = {
            let quotes = lock(&self.quotes);
            *quotes
                .get(symbol)
                .ok_or_else(|| AlpacaMarketDataError::ApiError {
                    status: 404,
                    body: format!("no quote configured for {symbol}"),
                })?
        };

        Ok(MarketQuote {
            symbol: symbol.clone(),
            bid_price: bid,
            ask_price: ask,
            timestamp: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use rain_math_float::Float;

    use super::*;

    fn float_val(value: u32) -> Float {
        Float::from_fixed_decimal_lossy(U256::from(value), 0)
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn returns_configured_quote() {
        let mock = AlpacaMarketDataMock::new();
        let symbol = Symbol::new("AAPL").unwrap();
        mock.set_quote(symbol.clone(), float_val(189), float_val(191));

        let quote = mock.get_latest_quote(&symbol).await.unwrap();

        assert_eq!(quote.symbol, symbol);
        let mid = quote.mid_price().unwrap();
        let formatted = mid.format().unwrap();
        assert_eq!(formatted, "190");
    }

    #[tokio::test]
    async fn returns_error_for_unknown_symbol() {
        let mock = AlpacaMarketDataMock::new();
        let symbol = Symbol::new("UNKNOWN").unwrap();

        let result = mock.get_latest_quote(&symbol).await;
        assert!(matches!(
            result.unwrap_err(),
            AlpacaMarketDataError::ApiError { status: 404, .. }
        ),);
    }
}
