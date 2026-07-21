use std::time::Duration;

use st0x_alpaca::AlpacaClient;
use st0x_alpaca::market_data::AlpacaMarketDataClient;

use super::AlpacaBrokerApiError;
use super::auth::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};

/// Request timeout applied to every Alpaca Broker API HTTP call.
///
/// Exposed so timing-sensitive integration tests derive their boundaries from
/// the same timeout used by the shared client.
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Liquidity-owned client bundle around the shared Alpaca transports.
///
/// The shared package owns authentication, HTTP behavior, endpoint calls, and
/// wire types. Liquidity keeps only the market-data host and environment mode
/// needed by its executor abstraction.
#[derive(Debug)]
pub(crate) struct AlpacaBrokerApiClient {
    broker: AlpacaClient,
    market_data: AlpacaMarketDataClient,
    mode: AlpacaBrokerApiMode,
}

impl AlpacaBrokerApiClient {
    pub(crate) fn new(ctx: &AlpacaBrokerApiCtx) -> Result<Self, AlpacaBrokerApiError> {
        let broker = AlpacaClient::new(
            ctx.base_url().to_string(),
            ctx.account_id.to_string(),
            ctx.api_key.clone(),
            ctx.api_secret.clone(),
            HTTP_CONNECT_TIMEOUT,
            HTTP_REQUEST_TIMEOUT,
        )?;
        let market_data = AlpacaMarketDataClient::new(
            ctx.mode().market_data_base_url().to_string(),
            &ctx.api_key,
            &ctx.api_secret,
            HTTP_CONNECT_TIMEOUT,
            HTTP_REQUEST_TIMEOUT,
        )?;

        Ok(Self {
            broker,
            market_data,
            mode: ctx.mode(),
        })
    }

    pub(crate) const fn broker(&self) -> &AlpacaClient {
        &self.broker
    }

    pub(crate) const fn market_data(&self) -> &AlpacaMarketDataClient {
        &self.market_data
    }

    pub(crate) fn is_sandbox(&self) -> bool {
        !matches!(self.mode, AlpacaBrokerApiMode::Production)
    }
}
