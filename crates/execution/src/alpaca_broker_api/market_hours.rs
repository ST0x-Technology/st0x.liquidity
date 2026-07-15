use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::MarketSession;

#[cfg(all(test, feature = "mock"))]
use chrono::{DateTime, Utc};

pub(super) async fn is_market_open(
    client: &AlpacaBrokerApiClient,
) -> Result<bool, AlpacaBrokerApiError> {
    st0x_alpaca::broker::is_market_open(client.broker())
        .await
        .map_err(Into::into)
}

pub(super) async fn market_session(
    client: &AlpacaBrokerApiClient,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    st0x_alpaca::broker::market_session(client.broker())
        .await
        .map(Into::into)
        .map_err(Into::into)
}

#[cfg(all(test, feature = "mock"))]
pub(super) async fn market_session_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    st0x_alpaca::broker::market_session_at(client.broker(), now)
        .await
        .map(Into::into)
        .map_err(Into::into)
}

impl From<st0x_alpaca::broker::MarketSession> for MarketSession {
    fn from(value: st0x_alpaca::broker::MarketSession) -> Self {
        match value {
            st0x_alpaca::broker::MarketSession::Regular => Self::Regular,
            st0x_alpaca::broker::MarketSession::Extended => Self::Extended,
            st0x_alpaca::broker::MarketSession::Closed => Self::Closed,
        }
    }
}
