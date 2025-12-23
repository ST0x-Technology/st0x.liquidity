use chrono::Utc;
use tracing::{debug, info};

use super::AlpacaBrokerApiError;
use super::auth::AlpacaBrokerApiClient;

pub(super) async fn wait_until_market_open(
    client: &AlpacaBrokerApiClient,
) -> Result<std::time::Duration, AlpacaBrokerApiError> {
    loop {
        debug!("Checking market status via Alpaca Broker API Clock endpoint");

        let clock_data = client.get_clock().await?;

        if clock_data.is_open {
            let now = Utc::now();
            let next_close_utc = clock_data.next_close.to_utc();

            if next_close_utc <= now {
                return Err(AlpacaBrokerApiError::MarketOpenButCloseInPast {
                    next_close: next_close_utc,
                    now,
                });
            }

            let duration = (next_close_utc - now)
                .to_std()
                .map_err(|_| AlpacaBrokerApiError::DurationConversion)?;

            return Ok(duration);
        }

        let now = Utc::now();
        let next_open_utc = clock_data.next_open.to_utc();

        if next_open_utc <= now {
            return Err(AlpacaBrokerApiError::MarketClosedButOpenInPast {
                next_open: next_open_utc,
                now,
            });
        }

        let wait_duration = (next_open_utc - now)
            .to_std()
            .map_err(|_| AlpacaBrokerApiError::DurationConversion)?;

        info!(
            "Market closed, waiting {} seconds until open",
            wait_duration.as_secs()
        );
        tokio::time::sleep(wait_duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_config(base_url: &str) -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key".to_string(),
            alpaca_broker_api_secret: "test_secret".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(base_url.to_string()),
        }
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_open() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2025-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2030-01-06T14:30:00+00:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = wait_until_market_open(&client).await;

        mock.assert();
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_wait_until_market_open_returns_duration_until_close() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2026-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2030-01-06T14:30:00+00:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = wait_until_market_open(&client).await;

        mock.assert();
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_market_open_but_close_in_past_returns_error() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2025-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2020-01-06T14:30:00+00:00",
                    "next_close": "2020-01-06T21:00:00+00:00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = wait_until_market_open(&client).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::MarketOpenButCloseInPast { .. }
        ));
    }
}
