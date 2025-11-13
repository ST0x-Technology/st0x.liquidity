use apca::api::v2::clock;
use apca::{Client, RequestError};
use chrono::Utc;
use tracing::{debug, info};

#[derive(Debug, thiserror::Error)]
pub enum MarketHoursError {
    #[error("Alpaca API request failed")]
    ApiRequest(#[from] RequestError<clock::GetError>),

    #[error(
        "Duration conversion failed: chrono duration cannot be converted to std::time::Duration"
    )]
    DurationConversion,

    #[error(
        "Inconsistent market data from Alpaca: market is open but next_close ({next_close}) is not in the future (now: {now})"
    )]
    MarketOpenButCloseInPast {
        next_close: chrono::DateTime<Utc>,
        now: chrono::DateTime<Utc>,
    },

    #[error(
        "Inconsistent market data from Alpaca: market is closed but next_open ({next_open}) is not in the future (now: {now})"
    )]
    MarketClosedButOpenInPast {
        next_open: chrono::DateTime<Utc>,
        now: chrono::DateTime<Utc>,
    },
}

pub(super) async fn wait_until_market_open(
    client: &Client,
) -> Result<std::time::Duration, MarketHoursError> {
    loop {
        debug!("Checking market status via Alpaca Clock API");

        let clock_data = client.issue::<clock::Get>(&()).await?;

        if clock_data.open {
            let now = Utc::now();
            let next_close_utc = clock_data.next_close;

            if next_close_utc <= now {
                return Err(MarketHoursError::MarketOpenButCloseInPast {
                    next_close: next_close_utc,
                    now,
                });
            }

            let duration = (next_close_utc - now)
                .to_std()
                .map_err(|_| MarketHoursError::DurationConversion)?;

            return Ok(duration);
        }

        let now = Utc::now();
        let next_open_utc = clock_data.next_open;

        if next_open_utc <= now {
            return Err(MarketHoursError::MarketClosedButOpenInPast {
                next_open: next_open_utc,
                now,
            });
        }

        let wait_duration = (next_open_utc - now)
            .to_std()
            .map_err(|_| MarketHoursError::DurationConversion)?;

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
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_client(mock_server: &MockServer) -> Client {
        let api_info =
            apca::ApiInfo::from_parts(mock_server.base_url(), "test_key", "test_secret").unwrap();
        Client::new(api_info)
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_open() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2025-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2030-01-06T14:30:00+00:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let client = create_test_client(&server);
        let result = wait_until_market_open(&client).await;

        mock.assert();
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_closed_then_open() {
        let server = MockServer::start();

        let closed_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2025-01-03T20:00:00-05:00",
                    "is_open": false,
                    "next_open": "2025-01-03T20:00:01-05:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let open_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2025-01-03T20:00:02-05:00",
                    "is_open": true,
                    "next_open": "2030-01-06T14:30:00+00:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let client = create_test_client(&server);
        let result = wait_until_market_open(&client).await;

        closed_mock.assert();
        open_mock.assert();
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }
}
