use apca::api::v2::clock;
use apca::{Client, RequestError};

#[derive(Debug, thiserror::Error)]
pub enum MarketHoursError {
    #[error("Alpaca API request failed")]
    ApiRequest(#[from] RequestError<clock::GetError>),
}

pub(super) async fn is_market_open(client: &Client) -> Result<bool, MarketHoursError> {
    let clock_data = client.issue::<clock::Get>(&()).await?;
    Ok(clock_data.open)
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;

    fn create_test_client(mock_server: &MockServer) -> Client {
        let api_info =
            apca::ApiInfo::from_parts(mock_server.base_url(), "test_key", "test_secret").unwrap();
        Client::new(api_info)
    }

    #[tokio::test]
    async fn is_market_open_returns_true_when_open() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200).json_body(json!({
                "timestamp": "2024-01-15T10:30:00-05:00",
                "is_open": true,
                "next_open": "2024-01-16T09:30:00-05:00",
                "next_close": "2024-01-15T16:00:00-05:00"
            }));
        });

        let client = create_test_client(&server);
        let result = is_market_open(&client).await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn is_market_open_returns_false_when_closed() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200).json_body(json!({
                "timestamp": "2024-01-15T18:00:00-05:00",
                "is_open": false,
                "next_open": "2024-01-16T09:30:00-05:00",
                "next_close": "2024-01-16T16:00:00-05:00"
            }));
        });

        let client = create_test_client(&server);
        let result = is_market_open(&client).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn is_market_open_returns_error_on_api_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(500).body("Internal Server Error");
        });

        let client = create_test_client(&server);
        let result = is_market_open(&client).await;
        assert!(matches!(result, Err(MarketHoursError::ApiRequest(_))));
    }
}
