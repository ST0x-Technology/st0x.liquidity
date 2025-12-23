use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::auth::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiClient};
use crate::{Executor, MarketOrder, OrderPlacement, OrderState, OrderUpdate};

/// Alpaca Broker API executor implementation
#[derive(Debug, Clone)]
pub struct AlpacaBrokerApi {
    client: Arc<AlpacaBrokerApiClient>,
}

#[async_trait]
impl Executor for AlpacaBrokerApi {
    type Error = AlpacaBrokerApiError;
    type OrderId = String;
    type Config = AlpacaBrokerApiAuthEnv;

    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error> {
        let client = AlpacaBrokerApiClient::new(&config)?;

        client.verify_account().await?;

        info!(
            "Alpaca Broker API executor initialized in {} mode",
            if client.is_sandbox() {
                "sandbox"
            } else {
                "production"
            }
        );

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error> {
        super::market_hours::wait_until_market_open(&self.client).await
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        super::order::place_market_order(&self.client, order).await
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        let order_update = super::order::get_order_status(&self.client, order_id).await?;

        match order_update.status {
            crate::OrderStatus::Pending | crate::OrderStatus::Submitted => {
                Ok(OrderState::Submitted {
                    order_id: order_id.clone(),
                })
            }
            crate::OrderStatus::Filled => Ok(OrderState::Filled {
                executed_at: order_update.updated_at,
                order_id: order_id.clone(),
                price_cents: order_update.price_cents.unwrap_or(0),
            }),
            crate::OrderStatus::Failed => Ok(OrderState::Failed {
                failed_at: order_update.updated_at,
                error_reason: Some(format!("Order status: {:?}", order_update.status)),
            }),
        }
    }

    async fn poll_pending_orders(&self) -> Result<Vec<OrderUpdate<Self::OrderId>>, Self::Error> {
        super::order::poll_pending_orders(&self.client).await
    }

    fn to_supported_executor(&self) -> crate::SupportedExecutor {
        crate::SupportedExecutor::AlpacaBrokerApi
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        Uuid::parse_str(order_id_str)?;
        Ok(order_id_str.to_string())
    }

    async fn run_executor_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Alpaca uses API keys, no token refresh needed
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca_broker_api::auth::AlpacaBrokerApiMode;
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

    fn create_account_mock(server: &MockServer) -> httpmock::Mock {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        })
    }

    #[tokio::test]
    async fn test_try_from_config_success() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);

        let result = AlpacaBrokerApi::try_from_config(config).await;

        account_mock.assert();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_try_from_config_unauthorized() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40110000,
                    "message": "Invalid credentials"
                }));
        });

        let result = AlpacaBrokerApi::try_from_config(config).await;

        account_mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 401
        ));
    }

    #[tokio::test]
    async fn test_wait_until_market_open() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);
        let clock_mock = server.mock(|when, then| {
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

        let executor = AlpacaBrokerApi::try_from_config(config).await.unwrap();
        let result = executor.wait_until_market_open().await;

        account_mock.assert();
        clock_mock.assert();
        assert!(result.is_ok());
        assert!(result.unwrap().as_secs() > 0);
    }

    #[tokio::test]
    async fn test_parse_order_id_valid() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_config(config).await.unwrap();

        account_mock.assert();

        let valid_uuid = "904837e3-3b76-47ec-b432-046db621571b";
        let result = executor.parse_order_id(valid_uuid);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), valid_uuid);
    }

    #[tokio::test]
    async fn test_parse_order_id_invalid() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_config(config).await.unwrap();

        account_mock.assert();

        let invalid_uuid = "not-a-valid-uuid";
        let result = executor.parse_order_id(invalid_uuid);

        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::InvalidOrderId(_)
        ));
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_config(config).await.unwrap();

        account_mock.assert();

        assert_eq!(
            executor.to_supported_executor(),
            crate::SupportedExecutor::AlpacaBrokerApi
        );
    }

    #[tokio::test]
    async fn test_run_executor_maintenance_returns_none() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_config(config).await.unwrap();

        account_mock.assert();

        let result = executor.run_executor_maintenance().await;

        assert!(result.is_none());
    }
}
