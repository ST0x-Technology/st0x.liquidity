use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;
use uuid::Uuid;

use super::AlpacaTradingApiError;
use super::auth::{AlpacaTradingApiAuthConfig, AlpacaTradingApiClient};
use crate::{
    Executor, MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate, SupportedExecutor,
    TryIntoExecutor,
};

/// Alpaca Trading API executor implementation
#[derive(Debug, Clone)]
pub struct AlpacaTradingApi {
    client: Arc<AlpacaTradingApiClient>,
}

#[async_trait]
impl Executor for AlpacaTradingApi {
    type Error = AlpacaTradingApiError;
    type OrderId = String;
    type Config = AlpacaTradingApiAuthConfig;

    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error> {
        let client = AlpacaTradingApiClient::new(&config)?;

        client.verify_account().await?;

        info!(
            "Alpaca Trading API executor initialized in {} mode",
            if client.is_paper_trading() {
                "paper trading"
            } else {
                "live trading"
            }
        );

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error> {
        Ok(super::market_hours::wait_until_market_open(self.client.client()).await?)
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        super::order::place_market_order(self.client.client(), order).await
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        let order_update = super::order::get_order_status(self.client.client(), order_id).await?;

        match order_update.status {
            OrderStatus::Pending | OrderStatus::Submitted => Ok(OrderState::Submitted {
                order_id: order_id.clone(),
            }),
            OrderStatus::Filled => {
                let price_cents = order_update.price_cents.ok_or_else(|| {
                    AlpacaTradingApiError::IncompleteFilledOrder {
                        order_id: order_id.clone(),
                        field: "price".to_string(),
                    }
                })?;

                Ok(OrderState::Filled {
                    executed_at: order_update.updated_at,
                    order_id: order_id.clone(),
                    price_cents,
                })
            }
            OrderStatus::Failed => Ok(OrderState::Failed {
                failed_at: order_update.updated_at,
                error_reason: None,
            }),
        }
    }

    async fn poll_pending_orders(&self) -> Result<Vec<OrderUpdate<Self::OrderId>>, Self::Error> {
        super::order::poll_pending_orders(self.client.client()).await
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::AlpacaTradingApi
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

#[async_trait]
impl TryIntoExecutor for AlpacaTradingApiAuthConfig {
    type Executor = AlpacaTradingApi;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        AlpacaTradingApi::try_from_config(self).await
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_trading_api::AlpacaTradingApiMode;

    fn create_test_auth_env(base_url: &str) -> AlpacaTradingApiAuthConfig {
        AlpacaTradingApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Mock(base_url.to_string())),
        }
    }

    fn create_account_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "account_number": "PA1234567890",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "100000.00",
                    "regt_buying_power": "100000.00",
                    "daytrading_buying_power": "400000.00",
                    "non_marginable_buying_power": "100000.00",
                    "cash": "100000.00",
                    "accrued_fees": "0",
                    "pending_transfer_out": "0",
                    "pending_transfer_in": "0",
                    "portfolio_value": "100000.00",
                    "pattern_day_trader": false,
                    "trading_blocked": false,
                    "transfers_blocked": false,
                    "account_blocked": false,
                    "created_at": "2020-01-01T00:00:00Z",
                    "trade_suspended_by_user": false,
                    "multiplier": "4",
                    "shorting_enabled": true,
                    "equity": "100000.00",
                    "last_equity": "100000.00",
                    "long_market_value": "0",
                    "short_market_value": "0",
                    "initial_margin": "0",
                    "maintenance_margin": "0",
                    "last_maintenance_margin": "0",
                    "sma": "0",
                    "daytrade_count": 0
                }));
        })
    }

    #[tokio::test]
    async fn test_try_from_config_with_valid_credentials() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let result = AlpacaTradingApi::try_from_config(auth).await;

        account_mock.assert();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_try_from_config_with_invalid_credentials() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        // Mock account verification endpoint to return 401
        let account_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_110_000,
                    "message": "Invalid credentials"
                }));
        });

        let result = AlpacaTradingApi::try_from_config(auth).await;

        account_mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            AlpacaTradingApiError::AccountVerification(_)
        ));
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_open() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let clock_mock = server.mock(|when, then| {
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

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();
        let result = executor.wait_until_market_open().await;

        account_mock.assert();
        clock_mock.assert();
        assert!(result.is_ok());
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_open_returns_duration() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let mock = server.mock(|when, then| {
            when.method(GET).path("/v2/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "timestamp": "2026-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2030-01-06T14:30:00+00:00",
                    "next_close": "2030-01-06T21:00:00+00:00"
                }));
        });

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();
        let result = executor.wait_until_market_open().await;

        account_mock.assert();
        mock.assert();
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_parse_order_id_valid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let valid_uuid = "904837e3-3b76-47ec-b432-046db621571b";
        let result = executor.parse_order_id(valid_uuid);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), valid_uuid);
    }

    #[tokio::test]
    async fn test_parse_order_id_invalid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let invalid_uuid = "not-a-valid-uuid";
        let result = executor.parse_order_id(invalid_uuid);

        assert!(matches!(
            result.unwrap_err(),
            AlpacaTradingApiError::InvalidOrderId(_)
        ));
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();

        account_mock.assert();

        assert_eq!(
            executor.to_supported_executor(),
            SupportedExecutor::AlpacaTradingApi
        );
    }

    #[tokio::test]
    async fn test_run_executor_maintenance_returns_none() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let result = executor.run_executor_maintenance().await;

        assert!(result.is_none());
    }
}
