use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;
use uuid::Uuid;

use super::auth::{AlpacaAuthEnv, AlpacaClient};
use crate::{Broker, BrokerError, MarketOrder, OrderPlacement, OrderState, OrderUpdate};

/// Alpaca broker implementation
#[derive(Debug, Clone)]
pub struct AlpacaBroker {
    client: Arc<AlpacaClient>,
}

#[async_trait]
impl Broker for AlpacaBroker {
    type Error = BrokerError;
    type OrderId = String;
    type Config = AlpacaAuthEnv;

    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error> {
        let client = AlpacaClient::new(&config)?;

        client.verify_account().await.map_err(|e| {
            BrokerError::Authentication(format!("Alpaca account verification failed: {e}"))
        })?;

        info!(
            "Alpaca broker initialized in {} mode",
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
        super::order::poll_pending_orders(self.client.client()).await
    }

    fn to_supported_broker(&self) -> crate::SupportedBroker {
        crate::SupportedBroker::Alpaca
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        // Validate that it's a valid UUID format (Alpaca uses UUIDs for order IDs)
        Uuid::parse_str(order_id_str).map_err(|e| BrokerError::InvalidOrder {
            reason: format!("Invalid Alpaca order ID format (expected UUID): {e}"),
        })?;

        Ok(order_id_str.to_string())
    }

    async fn run_broker_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Alpaca uses API keys, no token refresh needed
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca::auth::AlpacaTradingMode;
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_auth_env(base_url: &str) -> AlpacaAuthEnv {
        AlpacaAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Mock(base_url.to_string()),
        }
    }

    fn create_account_mock(server: &MockServer) -> httpmock::Mock {
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

        let result = AlpacaBroker::try_from_config(auth).await;

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

        let result = AlpacaBroker::try_from_config(auth).await;

        account_mock.assert();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::Authentication(_)
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

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();
        let result = broker.wait_until_market_open().await;

        account_mock.assert();
        clock_mock.assert();
        assert!(result.is_ok());
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_wait_until_market_open_when_closed_then_open() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

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

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();
        let result = broker.wait_until_market_open().await;

        account_mock.assert();
        closed_mock.assert();
        open_mock.assert();
        assert!(result.is_ok());
        let duration = result.unwrap();
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_parse_order_id_valid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let valid_uuid = "904837e3-3b76-47ec-b432-046db621571b";
        let result = broker.parse_order_id(valid_uuid);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), valid_uuid);
    }

    #[tokio::test]
    async fn test_parse_order_id_invalid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let invalid_uuid = "not-a-valid-uuid";
        let result = broker.parse_order_id(invalid_uuid);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::InvalidOrder { .. }
        ));
    }

    #[tokio::test]
    async fn test_to_supported_broker() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();

        account_mock.assert();

        assert_eq!(broker.to_supported_broker(), crate::SupportedBroker::Alpaca);
    }

    #[tokio::test]
    async fn test_run_broker_maintenance_returns_none() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let broker = AlpacaBroker::try_from_config(auth).await.unwrap();

        account_mock.assert();

        let result = broker.run_broker_maintenance().await;

        assert!(result.is_none());
    }
}
