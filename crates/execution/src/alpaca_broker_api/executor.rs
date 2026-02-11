use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tracing::info;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::auth::{AccountStatus, AlpacaBrokerApiCtx};
use super::client::AlpacaBrokerApiClient;
use super::order::{ConversionDirection, CryptoOrderResponse};
use crate::{
    Executor, MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate, SupportedExecutor,
    TryIntoExecutor,
};

/// Alpaca Broker API executor implementation
#[derive(Debug, Clone)]
pub struct AlpacaBrokerApi {
    client: Arc<AlpacaBrokerApiClient>,
}

#[async_trait]
impl Executor for AlpacaBrokerApi {
    type Error = AlpacaBrokerApiError;
    type OrderId = String;
    type Ctx = AlpacaBrokerApiCtx;

    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error> {
        let client = AlpacaBrokerApiClient::new(&ctx)?;

        let account = client.verify_account().await?;

        if account.status != AccountStatus::Active {
            return Err(AlpacaBrokerApiError::AccountNotActive {
                account_id: account.id,
                status: account.status,
            });
        }

        info!(
            account_id = %account.id,
            mode = if client.is_sandbox() { "sandbox" } else { "production" },
            "Alpaca Broker API executor initialized"
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
            OrderStatus::Pending | OrderStatus::Submitted => Ok(OrderState::Submitted {
                order_id: order_id.clone(),
            }),
            OrderStatus::Filled => {
                let price_cents = order_update.price_cents.ok_or_else(|| {
                    AlpacaBrokerApiError::IncompleteFilledOrder {
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
        super::order::poll_pending_orders(&self.client).await
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::AlpacaBrokerApi
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        Uuid::parse_str(order_id_str)?;
        Ok(order_id_str.to_string())
    }

    async fn run_executor_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Alpaca uses API keys, no token refresh needed
        None
    }

    async fn get_inventory(&self) -> Result<crate::InventoryResult, Self::Error> {
        let inventory = super::positions::fetch_inventory(&self.client).await?;
        Ok(crate::InventoryResult::Fetched(inventory))
    }
}

#[async_trait]
impl TryIntoExecutor for AlpacaBrokerApiCtx {
    type Executor = AlpacaBrokerApi;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        AlpacaBrokerApi::try_from_ctx(self).await
    }
}

impl AlpacaBrokerApi {
    /// Convert USDC to/from USD buying power.
    ///
    /// This uses the USDC/USD trading pair on Alpaca:
    /// - `UsdcToUsd`: Sells USDC for USD buying power
    /// - `UsdToUsdc`: Buys USDC with USD buying power
    ///
    /// Returns the completed order response after the order is filled.
    pub async fn convert_usdc_usd(
        &self,
        amount: Decimal,
        direction: ConversionDirection,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let order = super::order::convert_usdc_usd(&self.client, amount, direction).await?;

        info!(
            order_id = %order.id,
            amount = %amount,
            direction = ?direction,
            "USDC/USD conversion order placed, polling for completion..."
        );

        super::order::poll_crypto_order_until_filled(&self.client, order.id).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use chrono_tz::America::New_York;
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::auth::AlpacaBrokerApiMode;

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(mode),
        }
    }

    fn create_account_mock(server: &MockServer) -> httpmock::Mock<'_> {
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
    async fn test_try_from_ctx_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        account_mock.assert();
    }

    #[tokio::test]
    async fn test_try_from_ctx_unauthorized() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_110_000,
                    "message": "Invalid credentials"
                }));
        });

        let error = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap_err();

        account_mock.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 401
        ));
    }

    #[tokio::test]
    async fn test_wait_until_market_open() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        // Get today's date in ET timezone to build a response that represents
        // market currently being open
        let now = Utc::now();
        let now_et = now.with_timezone(&New_York);
        let today = now_et.date_naive();
        let today_str = today.format("%Y-%m-%d").to_string();

        // Mock calendar endpoint - returns today as a trading day with market
        // hours that span the entire day so the test always finds market "open"
        let calendar_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/calendar");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": today_str,
                        "open": "00:00",
                        "close": "23:59"
                    }
                ]));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        let result = executor.wait_until_market_open().await;

        account_mock.assert();
        calendar_mock.assert();
        assert!(result.unwrap().as_secs() > 0);
    }

    #[tokio::test]
    async fn test_parse_order_id_valid() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        account_mock.assert();

        let valid_uuid = "904837e3-3b76-47ec-b432-046db621571b";

        let order_id = executor.parse_order_id(valid_uuid).unwrap();
        assert_eq!(order_id, valid_uuid);
    }

    #[tokio::test]
    async fn test_parse_order_id_invalid() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

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
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        account_mock.assert();

        assert_eq!(
            executor.to_supported_executor(),
            SupportedExecutor::AlpacaBrokerApi
        );
    }

    #[tokio::test]
    async fn test_run_executor_maintenance_returns_none() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        account_mock.assert();

        let result = executor.run_executor_maintenance().await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_inventory_returns_fetched() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // Mock account endpoint with all fields needed for both verify_account and fetch_inventory
        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "cash": "25000.00"
                }));
        });

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty": "10.0",
                        "market_value": "1500.00"
                    }
                ]));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        let result = executor.get_inventory().await.unwrap();

        account_mock.assert_hits(2); // Once for verify, once for fetch_inventory
        positions_mock.assert();

        match result {
            crate::InventoryResult::Fetched(inventory) => {
                assert_eq!(inventory.positions.len(), 1);
                assert_eq!(inventory.cash_balance_cents, 2_500_000);
            }
            crate::InventoryResult::Unimplemented => {
                panic!("Expected Fetched, got Unimplemented");
            }
        }
    }
}
