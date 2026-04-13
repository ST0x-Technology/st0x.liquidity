use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;
use uuid::Uuid;

use super::AlpacaTradingApiError;
use super::auth::{AlpacaTradingApiClient, AlpacaTradingApiCtx};
use crate::{
    CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason, Direction, Executor,
    InventoryResult, MarketOrder, OrderPlacement, OrderState, OrderStatus, SupportedExecutor,
    TryIntoExecutor, buying_power_counter_trade_preflight, estimate_buffered_cost_cents,
};

/// Alpaca Trading API executor implementation
#[derive(Debug, Clone)]
pub struct AlpacaTradingApi {
    client: Arc<AlpacaTradingApiClient>,
    counter_trade_slippage_bps: u16,
}

#[async_trait]
impl Executor for AlpacaTradingApi {
    type Error = AlpacaTradingApiError;
    type OrderId = String;
    type Ctx = AlpacaTradingApiCtx;

    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error> {
        let client = AlpacaTradingApiClient::new(&ctx)?;

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
            counter_trade_slippage_bps: ctx.counter_trade_slippage_bps,
        })
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        Ok(super::market_hours::is_market_open(self.client.client()).await?)
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
                let price = order_update.price.ok_or_else(|| {
                    AlpacaTradingApiError::IncompleteFilledOrder {
                        order_id: order_id.clone(),
                        field: "price".to_string(),
                    }
                })?;

                Ok(OrderState::Filled {
                    executed_at: order_update.updated_at,
                    order_id: order_id.clone(),
                    price,
                })
            }
            OrderStatus::Failed => Ok(OrderState::Failed {
                failed_at: order_update.updated_at,
                error_reason: None,
            }),
        }
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

    async fn get_inventory(&self) -> Result<crate::InventoryResult, Self::Error> {
        Ok(InventoryResult::Fetched(
            super::inventory::fetch_inventory(&self.client).await?,
        ))
    }

    async fn preflight_counter_trade(
        &self,
        order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        match order.direction {
            Direction::Sell => {
                let inventory = super::inventory::fetch_inventory(&self.client).await?;
                let available = inventory
                    .positions
                    .into_iter()
                    .find(|position| position.symbol == order.symbol)
                    .map_or(crate::FractionalShares::ZERO, |position| position.quantity);

                if available.inner().gte(order.shares.inner().inner())? {
                    Ok(CounterTradePreflight::Allowed {
                        reservation: Some(CounterTradeReservation::Equity {
                            symbol: order.symbol,
                            required: order.shares,
                            available,
                        }),
                    })
                } else {
                    Ok(CounterTradePreflight::Skipped(
                        CounterTradeSkipReason::InsufficientEquity {
                            required: order.shares,
                            available,
                        },
                    ))
                }
            }
            Direction::Buy => {
                let latest_trade_price = crate::alpaca_market_data::fetch_latest_trade_price(
                    self.client.http_client(),
                    self.client.market_data_base_url(),
                    &order.symbol,
                )
                .await?;
                let account_funds = super::inventory::get_account_funds(&self.client).await?;
                let estimated_cost_cents = estimate_buffered_cost_cents(
                    order.shares,
                    latest_trade_price.inner(),
                    self.counter_trade_slippage_bps,
                )?;

                Ok(buying_power_counter_trade_preflight(
                    estimated_cost_cents,
                    account_funds.margin_safe_buying_power_cents,
                ))
            }
        }
    }
}

#[async_trait]
impl TryIntoExecutor for AlpacaTradingApiCtx {
    type Executor = AlpacaTradingApi;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        AlpacaTradingApi::try_from_ctx(self).await
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;
    use crate::alpaca_trading_api::AlpacaTradingApiMode;
    use crate::{
        CounterTradePreflight, CounterTradeSkipReason, FractionalShares, Positive, Symbol,
    };

    fn create_test_auth_env(base_url: &str) -> AlpacaTradingApiCtx {
        AlpacaTradingApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Mock(base_url.to_string())),
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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

    fn create_positions_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET).path("/v2/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": "10.5",
                        "market_value": "1575.00"
                    }
                ]));
        })
    }

    fn create_latest_trade_mock<'a>(
        server: &'a MockServer,
        price: &'static str,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path("/v2/stocks/AAPL/trades/latest");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "trade": {
                        "p": price
                    }
                }));
        })
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_valid_credentials() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

        account_mock.assert();
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_invalid_credentials() {
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

        let error = AlpacaTradingApi::try_from_ctx(auth).await.unwrap_err();

        account_mock.assert();
        assert!(matches!(
            error,
            AlpacaTradingApiError::AccountVerification(_)
        ));
    }

    #[tokio::test]
    async fn test_parse_order_id_valid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

        account_mock.assert();

        let valid_uuid = "904837e3-3b76-47ec-b432-046db621571b";

        let order_id = executor.parse_order_id(valid_uuid).unwrap();
        assert_eq!(order_id, valid_uuid);
    }

    #[tokio::test]
    async fn test_parse_order_id_invalid_uuid() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

        account_mock.assert();

        let invalid_uuid = "not-a-valid-uuid";

        assert!(matches!(
            executor.parse_order_id(invalid_uuid).unwrap_err(),
            AlpacaTradingApiError::InvalidOrderId(_)
        ));
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

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

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

        account_mock.assert();

        assert!(executor.run_executor_maintenance().await.is_none());
    }

    #[tokio::test]
    async fn test_get_inventory_returns_fetched() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = create_account_mock(&server);
        let positions_mock = create_positions_mock(&server);

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();

        let result = executor.get_inventory().await.unwrap();

        account_mock.assert_calls(2);
        positions_mock.assert();
        assert!(matches!(result, crate::InventoryResult::Fetched(_)));
    }

    #[tokio::test]
    async fn test_preflight_counter_trade_skips_buy_without_margin_safe_buying_power() {
        let server = MockServer::start();
        let auth = create_test_auth_env(&server.base_url());

        let account_mock = server.mock(|when, then| {
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
                    "non_marginable_buying_power": "100.00",
                    "cash": "1000.00",
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
        });
        let latest_trade_mock = create_latest_trade_mock(&server, "100.00");

        let executor = AlpacaTradingApi::try_from_ctx(auth).await.unwrap();
        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
                direction: Direction::Buy,
            })
            .await
            .unwrap();

        account_mock.assert_calls(2);
        latest_trade_mock.assert();
        assert!(matches!(
            preflight,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientBuyingPower {
                estimated_cost_cents,
                available_buying_power_cents,
            }) if estimated_cost_cents == 20_200 && available_buying_power_cents == 10_000
        ));
    }
}
