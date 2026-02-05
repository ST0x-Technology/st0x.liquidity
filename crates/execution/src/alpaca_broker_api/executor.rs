use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use super::auth::{AccountStatus, AlpacaBrokerApiCtx};
use super::client::AlpacaBrokerApiClient;
use super::order::{ConversionDirection, CryptoOrderResponse};
use super::{AlpacaBrokerApiError, AssetStatus, TimeInForce};
use crate::{
    Executor, MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate, SupportedExecutor,
    Symbol, TryIntoExecutor,
};

/// Response from the asset endpoint
#[derive(Debug, Clone, Deserialize)]
pub(super) struct AssetResponse {
    pub status: AssetStatus,
    pub tradable: bool,
}

/// Cached asset information with expiration tracking
#[derive(Debug, Clone)]
struct CachedAsset {
    status: AssetStatus,
    tradable: bool,
    cached_at: Instant,
}

impl CachedAsset {
    fn from_response(response: &AssetResponse) -> Self {
        Self {
            status: response.status,
            tradable: response.tradable,
            cached_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Alpaca Broker API executor implementation
pub struct AlpacaBrokerApi {
    client: Arc<AlpacaBrokerApiClient>,
    asset_cache: Arc<RwLock<HashMap<String, CachedAsset>>>,
    asset_cache_ttl: Duration,
    time_in_force: TimeInForce,
}

impl Clone for AlpacaBrokerApi {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            asset_cache: Arc::clone(&self.asset_cache),
            asset_cache_ttl: self.asset_cache_ttl,
            time_in_force: self.time_in_force,
        }
    }
}

impl std::fmt::Debug for AlpacaBrokerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApi")
            .field("client", &self.client)
            .field("asset_cache_ttl", &self.asset_cache_ttl)
            .field("time_in_force", &self.time_in_force)
            .finish_non_exhaustive()
    }
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
            time_in_force = ?ctx.time_in_force,
            "Alpaca Broker API executor initialized"
        );

        Ok(Self {
            client: Arc::new(client),
            asset_cache: Arc::new(RwLock::new(HashMap::new())),
            asset_cache_ttl: ctx.asset_cache_ttl,
            time_in_force: ctx.time_in_force,
        })
    }

    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error> {
        super::market_hours::wait_until_market_open(&self.client).await
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        super::market_hours::is_market_open(&self.client).await
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        let asset = self.get_asset_cached(&order.symbol).await?;

        if asset.status != AssetStatus::Active {
            return Err(AlpacaBrokerApiError::AssetNotActive {
                symbol: order.symbol,
                status: asset.status,
            });
        }

        if !asset.tradable {
            return Err(AlpacaBrokerApiError::AssetNotTradable {
                symbol: order.symbol,
            });
        }

        super::order::place_market_order(&self.client, order, self.time_in_force).await
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

    async fn get_asset_cached(&self, symbol: &Symbol) -> Result<CachedAsset, AlpacaBrokerApiError> {
        let symbol_str = symbol.to_string();

        {
            let cache = self.asset_cache.read().await;
            if let Some(cached) = cache.get(&symbol_str)
                && !cached.is_expired(self.asset_cache_ttl)
            {
                return Ok(cached.clone());
            }
        }

        let response = self.client.get_asset(symbol).await?;
        let cached = CachedAsset::from_response(&response);

        {
            let mut cache = self.asset_cache.write().await;
            cache.insert(symbol_str, cached.clone());
        }

        Ok(cached)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use chrono_tz::America::New_York;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::thread;

    use super::*;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
    use crate::{Direction, FractionalShares, Positive};

    #[test]
    fn test_asset_status_deserialize_active() {
        let json = r#""active""#;
        let status: AssetStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status, AssetStatus::Active);
    }

    #[test]
    fn test_asset_status_deserialize_inactive() {
        let json = r#""inactive""#;
        let status: AssetStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status, AssetStatus::Inactive);
    }

    #[test]
    fn test_asset_status_deserialize_rejects_uppercase() {
        let json = r#""ACTIVE""#;
        let result: Result<AssetStatus, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected error for uppercase, got: {result:?}"
        );
    }

    #[test]
    fn test_asset_response_deserialize() {
        // API returns more fields than we need - serde ignores the extras
        let json = json!({
            "id": "904837e3-3b76-47ec-b432-046db621571b",
            "symbol": "AAPL",
            "status": "active",
            "tradable": true
        });

        let response: AssetResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response.status, AssetStatus::Active);
        assert!(response.tradable);
    }

    #[test]
    fn test_cached_asset_from_response() {
        let response = AssetResponse {
            status: AssetStatus::Active,
            tradable: true,
        };

        let cached = CachedAsset::from_response(&response);

        assert_eq!(cached.status, AssetStatus::Active);
        assert!(cached.tradable);
        // cached_at should be very recent (within last second)
        assert!(cached.cached_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_cached_asset_is_expired_returns_false_when_fresh() {
        let cached = CachedAsset {
            status: AssetStatus::Active,
            tradable: true,
            cached_at: Instant::now(),
        };

        assert!(
            !cached.is_expired(Duration::from_secs(3600)),
            "Fresh cache entry should not be expired"
        );
    }

    #[test]
    fn test_cached_asset_is_expired_returns_true_when_expired() {
        let cached = CachedAsset {
            status: AssetStatus::Active,
            tradable: true,
            cached_at: Instant::now()
                .checked_sub(Duration::from_secs(100))
                .unwrap(),
        };

        assert!(
            cached.is_expired(Duration::from_secs(60)),
            "Cache entry older than TTL should be expired"
        );
    }

    #[test]
    fn test_cached_asset_is_expired_boundary_not_expired() {
        // Entry at exactly TTL should not be expired (uses > not >=)
        let ttl = Duration::from_millis(50);
        let cached = CachedAsset {
            status: AssetStatus::Active,
            tradable: true,
            cached_at: Instant::now(),
        };

        // Should not be expired immediately
        assert!(!cached.is_expired(ttl));
    }

    #[test]
    fn test_cached_asset_is_expired_boundary_expired() {
        let ttl = Duration::from_millis(10);
        let cached = CachedAsset {
            status: AssetStatus::Active,
            tradable: true,
            cached_at: Instant::now(),
        };

        // Wait past TTL
        thread::sleep(Duration::from_millis(20));

        assert!(
            cached.is_expired(ttl),
            "Cache entry should be expired after TTL passes"
        );
    }

    #[test]
    fn test_cached_asset_is_expired_zero_ttl() {
        let cached = CachedAsset {
            status: AssetStatus::Active,
            tradable: true,
            cached_at: Instant::now(),
        };

        // Zero TTL means always expired after any time passes
        thread::sleep(Duration::from_millis(1));
        assert!(
            cached.is_expired(Duration::ZERO),
            "Zero TTL should expire immediately"
        );
    }

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
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
        let wait = executor.wait_until_market_open().await.unwrap();

        account_mock.assert();
        calendar_mock.assert();
        assert!(wait.as_secs() > 0);
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

        assert!(matches!(
            executor.parse_order_id(invalid_uuid).unwrap_err(),
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

        assert!(executor.run_executor_maintenance().await.is_none());
    }

    #[tokio::test]
    async fn test_get_inventory_returns_fetched() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "cash": "50000.00"
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
                        "qty": "10.5",
                        "market_value": "1575.00"
                    }
                ]));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        let result = executor.get_inventory().await.unwrap();

        // Account endpoint is hit twice: once during try_from_ctx (verify_account)
        // and once during get_inventory (get_account_details for cash balance)
        account_mock.assert_hits(2);
        positions_mock.assert();
        assert!(matches!(result, crate::InventoryResult::Fetched(_)));
    }

    fn create_asset_mock<'a>(
        server: &'a MockServer,
        symbol: &str,
        status: &str,
        tradable: bool,
    ) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path(format!("/v1/assets/{symbol}"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": symbol,
                    "status": status,
                    "tradable": tradable
                }));
        })
    }

    fn create_order_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        })
    }

    #[tokio::test]
    async fn test_place_market_order_fails_for_inactive_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "inactive", true);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };

        let result = executor.place_market_order(order).await;

        asset_mock.assert();
        let err = result.unwrap_err();
        assert!(
            matches!(
                &err,
                AlpacaBrokerApiError::AssetNotActive { symbol, status }
                    if *symbol == Symbol::new("AAPL").unwrap() && *status == AssetStatus::Inactive
            ),
            "Expected AssetNotActive error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_place_market_order_fails_for_non_tradable_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "active", false);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };

        let result = executor.place_market_order(order).await;

        asset_mock.assert();
        let err = result.unwrap_err();
        assert!(
            matches!(
                &err,
                AlpacaBrokerApiError::AssetNotTradable { symbol } if *symbol == Symbol::new("AAPL").unwrap()
            ),
            "Expected AssetNotTradable error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_place_market_order_succeeds_for_active_tradable_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "active", true);
        let order_mock = create_order_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };

        let result = executor.place_market_order(order).await;

        asset_mock.assert();
        order_mock.assert();
        let placement = result.unwrap();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "AAPL");
    }

    #[tokio::test]
    async fn test_asset_validation_uses_cache() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        // Asset mock should be called exactly once due to caching
        let asset_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true
                }));
        });

        // Order mock for both orders
        let order_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        // Place first order
        let order1 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };
        executor.place_market_order(order1).await.unwrap();

        // Place second order for same symbol - should use cached asset info
        let order2 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(50))).unwrap(),
            direction: Direction::Buy,
        };
        executor.place_market_order(order2).await.unwrap();

        // Asset endpoint should only be called once
        asset_mock.assert_hits(1);
        // Order endpoint should be called twice
        order_mock.assert_hits(2);
    }

    #[tokio::test]
    async fn test_asset_cache_expires_after_ttl() {
        let server = MockServer::start();

        // Use a very short TTL for testing (0 seconds)
        let ctx = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::ZERO,
            time_in_force: TimeInForce::Day,
        };

        let account_mock = create_account_mock(&server);

        // Asset mock should be called twice due to cache expiration
        let asset_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true
                }));
        });

        let order_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        // Place first order
        let order1 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(100))).unwrap(),
            direction: Direction::Buy,
        };
        executor.place_market_order(order1).await.unwrap();

        // Wait for cache to expire (TTL is 0 seconds, so any delay causes expiration)
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Place second order - cache should be expired, so asset API called again
        let order2 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(50))).unwrap(),
            direction: Direction::Buy,
        };
        executor.place_market_order(order2).await.unwrap();

        // Asset endpoint should be called twice due to cache expiration
        asset_mock.assert_hits(2);
        order_mock.assert_hits(2);
    }
}
