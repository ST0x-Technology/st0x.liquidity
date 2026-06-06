use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

use rain_math_float::Float;
use st0x_float_serde::format_float_with_fallback;

use super::auth::{AccountStatus, AlpacaAccountId, AlpacaBrokerApiCtx};
use super::client::AlpacaBrokerApiClient;
use super::journal::JournalResponse;
use super::order::{
    AlpacaLimitOrder, ConversionDirection, CryptoOrderOutcome, CryptoOrderResponse,
};
use super::{AlpacaBrokerApiError, AssetStatus, MissingOrderField, TimeInForce};
use crate::{
    CancellationOutcome, ClientOrderId, CounterTradePreflight, Direction, Executor,
    ExecutorOrderId, FractionalShares, InventoryResult, LimitOrder, MarketOrder, MarketSession,
    OrderPlacement, OrderState, OrderStatus, Positive, SupportedExecutor, Symbol, TryIntoExecutor,
    Usd, buying_power_counter_trade_preflight, estimate_buffered_cost_cents,
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
    counter_trade_slippage_bps: u16,
}

impl Clone for AlpacaBrokerApi {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            asset_cache: Arc::clone(&self.asset_cache),
            asset_cache_ttl: self.asset_cache_ttl,
            time_in_force: self.time_in_force,
            counter_trade_slippage_bps: self.counter_trade_slippage_bps,
        }
    }
}

impl std::fmt::Debug for AlpacaBrokerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApi")
            .field("client", &self.client)
            .field("asset_cache_ttl", &self.asset_cache_ttl)
            .field("time_in_force", &self.time_in_force)
            .field(
                "counter_trade_slippage_bps",
                &self.counter_trade_slippage_bps,
            )
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
            counter_trade_slippage_bps: ctx.counter_trade_slippage_bps,
        })
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        super::market_hours::is_market_open(&self.client).await
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        let asset = self.get_asset_cached(&order.symbol).await?;
        Self::validate_asset(&order.symbol, &asset)?;

        super::order::place_market_order(&self.client, order, self.time_in_force).await
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        let order_update = super::order::get_order_status(&self.client, order_id).await?;

        match order_update.status {
            OrderStatus::Pending | OrderStatus::Submitted => Ok(OrderState::Submitted {
                order_id: ExecutorOrderId::new(order_id),
            }),
            OrderStatus::PartiallyFilled => {
                let shares_filled = order_update.shares_filled.ok_or_else(|| {
                    AlpacaBrokerApiError::IncompleteOrder {
                        order_id: ExecutorOrderId::new(order_id),
                        field: MissingOrderField::FilledQty,
                    }
                })?;

                Ok(OrderState::PartiallyFilled {
                    order_id: ExecutorOrderId::new(order_id),
                    shares_filled,
                    avg_price: order_update.price.map(Usd::new),
                    partially_filled_at: order_update.updated_at,
                })
            }
            OrderStatus::Filled => {
                let price =
                    order_update
                        .price
                        .ok_or_else(|| AlpacaBrokerApiError::IncompleteOrder {
                            order_id: ExecutorOrderId::new(order_id),
                            field: MissingOrderField::Price,
                        })?;

                Ok(OrderState::Filled {
                    executed_at: order_update.updated_at,
                    order_id: ExecutorOrderId::new(order_id),
                    price: Usd::new(price),
                })
            }
            // Alpaca always reports filled_qty ("0" when unfilled), so a
            // terminal response without it is malformed: passing None through
            // would be indistinguishable from a no-fill terminal order
            // downstream, silently dropping any partial fill (the
            // double-hedge failure mode). Fail closed like PartiallyFilled.
            OrderStatus::Cancelled => {
                let shares_filled = order_update.shares_filled.ok_or_else(|| {
                    AlpacaBrokerApiError::IncompleteOrder {
                        order_id: ExecutorOrderId::new(order_id),
                        field: MissingOrderField::FilledQty,
                    }
                })?;
                Ok(OrderState::Cancelled {
                    cancelled_at: order_update.updated_at,
                    order_id: ExecutorOrderId::new(order_id),
                    shares_filled: Some(shares_filled),
                    avg_price: order_update.price.map(Usd::new),
                })
            }
            // Unlike Cancelled, Failed does NOT fail closed on a missing
            // filled_qty: rejected-order payloads have been observed without
            // the field, and erroring here would wedge rejection handling
            // (the poll retries the same read forever and the position never
            // releases). A genuine partial fill on a failed order carries
            // filled_qty, which the retained-fill path records; an absent
            // field on a rejection means no fill.
            OrderStatus::Failed => Ok(OrderState::Failed {
                failed_at: order_update.updated_at,
                error_reason: None,
                shares_filled: order_update.shares_filled,
                avg_price: order_update.price.map(Usd::new),
            }),
        }
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::AlpacaBrokerApi
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        Uuid::parse_str(order_id_str)?;
        Ok(order_id_str.to_string())
    }

    async fn get_inventory(&self) -> Result<crate::InventoryResult, Self::Error> {
        let inventory = super::positions::fetch_inventory(&self.client).await?;
        Ok(InventoryResult::Fetched(inventory))
    }

    async fn preflight_counter_trade(
        &self,
        order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        match order.direction {
            Direction::Sell => {
                let inventory = super::positions::fetch_inventory(&self.client).await?;
                let available = inventory
                    .positions
                    .into_iter()
                    .find(|position| position.symbol == order.symbol)
                    .map_or(FractionalShares::ZERO, |position| position.quantity);

                Ok(crate::resolve_sell_preflight(order, available)?)
            }
            Direction::Buy => {
                let latest_trade_price = crate::alpaca_market_data::fetch_latest_trade_price(
                    self.client.market_data_http_client(),
                    self.client.market_data_base_url(),
                    &order.symbol,
                )
                .await?;
                let account_funds = super::positions::get_account_funds(&self.client).await?;
                let estimated_cost_cents = estimate_buffered_cost_cents(
                    order.shares,
                    latest_trade_price.inner().inner(),
                    self.counter_trade_slippage_bps,
                )?;

                let available_buying_power_cents = account_funds.buying_power;
                let preflight = buying_power_counter_trade_preflight(
                    estimated_cost_cents,
                    available_buying_power_cents,
                );

                if matches!(preflight, CounterTradePreflight::Allowed { .. }) {
                    debug!(
                        target: "broker",
                        symbol = %order.symbol,
                        estimated_cost_cents,
                        available_buying_power_cents,
                        "Preflight passed: sufficient buying power for buy"
                    );
                }

                Ok(preflight)
            }
        }
    }

    async fn fetch_latest_trade_price(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<Positive<Usd>>, Self::Error> {
        let price = crate::alpaca_market_data::fetch_latest_trade_price(
            self.client.market_data_http_client(),
            self.client.market_data_base_url(),
            symbol,
        )
        .await?;
        Ok(Some(price))
    }

    async fn market_session(&self) -> Result<MarketSession, Self::Error> {
        super::market_hours::market_session(&self.client).await
    }

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        let asset = self.get_asset_cached(&order.symbol).await?;
        Self::validate_asset(&order.symbol, &asset)?;

        let alpaca_limit_price = super::order::AlpacaLimitPrice::try_new(order.limit_price)?;

        let alpaca_order = AlpacaLimitOrder {
            symbol: order.symbol,
            shares: order.shares,
            direction: order.direction,
            limit_price: alpaca_limit_price,
            extended_hours: order.extended_hours,
            client_order_id: order.client_order_id,
        };

        super::order::place_limit_order(&self.client, alpaca_order).await
    }

    async fn cancel_order(
        &self,
        order_id: &Self::OrderId,
    ) -> Result<CancellationOutcome, Self::Error> {
        let order_uuid = Uuid::parse_str(order_id)?;
        self.client.cancel_order(order_uuid).await
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
        amount: Float,
        direction: ConversionDirection,
        client_order_id: &ClientOrderId,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let order =
            super::order::convert_usdc_usd(&self.client, amount, direction, client_order_id)
                .await?;

        info!(
            order_id = %order.id,
            amount = %format_float_with_fallback(&amount),
            direction = ?direction,
            "USDC/USD conversion order placed, polling for completion..."
        );

        super::order::poll_crypto_order_until_filled(&self.client, order.id).await
    }

    /// Looks up a previously-placed conversion order by its `client_order_id`
    /// (the correlation id recorded before placement), for crash-safe resume.
    /// Returns the current snapshot, or `None` if the order never reached Alpaca.
    pub async fn find_conversion_order(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<CryptoOrderResponse>, AlpacaBrokerApiError> {
        self.client
            .get_crypto_order_by_client_order_id(client_order_id)
            .await
    }

    /// Polls a previously-placed conversion order until it reaches a terminal
    /// state (filled or terminally failed), returning the final snapshot.
    ///
    /// Crash-safe resume uses this to await a still-settling conversion -- the
    /// same wait the original placement performs in `convert_usdc_usd` -- instead
    /// of failing the job. Failing a healthy-but-slow conversion would burn the
    /// finite apalis retry budget and risk the timeout sweep clearing the
    /// in-progress guard while the conversion is still settling.
    pub async fn poll_conversion_to_terminal(
        &self,
        order_id: Uuid,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        loop {
            let order = self.client.get_crypto_order(order_id).await?;

            match order.classify() {
                CryptoOrderOutcome::Filled | CryptoOrderOutcome::Failed(_) => return Ok(order),
                CryptoOrderOutcome::Pending => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Journal (transfer) equities from the configured account to a
    /// destination account under the same Alpaca broker firm.
    pub async fn create_journal(
        &self,
        destination: AlpacaAccountId,
        symbol: &Symbol,
        quantity: Positive<FractionalShares>,
    ) -> Result<JournalResponse, AlpacaBrokerApiError> {
        self.client
            .create_journal(destination, symbol, quantity)
            .await
    }

    /// Place a manual Alpaca-specific limit order for operator intervention.
    ///
    /// Accepts `AlpacaLimitOrder` directly (pre-validated Alpaca price type).
    /// For automated counter-trading, use the `Executor::place_limit_order`
    /// trait method which accepts the broker-agnostic `LimitOrder` type.
    pub async fn place_alpaca_limit_order(
        &self,
        order: AlpacaLimitOrder,
    ) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
        let asset = self.get_asset_cached(&order.symbol).await?;
        Self::validate_asset(&order.symbol, &asset)?;

        super::order::place_limit_order(&self.client, order).await
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

    fn validate_asset(symbol: &Symbol, asset: &CachedAsset) -> Result<(), AlpacaBrokerApiError> {
        if asset.status != AssetStatus::Active {
            return Err(AlpacaBrokerApiError::AssetNotActive {
                symbol: symbol.clone(),
                status: asset.status,
            });
        }

        if !asset.tradable {
            return Err(AlpacaBrokerApiError::AssetNotTradable {
                symbol: symbol.clone(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use st0x_float_macro::float;
    use std::thread;
    use uuid::uuid;

    use super::*;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };
    use crate::alpaca_broker_api::order::AlpacaLimitPrice;
    use crate::{
        ClientOrderId, CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason,
        Direction, FractionalShares, Positive, Usd,
    };

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

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
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    fn create_account_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
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
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
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
    async fn test_maintenance_interval_returns_none() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        account_mock.assert();

        assert!(executor.maintenance_interval().is_none());
    }

    #[tokio::test]
    async fn test_get_inventory_returns_fetched() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "cash": "50000.00",
                    "non_marginable_buying_power": "50000.00"
                }));
        });

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": "10.5",
                        "market_value": "1575.00"
                    }
                ]));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        let result = executor.get_inventory().await.unwrap();

        // Account endpoint is hit twice: once during try_from_ctx (verify_account)
        // and once during get_inventory (get_account_details for cash balance)
        account_mock.assert_calls(2);
        positions_mock.assert();
        assert!(matches!(result, crate::InventoryResult::Fetched(_)));
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
    async fn test_preflight_counter_trade_skips_buy_without_cash() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "cash": "100.00"
                }));
        });
        let latest_trade_mock = create_latest_trade_mock(&server, "100.00");

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
                direction: Direction::Buy,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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

    #[tokio::test]
    async fn test_preflight_counter_trade_allows_buy_using_unsettled_proceeds() {
        // Verifies the policy from adrs/1-cash-bp-for-equity-hedges.md: cash
        // (which includes unsettled T+1 equity-sale proceeds) is the budget,
        // not non_marginable_buying_power. Here NM-BP would have rejected the
        // buy, but cash covers it -- so the preflight allows it.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "cash": "35000.00",
                    "non_marginable_buying_power": "31.55"
                }));
        });
        let latest_trade_mock = create_latest_trade_mock(&server, "100.00");

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
                direction: Direction::Buy,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .unwrap();

        account_mock.assert_calls(2);
        latest_trade_mock.assert();
        assert!(matches!(
            preflight,
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::BuyingPower {
                    estimated_cost_cents,
                    available_buying_power_cents,
                }),
            } if estimated_cost_cents == 20_200 && available_buying_power_cents == 3_500_000
        ));
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
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
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

    fn create_limit_order_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "195.25",
                    "time_in_force": "day",
                    "extended_hours": true,
                    "client_order_id": "88888888-8888-4888-8888-888888888888"
                }));
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
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
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
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
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
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        executor.place_market_order(order1).await.unwrap();

        // Place second order for same symbol - should use cached asset info
        let order2 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("50".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        executor.place_market_order(order2).await.unwrap();

        // Asset endpoint should only be called once
        asset_mock.assert_calls(1);
        // Order endpoint should be called twice
        order_mock.assert_calls(2);
    }

    #[tokio::test]
    async fn test_asset_cache_expires_after_ttl() {
        let server = MockServer::start();

        // Use a very short TTL for testing (0 seconds)
        let ctx = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::ZERO,
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
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
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        executor.place_market_order(order1).await.unwrap();

        // Wait for cache to expire (TTL is 0 seconds, so any delay causes expiration)
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Place second order - cache should be expired, so asset API called again
        let order2 = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("50".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        executor.place_market_order(order2).await.unwrap();

        // Asset endpoint should be called twice due to cache expiration
        asset_mock.assert_calls(2);
        order_mock.assert_calls(2);
    }

    #[tokio::test]
    async fn test_place_limit_order_succeeds_for_active_tradable_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "active", true);
        let order_mock = create_limit_order_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(Float::parse("195.25".to_string()).unwrap())).unwrap(),
            )
            .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "88888888-8888-4888-8888-888888888888"
            )),
        };

        let result = executor.place_alpaca_limit_order(order).await.unwrap();

        asset_mock.assert();
        order_mock.assert();
        assert_eq!(result.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(result.symbol, Symbol::new("AAPL").unwrap());
    }

    #[tokio::test]
    async fn test_place_limit_order_fails_for_inactive_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "inactive", true);
        let order_mock = create_order_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(Float::parse("195.25".to_string()).unwrap())).unwrap(),
            )
            .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        let result = executor.place_alpaca_limit_order(order).await;

        asset_mock.assert();
        order_mock.assert_calls(0);
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
    async fn test_place_limit_order_fails_for_non_tradable_asset() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let account_mock = create_account_mock(&server);
        let asset_mock = create_asset_mock(&server, "AAPL", "active", false);
        let order_mock = create_order_mock(&server);

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("100".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(Float::parse("195.25".to_string()).unwrap())).unwrap(),
            )
            .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        let result = executor.place_alpaca_limit_order(order).await;

        asset_mock.assert();
        order_mock.assert_calls(0);
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
    async fn cancel_order_surfaces_404_as_order_not_found() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let account_mock = create_account_mock(&server);
        let order_id = "904837e3-3b76-47ec-b432-046db621571b".to_string();

        let cancel_mock = server.mock(|when, then| {
            when.method(DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({ "code": 40_410_000, "message": "order not found" }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let outcome = executor.cancel_order(&order_id).await.unwrap();

        cancel_mock.assert();
        assert_eq!(outcome, crate::CancellationOutcome::OrderNotFound);
    }

    #[tokio::test]
    async fn cancel_order_surfaces_2xx_as_requested() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let account_mock = create_account_mock(&server);
        let order_id = "904837e3-3b76-47ec-b432-046db621571b".to_string();

        let cancel_mock = server.mock(|when, then| {
            when.method(DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(204);
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let outcome = executor.cancel_order(&order_id).await.unwrap();

        cancel_mock.assert();
        assert_eq!(outcome, crate::CancellationOutcome::Requested);
    }

    #[tokio::test]
    async fn get_order_status_maps_cancelled_broker_order_with_fill_to_order_state() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let account_mock = create_account_mock(&server);
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d".to_string();

        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "filled_qty": "40.5",
                    "side": "buy",
                    "status": "canceled",
                    "filled_avg_price": "199.50",
                    "canceled_at": "2025-01-06T14:32:01.111111Z"
                }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let state = executor.get_order_status(&order_id).await.unwrap();

        status_mock.assert();
        let OrderState::Cancelled {
            shares_filled,
            avg_price,
            cancelled_at,
            ..
        } = state
        else {
            panic!("expected OrderState::Cancelled, got {state:?}");
        };
        assert_eq!(shares_filled, Some(FractionalShares::new(float!(40.5))));
        assert_eq!(avg_price, Some(Usd::new(float!(199.50))));
        // Broker cancellation time, not the local observation time.
        assert_eq!(
            cancelled_at,
            "2025-01-06T14:32:01.111111Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_order_status_rejected_without_filled_qty_maps_to_failed() {
        // Rejected-order payloads have been observed without filled_qty.
        // Failing closed here would wedge rejection handling: the poll
        // would retry the same erroring status read forever and the
        // position would never release its pending slot.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let account_mock = create_account_mock(&server);
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d".to_string();

        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "rejected"
                }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let state = executor.get_order_status(&order_id).await.unwrap();

        status_mock.assert();
        let OrderState::Failed {
            shares_filled,
            avg_price,
            ..
        } = state
        else {
            panic!("expected OrderState::Failed, got {state:?}");
        };
        assert_eq!(shares_filled, None);
        assert_eq!(avg_price, None);
    }

    #[tokio::test]
    async fn get_order_status_partially_filled_without_filled_qty_errors() {
        // The PartiallyFilled arm requires filled_qty: a broker payload
        // claiming a partial fill without the quantity must fail fast rather
        // than fabricate a fill amount.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let account_mock = create_account_mock(&server);
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d".to_string();

        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "partially_filled",
                    "filled_avg_price": "199.50"
                }));
        });

        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();
        account_mock.assert();

        let error = executor.get_order_status(&order_id).await.unwrap_err();

        status_mock.assert();
        assert!(
            matches!(
                &error,
                AlpacaBrokerApiError::IncompleteOrder { order_id: id, field: MissingOrderField::FilledQty }
                    if *id == ExecutorOrderId::new(&order_id)
            ),
            "expected IncompleteOrder for FilledQty, got {error:?}"
        );
    }
}
