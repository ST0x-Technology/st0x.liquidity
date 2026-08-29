use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use st0x_float_macro::float;
use tracing::{debug, info};

/// Hardcoded mock price returned by `MockExecutor::get_order_status`.
static MOCK_FILL_PRICE: LazyLock<Float> = LazyLock::new(|| float!(100));

use crate::{
    CancellationOutcome, CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason,
    DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, Direction, ExecutionError, Executor,
    ExecutorOrderId, IndicativeQuote, Inventory, InventoryResult, LatestQuote, LimitOrder,
    MarketOrder, MarketSession, MarketSessionStatus, OrderPlacement, OrderState, Positive,
    PostCloseGap, SupportedExecutor, Symbol, TryIntoExecutor, Usd, estimate_buffered_cost_cents,
};

/// Context for MockExecutor (unit struct - no context needed)
#[derive(Debug, Clone, Default)]
pub struct MockExecutorCtx;

/// Whether the mock executes operations or fails them. Couples the
/// failure flag to its message so a message without a failure (or a
/// failure without a message) is unrepresentable.
#[derive(Debug, Clone)]
enum Health {
    Healthy,
    Failing { message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum MockOrderIdError {
    #[error("mock order ID {order_id} does not contain a filled quantity")]
    MissingQuantity { order_id: String },
    #[error("mock order ID {order_id} contains an invalid filled quantity")]
    InvalidQuantity {
        order_id: String,
        #[source]
        source: rain_math_float::FloatError,
    },
    #[error("mock order ID {order_id} contains a non-positive filled quantity")]
    NonPositiveQuantity {
        order_id: String,
        #[source]
        source: crate::NotPositive<crate::FractionalShares>,
    },
}

/// Unified test executor for dry-run mode and testing that logs operations without executing real trades
#[derive(Debug, Clone)]
pub struct MockExecutor {
    health: Health,
    inventory_result: InventoryResult,
    order_status_override: Option<OrderState>,
    market_open: bool,
    market_session_override: Option<MarketSession>,
    market_session_status_calls: Arc<AtomicU64>,
    market_session_status_failure: Option<String>,
    extended_session_closes_at_override: Option<DateTime<Utc>>,
    post_close_gap_override: PostCloseGap,
    primary_limit_quote_override: Option<LatestQuote>,
    latest_quote_override: Option<LatestQuote>,
    overnight_quote_override: Option<IndicativeQuote>,
    position_mark_override: Option<Positive<Usd>>,
    preflight_price: Float,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self {
            health: Health::Healthy,
            inventory_result: InventoryResult::Unimplemented,
            order_status_override: None,
            market_open: true,
            market_session_override: None,
            market_session_status_calls: Arc::new(AtomicU64::new(0)),
            market_session_status_failure: None,
            extended_session_closes_at_override: None,
            post_close_gap_override: PostCloseGap::Unknown,
            primary_limit_quote_override: None,
            latest_quote_override: None,
            overnight_quote_override: None,
            position_mark_override: None,
            preflight_price: *MOCK_FILL_PRICE,
        }
    }

    pub fn with_failure(message: impl Into<String>) -> Self {
        Self {
            health: Health::Failing {
                message: message.into(),
            },
            ..Self::new()
        }
    }

    /// Configures the executor to return the specified inventory when `get_inventory()` is called.
    #[must_use]
    pub fn with_inventory(mut self, inventory: Inventory) -> Self {
        self.inventory_result = InventoryResult::Fetched(inventory);
        self
    }

    /// Configures the executor to return the specified order state from `get_order_status()`.
    #[must_use]
    pub fn with_order_status(mut self, status: OrderState) -> Self {
        self.order_status_override = Some(status);
        self
    }

    /// Configures the executor to report the market as closed (open is
    /// the default).
    #[must_use]
    pub fn with_market_closed(mut self) -> Self {
        self.market_open = false;
        self
    }

    /// Configures the market session returned by `market_session()`.
    /// When set, this takes precedence over `market_open` for the
    /// `market_session()` method.
    #[must_use]
    pub fn with_market_session(mut self, session: MarketSession) -> Self {
        self.market_session_override = Some(session);
        self
    }

    /// Serves `quote` from `fetch_latest_overnight_quote`; without this
    /// knob the mock reports the overnight feed as unavailable.
    #[must_use]
    pub fn with_overnight_quote(mut self, quote: IndicativeQuote) -> Self {
        self.overnight_quote_override = Some(quote);
        self
    }

    #[must_use]
    pub fn market_session_status_call_count(&self) -> u64 {
        self.market_session_status_calls.load(Ordering::SeqCst)
    }

    /// Configures `market_session_status()` to fail with the given message,
    /// independent of `market_session()` (which is left succeeding). This
    /// mirrors the real executor issuing a separate calendar HTTP call for
    /// close-flatten window status, letting tests prove that call is never
    /// reached when it shouldn't be (e.g. outside the extended session).
    #[must_use]
    pub fn with_market_session_status_failure(mut self, message: impl Into<String>) -> Self {
        self.market_session_status_failure = Some(message.into());
        self
    }

    #[must_use]
    pub fn with_extended_session_closes_at(mut self, closes_at: DateTime<Utc>) -> Self {
        self.extended_session_closes_at_override = Some(closes_at);
        self
    }

    #[must_use]
    pub fn with_post_close_gap(mut self, post_close_gap: PostCloseGap) -> Self {
        self.post_close_gap_override = post_close_gap;
        self
    }

    /// Stubs the broker mark extended-hours hedges price against.
    #[must_use]
    pub fn with_position_mark(mut self, mark: Positive<Usd>) -> Self {
        self.position_mark_override = Some(mark);
        self
    }

    #[must_use]
    pub fn with_primary_limit_quote(mut self, quote: LatestQuote) -> Self {
        self.primary_limit_quote_override = Some(quote);
        self
    }

    #[must_use]
    pub fn with_latest_quote(mut self, quote: LatestQuote) -> Self {
        self.latest_quote_override = Some(quote);
        self
    }

    /// Returns the failure error when the executor is configured to
    /// fail, shared by every fallible operation.
    fn fail_if_unhealthy(&self) -> Result<(), ExecutionError> {
        if let Health::Failing { message } = &self.health {
            return Err(ExecutionError::MockFailure {
                message: message.clone(),
            });
        }

        Ok(())
    }

    #[must_use]
    pub fn with_preflight_price(mut self, price: Float) -> Self {
        self.preflight_price = price;
        self
    }

    fn order_id(
        client_order_id: &crate::ClientOrderId,
        shares: Positive<crate::FractionalShares>,
    ) -> String {
        format!("TEST:{client_order_id}:{shares}")
    }

    fn shares_from_order_id(
        order_id: &str,
    ) -> Result<Positive<crate::FractionalShares>, MockOrderIdError> {
        let (_, shares) =
            order_id
                .rsplit_once(':')
                .ok_or_else(|| MockOrderIdError::MissingQuantity {
                    order_id: order_id.to_string(),
                })?;
        let shares = shares
            .parse()
            .map_err(|source| MockOrderIdError::InvalidQuantity {
                order_id: order_id.to_string(),
                source,
            })?;
        Positive::new(shares).map_err(|source| MockOrderIdError::NonPositiveQuantity {
            order_id: order_id.to_string(),
            source,
        })
    }

    fn preflight_sell(&self, order: MarketOrder) -> Result<CounterTradePreflight, ExecutionError> {
        let InventoryResult::Fetched(inventory) = &self.inventory_result else {
            return Ok(CounterTradePreflight::Allowed { reservation: None });
        };

        let available = inventory
            .positions
            .iter()
            .find(|position| position.symbol == order.symbol)
            .map_or(crate::FractionalShares::ZERO, |position| position.quantity);

        Ok(crate::resolve_sell_preflight(order, available)?)
    }

    /// Shared cash check for counter-trade buy branches.
    fn preflight_buy_cash(
        &self,
        order: &MarketOrder,
        reference_price: crate::Positive<Usd>,
        slippage_bps: u16,
    ) -> Result<CounterTradePreflight, ExecutionError> {
        let InventoryResult::Fetched(inventory) = &self.inventory_result else {
            return Ok(CounterTradePreflight::Allowed { reservation: None });
        };

        let estimated_cost_cents = estimate_buffered_cost_cents(
            order.shares,
            reference_price.inner().inner(),
            slippage_bps,
        )?;

        if inventory.usd_balance_cents >= estimated_cost_cents {
            Ok(CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::BuyingPower {
                    estimated_cost_cents,
                    available_buying_power_cents: inventory.usd_balance_cents,
                }),
            })
        } else {
            Ok(CounterTradePreflight::Skipped(
                CounterTradeSkipReason::InsufficientBuyingPower {
                    estimated_cost_cents,
                    available_buying_power_cents: inventory.usd_balance_cents,
                },
            ))
        }
    }
}

impl Default for MockExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    type Error = ExecutionError;
    type OrderId = String;
    type Ctx = MockExecutorCtx;

    async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
        info!(target: "broker", "[MOCK] Initializing mock executor - always ready in dry-run mode");
        Ok(Self::new())
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        Ok(self.market_open)
    }

    #[tracing::instrument(target = "broker", skip(self), fields(symbol = %order.symbol, shares = %order.shares, direction = %order.direction), level = tracing::Level::INFO)]
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        self.fail_if_unhealthy()?;

        let order_id = Self::order_id(&order.client_order_id, order.shares);

        debug!(
            target: "broker",
            "[TEST] Would execute order: {} {} shares of {} (order_id: {})",
            order.direction, order.shares, order.symbol, order_id
        );

        Ok(OrderPlacement {
            order_id,
            symbol: order.symbol,
            shares: order.shares,
            direction: order.direction,
            placed_at: chrono::Utc::now(),
            extended_hours: false,
            limit_price: None,
        })
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        self.fail_if_unhealthy()?;

        if let Some(ref override_state) = self.order_status_override {
            debug!(target: "broker", "[TEST] Checking status for order: {}", order_id);
            debug!(target: "broker", "[TEST] Returning overridden status: {:?}", override_state);
            return Ok(override_state.clone());
        }

        debug!(target: "broker", "[TEST] Checking status for order: {}", order_id);
        debug!(target: "broker", "[TEST] Returning mock FILLED status with test price");

        // Always return filled status in test mode with mock price
        let price = *MOCK_FILL_PRICE;
        let shares_filled = Self::shares_from_order_id(order_id)?;

        Ok(OrderState::Filled {
            executed_at: chrono::Utc::now(),
            order_id: ExecutorOrderId::new(order_id),
            shares_filled,
            price: Usd::new(price),
        })
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::DryRun
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        // For MockExecutor, OrderId is String, so just clone the input
        Ok(order_id_str.to_string())
    }

    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
        self.fail_if_unhealthy()?;

        Ok(self.inventory_result.clone())
    }

    async fn preflight_counter_trade(
        &self,
        order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.fail_if_unhealthy()?;

        match order.direction {
            Direction::Sell => self.preflight_sell(order),
            Direction::Buy => {
                // `preflight_price` is a plain `Float` test knob (set via
                // `with_preflight_price`), validated here rather than at
                // construction so `MockExecutor::new()` stays infallible.
                let reference_price = crate::Positive::new(Usd::new(self.preflight_price))?;
                self.preflight_buy_cash(
                    &order,
                    reference_price,
                    DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
                )
            }
        }
    }

    async fn preflight_counter_trade_at_price(
        &self,
        order: MarketOrder,
        limit_price: crate::Positive<Usd>,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.fail_if_unhealthy()?;

        match order.direction {
            // Inventory availability doesn't depend on price.
            Direction::Sell => self.preflight_sell(order),
            Direction::Buy => self.preflight_buy_cash(&order, limit_price, 0),
        }
    }

    async fn market_session(&self) -> Result<MarketSession, Self::Error> {
        if let Some(session) = self.market_session_override {
            return Ok(session);
        }

        if self.market_open {
            Ok(MarketSession::Regular)
        } else {
            Ok(MarketSession::Closed)
        }
    }

    async fn market_session_status(&self) -> Result<MarketSessionStatus, Self::Error> {
        self.fail_if_unhealthy()?;

        self.market_session_status_calls
            .fetch_add(1, Ordering::SeqCst);

        if let Some(message) = &self.market_session_status_failure {
            return Err(ExecutionError::MockFailure {
                message: message.clone(),
            });
        }

        Ok(MarketSessionStatus {
            session: self.market_session().await?,
            extended_session_closes_at: self.extended_session_closes_at_override,
            post_close_gap: self.post_close_gap_override,
        })
    }

    async fn fetch_position_mark(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<Positive<Usd>>, Self::Error> {
        self.fail_if_unhealthy()?;
        Ok(self.position_mark_override)
    }

    async fn fetch_primary_limit_quote(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<LatestQuote>, Self::Error> {
        self.fail_if_unhealthy()?;
        Ok(self.primary_limit_quote_override)
    }

    async fn fetch_latest_quote(
        &self,
        _symbol: &crate::Symbol,
    ) -> Result<Option<LatestQuote>, Self::Error> {
        self.fail_if_unhealthy()?;
        Ok(self.latest_quote_override)
    }

    async fn fetch_latest_overnight_quote(
        &self,
        symbol: &crate::Symbol,
    ) -> Result<IndicativeQuote, Self::Error> {
        self.fail_if_unhealthy()?;
        self.overnight_quote_override
            .ok_or_else(|| ExecutionError::OvernightQuoteUnavailable {
                symbol: symbol.clone(),
            })
    }

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        self.fail_if_unhealthy()?;

        let order_id = Self::order_id(&order.client_order_id, order.shares);

        debug!(
            target: "broker",
            "[TEST] Would execute limit order: {} {} shares of {} at {} (order_id: {})",
            order.direction, order.shares, order.symbol, order.limit_price, order_id
        );

        Ok(OrderPlacement {
            order_id,
            symbol: order.symbol,
            shares: order.shares,
            direction: order.direction,
            placed_at: chrono::Utc::now(),
            extended_hours: order.extended_hours,
            limit_price: Some(order.limit_price),
        })
    }

    async fn cancel_order(
        &self,
        order_id: &Self::OrderId,
    ) -> Result<CancellationOutcome, Self::Error> {
        self.fail_if_unhealthy()?;

        debug!(target: "broker", "[TEST] Would cancel order: {}", order_id);
        Ok(CancellationOutcome::Requested)
    }
}

#[async_trait]
impl TryIntoExecutor for MockExecutorCtx {
    type Executor = MockExecutor;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        MockExecutor::try_from_ctx(self).await
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{ClientOrderId, Direction, FractionalShares, Positive, Symbol};

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(Float::parse(value.to_string()).unwrap())
    }

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        Positive::new(shares(value)).unwrap()
    }

    #[tokio::test]
    async fn test_try_from_ctx_success() {
        let executor = MockExecutor::try_from_ctx(MockExecutorCtx).await.unwrap();

        // A constructed executor is operational: a fallible op succeeds
        // rather than returning the unhealthy MockFailure.
        executor.get_inventory().await.unwrap();
    }

    #[tokio::test]
    async fn test_parse_order_id() {
        let executor = MockExecutor::new();
        let test_id = "TEST_123";
        let parsed = executor.parse_order_id(test_id).unwrap();
        assert_eq!(parsed, test_id);
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let executor = MockExecutor::new();
        assert_eq!(executor.to_supported_executor(), SupportedExecutor::DryRun);
    }

    #[tokio::test]
    async fn test_place_market_order_success() {
        let executor = MockExecutor::new();
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        let placement = executor.place_market_order(order).await.unwrap();

        assert!(placement.order_id.starts_with("TEST:"));
        assert_eq!(placement.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(placement.shares, positive_shares("10"));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_failure() {
        let executor = MockExecutor::with_failure("Simulated API error");
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        assert!(matches!(
            executor.place_market_order(order).await.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Simulated API error"
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_success() {
        let executor = MockExecutor::new();
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("0.004115451"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        let placement = executor.place_market_order(order).await.unwrap();

        let state = executor
            .get_order_status(&placement.order_id)
            .await
            .unwrap();
        assert!(matches!(
            state,
            OrderState::Filled { shares_filled, .. }
                if shares_filled == positive_shares("0.004115451")
        ));
    }

    #[tokio::test]
    async fn get_order_status_returns_limit_order_quantity() {
        let executor = MockExecutor::new();
        let order = LimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("0.004115451"),
            direction: Direction::Buy,
            limit_price: Positive::new(Usd::new(float!(199.50))).unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        let placement = executor.place_limit_order(order).await.unwrap();

        let state = executor
            .get_order_status(&placement.order_id)
            .await
            .unwrap();

        assert!(matches!(
            state,
            OrderState::Filled { shares_filled, .. }
                if shares_filled == positive_shares("0.004115451")
        ));
    }

    #[tokio::test]
    async fn new_executor_recovers_filled_quantity_from_order_id() {
        let placing_executor = MockExecutor::new();
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("0.004115451"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };
        let placement = placing_executor.place_market_order(order).await.unwrap();
        let restarted_executor = MockExecutor::new();

        assert!(matches!(
            restarted_executor
                .get_order_status(&placement.order_id)
                .await
                .unwrap(),
            OrderState::Filled { shares_filled, .. }
                if shares_filled == positive_shares("0.004115451")
        ));
    }

    #[tokio::test]
    async fn malformed_order_id_quantity_errors() {
        let executor = MockExecutor::new();

        assert!(matches!(
            executor
                .get_order_status(&"TEST-without-quantity".to_string())
                .await
                .unwrap_err(),
            ExecutionError::InvalidMockOrderId(MockOrderIdError::MissingQuantity { .. })
        ));
        assert!(matches!(
            executor
                .get_order_status(&"TEST:client:not-a-number".to_string())
                .await
                .unwrap_err(),
            ExecutionError::InvalidMockOrderId(MockOrderIdError::InvalidQuantity { .. })
        ));
        assert!(matches!(
            executor
                .get_order_status(&"TEST:client:0".to_string())
                .await
                .unwrap_err(),
            ExecutionError::InvalidMockOrderId(MockOrderIdError::NonPositiveQuantity { .. })
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_failure() {
        let executor = MockExecutor::with_failure("Test failure");

        assert!(matches!(
            executor.get_order_status(&"TEST_1".to_string()).await.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Test failure"
        ));
    }

    #[tokio::test]
    async fn get_inventory_returns_unimplemented_by_default() {
        let executor = MockExecutor::new();

        let result = executor.get_inventory().await.unwrap();

        assert!(
            matches!(result, InventoryResult::Unimplemented),
            "Expected Unimplemented, got {result:?}"
        );
    }

    #[tokio::test]
    async fn get_inventory_returns_configured_inventory() {
        let inventory = crate::Inventory {
            positions: vec![crate::EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: shares("100"),
                market_value: Some(Float::parse("15000".to_string()).unwrap()),
            }],
            usd_balance_cents: 5_000_000,
            cash_buying_power_cents: Some(5_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };

        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let result = executor.get_inventory().await.unwrap();

        match result {
            InventoryResult::Fetched(fetched) => {
                assert_eq!(fetched.positions.len(), 1);
                assert_eq!(fetched.positions[0].symbol, Symbol::new("AAPL").unwrap());
                assert_eq!(fetched.positions[0].quantity, shares("100"));
                assert_eq!(fetched.usd_balance_cents, 5_000_000);
                assert_eq!(fetched.cash_buying_power_cents, Some(5_000_000));
            }
            InventoryResult::Unimplemented => {
                panic!("Expected Fetched, got Unimplemented")
            }
        }
    }

    #[tokio::test]
    async fn market_session_status_fails_when_executor_unhealthy() {
        let executor = MockExecutor::with_failure("broker down");

        assert!(matches!(
            executor.market_session_status().await.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "broker down"
        ));
        assert_eq!(executor.market_session_status_call_count(), 0);
    }

    #[tokio::test]
    async fn overnight_quote_is_unavailable_until_configured() {
        // Fail-loud default: an executor that was not explicitly given
        // an overnight quote must never silently serve one.
        let executor = MockExecutor::new();
        let symbol = Symbol::new("AAPL").unwrap();

        let error = executor
            .fetch_latest_overnight_quote(&symbol)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                ExecutionError::OvernightQuoteUnavailable { ref symbol }
                    if *symbol == Symbol::new("AAPL").unwrap()
            ),
            "expected OvernightQuoteUnavailable, got {error:?}"
        );
    }

    #[tokio::test]
    async fn overnight_quote_returns_the_configured_quote() {
        let quote = IndicativeQuote {
            quote: LatestQuote::new(
                Positive::new(Usd::new(float!(24.10))).unwrap(),
                Positive::new(Usd::new(float!(24.30))).unwrap(),
            )
            .unwrap(),
            at: chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 8, 29, 1, 0, 0).unwrap(),
        };
        let executor = MockExecutor::new().with_overnight_quote(quote);
        let symbol = Symbol::new("AAPL").unwrap();

        assert_eq!(
            executor
                .fetch_latest_overnight_quote(&symbol)
                .await
                .unwrap(),
            quote
        );
    }

    #[tokio::test]
    async fn get_inventory_returns_error_when_should_fail() {
        let executor = MockExecutor::with_failure("Inventory fetch failed");

        assert!(matches!(
            executor.get_inventory().await.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Inventory fetch failed"
        ));
    }

    #[tokio::test]
    async fn with_inventory_preserves_other_settings() {
        let inventory = crate::Inventory {
            positions: vec![],
            usd_balance_cents: 10_000,
            cash_buying_power_cents: Some(10_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };

        let executor = MockExecutor::new().with_inventory(inventory);

        // `with_inventory` leaves the executor healthy: a fallible op still
        // succeeds rather than returning the unhealthy MockFailure.
        executor.get_inventory().await.unwrap();
        assert_eq!(executor.to_supported_executor(), SupportedExecutor::DryRun);
    }

    #[tokio::test]
    async fn preflight_counter_trade_skips_sell_without_inventory() {
        let executor = MockExecutor::new().with_inventory(crate::Inventory {
            positions: vec![],
            usd_balance_cents: 50_000,
            cash_buying_power_cents: Some(50_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: positive_shares("2"),
                direction: Direction::Sell,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .unwrap();

        assert!(matches!(
            preflight,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientEquity {
                required,
                available,
            }) if required == positive_shares("2") && available == shares("0")
        ));
    }

    #[tokio::test]
    async fn preflight_counter_trade_allows_partial_sell_with_capped_shares() {
        let executor = MockExecutor::new().with_inventory(crate::Inventory {
            positions: vec![crate::EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: shares("10"),
                market_value: None,
            }],
            usd_balance_cents: 50_000,
            cash_buying_power_cents: Some(50_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: positive_shares("20"),
                direction: Direction::Sell,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .unwrap();

        match preflight {
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::Equity { required, .. }),
            } => {
                assert_eq!(
                    required,
                    positive_shares("10"),
                    "Should cap to available shares"
                );
            }
            other => panic!("Expected Allowed with capped shares, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn preflight_counter_trade_skips_sell_with_zero_inventory() {
        let executor = MockExecutor::new().with_inventory(crate::Inventory {
            positions: vec![crate::EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: shares("0"),
                market_value: None,
            }],
            usd_balance_cents: 50_000,
            cash_buying_power_cents: Some(50_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: positive_shares("5"),
                direction: Direction::Sell,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .unwrap();

        assert!(matches!(
            preflight,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientEquity {
                required,
                available,
            }) if required == positive_shares("5") && available == shares("0")
        ));
    }

    #[tokio::test]
    async fn preflight_counter_trade_skips_buy_without_cash() {
        let executor = MockExecutor::new()
            .with_inventory(crate::Inventory {
                positions: vec![],
                usd_balance_cents: 10_000,
                cash_buying_power_cents: Some(10_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            })
            .with_preflight_price(float!(100));

        let preflight = executor
            .preflight_counter_trade(MarketOrder {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: positive_shares("2"),
                direction: Direction::Buy,
                client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .unwrap();

        assert!(matches!(
            preflight,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientBuyingPower {
                estimated_cost_cents,
                available_buying_power_cents,
            }) if estimated_cost_cents == 20_200 && available_buying_power_cents == 10_000
        ));
    }

    #[tokio::test]
    async fn preflight_counter_trade_at_price_uses_exact_limit_cost() {
        let executor = MockExecutor::new().with_inventory(crate::Inventory {
            positions: vec![],
            usd_balance_cents: 20_000,
            cash_buying_power_cents: Some(20_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let preflight = executor
            .preflight_counter_trade_at_price(
                MarketOrder {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: positive_shares("2"),
                    direction: Direction::Buy,
                    client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                },
                Positive::new(Usd::new(float!(100))).unwrap(),
            )
            .await
            .unwrap();

        assert!(matches!(
            preflight,
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::BuyingPower {
                    estimated_cost_cents,
                    available_buying_power_cents,
                }),
            } if estimated_cost_cents == 20_000 && available_buying_power_cents == 20_000
        ));
    }

    #[tokio::test]
    async fn fetch_position_mark_returns_configured_failure() {
        let mark = Positive::new(Usd::new(float!(100))).unwrap();
        let executor = MockExecutor::with_failure("mark lookup failed").with_position_mark(mark);

        assert!(matches!(
            executor
                .fetch_position_mark(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap_err(),
            ExecutionError::MockFailure { message } if message == "mark lookup failed"
        ));
    }

    #[tokio::test]
    async fn fetch_position_mark_returns_the_configured_mark() {
        let mark = Positive::new(Usd::new(float!(223.80))).unwrap();
        let executor = MockExecutor::new().with_position_mark(mark);

        assert_eq!(
            executor
                .fetch_position_mark(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap(),
            Some(mark)
        );
    }

    #[tokio::test]
    async fn fetch_position_mark_is_absent_without_an_override() {
        assert_eq!(
            MockExecutor::new()
                .fetch_position_mark(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn fetch_primary_limit_quote_returns_the_configured_quote() {
        let quote = LatestQuote::new(
            Positive::new(Usd::new(float!(99))).unwrap(),
            Positive::new(Usd::new(float!(101))).unwrap(),
        )
        .unwrap();
        let executor = MockExecutor::new().with_primary_limit_quote(quote);

        assert_eq!(
            executor
                .fetch_primary_limit_quote(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap(),
            Some(quote)
        );
    }

    #[tokio::test]
    async fn fetch_primary_limit_quote_is_absent_without_an_override() {
        assert_eq!(
            MockExecutor::new()
                .fetch_primary_limit_quote(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn preflight_counter_trade_returns_error_when_should_fail() {
        let executor = MockExecutor::with_failure("preflight failed");

        assert!(matches!(
            executor
                .preflight_counter_trade(MarketOrder {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: positive_shares("2"),
                    direction: Direction::Buy,
                    client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                })
                .await
                .unwrap_err(),
            ExecutionError::MockFailure { message } if message == "preflight failed"
        ));
    }
}
