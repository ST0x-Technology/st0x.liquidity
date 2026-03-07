use std::sync::LazyLock;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use rain_math_float::FloatError;
use st0x_exact_decimal::ExactDecimal;
use tokio::task::JoinHandle;
use tracing::warn;

/// Hardcoded mock price returned by `MockExecutor::get_order_status`.
static MOCK_FILL_PRICE: LazyLock<Result<ExactDecimal, FloatError>> =
    LazyLock::new(|| ExactDecimal::parse("100"));

use crate::{
    ExecutionError, Executor, Inventory, InventoryResult, MarketOrder, OrderPlacement, OrderState,
    SupportedExecutor, TryIntoExecutor,
};

/// Context for MockExecutor (unit struct - no context needed)
#[derive(Debug, Clone, Default)]
pub struct MockExecutorCtx;

/// Unified test executor for dry-run mode and testing that logs operations without executing real trades
#[derive(Debug, Clone)]
pub struct MockExecutor {
    order_counter: Arc<AtomicU64>,
    should_fail: bool,
    failure_message: String,
    inventory_result: InventoryResult,
    order_status_override: Option<OrderState>,
    market_open: bool,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: false,
            failure_message: String::new(),
            inventory_result: InventoryResult::Unimplemented,
            order_status_override: None,
            market_open: true,
        }
    }

    pub fn with_failure(message: impl Into<String>) -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: true,
            failure_message: message.into(),
            inventory_result: InventoryResult::Unimplemented,
            order_status_override: None,
            market_open: true,
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

    /// Configures whether the market is considered open.
    #[must_use]
    pub fn with_market_open(mut self, open: bool) -> Self {
        self.market_open = open;
        self
    }

    fn generate_order_id(&self) -> String {
        let id = self.order_counter.fetch_add(1, Ordering::SeqCst);
        format!("TEST_{id}")
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
        warn!("[MOCK] Initializing mock executor - always ready in dry-run mode");
        Ok(Self::new())
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        Ok(self.market_open)
    }

    #[tracing::instrument(skip(self), fields(symbol = %order.symbol, shares = %order.shares, direction = %order.direction), level = tracing::Level::INFO)]
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        if self.should_fail {
            return Err(ExecutionError::MockFailure {
                message: self.failure_message.clone(),
            });
        }

        let order_id = self.generate_order_id();

        warn!(
            "[TEST] Would execute order: {} {} shares of {} (order_id: {})",
            order.direction, order.shares, order.symbol, order_id
        );

        Ok(OrderPlacement {
            order_id,
            symbol: order.symbol,
            shares: order.shares,
            direction: order.direction,
            placed_at: chrono::Utc::now(),
        })
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        if self.should_fail {
            return Err(ExecutionError::OrderNotFound {
                order_id: order_id.clone(),
            });
        }

        if let Some(ref override_state) = self.order_status_override {
            warn!("[TEST] Checking status for order: {}", order_id);
            warn!("[TEST] Returning overridden status: {:?}", override_state);
            return Ok(override_state.clone());
        }

        warn!("[TEST] Checking status for order: {}", order_id);
        warn!("[TEST] Returning mock FILLED status with test price");

        // Always return filled status in test mode with mock price
        let price =
            MOCK_FILL_PRICE
                .as_ref()
                .copied()
                .map_err(|error| ExecutionError::MockFailure {
                    message: format!("mock fill price parse failed: {error}"),
                })?;

        Ok(OrderState::Filled {
            executed_at: chrono::Utc::now(),
            order_id: order_id.clone(),
            price,
        })
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::DryRun
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        // For MockExecutor, OrderId is String, so just clone the input
        Ok(order_id_str.to_string())
    }

    async fn run_executor_maintenance(&self) -> Option<JoinHandle<()>> {
        None
    }

    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
        if self.should_fail {
            return Err(ExecutionError::MockFailure {
                message: self.failure_message.clone(),
            });
        }

        Ok(self.inventory_result.clone())
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
    use super::*;
    use crate::{Direction, FractionalShares, Positive, Symbol};

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(ExactDecimal::parse(value).unwrap())
    }

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        Positive::new(shares(value)).unwrap()
    }

    #[tokio::test]
    async fn test_try_from_ctx_success() {
        let executor = MockExecutor::try_from_ctx(MockExecutorCtx).await.unwrap();
        assert!(!executor.should_fail);
        assert_eq!(executor.failure_message, "");
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
        };

        let placement = executor.place_market_order(order).await.unwrap();

        assert!(placement.order_id.starts_with("TEST_"));
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
        };

        assert!(matches!(
            executor.place_market_order(order).await.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Simulated API error"
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_success() {
        let executor = MockExecutor::new();

        let state = executor
            .get_order_status(&"TEST_1".to_string())
            .await
            .unwrap();
        assert!(matches!(state, OrderState::Filled { .. }));
    }

    #[tokio::test]
    async fn test_get_order_status_failure() {
        let executor = MockExecutor::with_failure("Test failure");

        assert!(matches!(
            executor.get_order_status(&"TEST_1".to_string()).await.unwrap_err(),
            ExecutionError::OrderNotFound { order_id } if order_id == "TEST_1"
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
                market_value: Some(ExactDecimal::parse("15000").unwrap()),
            }],
            cash_balance_cents: 5_000_000,
        };

        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let result = executor.get_inventory().await.unwrap();

        match result {
            InventoryResult::Fetched(fetched) => {
                assert_eq!(fetched.positions.len(), 1);
                assert_eq!(fetched.positions[0].symbol, Symbol::new("AAPL").unwrap());
                assert_eq!(fetched.positions[0].quantity, shares("100"));
                assert_eq!(fetched.cash_balance_cents, 5_000_000);
            }
            InventoryResult::Unimplemented => {
                panic!("Expected Fetched, got Unimplemented")
            }
        }
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
            cash_balance_cents: 10_000,
        };

        let executor = MockExecutor::new().with_inventory(inventory);

        assert!(!executor.should_fail);
        assert_eq!(executor.to_supported_executor(), SupportedExecutor::DryRun);
    }
}
