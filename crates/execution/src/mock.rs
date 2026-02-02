use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{
    ExecutionError, Executor, Inventory, InventoryResult, MarketOrder, OrderPlacement, OrderState,
    OrderUpdate, SupportedExecutor, TryIntoExecutor,
};

/// Configuration for MockExecutor
#[derive(Debug, Clone, Default)]
pub struct MockExecutorConfig;

/// Unified test executor for dry-run mode and testing that logs operations without executing real trades
#[derive(Debug, Clone)]
pub struct MockExecutor {
    order_counter: Arc<AtomicU64>,
    should_fail: bool,
    failure_message: String,
    inventory_result: InventoryResult,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: false,
            failure_message: String::new(),
            inventory_result: InventoryResult::Unimplemented,
        }
    }

    pub fn with_failure(message: impl Into<String>) -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: true,
            failure_message: message.into(),
            inventory_result: InventoryResult::Unimplemented,
        }
    }

    /// Configures the executor to return the specified inventory when `get_inventory()` is called.
    #[must_use]
    pub fn with_inventory(mut self, inventory: Inventory) -> Self {
        self.inventory_result = InventoryResult::Fetched(inventory);
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
    type Config = MockExecutorConfig;

    async fn try_from_config(_config: Self::Config) -> Result<Self, Self::Error> {
        warn!("[MOCK] Initializing mock executor - always ready in dry-run mode");
        Ok(Self::new())
    }

    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error> {
        info!("[TEST] Market hours check - market is always open in test mode");
        // Test executor should never block on market hours, so return Duration::MAX
        // to signal no time limit
        Ok(std::time::Duration::MAX)
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

        warn!("[TEST] Checking status for order: {}", order_id);
        warn!("[TEST] Returning mock FILLED status with test price");

        // Always return filled status in test mode with mock price
        Ok(OrderState::Filled {
            executed_at: chrono::Utc::now(),
            order_id: order_id.clone(),
            price_cents: 10000, // $100.00 mock price
        })
    }

    async fn poll_pending_orders(&self) -> Result<Vec<OrderUpdate<Self::OrderId>>, Self::Error> {
        if self.should_fail {
            return Err(ExecutionError::MockFailure {
                message: self.failure_message.clone(),
            });
        }

        warn!("[TEST] Polling pending orders - no pending orders in test mode");

        // Return empty list since test orders are immediately "filled"
        Ok(Vec::new())
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
impl TryIntoExecutor for MockExecutorConfig {
    type Executor = MockExecutor;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        MockExecutor::try_from_config(self).await
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::{Direction, FractionalShares, Positive, Symbol};

    #[tokio::test]
    async fn test_try_from_config_success() {
        let result = MockExecutor::try_from_config(MockExecutorConfig).await;
        assert!(result.is_ok());

        let executor = result.unwrap();
        assert!(!executor.should_fail);
        assert_eq!(executor.failure_message, "");
    }

    #[tokio::test]
    async fn test_wait_until_market_open_always_returns_none() {
        let executor = MockExecutor::new();
        let result = executor.wait_until_market_open().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), std::time::Duration::MAX);
    }

    #[tokio::test]
    async fn test_failure_executor_wait_until_market_open() {
        let executor = MockExecutor::with_failure("Test failure");
        let result = executor.wait_until_market_open().await;

        assert!(result.is_ok());
        let dur = result.unwrap();
        assert_eq!(dur, std::time::Duration::MAX);
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
        let shares = Positive::new(FractionalShares::new(Decimal::from(10))).unwrap();
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares,
            direction: Direction::Buy,
        };

        let result = executor.place_market_order(order).await;
        let placement = result.unwrap();

        assert!(placement.order_id.starts_with("TEST_"));
        assert_eq!(placement.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(
            placement.shares,
            Positive::new(FractionalShares::new(Decimal::from(10))).unwrap()
        );
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_failure() {
        let executor = MockExecutor::with_failure("Simulated API error");
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(10))).unwrap(),
            direction: Direction::Buy,
        };

        let result = executor.place_market_order(order).await;
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Simulated API error"
        ));
    }

    #[tokio::test]
    async fn test_poll_pending_orders_success() {
        let executor = MockExecutor::new();
        let result = executor.poll_pending_orders().await;

        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_poll_pending_orders_failure() {
        let executor = MockExecutor::with_failure("Connection timeout");
        let result = executor.poll_pending_orders().await;

        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::MockFailure { message } if message == "Connection timeout"
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_success() {
        let executor = MockExecutor::new();
        let result = executor.get_order_status(&"TEST_1".to_string()).await;

        let state = result.unwrap();
        assert!(matches!(state, OrderState::Filled { .. }));
    }

    #[tokio::test]
    async fn test_get_order_status_failure() {
        let executor = MockExecutor::with_failure("Test failure");
        let result = executor.get_order_status(&"TEST_1".to_string()).await;

        assert!(matches!(
            result.unwrap_err(),
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
                quantity: FractionalShares::new(Decimal::from(100)),
                market_value_cents: Some(15000_00),
            }],
            cash_balance_cents: 50_000_00,
        };

        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let result = executor.get_inventory().await.unwrap();

        match result {
            InventoryResult::Fetched(fetched) => {
                assert_eq!(fetched.positions.len(), 1);
                assert_eq!(fetched.positions[0].symbol, Symbol::new("AAPL").unwrap());
                assert_eq!(
                    fetched.positions[0].quantity,
                    FractionalShares::new(Decimal::from(100))
                );
                assert_eq!(fetched.cash_balance_cents, 50_000_00);
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
            cash_balance_cents: 100_00,
        };

        let executor = MockExecutor::new().with_inventory(inventory);

        assert!(!executor.should_fail);
        assert_eq!(executor.to_supported_executor(), SupportedExecutor::DryRun);
    }
}
