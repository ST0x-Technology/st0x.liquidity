use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{
    ExecutionError, Executor, MarketOrder, OrderPlacement, OrderState, OrderUpdate,
    SupportedExecutor, TryIntoExecutor,
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
}

impl MockExecutor {
    pub fn new() -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: false,
            failure_message: String::new(),
        }
    }

    pub fn with_failure(message: impl Into<String>) -> Self {
        Self {
            order_counter: Arc::new(AtomicU64::new(1)),
            should_fail: true,
            failure_message: message.into(),
        }
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
            return Err(ExecutionError::OrderPlacement {
                reason: self.failure_message.clone(),
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
            return Err(ExecutionError::OrderPlacement {
                reason: self.failure_message.clone(),
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
    use super::*;

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
}
