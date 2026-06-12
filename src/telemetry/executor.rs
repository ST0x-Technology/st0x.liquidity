//! Latency/error capture decorator over any [`Executor`].
//!
//! Wraps the hedge broker executor once at conductor startup, so every hedge
//! consumer cloning it (order placement, status polling, inventory polling,
//! maintenance) emits dependency-call telemetry without knowing about it. The
//! rebalancer builds its own broker handle and is not covered here. Lives in
//! the main crate: the execution crate stays independent of the telemetry
//! store.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;

use st0x_execution::{
    CounterTradePreflight, Executor, InventoryResult, MarketOrder, OrderPlacement, OrderState,
    SupportedExecutor,
};

use super::{Dependency, DependencyCallSample, TelemetrySender, scrub_secrets};

/// An [`Executor`] whose remote calls are timed and recorded.
///
/// Local methods (`to_supported_executor`, `parse_order_id`,
/// `maintenance_interval`) delegate without recording: they never leave the
/// process, so timing them would only dilute the dependency signal.
#[derive(Debug, Clone)]
pub(crate) struct InstrumentedExecutor<Inner> {
    inner: Inner,
    telemetry: TelemetrySender,
}

impl<Inner> InstrumentedExecutor<Inner> {
    pub(crate) fn new(inner: Inner, telemetry: TelemetrySender) -> Self {
        Self { inner, telemetry }
    }
}

impl<Inner: Executor> InstrumentedExecutor<Inner> {
    fn record<Value>(
        &self,
        operation: &'static str,
        started: Instant,
        result: &Result<Value, Inner::Error>,
    ) {
        self.telemetry.record(DependencyCallSample {
            recorded_at: Utc::now(),
            dependency: Dependency::Broker,
            operation: operation.into(),
            duration: started.elapsed(),
            // Scrub any URL credentials a broker error might surface before
            // persisting, matching the RPC path.
            error: result
                .as_ref()
                .err()
                .map(|error| scrub_secrets(&error.to_string())),
        });
    }
}

#[async_trait]
impl<Inner: Executor + Clone> Executor for InstrumentedExecutor<Inner> {
    type Error = Inner::Error;
    type OrderId = Inner::OrderId;
    type Ctx = Inner::Ctx;

    /// Builds the inner executor with a disabled telemetry sender. Dead in
    /// practice: the conductor wraps an already-constructed executor and
    /// supplies a connected sender; nothing constructs the wrapper through
    /// its context.
    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: Inner::try_from_ctx(ctx).await?,
            telemetry: TelemetrySender::disabled(),
        })
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        let started = Instant::now();
        let result = self.inner.is_market_open().await;
        self.record("is_market_open", started, &result);
        result
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        let started = Instant::now();
        let result = self.inner.place_market_order(order).await;
        self.record("place_market_order", started, &result);
        result
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        let started = Instant::now();
        let result = self.inner.get_order_status(order_id).await;
        self.record("get_order_status", started, &result);
        result
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        self.inner.to_supported_executor()
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        self.inner.parse_order_id(order_id_str)
    }

    fn maintenance_interval(&self) -> Option<Duration> {
        self.inner.maintenance_interval()
    }

    async fn maintenance_tick(&self) -> Result<(), Self::Error> {
        let started = Instant::now();
        let result = self.inner.maintenance_tick().await;
        self.record("maintenance_tick", started, &result);
        result
    }

    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
        let started = Instant::now();
        let result = self.inner.get_inventory().await;
        self.record("get_inventory", started, &result);
        result
    }

    async fn preflight_counter_trade(
        &self,
        order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        let started = Instant::now();
        let result = self.inner.preflight_counter_trade(order).await;
        self.record("preflight_counter_trade", started, &result);
        result
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use st0x_execution::{
        ClientOrderId, Direction, FractionalShares, MockExecutor, Positive, Symbol,
    };
    use st0x_float_macro::float;

    use crate::telemetry::spawn_dependency_call_writer;
    use crate::test_utils::setup_test_db;

    use super::*;

    fn market_order() -> MarketOrder {
        MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        }
    }

    #[tokio::test]
    async fn records_broker_calls_with_operation_labels() {
        let pool = setup_test_db().await;
        let (sender, receiver) = TelemetrySender::channel();
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);
        let executor = InstrumentedExecutor::new(MockExecutor::new(), sender.clone());

        executor.is_market_open().await.unwrap();
        executor.place_market_order(market_order()).await.unwrap();

        drop(executor);
        drop(sender);
        writer.await.unwrap();

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT dependency, operation, outcome \
             FROM dependency_call_samples ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "broker");
        assert_eq!(rows[0].1, "is_market_open");
        assert_eq!(rows[0].2, "ok");
        assert_eq!(rows[1].0, "broker");
        assert_eq!(rows[1].1, "place_market_order");
        assert_eq!(rows[1].2, "ok");
    }

    #[tokio::test]
    async fn records_failed_calls_as_errors() {
        let pool = setup_test_db().await;
        let (sender, receiver) = TelemetrySender::channel();
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);
        let executor =
            InstrumentedExecutor::new(MockExecutor::with_failure("broker down"), sender.clone());

        executor
            .place_market_order(market_order())
            .await
            .unwrap_err();

        drop(executor);
        drop(sender);
        writer.await.unwrap();

        let (outcome, error): (String, Option<String>) =
            sqlx::query_as("SELECT outcome, error FROM dependency_call_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(outcome, "error");
        assert!(
            error.unwrap().contains("broker down"),
            "failure message must be recorded"
        );
    }

    #[tokio::test]
    async fn delegates_local_methods_without_recording() {
        let (sender, mut receiver) = TelemetrySender::channel();
        let executor = InstrumentedExecutor::new(MockExecutor::new(), sender);

        assert_eq!(
            executor.to_supported_executor(),
            MockExecutor::new().to_supported_executor()
        );
        executor.parse_order_id("order-1").unwrap();
        assert_eq!(executor.maintenance_interval(), None);

        drop(executor);
        assert!(
            matches!(receiver.try_recv().unwrap_err(), TryRecvError::Disconnected),
            "local methods must not emit samples"
        );
    }
}
