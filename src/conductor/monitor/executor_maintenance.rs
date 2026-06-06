//! Supervised executor maintenance loop.
//!
//! [`ExecutorMaintenance`] is a long-running [`SupervisedTask`] that ticks
//! at the executor's [`Executor::maintenance_interval`] and invokes
//! [`Executor::maintenance_tick`]. Tick errors are transient (token refresh
//! failures, transient broker connectivity) -- they are logged and swallowed
//! so a hiccup never halts the loop; the supervisor restarts the task only
//! if the tick loop itself panics.
//!
//! The previous implementation spawned a raw `tokio::spawn` from inside the
//! executor and returned the `JoinHandle` to the conductor. That handle was
//! aborted on shutdown but never restarted: if token refresh died, the
//! executor silently stopped working. Wrapping the tick in a supervised
//! task closes that gap.

use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use st0x_execution::Executor;

#[derive(Clone)]
pub(crate) struct ExecutorMaintenance<Exec> {
    executor: Exec,
    interval: Duration,
}

impl<Exec: Executor + Clone> ExecutorMaintenance<Exec> {
    pub(crate) fn new(executor: Exec, interval: Duration) -> Self {
        Self { executor, interval }
    }
}

impl<Exec: Executor + Clone> SupervisedTask for ExecutorMaintenance<Exec> {
    async fn run(&mut self) -> TaskResult {
        info!(
            interval_secs = self.interval.as_secs(),
            "Executor maintenance started"
        );

        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            if let Err(error) = self.executor.maintenance_tick().await {
                warn!(?error, "Executor maintenance tick failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    use st0x_execution::{
        CounterTradePreflight, ExecutionError, InventoryResult, LimitOrder, MarketOrder,
        OrderPlacement, OrderState, SupportedExecutor, TryIntoExecutor,
    };

    use super::*;

    /// Executor that pulses on `tx` for every maintenance tick. `fail`
    /// toggles whether tick returns an error so the test can prove the
    /// loop survives transient failures.
    #[derive(Clone)]
    struct NotifyingExecutor {
        tx: UnboundedSender<()>,
        fail: Arc<AtomicBool>,
    }

    #[derive(Debug, Clone, Default)]
    struct NotifyingExecutorCtx;

    #[async_trait]
    impl TryIntoExecutor for NotifyingExecutorCtx {
        type Executor = NotifyingExecutor;
        async fn try_into_executor(
            self,
        ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
            unreachable!("test executor is constructed directly")
        }
    }

    #[async_trait]
    impl Executor for NotifyingExecutor {
        type Error = ExecutionError;
        type OrderId = String;
        type Ctx = NotifyingExecutorCtx;

        async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
            unreachable!("test executor is constructed directly")
        }

        async fn is_market_open(&self) -> Result<bool, Self::Error> {
            Ok(true)
        }

        async fn place_market_order(
            &self,
            _order: MarketOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            unreachable!("not used in maintenance tests")
        }

        async fn place_limit_order(
            &self,
            _order: LimitOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            unreachable!("not used in maintenance tests")
        }

        async fn cancel_order(
            &self,
            _order_id: &Self::OrderId,
        ) -> Result<st0x_execution::CancellationOutcome, Self::Error> {
            unreachable!("not used in maintenance tests")
        }

        async fn get_order_status(
            &self,
            _order_id: &Self::OrderId,
        ) -> Result<OrderState, Self::Error> {
            unreachable!("not used in maintenance tests")
        }

        fn to_supported_executor(&self) -> SupportedExecutor {
            SupportedExecutor::DryRun
        }

        fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
            Ok(order_id_str.to_string())
        }

        async fn maintenance_tick(&self) -> Result<(), Self::Error> {
            self.tx.send(()).unwrap();

            if self.fail.load(Ordering::SeqCst) {
                Err(ExecutionError::MockFailure {
                    message: "forced maintenance failure".to_string(),
                })
            } else {
                Ok(())
            }
        }

        async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
            Ok(InventoryResult::Unimplemented)
        }

        async fn preflight_counter_trade(
            &self,
            _order: MarketOrder,
        ) -> Result<CounterTradePreflight, Self::Error> {
            Ok(CounterTradePreflight::Allowed { reservation: None })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ticks_at_configured_interval() {
        let (tx, mut rx) = unbounded_channel();
        let mut maintenance = ExecutorMaintenance::new(
            NotifyingExecutor {
                tx,
                fail: Arc::new(AtomicBool::new(false)),
            },
            Duration::from_secs(10),
        );

        let handle = tokio::spawn(async move { maintenance.run().await });

        for tick in 0..4 {
            rx.recv()
                .await
                .unwrap_or_else(|| panic!("maintenance failed to tick on iteration {tick}"));
        }

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_ticking_after_tick_error() {
        let (tx, mut rx) = unbounded_channel();
        let mut maintenance = ExecutorMaintenance::new(
            NotifyingExecutor {
                tx,
                fail: Arc::new(AtomicBool::new(true)),
            },
            Duration::from_secs(10),
        );

        let handle = tokio::spawn(async move { maintenance.run().await });

        for tick in 0..3 {
            rx.recv()
                .await
                .unwrap_or_else(|| panic!("maintenance stopped ticking after error on {tick}"));
        }

        assert!(
            !handle.is_finished(),
            "maintenance must keep running after a tick error"
        );

        handle.abort();
    }
}
