//! Supervised inventory poller.
//!
//! [`InventoryMonitor`] is a long-running [`SupervisedTask`] that ticks
//! on a fixed interval and drives the underlying [`Poller`] to refresh
//! the inventory snapshot. Polling failures are transient (RPC blips,
//! vault contention) -- they are logged and swallowed so a hiccup never
//! halts the monitor; the supervisor restarts the task only if the
//! tick loop itself panics.

use std::sync::Arc;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::inventory::Poller;

#[derive(Clone)]
pub(crate) struct InventoryMonitor {
    pub(crate) poller: Arc<dyn Poller>,
    pub(crate) interval: Duration,
}

impl SupervisedTask for InventoryMonitor {
    async fn run(&mut self) -> TaskResult {
        info!("Inventory monitor started");

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if let Err(error) = self.poller.poll().await {
                warn!(target: "inventory", ?error, "Inventory polling failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    use crate::inventory::{Poller, PollerError};

    use super::*;

    /// Test poller that sends on `tx` each time it is polled and optionally
    /// returns an error. Using a channel lets the test deterministically
    /// observe each call under `start_paused = true`: when the test awaits
    /// `rx.recv()`, the tokio runtime auto-advances paused time to the next
    /// pending timer (the interval tick), driving the monitor forward one
    /// poll at a time.
    struct NotifyingPoller {
        tx: UnboundedSender<()>,
        fail: bool,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("forced poller failure")]
    struct ForcedFailure;

    #[async_trait]
    impl Poller for NotifyingPoller {
        async fn poll(&self) -> Result<(), PollerError> {
            self.tx.send(()).unwrap();

            if self.fail {
                Err(PollerError(Box::new(ForcedFailure)))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_polls_on_each_interval_tick() {
        let (tx, mut rx) = unbounded_channel();
        let mut monitor = InventoryMonitor {
            poller: Arc::new(NotifyingPoller { tx, fail: false }),
            interval: Duration::from_secs(10),
        };

        let handle = tokio::spawn(async move { monitor.run().await });

        // `interval` fires immediately on the first tick, then once per
        // `interval` thereafter. With paused time, each `recv` blocks until
        // the next timer fires, so observing four notifications proves the
        // monitor is polling once per tick.
        for tick in 0..4 {
            rx.recv()
                .await
                .unwrap_or_else(|| panic!("monitor failed to poll on tick {tick}"));
        }

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn run_keeps_ticking_after_poll_error() {
        let (tx, mut rx) = unbounded_channel();
        let mut monitor = InventoryMonitor {
            poller: Arc::new(NotifyingPoller { tx, fail: true }),
            interval: Duration::from_secs(10),
        };

        let handle = tokio::spawn(async move { monitor.run().await });

        // A failing poll must NOT halt the loop. If errors propagated, the
        // task would return after the first call and subsequent `recv`s
        // would block forever (under paused time, that becomes a deadlock).
        for tick in 0..3 {
            rx.recv()
                .await
                .unwrap_or_else(|| panic!("monitor stopped polling after error on tick {tick}"));
        }

        assert!(
            !handle.is_finished(),
            "monitor must keep running after a poll error"
        );

        handle.abort();
    }
}
