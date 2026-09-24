//! Supervised inventory poller.
//!
//! [`InventoryMonitor`] is a long-running [`SupervisedTask`] that ticks on a
//! fixed interval and drives the underlying [`Poller`] to refresh the inventory
//! snapshot. Each poll holds the projection gate because snapshot processing can
//! synchronously fail timed-out operations and update their projections. Polling
//! failures are transient (RPC blips, vault contention) -- they are logged and
//! swallowed so a hiccup never halts the monitor; the supervisor restarts the
//! task only if the tick loop itself panics.

use std::sync::Arc;
use std::time::Duration;

use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::conductor::projection_pause::enter_projection_gate;
use crate::inventory::Poller;
use crate::quiesce;

#[derive(Clone)]
pub(crate) struct InventoryMonitor {
    pub(crate) poller: Arc<dyn Poller>,
    pub(crate) interval: Duration,
}

impl InventoryMonitor {
    async fn poll_once(&self, projection_slot: Option<quiesce::InFlight>) {
        let result = self.poller.poll().await;
        drop(projection_slot);

        if let Err(error) = result {
            warn!(target: "inventory", ?error, "Inventory polling failed");
        }
    }
}

impl SupervisedTask for InventoryMonitor {
    async fn run(&mut self) -> TaskResult {
        info!("Inventory monitor started");

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            // Snapshot processing runs its reactors inline and can write failure
            // projections for timed-out operations. Keep the slot through the
            // whole poll so a rebuild drains those writes before replaying rows.
            let projection_slot = enter_projection_gate().await;
            self.poll_once(projection_slot).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tokio::sync::Notify;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
    use tokio::time::timeout;

    use super::*;
    use crate::inventory::{Poller, PollerError};

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

    struct BlockingPoller {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl Poller for BlockingPoller {
        async fn poll(&self) -> Result<(), PollerError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn poll_holds_projection_slot_until_snapshot_processing_finishes() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let poll_started = started.notified();
        let monitor = InventoryMonitor {
            poller: Arc::new(BlockingPoller {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
            interval: Duration::from_secs(10),
        };
        let (control, gate) = quiesce::quiesce(Duration::from_secs(1));

        let poll = tokio::spawn(async move {
            let projection_slot = Some(gate.enter().await);
            monitor.poll_once(projection_slot).await;
        });
        poll_started.await;

        match timeout(Duration::from_millis(20), control.pause()).await {
            Err(_) => {}
            Ok(_) => panic!("the pause must wait for snapshot processing to finish"),
        }

        release.notify_one();
        poll.await.expect("the inventory poll must finish");

        let guard = control
            .pause()
            .await
            .expect("the pause must succeed after snapshot processing finishes");
        drop(guard);
    }

    /// Drives the real `run` loop through the process global projection gate
    /// that `init_projection_gate` wires at startup: while a rebuild
    /// holds the pause the monitor must not poll, and it polls once the pause
    /// is released. Pausing the global gate is process scoped, so this relies
    /// on nextest running each test in its own process.
    #[tokio::test]
    async fn run_parks_on_the_global_projection_gate_while_a_rebuild_is_paused() {
        let rebuild = crate::conductor::projection_pause::pause_projection_gate_for_test().await;

        let (tx, mut rx) = unbounded_channel();
        let mut monitor = InventoryMonitor {
            poller: Arc::new(NotifyingPoller { tx, fail: false }),
            interval: Duration::from_secs(10),
        };
        let handle = tokio::spawn(async move { monitor.run().await });

        assert!(
            timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "the monitor must not poll while a rebuild holds the projection gate"
        );

        drop(rebuild);
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the monitor must poll once the rebuild releases the gate")
            .expect("the poller channel must stay open");

        handle.abort();
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
