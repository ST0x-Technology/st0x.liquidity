//! Pause control for the USDC rebalancing driver: the two apalis workers
//! (`TransferUsdcToHedging`, `TransferUsdcToMarketMaking`) whose executions
//! advance a `UsdcRebalance` aggregate and spend the rebalancing wallets.
//!
//! Lets an operator HTTP handler quiesce the live driver, meaning every
//! execution already in flight has finished and no new one can start, before
//! it reads aggregate or on-chain state and then sends a command or a
//! transaction that a concurrent execution would otherwise race. The guard it
//! hands back resumes the driver on every exit path.
//!
//! The control is a reusable primitive for any operator operation that must
//! mutate while the rebalancer is live, not a one-off for its first users.
//! It deliberately does not touch the `usdc_in_progress` latch: that latch
//! only admits the creation of a new transfer by the trigger and is neither
//! read nor held by the workers that execute one.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

/// How long [`UsdcDriverPause::pause`] waits for in-flight executions to
/// finish before giving up. One execution drives a whole transfer attempt and
/// can run for as long as the per-attempt budget, so a pause requested while
/// a transfer is genuinely moving funds is refused rather than parking the
/// caller for that long. The operations that need a pause target a transfer
/// that is stuck or failed, where no execution is running and the pause
/// confirms at once.
const DRIVER_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// In-flight executions did not finish within [`DRIVER_QUIESCE_TIMEOUT`], so
/// the caller must not mutate state the live driver would race.
#[derive(Debug, thiserror::Error)]
#[error("the USDC rebalancing driver did not quiesce: a transfer is executing")]
pub(crate) struct DriverNotQuiesced;

/// Controller side of the pause, published to the API after startup.
pub(crate) struct UsdcDriverPause {
    pause: watch::Sender<bool>,
    in_flight: watch::Receiver<usize>,
    /// Serializes pausers so one guard's resume cannot free the driver while
    /// another pauser is still mutating; held for the guard's lifetime.
    serialize: Arc<Mutex<()>>,
}

impl UsdcDriverPause {
    /// Requests a pause and returns once the driver is quiesced: every
    /// execution in flight has finished and none can start. The guard resumes
    /// the driver when dropped, so a caller cannot forget to resume on an
    /// error or panic path.
    ///
    /// Pausers are serialized: a second caller waits until the first guard
    /// drops before it pauses, so overlapping operator operations cannot resume
    /// the driver out from under one another.
    ///
    /// Returns [`DriverNotQuiesced`] when executions are still in flight after
    /// [`DRIVER_QUIESCE_TIMEOUT`], leaving the driver running.
    pub(crate) async fn pause(&self) -> Result<UsdcDriverPauseGuard, DriverNotQuiesced> {
        let permit = Arc::clone(&self.serialize).lock_owned().await;

        // Raise the flag first so no new execution passes the gate, then wait
        // for the ones already past it to finish.
        let _ = self.pause.send(true);
        let mut in_flight = self.in_flight.clone();
        let drained = tokio::time::timeout(DRIVER_QUIESCE_TIMEOUT, async {
            while *in_flight.borrow_and_update() > 0 {
                in_flight.changed().await.map_err(|_| ())?;
            }
            Ok::<(), ()>(())
        })
        .await;

        if matches!(drained, Ok(Ok(()))) {
            Ok(UsdcDriverPauseGuard {
                pause: self.pause.clone(),
                _permit: permit,
            })
        } else {
            // Not quiesced: lower the flag so the driver is not left paused
            // without a guard. The permit drops here, freeing the next pauser.
            let _ = self.pause.send(false);
            Err(DriverNotQuiesced)
        }
    }
}

/// Resumes the driver when dropped. Resuming is a plain signal, so it runs
/// from `Drop` on the success, error, and panic paths alike.
pub(crate) struct UsdcDriverPauseGuard {
    pause: watch::Sender<bool>,
    /// Held so the serialize mutex is released, unblocking the next pauser,
    /// only after this guard drops and the driver resumes.
    _permit: OwnedMutexGuard<()>,
}

impl Drop for UsdcDriverPauseGuard {
    fn drop(&mut self) {
        let _ = self.pause.send(false);
    }
}

/// Driver side of the pause, one clone per worker. A worker calls
/// [`Self::enter`] at the top of each execution and holds the returned
/// [`InFlight`] token for the execution's lifetime; the trigger consults
/// [`Self::is_paused`] before it enqueues or sweeps.
#[derive(Clone)]
pub(crate) struct UsdcDriverGate {
    pause: watch::Receiver<bool>,
    in_flight: watch::Sender<usize>,
}

impl UsdcDriverGate {
    /// Parks while a pause is requested, then claims an in-flight slot for the
    /// execution about to run. Dropping the token releases the slot.
    ///
    /// The slot is claimed and the flag re-read afterwards: a pause requested
    /// between the flag check and the claim then sees the claim and waits,
    /// while this side sees the flag, releases the claim, and parks. Without
    /// that second read an execution could slip past a pause the controller
    /// had just confirmed.
    pub(crate) async fn enter(&self) -> InFlight {
        let mut pause = self.pause.clone();
        loop {
            while *pause.borrow_and_update() {
                // A dropped controller can only leave the flag lowered (every
                // guard lowers it on drop), so run unpaused.
                if pause.changed().await.is_err() {
                    break;
                }
            }

            self.in_flight.send_modify(|count| *count += 1);
            if !*pause.borrow() {
                return InFlight {
                    in_flight: self.in_flight.clone(),
                };
            }
            self.in_flight.send_modify(|count| *count -= 1);
        }
    }

    /// Whether a pause is requested or held. The trigger checks this without
    /// blocking so it neither enqueues a transfer nor sweeps while an operator
    /// operation holds the driver quiesced.
    pub(crate) fn is_paused(&self) -> bool {
        *self.pause.borrow()
    }

    /// A gate with no controller: never parks. For tests that exercise a
    /// worker without an operator pause in play.
    #[cfg(test)]
    pub(crate) fn unpaused() -> Self {
        usdc_driver_pause().1
    }
}

/// An execution's claim on the driver. Held for the execution's lifetime so
/// [`UsdcDriverPause::pause`] can wait for it to finish.
pub(crate) struct InFlight {
    in_flight: watch::Sender<usize>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.in_flight.send_modify(|count| *count -= 1);
    }
}

/// Builds a linked controller/driver pair sharing two watch channels: `pause`
/// (controller to driver) and `in_flight` (driver to controller).
pub(crate) fn usdc_driver_pause() -> (UsdcDriverPause, UsdcDriverGate) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (in_flight_tx, in_flight_rx) = watch::channel(0);
    (
        UsdcDriverPause {
            pause: pause_tx,
            in_flight: in_flight_rx,
            serialize: Arc::new(Mutex::new(())),
        },
        UsdcDriverGate {
            pause: pause_rx,
            in_flight: in_flight_tx,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    /// A worker loop shaped like the apalis workers: claim the gate, run an
    /// execution, release, idle until the next row.
    fn spawn_worker(
        gate: UsdcDriverGate,
        executions: Arc<AtomicUsize>,
        mid_execution: Arc<AtomicBool>,
        execution: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let in_flight = gate.enter().await;
                mid_execution.store(true, Ordering::SeqCst);
                tokio::time::sleep(execution).await;
                mid_execution.store(false, Ordering::SeqCst);
                executions.fetch_add(1, Ordering::SeqCst);
                drop(in_flight);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }

    /// A pause must wait for the execution already running to finish, hold
    /// off every new execution on both workers while held, and let the driver
    /// run again once the guard drops.
    #[tokio::test(start_paused = true)]
    async fn pause_drains_in_flight_executions_and_resume_continues() {
        let (control, gate) = usdc_driver_pause();
        let executions = Arc::new(AtomicUsize::new(0));
        let mid_execution = Arc::new(AtomicBool::new(false));
        let hedging = spawn_worker(
            gate.clone(),
            executions.clone(),
            mid_execution.clone(),
            Duration::from_secs(1),
        );
        let market_making = spawn_worker(
            gate,
            executions.clone(),
            Arc::new(AtomicBool::new(false)),
            Duration::from_secs(1),
        );

        // Land inside the hedging execution, then request the pause.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            mid_execution.load(Ordering::SeqCst),
            "expected an execution in flight"
        );

        let guard = control.pause().await.unwrap();
        assert!(
            !mid_execution.load(Ordering::SeqCst),
            "pause must wait for the in-flight execution to finish"
        );

        let executions_when_paused = executions.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            executions_when_paused,
            "no execution may run while paused"
        );

        drop(guard);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            executions.load(Ordering::SeqCst) > executions_when_paused,
            "the driver must resume executing after the guard drops"
        );

        hedging.abort();
        market_making.abort();
    }

    /// An execution that outlasts the quiesce window is a transfer genuinely
    /// moving funds: the pause is refused and the driver left running, so a
    /// later request can succeed once the execution ends.
    #[tokio::test(start_paused = true)]
    async fn pause_refuses_while_an_execution_outlasts_the_quiesce_window() {
        let (control, gate) = usdc_driver_pause();
        // One execution that outlasts the quiesce window; the worker then idles.
        let worker_gate = gate.clone();
        let worker = tokio::spawn(async move {
            let _in_flight = worker_gate.enter().await;
            tokio::time::sleep(DRIVER_QUIESCE_TIMEOUT * 2).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(control.pause().await.is_err());
        assert!(
            !gate.is_paused(),
            "a refused pause must not leave the driver flagged paused"
        );

        // Once the long execution ends, the driver is pausable again.
        worker.await.unwrap();
        assert!(control.pause().await.is_ok());
    }

    /// A second pauser waits for the first guard to drop rather than sharing
    /// the quiesced window, so one operation's resume cannot free the driver
    /// under another.
    #[tokio::test(start_paused = true)]
    async fn pausers_are_serialized_behind_the_held_guard() {
        let (control, gate) = usdc_driver_pause();
        let control = Arc::new(control);

        let first = control.pause().await.unwrap();
        assert!(gate.is_paused());

        let second_control = Arc::clone(&control);
        let second = tokio::spawn(async move { second_control.pause().await.map(|_| ()) });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !second.is_finished(),
            "the second pauser must wait for the first guard"
        );

        drop(first);
        second.await.unwrap().unwrap();
    }

    /// The flag the trigger consults tracks the guard exactly: raised while a
    /// guard is held, lowered once it drops.
    #[tokio::test]
    async fn is_paused_tracks_the_guard() {
        let (control, gate) = usdc_driver_pause();
        assert!(!gate.is_paused());

        let guard = control.pause().await.unwrap();
        assert!(gate.is_paused());

        drop(guard);
        assert!(!gate.is_paused());
    }
}
