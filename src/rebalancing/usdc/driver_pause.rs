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
    /// [`DRIVER_QUIESCE_TIMEOUT`], leaving the driver running. A caller that
    /// drops this future mid wait (a cancelled request) likewise leaves the
    /// driver running: the flag is lowered on the way out.
    pub(crate) async fn pause(&self) -> Result<UsdcDriverPauseGuard, DriverNotQuiesced> {
        let permit = Arc::clone(&self.serialize).lock_owned().await;

        // Raise the flag first so no new execution passes the gate, then wait
        // for the ones already past it to finish. Until the guard exists, the
        // flag is owned by `lower_on_exit`, which lowers it on every path that
        // leaves without a guard: refusal, and a dropped future.
        let _ = self.pause.send(true);
        let mut lower_on_exit = LowerFlagOnDrop {
            pause: self.pause.clone(),
            armed: true,
        };
        let mut in_flight = self.in_flight.clone();
        let drained = tokio::time::timeout(DRIVER_QUIESCE_TIMEOUT, async {
            while *in_flight.borrow_and_update() > 0 {
                in_flight.changed().await.map_err(|_| ())?;
            }
            Ok::<(), ()>(())
        })
        .await;

        match drained {
            Ok(Ok(())) => {
                // The guard owns the flag from here.
                lower_on_exit.armed = false;
                Ok(UsdcDriverPauseGuard {
                    pause: self.pause.clone(),
                    _permit: permit,
                })
            }
            // Executions still in flight after the window, or every gate gone
            // while one was still counted: not quiesced. `lower_on_exit` and
            // the permit drop here, resuming the driver and freeing the next
            // pauser.
            Ok(Err(())) | Err(_) => Err(DriverNotQuiesced),
        }
    }
}

/// Lowers the pause flag when dropped while armed, so a `pause()` that exits
/// without producing a guard, whether refused or cancelled, cannot leave the
/// driver parked.
struct LowerFlagOnDrop {
    pause: watch::Sender<bool>,
    armed: bool,
}

impl Drop for LowerFlagOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.pause.send(false);
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
/// [`InFlight`] token for the execution's lifetime; the trigger's check and
/// sweep claim through [`Self::try_enter`], which never parks.
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

    /// Claims an in-flight slot without parking: `None` when a pause is
    /// requested or held, so a caller that must not block, like the trigger's
    /// check or sweep running inline on a reactor, skips its work instead.
    /// A returned token makes a pause wait for that work to finish, exactly
    /// as it waits for a worker execution.
    ///
    /// Claims then re-reads the flag for the same reason [`Self::enter`] does.
    pub(crate) fn try_enter(&self) -> Option<InFlight> {
        if *self.pause.borrow() {
            return None;
        }

        self.in_flight.send_modify(|count| *count += 1);
        if *self.pause.borrow() {
            self.in_flight.send_modify(|count| *count -= 1);
            return None;
        }

        Some(InFlight {
            in_flight: self.in_flight.clone(),
        })
    }

    /// Test hook: whether a pause is requested or held.
    #[cfg(test)]
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
        assert!(matches!(control.pause().await, Err(DriverNotQuiesced)));
        assert!(
            !gate.is_paused(),
            "a refused pause must not leave the driver flagged paused"
        );

        // Once the long execution ends, the driver is pausable again.
        worker.await.unwrap();
        let guard = control.pause().await.unwrap();
        assert!(gate.is_paused(), "a granted pause must park the driver");
        drop(guard);
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

    /// A pause whose future is dropped mid wait (a cancelled request) must
    /// lower the flag on the way out: the driver is never left parked with no
    /// guard to resume it.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_pause_does_not_leave_the_driver_parked() {
        let (control, gate) = usdc_driver_pause();
        let control = Arc::new(control);
        let executing = gate.enter().await;

        let pauser_control = Arc::clone(&control);
        let pauser = tokio::spawn(async move { pauser_control.pause().await.map(|_| ()) });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            gate.is_paused(),
            "the pause request must have raised the flag"
        );

        pauser.abort();
        let _ = pauser.await;
        assert!(
            !gate.is_paused(),
            "dropping the pause mid wait must lower the flag"
        );

        // The driver is fully usable afterwards: once the execution ends, a
        // new pause succeeds.
        drop(executing);
        let guard = control.pause().await.unwrap();
        drop(guard);
    }

    /// `try_enter` never parks: it claims while the driver runs, so a pause
    /// waits for that work, and refuses while a pause is requested or held.
    #[tokio::test(start_paused = true)]
    async fn try_enter_claims_unpaused_and_refuses_while_paused() {
        let (control, gate) = usdc_driver_pause();
        let control = Arc::new(control);

        let claim = gate
            .try_enter()
            .expect("an unpaused driver must be claimable");
        let pauser_control = Arc::clone(&control);
        let pauser = tokio::spawn(async move { pauser_control.pause().await.map(|_| ()) });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !pauser.is_finished(),
            "a pause must wait for a claimed sweep or check to finish"
        );
        assert!(
            gate.try_enter().is_none(),
            "no new claim may start once a pause is requested"
        );

        drop(claim);
        pauser.await.unwrap().unwrap();
    }
}
