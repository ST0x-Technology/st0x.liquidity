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

use std::time::Duration;

use crate::quiesce::{self, NotQuiesced, Quiesce, QuiesceGate, QuiesceGuard};

/// How long [`UsdcDriverPause::pause`] waits for in-flight executions to
/// finish before giving up. One execution drives a whole transfer attempt and
/// can run for as long as the per-attempt budget, so a pause requested while
/// a transfer is genuinely moving funds is refused rather than parking the
/// caller for that long. The operations that need a pause target a transfer
/// that is stuck or failed, where no execution is running and the pause
/// confirms at once. Keep this well below the operator client's 30-second
/// request timeout so a refusal reaches the caller and a late grant leaves
/// time for the recovery handler to finish.
const DRIVER_QUIESCE_TIMEOUT: Duration = Duration::from_secs(5);

/// In-flight executions did not finish within [`DRIVER_QUIESCE_TIMEOUT`], so
/// the caller must not mutate state the live driver would race.
#[derive(Debug, thiserror::Error)]
#[error("the USDC rebalancing driver did not quiesce: a transfer is executing")]
pub(crate) struct DriverNotQuiesced;

/// Controller side of the pause, published to the API after startup. A thin
/// USDC-domain wrapper over the shared [`Quiesce`] primitive: it maps the
/// generic refusal to [`DriverNotQuiesced`] and fixes the quiesce timeout to
/// [`DRIVER_QUIESCE_TIMEOUT`]. See [`crate::quiesce`] for the mechanism.
pub(crate) struct UsdcDriverPause(Quiesce);

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
        self.0
            .pause()
            .await
            .map(|guard| UsdcDriverPauseGuard { _inner: guard })
            .map_err(|NotQuiesced| DriverNotQuiesced)
    }
}

/// Resumes the driver when dropped, via the inner [`QuiesceGuard`]'s own `Drop`.
pub(crate) struct UsdcDriverPauseGuard {
    _inner: QuiesceGuard,
}

/// Driver side of the pause, one clone per worker. A worker and the queued
/// trigger check call [`Self::enter`] at the top of each execution and hold the
/// returned [`InFlight`] token for the execution's lifetime; the inline trigger
/// sweep claims through [`Self::try_enter`], which never parks.
#[derive(Clone)]
pub(crate) struct UsdcDriverGate(QuiesceGate);

impl UsdcDriverGate {
    /// Parks while a pause is requested, then claims an in-flight slot for the
    /// execution about to run. Dropping the token releases the slot.
    pub(crate) async fn enter(&self) -> InFlight {
        InFlight {
            _inner: self.0.enter().await,
        }
    }

    /// Claims an in-flight slot without parking: `None` when a pause is
    /// requested or held, so a caller that must not block, like the trigger's
    /// inline sweep, skips its work instead. A returned token makes a pause
    /// wait for that work to finish, exactly as it waits for a worker
    /// execution.
    ///
    /// Claims then re-reads the flag for the same reason [`Self::enter`] does.
    pub(crate) fn try_enter(&self) -> Option<InFlight> {
        self.0.try_enter().map(|token| InFlight { _inner: token })
    }

    /// Test hook: whether a pause is requested or held.
    #[cfg(test)]
    pub(crate) fn is_paused(&self) -> bool {
        self.0.is_paused()
    }

    /// A gate with no controller: never parks. For tests that exercise a
    /// worker without an operator pause in play.
    #[cfg(test)]
    pub(crate) fn unpaused() -> Self {
        usdc_driver_pause().1
    }
}

/// An execution's claim on the driver. Held for the execution's lifetime so
/// [`UsdcDriverPause::pause`] can wait for it to finish, via the inner token's
/// `Drop`.
pub(crate) struct InFlight {
    _inner: quiesce::InFlight,
}

/// Builds a linked controller/driver pair over a [`Quiesce`] fixed to the USDC
/// [`DRIVER_QUIESCE_TIMEOUT`].
pub(crate) fn usdc_driver_pause() -> (UsdcDriverPause, UsdcDriverGate) {
    let (control, gate) = quiesce::quiesce(DRIVER_QUIESCE_TIMEOUT);
    (UsdcDriverPause(control), UsdcDriverGate(gate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The USDC wrapper maps the generic refusal to [`DriverNotQuiesced`] and
    /// applies [`DRIVER_QUIESCE_TIMEOUT`]: an execution that outlasts the window
    /// is a transfer genuinely moving funds, so the pause is refused and the
    /// driver left running, and a later request succeeds once it ends. The
    /// generic quiesce mechanics are covered in [`crate::quiesce`].
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
}
