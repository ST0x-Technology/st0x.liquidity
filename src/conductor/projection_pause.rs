//! Projection-maintenance pause: quiesces every projection writer -- the apalis
//! workers and the operator write routes -- so a rebuild can DELETE and replay a
//! materialized view without a live projection write racing in between.
//!
//! Event-sorcery folds projections synchronously inside `Store::send`, so gating
//! the send callers gates the projection writes. The two caller surfaces are the
//! generic apalis handler [`work`](super::job::work) (every worker) and the
//! operator HTTP write routes.
//!
//! A thin wrapper over the shared [`Quiesce`](crate::quiesce) primitive. The
//! worker gate is process-global: `work` is built through the worker macros with
//! no seam to thread per-worker data, and there is exactly one conductor per
//! process. The controller is held by the rebuild route; the route surface reads
//! its gate from `AppState` so route tests stay isolated from the global.

use std::sync::OnceLock;
use std::time::Duration;

use crate::quiesce::{self, NotQuiesced, Quiesce, QuiesceGate, QuiesceGuard};

/// How long a rebuild waits for in-flight projection writes to drain before
/// refusing. Coarse: a write is gated for the whole job or route that emits it,
/// so a long-running job holds the gate for its duration. A rebuild is a rare
/// operator action taken at a quiet moment, so refusing while a long job runs
/// and asking the operator to retry is acceptable.
const PROJECTION_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global worker gate, read by the generic apalis handler
/// [`work`](super::job::work). Set once at conductor startup, before any worker
/// is built, by [`init_projection_maintenance`].
static PROJECTION_GATE: OnceLock<QuiesceGate> = OnceLock::new();

/// Projection writers did not quiesce within [`PROJECTION_QUIESCE_TIMEOUT`], so
/// a rebuild is refused rather than allowed to race a live projection write.
#[derive(Debug, thiserror::Error)]
#[error("projection writers did not quiesce: a job or write is in flight")]
pub(crate) struct ProjectionBusy;

/// Controller side, held by the rebuild route. Pausing quiesces every gated
/// projection writer -- workers and routes -- for the guard's lifetime.
pub(crate) struct ProjectionMaintenance(Quiesce);

impl ProjectionMaintenance {
    /// Requests a pause and returns once every gated projection writer has
    /// drained and none can start. The guard resumes them when dropped, so a
    /// caller cannot forget to resume on an error or panic path. Returns
    /// [`ProjectionBusy`] when writers are still in flight after
    /// [`PROJECTION_QUIESCE_TIMEOUT`], leaving them running.
    // Consumed by the `rebuild_materialized_view` route in
    // `rai-2248-view-cctp-recovery`, restacked on top of this branch (RAI-2436);
    // remove this allow when that route lands the `pause()` call site.
    #[allow(dead_code)]
    pub(crate) async fn pause(&self) -> Result<ProjectionMaintenanceGuard, ProjectionBusy> {
        self.0
            .pause()
            .await
            .map(|guard| ProjectionMaintenanceGuard { _inner: guard })
            .map_err(|NotQuiesced| ProjectionBusy)
    }
}

/// Resumes the gated writers when dropped, via the inner [`QuiesceGuard`]'s
/// `Drop`.
pub(crate) struct ProjectionMaintenanceGuard {
    _inner: QuiesceGuard,
}

/// Builds the projection-maintenance controller and publishes its worker gate to
/// the process global that [`work`](super::job::work) reads. Called once at
/// conductor startup. The returned controller is held on the recovery handle for
/// the rebuild route to pause through.
///
/// A second call (a second conductor in one test process) keeps the first gate;
/// harmless because the production `work` is the sole global reader and there is
/// one conductor per process.
pub(crate) fn init_projection_maintenance() -> ProjectionMaintenance {
    let (control, gate) = quiesce::quiesce(PROJECTION_QUIESCE_TIMEOUT);
    let _ = PROJECTION_GATE.set(gate);
    ProjectionMaintenance(control)
}

/// Claims a projection-write slot for the generic apalis handler
/// [`work`](super::job::work), parking while a rebuild is paused. `None` before
/// startup wiring -- a bare test that never calls [`init_projection_maintenance`]
/// runs ungated -- so the caller holds the slot and drops it when the job
/// finishes. Production only: the test-support handler does not gate, so a global
/// gate one test set can never park another test's workers.
#[cfg(not(feature = "test-support"))]
pub(crate) async fn enter_projection_gate() -> Option<quiesce::InFlight> {
    match PROJECTION_GATE.get() {
        Some(gate) => Some(gate.enter().await),
        None => None,
    }
}

#[cfg(test)]
impl ProjectionMaintenance {
    /// A controller wired to a fresh, non-global gate, so a test drives the pause
    /// against its own writers without touching the process-global
    /// [`PROJECTION_GATE`] that `work` reads.
    fn with_gate_for_test() -> (Self, QuiesceGate) {
        let (control, gate) = quiesce::quiesce(PROJECTION_QUIESCE_TIMEOUT);
        (Self(control), gate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The rebuild race: a projection write already in flight must drain before a
    /// pause is granted, and a new writer cannot claim the gate while the pause
    /// is held. Proves the controller serializes a rebuild against live writers.
    // The join handle holds the pause guard as its output and is used twice (the
    // `is_finished` poll and the `await`), which the nursery lint misreads as a
    // single-use temporary.
    #[allow(clippy::significant_drop_tightening)]
    #[tokio::test]
    async fn pause_drains_an_in_flight_writer_then_blocks_new_ones() {
        let (control, gate) = ProjectionMaintenance::with_gate_for_test();
        let writing = gate.enter().await;

        // The pause cannot complete while a write is in flight. The guard owns
        // watch-sender clones, not a borrow of `control`, so it stays valid after
        // the spawned task drops `control`.
        let pause = tokio::spawn(async move { control.pause().await.ok() });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !pause.is_finished(),
            "the pause must wait for the in-flight write to drain"
        );

        // Draining the write lets the pause complete and hold the guard.
        drop(writing);
        let guard = pause
            .await
            .unwrap()
            .expect("the pause is granted once the write drains");

        // No new writer may claim the gate while the pause is held.
        assert!(
            gate.try_enter().is_none(),
            "a held pause must block new projection writers"
        );

        drop(guard);
        assert!(
            gate.try_enter().is_some(),
            "resuming must admit projection writers again"
        );
    }
}
