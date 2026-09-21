//! Projection-maintenance pause for runtime work that can write projections.
//!
//! Event-sorcery folds projections synchronously inside `Store::send`.
//! [`work`](super::job::work), the supervised inventory monitor, and detached
//! burn submission each call [`enter_projection_gate`] and hold the returned
//! slot for their full write-capable operation. Pausing the gate therefore
//! serializes a rebuild against projection writes emitted by those paths.
//!
//! Operator HTTP write routes are not currently covered. Routes that call
//! `Store::send` directly do not enter this gate and remain ungated. Publishing
//! [`ProjectionMaintenance`] on recovery state exposes the controller but does
//! not gate those direct-send paths.
//!
//! This is a thin wrapper over the shared [`Quiesce`](crate::quiesce) primitive.
//! The gate is process-global because `work` is built through worker macros with
//! no seam to inject per-worker data. Every gated runtime writer shares that
//! gate, and there is exactly one conductor per process.

use std::sync::OnceLock;
use std::time::Duration;

use crate::quiesce::{self, NotQuiesced, Quiesce, QuiesceGate, QuiesceGuard};

/// How long a rebuild waits for gated projection writers to drain before
/// refusing. Coarse: a slot is held for the whole write-capable operation, so
/// long-running work holds the gate for its duration. A rebuild is a rare
/// operator action taken at a quiet moment, so refusing while work runs and
/// asking the operator to retry is acceptable.
const PROJECTION_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global projection-write gate, read by workers and supervised runtime
/// tasks. Set once at conductor startup, before any of them are spawned, by
/// [`init_projection_maintenance`].
static PROJECTION_GATE: OnceLock<QuiesceGate> = OnceLock::new();

/// Projection writers did not quiesce within [`PROJECTION_QUIESCE_TIMEOUT`], so
/// a rebuild is refused rather than allowed to race a live projection write.
#[derive(Debug, thiserror::Error)]
#[error("projection writers did not quiesce: a job or write is in flight")]
pub(crate) struct ProjectionBusy;

/// Controller side, held by the rebuild route. Pausing quiesces gated runtime
/// projection writes for the guard's lifetime; direct-send routes remain outside
/// this gate.
pub(crate) struct ProjectionMaintenance(Quiesce);

impl ProjectionMaintenance {
    /// Requests a pause and returns once every gated projection writer has
    /// drained and none can start. The guard resumes them when dropped, so a
    /// caller cannot forget to resume on an error or panic path. Returns
    /// [`ProjectionBusy`] when writers are still in flight after
    /// [`PROJECTION_QUIESCE_TIMEOUT`], leaving them running.
    // Consumed by the materialized-view rebuild route on the dependent branch;
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

/// Builds the projection-maintenance controller and publishes its gate to the
/// process global that runtime projection writers read. Called once at conductor
/// startup, before those writers are spawned. The returned controller lets the
/// rebuild route pause gated runtime writers; publishing it does not gate
/// direct-send HTTP routes.
///
/// Production starts one conductor per process. A second call keeps the first
/// process-global gate and is unsupported outside isolated test setup.
pub(crate) fn init_projection_maintenance() -> ProjectionMaintenance {
    let (control, gate) = quiesce::quiesce(PROJECTION_QUIESCE_TIMEOUT);
    let _ = PROJECTION_GATE.set(gate);
    ProjectionMaintenance(control)
}

/// Claims a projection-write slot for work that can commit through event
/// sorcery, parking while a rebuild is paused. `None` before startup wiring, so
/// a bare test or process that never calls [`init_projection_maintenance`] runs
/// ungated. The caller holds the slot until all of its projection writes finish.
///
/// Test-support builds stay ungated so a process-global gate initialized by one
/// test cannot park another test's workers.
pub(crate) async fn enter_projection_gate() -> Option<quiesce::InFlight> {
    #[cfg(feature = "test-support")]
    {
        std::future::ready(None).await
    }
    #[cfg(not(feature = "test-support"))]
    {
        match PROJECTION_GATE.get() {
            Some(gate) => Some(gate.enter().await),
            None => None,
        }
    }
}

#[cfg(test)]
impl ProjectionMaintenance {
    /// A controller wired to a fresh, non-global gate, so a test drives the pause
    /// against its own writers without touching the process-global
    /// [`PROJECTION_GATE`] that runtime writers read.
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
