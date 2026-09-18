//! Projection-maintenance pause for every in-process projection writer.
//!
//! Event-sorcery folds projections synchronously inside `Store::send`.
//! [`work`](super::job::work) claims a slot for generic apalis jobs, the
//! inventory monitor claims one around each poll, and operator HTTP handlers
//! claim one around direct write operations. A rebuild pauses the shared gate,
//! waits for existing slots to drain, and holds the pause through delete and
//! replay.
//!
//! This is a thin wrapper over the shared [`Quiesce`](crate::quiesce) primitive.
//! The apalis worker side is process-global because `work` is built through
//! worker macros with no seam to thread per-worker data. The controller also
//! owns a clone of the same gate for explicitly wired writers.

use std::sync::OnceLock;
use std::time::Duration;

use crate::quiesce::{self, NotQuiesced, Quiesce, QuiesceGate, QuiesceGuard};

/// How long a rebuild waits for in-flight projection writers to drain before
/// refusing. Coarse: generic apalis jobs and inventory polls hold their slots
/// for the whole operation. A rebuild is a rare operator action taken at a
/// quiet moment, so refusing while a long operation runs and asking the
/// operator to retry is acceptable.
const PROJECTION_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global worker gate, read by the generic apalis handler
/// [`work`](super::job::work). Set once during server startup, before the
/// conductor builds any worker, by [`init_projection_maintenance`].
static PROJECTION_GATE: OnceLock<QuiesceGate> = OnceLock::new();

/// Projection writers did not quiesce within [`PROJECTION_QUIESCE_TIMEOUT`], so
/// a rebuild is refused rather than allowed to race a live projection write.
#[derive(Debug, thiserror::Error)]
#[error("projection writers did not quiesce: a job or write is in flight")]
pub(crate) struct ProjectionBusy;

/// Shared controller and explicit-writer side of projection maintenance.
pub(crate) struct ProjectionMaintenance {
    control: Quiesce,
    gate: QuiesceGate,
}

impl ProjectionMaintenance {
    fn new() -> Self {
        let (control, gate) = quiesce::quiesce(PROJECTION_QUIESCE_TIMEOUT);
        Self { control, gate }
    }

    /// Claims a slot for an explicitly wired projection writer, parking while a
    /// rebuild is paused. The caller holds the returned token for its complete
    /// read/write operation.
    pub(crate) async fn enter(&self) -> ProjectionWrite {
        ProjectionWrite {
            _inner: self.gate.enter().await,
        }
    }

    /// Requests a pause and returns once every projection writer has drained
    /// and none can start. The guard resumes them when dropped, so a caller
    /// cannot forget to resume on an error or panic path. Returns
    /// [`ProjectionBusy`] when writers are still in flight after
    /// [`PROJECTION_QUIESCE_TIMEOUT`], leaving them running.
    pub(crate) async fn pause(&self) -> Result<ProjectionMaintenanceGuard, ProjectionBusy> {
        self.control
            .pause()
            .await
            .map(|guard| ProjectionMaintenanceGuard { _inner: guard })
            .map_err(|NotQuiesced| ProjectionBusy)
    }
}

/// An explicitly wired projection writer's claim on the shared maintenance
/// gate.
pub(crate) struct ProjectionWrite {
    _inner: quiesce::InFlight,
}

/// Resumes the gated writers when dropped, via the inner [`QuiesceGuard`]'s
/// `Drop`.
pub(crate) struct ProjectionMaintenanceGuard {
    _inner: QuiesceGuard,
}

/// Builds the projection-maintenance controller and publishes its apalis-worker
/// gate to the process global that [`work`](super::job::work) reads. Called once
/// before the conductor and HTTP server start.
///
/// A second call (a second conductor in one test process) keeps the first
/// process-global gate. Production has one conductor; isolated tests use
/// [`ProjectionMaintenance::for_test`] instead.
pub(crate) fn init_projection_maintenance() -> ProjectionMaintenance {
    let maintenance = ProjectionMaintenance::new();
    let _ = PROJECTION_GATE.set(maintenance.gate.clone());
    maintenance
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
    /// A controller wired to a fresh, non-global gate, so tests cannot park
    /// another test's process-global workers.
    pub(crate) fn for_test() -> Self {
        Self::new()
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.gate.is_paused()
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
        let control = std::sync::Arc::new(ProjectionMaintenance::for_test());
        let writing = control.enter().await;

        // The pause cannot complete while a write is in flight.
        let pause_control = std::sync::Arc::clone(&control);
        let pause = tokio::spawn(async move { pause_control.pause().await.ok() });
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
            control.gate.try_enter().is_none(),
            "a held pause must block new projection writers"
        );

        drop(guard);
        assert!(
            control.gate.try_enter().is_some(),
            "resuming must admit projection writers again"
        );
    }
}
