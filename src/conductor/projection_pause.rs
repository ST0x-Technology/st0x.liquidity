//! Projection-maintenance pause for every in-process projection writer.
//!
//! Event-sorcery folds projections synchronously inside `Store::send`.
//! [`work`](super::job::work) and detached burn submission claim the
//! process-global gate through [`enter_projection_gate`], while the inventory
//! monitor and operator HTTP handlers claim through the injected
//! [`ProjectionMaintenance`] controller. Both are the same gate, so a rebuild
//! pauses it, waits for existing slots to drain, and holds the pause through
//! delete and replay.
//!
//! A job runs inside [`in_projection_slot`], so work it spawns and awaits takes
//! a non parking continuation of the job's slot through
//! [`projection_slot_for_detached_work`] instead of a second, parking claim.
//!
//! This is a thin wrapper over the shared [`Quiesce`](crate::quiesce) primitive.
//! The worker side is process-global because `work` is built through worker
//! macros with no seam to thread per-worker data, and there is exactly one
//! conductor per process. Test builds read the same global, so the wiring runs
//! under test; a test that pauses it relies on nextest running each test in its
//! own process. Tests that only need a controller use
//! [`ProjectionMaintenance::for_test`], which is not global.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::quiesce::{self, InFlight, NotQuiesced, Quiesce, QuiesceGate, QuiesceGuard};

/// How long a rebuild waits for gated projection writers to drain before
/// refusing. Coarse: a slot is held for the whole write-capable operation, so
/// long-running work holds the gate for its duration. A rebuild is a rare
/// operator action taken at a quiet moment, so refusing while work runs and
/// asking the operator to retry is acceptable.
const PROJECTION_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// The process-global projection maintenance: the controller the rebuild route
/// pauses and the gate every projection writer enters. Built once, during
/// server startup before the conductor builds any worker, by
/// [`init_projection_maintenance`].
static PROJECTION_MAINTENANCE: OnceLock<Arc<ProjectionMaintenance>> = OnceLock::new();

tokio::task_local! {
    /// The projection slot the current task's job was admitted with; `None`
    /// inside the scope when the gate is not wired.
    static JOB_PROJECTION_SLOT: Option<InFlight>;
}

/// Projection writers did not quiesce within [`PROJECTION_QUIESCE_TIMEOUT`], so
/// a rebuild is refused rather than allowed to race a live projection write.
#[derive(Debug, thiserror::Error)]
#[error("projection writers did not quiesce: a job or write is in flight")]
pub(crate) struct ProjectionBusy;

/// Controller and explicit-writer side of projection maintenance.
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
    _inner: InFlight,
}

/// Resumes the gated writers when dropped, via the inner [`QuiesceGuard`]'s
/// `Drop`.
pub(crate) struct ProjectionMaintenanceGuard {
    _inner: QuiesceGuard,
}

/// The process-global projection maintenance, built on first call. Called once
/// during server startup, before the conductor and HTTP server start, and the
/// returned controller is injected into the explicitly wired writers and the
/// rebuild route. A later call returns the same controller, so it always pauses
/// the gate the workers actually enter.
pub(crate) fn init_projection_maintenance() -> Arc<ProjectionMaintenance> {
    Arc::clone(PROJECTION_MAINTENANCE.get_or_init(|| Arc::new(ProjectionMaintenance::new())))
}

/// Claims a projection-write slot for work that can commit through event
/// sorcery, parking while a rebuild is paused. `None` before startup wiring, so
/// a bare test or process that never calls [`init_projection_maintenance`] runs
/// ungated. The caller holds the slot until all of its projection writes finish.
pub(crate) async fn enter_projection_gate() -> Option<InFlight> {
    match PROJECTION_MAINTENANCE.get() {
        Some(maintenance) => Some(maintenance.gate.enter().await),
        None => None,
    }
}

/// Runs `work` holding `slot`, the projection slot its job was admitted with,
/// so anything it spawns and awaits can continue that slot rather than claim a
/// second one. The slot is released when `work` completes.
pub(crate) async fn in_projection_slot<F: Future>(slot: Option<InFlight>, work: F) -> F::Output {
    JOB_PROJECTION_SLOT.scope(slot, work).await
}

/// The projection slot for detached work, taken in the spawning task before the
/// spawn. Inside a job it continues the job's admitted slot without parking: a
/// second, parking claim would wait on a pause that is itself waiting for the
/// job, which is waiting for this work. Outside any job it enters the gate.
pub(crate) async fn projection_slot_for_detached_work() -> Option<InFlight> {
    match JOB_PROJECTION_SLOT.try_with(|slot| slot.as_ref().map(InFlight::continuation)) {
        Ok(continuation) => continuation,
        Err(_) => enter_projection_gate().await,
    }
}

#[cfg(test)]
impl ProjectionMaintenance {
    /// A controller wired to a fresh, non-global gate, so a test drives the
    /// pause against its own writers without touching the process-global gate.
    pub(crate) fn for_test() -> Self {
        Self::new()
    }

    /// Whether a pause is requested or held.
    pub(crate) fn is_paused(&self) -> bool {
        self.gate.is_paused()
    }
}
