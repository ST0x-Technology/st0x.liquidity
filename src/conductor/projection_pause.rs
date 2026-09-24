//! Projection-write gate for runtime work that can write projections.
//!
//! Event-sorcery folds projections synchronously inside `Store::send`.
//! [`work`](super::job::work), the supervised inventory monitor, and detached
//! burn submission each call [`enter_projection_gate`] and hold the returned
//! slot for their full write-capable operation, so a pause on the gate
//! serializes a materialized-view rebuild against those projection writes.
//!
//! Operator HTTP write routes that call `Store::send` directly do not enter
//! this gate.
//!
//! This is a thin wrapper over the shared [`Quiesce`](crate::quiesce) primitive.
//! The gate is process-global because `work` is built through worker macros with
//! no seam to inject per-worker data. Every gated runtime writer shares that
//! gate, and there is exactly one conductor per process. Test builds read the
//! same global, so the wiring runs under test; a test that pauses it relies on
//! nextest running each test in its own process.

use std::sync::OnceLock;
use std::time::Duration;

use crate::quiesce::{self, Quiesce, QuiesceGate};

/// How long a pause waits for gated projection writers to drain before
/// refusing. Coarse: a slot is held for the whole write-capable operation, so
/// long-running work holds the gate for its duration.
const PROJECTION_QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-global projection gate: the controller that pauses it and the gate
/// workers and supervised runtime tasks enter. Built once, at conductor startup
/// before any writer is spawned, by [`init_projection_gate`].
static PROJECTION_GATE: OnceLock<(Quiesce, QuiesceGate)> = OnceLock::new();

/// Builds the process-global projection gate that runtime projection writers
/// enter. Called once at conductor startup, before those writers are spawned;
/// a later call keeps the gate already built.
pub(crate) fn init_projection_gate() {
    PROJECTION_GATE.get_or_init(|| quiesce::quiesce(PROJECTION_QUIESCE_TIMEOUT));
}

/// Claims a projection-write slot for work that can commit through event
/// sorcery, parking while the gate is paused. `None` before startup wiring, so
/// a bare test or process that never calls [`init_projection_gate`] runs
/// ungated. The caller holds the slot until all of its projection writes finish.
pub(crate) async fn enter_projection_gate() -> Option<quiesce::InFlight> {
    match PROJECTION_GATE.get() {
        Some((_, gate)) => Some(gate.enter().await),
        None => None,
    }
}

/// Pauses the process-global gate, building it first if needed, so a test can
/// hold a real writer parked on the gate it enters in production.
#[cfg(test)]
pub(crate) async fn pause_projection_gate_for_test() -> quiesce::QuiesceGuard {
    init_projection_gate();
    let (control, _) = PROJECTION_GATE
        .get()
        .expect("the projection gate was just initialized");
    control
        .pause()
        .await
        .unwrap_or_else(|_| panic!("an idle projection gate must pause"))
}
