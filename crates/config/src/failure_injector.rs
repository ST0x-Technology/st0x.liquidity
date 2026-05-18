//! Test-only failure injector used to force terminal job failures
//! at deterministic points in e2e tests.

use std::sync::{Arc, Mutex};

/// Identifies which job queue a [`FailureInjector`] targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JobKind {
    OrderFill,
    Hedge,
    Backfill,
    PollOrderStatus,
    ReconcileOrderFill,
    HandleOrderRejection,
    CheckPositions,
}

/// Allows e2e tests to force the next job of a specific kind to
/// fail terminally. Each [`JobKind`] has an independent injection
/// state so arming one queue cannot be consumed by the other.
#[derive(Clone, Debug)]
pub struct FailureInjector {
    order_fill: Arc<Mutex<InjectionState>>,
    hedge: Arc<Mutex<InjectionState>>,
    backfill: Arc<Mutex<InjectionState>>,
    poll_order_status: Arc<Mutex<InjectionState>>,
    reconcile_order_fill: Arc<Mutex<InjectionState>>,
    handle_order_rejection: Arc<Mutex<InjectionState>>,
    check_positions: Arc<Mutex<InjectionState>>,
}

#[derive(Debug, Default)]
enum InjectionState {
    #[default]
    Idle,
    Armed,
    Targeted(String),
}

impl Default for FailureInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl FailureInjector {
    pub fn new() -> Self {
        Self {
            order_fill: Arc::new(Mutex::new(InjectionState::Idle)),
            hedge: Arc::new(Mutex::new(InjectionState::Idle)),
            backfill: Arc::new(Mutex::new(InjectionState::Idle)),
            poll_order_status: Arc::new(Mutex::new(InjectionState::Idle)),
            reconcile_order_fill: Arc::new(Mutex::new(InjectionState::Idle)),
            handle_order_rejection: Arc::new(Mutex::new(InjectionState::Idle)),
            check_positions: Arc::new(Mutex::new(InjectionState::Idle)),
        }
    }

    pub fn arm(&self, kind: JobKind) {
        *self.lock_state(kind) = InjectionState::Armed;
    }

    pub fn is_armed(&self, kind: JobKind) -> bool {
        let state = &mut *self.lock_state(kind);
        let was_armed = matches!(state, InjectionState::Armed);

        if was_armed {
            *state = InjectionState::Idle;
        }

        was_armed
    }

    /// Returns `true` if the next job of this kind should be injected
    /// with a terminal failure. Latches onto the first label observed
    /// so subsequent retries of the same job continue to fail.
    pub fn should_inject(&self, kind: JobKind, label: &str) -> bool {
        let state = &mut *self.lock_state(kind);

        match state {
            InjectionState::Idle => false,
            InjectionState::Armed => {
                *state = InjectionState::Targeted(label.to_owned());
                true
            }
            InjectionState::Targeted(target_label) => target_label == label,
        }
    }

    fn lock_state(&self, kind: JobKind) -> std::sync::MutexGuard<'_, InjectionState> {
        let mutex = match kind {
            JobKind::OrderFill => &self.order_fill,
            JobKind::Hedge => &self.hedge,
            JobKind::Backfill => &self.backfill,
            JobKind::PollOrderStatus => &self.poll_order_status,
            JobKind::ReconcileOrderFill => &self.reconcile_order_fill,
            JobKind::HandleOrderRejection => &self.handle_order_rejection,
            JobKind::CheckPositions => &self.check_positions,
        };

        match mutex.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_armed_by_default() {
        let injector = FailureInjector::new();
        assert!(!injector.is_armed(JobKind::OrderFill));
        assert!(!injector.is_armed(JobKind::Hedge));
    }

    #[test]
    fn arm_then_check_auto_disarms() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::OrderFill);
        assert!(injector.is_armed(JobKind::OrderFill));
        assert!(
            !injector.is_armed(JobKind::OrderFill),
            "second check should be false (auto-disarmed)"
        );
    }

    #[test]
    fn kinds_are_independent() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::OrderFill);
        assert!(
            !injector.is_armed(JobKind::Hedge),
            "arming OrderFill should not affect Hedge"
        );
        assert!(injector.is_armed(JobKind::OrderFill));
    }

    #[test]
    fn targeted_label_latches_across_retries() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::OrderFill);

        assert!(
            injector.should_inject(JobKind::OrderFill, "job-a"),
            "Armed state should inject for the first label"
        );
        assert!(
            matches!(
                &*injector.lock_state(JobKind::OrderFill),
                InjectionState::Targeted(target) if target == "job-a"
            ),
            "state should latch to Targeted with the first label"
        );
        assert!(
            injector.should_inject(JobKind::OrderFill, "job-a"),
            "Targeted state should keep injecting for the same label across retries"
        );
        assert!(
            !injector.should_inject(JobKind::OrderFill, "job-b"),
            "Targeted state should not inject for a different label"
        );
    }

    #[test]
    fn shared_across_clones() {
        let injector = FailureInjector::new();
        let clone = injector.clone();

        injector.arm(JobKind::Hedge);
        assert!(
            clone.is_armed(JobKind::Hedge),
            "arming original should be visible from clone"
        );
        assert!(
            !injector.is_armed(JobKind::Hedge),
            "should be disarmed after clone consumed it"
        );
    }
}
