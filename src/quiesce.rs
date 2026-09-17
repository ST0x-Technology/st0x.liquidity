//! A reusable quiesce primitive: a controller that pauses a set of concurrent
//! workers, waits for every execution already in flight to finish, and blocks
//! new ones for the lifetime of a guard.
//!
//! The controller ([`Quiesce`]) and the worker side ([`QuiesceGate`]) share two
//! `watch` channels: `pause` (controller to workers) and `in_flight` (workers
//! to controller). A worker holds an [`InFlight`] token for the duration of each
//! execution; [`Quiesce::pause`] raises the flag so no new token can be claimed,
//! then waits for the outstanding count to reach zero. The returned
//! [`QuiesceGuard`] lowers the flag on drop, so a caller cannot forget to resume
//! on an error or panic path.
//!
//! This is the shared mechanism behind the USDC driver pause and the projection
//! maintenance pause; each wraps a [`Quiesce`] with its own timeout and refusal
//! type.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

/// Controller side of a quiesce, held by the code that pauses the workers.
pub(crate) struct Quiesce {
    pause: watch::Sender<bool>,
    in_flight: watch::Receiver<usize>,
    /// Serializes pausers so one guard's resume cannot free the workers while
    /// another pauser is still mutating; held for the guard's lifetime.
    serialize: Arc<Mutex<()>>,
    /// How long [`Self::pause`] waits for in-flight executions to drain before
    /// refusing.
    timeout: Duration,
}

/// Every execution in flight did not finish within the quiesce timeout, so the
/// caller must not mutate state a live worker would race.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NotQuiesced;

impl Quiesce {
    /// Requests a pause and returns once quiesced: every execution in flight has
    /// finished and none can start. The guard resumes the workers when dropped.
    ///
    /// Pausers are serialized: a second caller waits until the first guard drops
    /// before it pauses, so overlapping operations cannot resume the workers out
    /// from under one another.
    ///
    /// Returns [`NotQuiesced`] when executions are still in flight after the
    /// configured timeout, leaving the workers running. A caller that drops this
    /// future mid wait (a cancelled request) likewise leaves them running: the
    /// flag is lowered on the way out.
    pub(crate) async fn pause(&self) -> Result<QuiesceGuard, NotQuiesced> {
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
        let drained = tokio::time::timeout(self.timeout, async {
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
                Ok(QuiesceGuard {
                    pause: self.pause.clone(),
                    _permit: permit,
                })
            }
            // Executions still in flight after the window, or every gate gone
            // while one was still counted: not quiesced. `lower_on_exit` and the
            // permit drop here, resuming the workers and freeing the next pauser.
            Ok(Err(())) | Err(_) => Err(NotQuiesced),
        }
    }
}

/// Lowers the pause flag when dropped while armed, so a `pause()` that exits
/// without producing a guard, whether refused or cancelled, cannot leave the
/// workers parked.
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

/// Resumes the workers when dropped. Resuming is a plain signal, so it runs from
/// `Drop` on the success, error, and panic paths alike.
pub(crate) struct QuiesceGuard {
    pause: watch::Sender<bool>,
    /// Held so the serialize mutex is released, unblocking the next pauser, only
    /// after this guard drops and the workers resume.
    _permit: OwnedMutexGuard<()>,
}

impl Drop for QuiesceGuard {
    fn drop(&mut self) {
        let _ = self.pause.send(false);
    }
}

/// Worker side of a quiesce, one clone per worker. A worker calls [`Self::enter`]
/// at the top of each execution and holds the returned [`InFlight`] token for the
/// execution's lifetime; a caller that must not block claims through
/// [`Self::try_enter`], which never parks.
#[derive(Clone)]
pub(crate) struct QuiesceGate {
    pause: watch::Receiver<bool>,
    in_flight: watch::Sender<usize>,
}

impl QuiesceGate {
    /// Parks while a pause is requested, then claims an in-flight slot for the
    /// execution about to run. Dropping the token releases the slot.
    ///
    /// The slot is claimed and the flag re-read afterwards: a pause requested
    /// between the flag check and the claim then sees the claim and waits, while
    /// this side sees the flag, releases the claim, and parks. Without that
    /// second read an execution could slip past a pause the controller had just
    /// confirmed.
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

    /// Claims an in-flight slot without parking: `None` when a pause is requested
    /// or held, so a caller that must not block skips its work instead. A
    /// returned token makes a pause wait for that work to finish, exactly as it
    /// waits for a worker execution.
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

    /// Whether a pause is requested or held.
    #[cfg(test)]
    pub(crate) fn is_paused(&self) -> bool {
        *self.pause.borrow()
    }
}

/// An execution's claim on the workers. Held for the execution's lifetime so
/// [`Quiesce::pause`] can wait for it to finish.
pub(crate) struct InFlight {
    in_flight: watch::Sender<usize>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.in_flight.send_modify(|count| *count -= 1);
    }
}

/// Builds a linked controller/gate pair sharing two `watch` channels: `pause`
/// (controller to workers) and `in_flight` (workers to controller). `timeout`
/// bounds how long [`Quiesce::pause`] waits for in-flight executions to drain.
pub(crate) fn quiesce(timeout: Duration) -> (Quiesce, QuiesceGate) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (in_flight_tx, in_flight_rx) = watch::channel(0);
    (
        Quiesce {
            pause: pause_tx,
            in_flight: in_flight_rx,
            serialize: Arc::new(Mutex::new(())),
            timeout,
        },
        QuiesceGate {
            pause: pause_rx,
            in_flight: in_flight_tx,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn pause_quiesces_when_nothing_is_in_flight() {
        let (control, _gate) = quiesce(TEST_TIMEOUT);
        assert!(
            control.pause().await.is_ok(),
            "an idle set must quiesce at once"
        );
    }

    #[tokio::test]
    async fn a_held_token_refuses_the_pause_within_the_timeout() {
        let (control, gate) = quiesce(Duration::from_millis(50));
        let _executing = gate.enter().await;

        assert_eq!(
            control.pause().await.err(),
            Some(NotQuiesced),
            "a pause must refuse while an execution is in flight",
        );
    }

    #[tokio::test]
    async fn pause_returns_once_the_in_flight_token_drops() {
        let (control, gate) = quiesce(TEST_TIMEOUT);
        let executing = gate.enter().await;

        let pause = tokio::spawn(async move { control.pause().await.is_ok() });
        // Give the pause a moment to raise the flag and start waiting.
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(executing);

        assert!(
            pause.await.unwrap(),
            "the pause completes once in-flight drains"
        );
    }

    #[tokio::test]
    async fn try_enter_is_refused_while_paused_and_allowed_after_resume() {
        let (control, gate) = quiesce(TEST_TIMEOUT);

        let guard = control.pause().await.unwrap();
        assert!(gate.is_paused());
        assert!(
            gate.try_enter().is_none(),
            "try_enter must be refused while paused",
        );

        drop(guard);
        assert!(!gate.is_paused());
        assert!(
            gate.try_enter().is_some(),
            "try_enter must be allowed after resume",
        );
    }

    #[tokio::test]
    async fn a_refused_pause_leaves_the_workers_running() {
        let (control, gate) = quiesce(Duration::from_millis(50));
        let executing = gate.enter().await;

        assert_eq!(control.pause().await.err(), Some(NotQuiesced));
        drop(executing);

        // The flag was lowered on refusal, so a fresh execution runs unparked.
        assert!(
            gate.try_enter().is_some(),
            "a refused pause must not park the workers"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_waiting_pause_releases_the_flag_and_serializer() {
        let (control, gate) = quiesce(TEST_TIMEOUT);
        let executing = gate.enter().await;

        let cancelled = tokio::time::timeout(Duration::from_millis(10), control.pause()).await;
        assert!(
            cancelled.is_err(),
            "the outer timeout must cancel a pause waiting for an in-flight execution"
        );

        drop(executing);
        drop(
            gate.try_enter()
                .expect("cancelling a pause must lower the gate flag"),
        );

        let guard = tokio::time::timeout(Duration::from_millis(10), control.pause())
            .await
            .expect("cancelling a pause must release the serialization permit")
            .expect("an idle gate must quiesce");
        drop(guard);
    }

    // The join handle holds the second pause guard as its output and is used
    // twice (the `is_finished` poll and the `await`), which the nursery lint
    // misreads as a single-use temporary.
    #[allow(clippy::significant_drop_tightening)]
    #[tokio::test]
    async fn pausers_are_serialized() {
        let (control, _gate) = quiesce(TEST_TIMEOUT);
        let control = Arc::new(control);

        let first = control.pause().await.unwrap();

        let second_control = Arc::clone(&control);
        let second = tokio::spawn(async move { second_control.pause().await });
        // The second pauser must wait for the first guard to drop.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !second.is_finished(),
            "a second pauser waits for the first guard"
        );

        drop(first);
        assert!(
            second.await.unwrap().is_ok(),
            "the second pause proceeds after the first drops"
        );
    }
}
