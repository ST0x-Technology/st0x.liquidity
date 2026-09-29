//! Reports when the token file in the bucket differs from what this
//! instance runs. It never applies the change: a token change still takes
//! a roll, this only says one is due, or that the copy the next roll would
//! read is unusable.
//!
//! Two reads a tick. The latest copy answers "has something been
//! published that this instance does not run" (`registry_pending_restart`).
//! The copy the next roll reads, the pinned generation when there is one
//! and the latest otherwise, answers "would that roll boot"
//! (`registry_invalid`): a copy that is gone, refused to the service account
//! or oversized, or a latest copy the validation refuses. The pinned read
//! happens whatever the latest read did, so a lost pin is reported even while
//! the latest copy cannot be read. `registry_latest_refused` says the latest
//! copy alone would be refused, so a bad target shows before a pin moves to it.

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use st0x_config::{RegistryLive, registry, registry_check};

const REFRESH: Duration = Duration::from_secs(60);

/// What one read of a copy came back as. A transient failure is neither:
/// the tick is skipped and the next one judges afresh.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Copy {
    Bytes(Vec<u8>),
    /// Gone (404), refused to the service account (401, 403) or over the
    /// size cap: boot refuses it too.
    Unusable,
}

pub(crate) async fn watch(live: RegistryLive, shutdown: CancellationToken) {
    let http = match registry::http_client() {
        Ok(http) => http,
        Err(error) => {
            warn!(?error, "token file: refresh loop not started");
            return;
        }
    };
    let mut seen = Seen::default();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(REFRESH) => {}
        }
        let latest = tokio::select! {
            () = shutdown.cancelled() => return,
            read = registry::fetch(&http, &live.source.url, None) => read,
        };
        let latest = match latest {
            Ok(bytes) => Some(Copy::Bytes(bytes)),
            Err(error) if error.copy_is_unusable() => {
                warn!(url = %live.source.url, ?error, "token file: the latest copy is unusable");
                Some(Copy::Unusable)
            }
            Err(error) => {
                metrics::counter!("registry_fetch_errors_total").increment(1);
                warn!(?error, "token file: refresh read failed");
                None
            }
        };

        let pinned_readable = match live.source.generation {
            None => Some(true),
            Some(generation) => {
                let pinned = tokio::select! {
                    () = shutdown.cancelled() => return,
                    read = registry::fetch(&http, &live.source.url, Some(generation)) => read,
                };
                match pinned {
                    Ok(_) => Some(true),
                    Err(error) if error.copy_is_unusable() => {
                        warn!(
                            url = %live.source.url,
                            generation,
                            ?error,
                            "token file: the pinned generation is unusable; the next roll cannot boot"
                        );
                        Some(false)
                    }
                    Err(error) => {
                        metrics::counter!("registry_fetch_errors_total").increment(1);
                        warn!(?error, "token file: pinned read failed");
                        None
                    }
                }
            }
        };

        let Some(pinned_readable) = pinned_readable else {
            seen.forget();
            continue;
        };
        let Some(latest) = latest else {
            // The latest copy is unknown this tick, but a lost pin is
            // not, and it is the signal the pinned read exists to raise.
            if !pinned_readable {
                metrics::gauge!("registry_invalid").set(1.0);
            }
            seen.forget();
            continue;
        };

        if !seen.changed(&latest, pinned_readable) {
            continue;
        }
        let check = match &latest {
            Copy::Bytes(bytes) => Some(registry_check(&live, bytes)),
            Copy::Unusable => None,
        };
        match &check {
            None | Some(Ok(None)) => {}
            Some(Ok(Some(change))) => info!(
                url = %live.source.url,
                pinned = live.source.generation.is_some(),
                %change,
                "token file in the bucket differs from the running tables"
            ),
            Some(Err(error)) => warn!(
                url = %live.source.url,
                ?error,
                "the latest token file would be refused at boot"
            ),
        }
        let Gauges {
            pending_restart,
            invalid,
            latest_refused,
        } = gauges(
            check.as_ref(),
            live.source.generation.is_some(),
            pinned_readable,
        );
        metrics::gauge!("registry_pending_restart").set(f64::from(u8::from(pending_restart)));
        metrics::gauge!("registry_invalid").set(f64::from(u8::from(invalid)));
        metrics::gauge!("registry_latest_refused").set(f64::from(u8::from(latest_refused)));
    }
}

/// The inputs of the last judged tick, so an unchanged tick is not judged
/// (and logged) again.
#[derive(Debug, Default)]
struct Seen(Option<(Copy, bool)>);

impl Seen {
    /// Records this tick's inputs and says whether they differ from the
    /// last judged ones. The pinned copy's readability is part of them: a
    /// pin that comes back (restored from soft delete) must clear
    /// `registry_invalid` even when the latest copy has not moved.
    fn changed(&mut self, latest: &Copy, pinned_readable: bool) -> bool {
        let Self(seen) = self;
        if seen
            .as_ref()
            .is_some_and(|(copy, readable)| copy == latest && *readable == pinned_readable)
        {
            return false;
        }
        *seen = Some((latest.clone(), pinned_readable));
        true
    }

    fn forget(&mut self) {
        let Self(seen) = self;
        *seen = None;
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Gauges {
    pending_restart: bool,
    invalid: bool,
    latest_refused: bool,
}

/// What one judged tick reports. `check` is `None` when the latest copy is
/// unusable (gone, refused to the service account or oversized): nothing is
/// waiting to be picked up, and the next roll cannot read it. A latest copy that differs, or is refused, is a
/// change waiting; it makes the next roll fail only without a pin. With a
/// pin, only that copy being unusable makes the next roll fail.
fn gauges<Error>(
    check: Option<&Result<Option<String>, Error>>,
    pinned: bool,
    pinned_readable: bool,
) -> Gauges {
    let (pending_restart, latest_refused) = match check {
        None => (false, true),
        Some(Ok(None)) => (false, false),
        Some(Ok(Some(_))) => (true, false),
        Some(Err(_)) => (true, true),
    };
    Gauges {
        pending_restart,
        invalid: (latest_refused && !pinned) || !pinned_readable,
        latest_refused,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(text: &str) -> Copy {
        Copy::Bytes(text.as_bytes().to_vec())
    }

    #[test]
    fn an_unchanged_tick_is_not_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(&bytes("v1"), true));
        assert!(!seen.changed(&bytes("v1"), true));
        assert!(seen.changed(&bytes("v2"), true));
        assert!(seen.changed(&Copy::Unusable, true));
        assert!(!seen.changed(&Copy::Unusable, true));
    }

    /// A pin that went missing and came back, with the latest copy the
    /// same throughout, is judged again, so `registry_invalid` clears.
    #[test]
    fn a_pin_that_comes_back_is_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(&bytes("v1"), true));
        assert!(seen.changed(&bytes("v1"), false));
        assert!(!seen.changed(&bytes("v1"), false));
        assert!(seen.changed(&bytes("v1"), true));
        assert_eq!(
            gauges::<()>(Some(&Ok(None)), true, true),
            Gauges {
                pending_restart: false,
                invalid: false,
                latest_refused: false,
            }
        );
    }

    #[test]
    fn a_forgotten_tick_is_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(&bytes("v1"), true));
        seen.forget();
        assert!(seen.changed(&bytes("v1"), true));
    }

    #[test]
    fn a_changed_copy_is_pending_but_valid() {
        for pinned in [false, true] {
            assert_eq!(
                gauges::<()>(
                    Some(&Ok(Some("rows changed [base/FGI]".into()))),
                    pinned,
                    true
                ),
                Gauges {
                    pending_restart: true,
                    invalid: false,
                    latest_refused: false,
                }
            );
        }
    }

    /// With a pin a refused latest copy does not stop the next roll, but
    /// `registry_latest_refused` still says the next pin target is bad.
    #[test]
    fn a_refused_latest_copy_is_invalid_only_without_a_pin() {
        assert_eq!(
            gauges(Some(&Err(())), false, true),
            Gauges {
                pending_restart: true,
                invalid: true,
                latest_refused: true,
            }
        );
        assert_eq!(
            gauges(Some(&Err(())), true, true),
            Gauges {
                pending_restart: true,
                invalid: false,
                latest_refused: true,
            }
        );
    }

    /// A latest copy that is gone, refused or oversized: boot refuses it, so
    /// the gauge says so without a pin, and nothing is pending.
    #[test]
    fn an_unusable_latest_copy_is_invalid_without_a_pin() {
        assert_eq!(
            gauges::<()>(None, false, true),
            Gauges {
                pending_restart: false,
                invalid: true,
                latest_refused: true,
            }
        );
        assert_eq!(
            gauges::<()>(None, true, true),
            Gauges {
                pending_restart: false,
                invalid: false,
                latest_refused: true,
            }
        );
    }

    #[test]
    fn a_missing_pin_is_invalid_whatever_the_latest_copy_holds() {
        assert_eq!(
            gauges::<()>(Some(&Ok(None)), true, false),
            Gauges {
                pending_restart: false,
                invalid: true,
                latest_refused: false,
            }
        );
        assert_eq!(
            gauges(Some(&Err(())), true, false),
            Gauges {
                pending_restart: true,
                invalid: true,
                latest_refused: true,
            }
        );
        assert_eq!(
            gauges::<()>(None, true, false),
            Gauges {
                pending_restart: false,
                invalid: true,
                latest_refused: true,
            }
        );
    }
}
