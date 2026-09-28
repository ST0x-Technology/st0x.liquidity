//! Reports when the token file in the bucket differs from what this
//! instance runs. It never applies the change: a token change still takes
//! a roll, this only says one is due, or that the copy the next roll would
//! read is unusable.
//!
//! Two reads a tick. The latest copy answers "has something been
//! published that this instance does not run" (`registry_pending_restart`).
//! The copy the next roll reads, the pinned generation when there is one
//! and the latest otherwise, answers "would that roll boot"
//! (`registry_invalid`): a pinned copy that is gone, or a latest copy the
//! validation refuses.

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use st0x_config::{RegistryLive, registry, registry_check};

const REFRESH: Duration = Duration::from_secs(60);

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
            Ok(bytes) => bytes,
            Err(error) => {
                metrics::counter!("registry_fetch_errors_total").increment(1);
                warn!(?error, "token file: refresh read failed");
                // Judge the next good read afresh, whatever it holds.
                seen.forget();
                continue;
            }
        };

        let pinned_readable = match live.source.generation {
            None => true,
            Some(generation) => {
                let pinned = tokio::select! {
                    () = shutdown.cancelled() => return,
                    read = registry::fetch(&http, &live.source.url, Some(generation)) => read,
                };
                match pinned {
                    Ok(_) => true,
                    Err(registry::RegistryError::Status { status, .. })
                        if status == reqwest::StatusCode::NOT_FOUND =>
                    {
                        warn!(
                            url = %live.source.url,
                            generation,
                            "token file: the pinned generation is no longer readable; the next roll cannot boot"
                        );
                        false
                    }
                    Err(error) => {
                        metrics::counter!("registry_fetch_errors_total").increment(1);
                        warn!(?error, "token file: pinned read failed");
                        seen.forget();
                        continue;
                    }
                }
            }
        };

        if !seen.changed(&latest, pinned_readable) {
            continue;
        }
        let check = registry_check(&live, &latest);
        match &check {
            Ok(None) => {}
            Ok(Some(change)) => info!(
                url = %live.source.url,
                pinned = live.source.generation.is_some(),
                %change,
                "token file in the bucket differs from the running tables"
            ),
            Err(error) => warn!(
                url = %live.source.url,
                ?error,
                "the latest token file would be refused at boot"
            ),
        }
        let Gauges {
            pending_restart,
            invalid,
        } = gauges(&check, live.source.generation.is_some(), pinned_readable);
        metrics::gauge!("registry_pending_restart").set(f64::from(u8::from(pending_restart)));
        metrics::gauge!("registry_invalid").set(f64::from(u8::from(invalid)));
    }
}

/// The inputs of the last judged tick, so an unchanged tick is not judged
/// (and logged) again.
#[derive(Debug, Default)]
struct Seen(Option<(Vec<u8>, bool)>);

impl Seen {
    /// Records this tick's inputs and says whether they differ from the
    /// last judged ones. The pinned copy's readability is part of them: a
    /// pin that comes back (restored from soft delete) must clear
    /// `registry_invalid` even when the latest copy has not moved.
    fn changed(&mut self, latest: &[u8], pinned_readable: bool) -> bool {
        let Self(seen) = self;
        if seen
            .as_ref()
            .is_some_and(|(bytes, readable)| bytes == latest && *readable == pinned_readable)
        {
            return false;
        }
        *seen = Some((latest.to_vec(), pinned_readable));
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
}

/// What one judged tick reports. A latest copy that differs, or is refused,
/// is a change waiting. It makes the next roll fail only without a pin: with
/// one the next roll reads the pinned copy, and only that copy being gone
/// makes it fail.
fn gauges<Error>(
    check: &Result<Option<String>, Error>,
    pinned: bool,
    pinned_readable: bool,
) -> Gauges {
    let (pending_restart, latest_refused) = match check {
        Ok(None) => (false, false),
        Ok(Some(_)) => (true, false),
        Err(_) => (true, true),
    };
    Gauges {
        pending_restart,
        invalid: (latest_refused && !pinned) || !pinned_readable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unchanged_tick_is_not_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(b"v1", true));
        assert!(!seen.changed(b"v1", true));
        assert!(seen.changed(b"v2", true));
    }

    /// A pin that went missing and came back, with the latest copy the
    /// same throughout, is judged again, so `registry_invalid` clears.
    #[test]
    fn a_pin_that_comes_back_is_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(b"v1", true));
        assert!(seen.changed(b"v1", false));
        assert!(!seen.changed(b"v1", false));
        assert!(seen.changed(b"v1", true));
        assert_eq!(
            gauges::<()>(&Ok(None), true, true),
            Gauges {
                pending_restart: false,
                invalid: false,
            }
        );
    }

    #[test]
    fn a_forgotten_tick_is_judged_again() {
        let mut seen = Seen::default();
        assert!(seen.changed(b"v1", true));
        seen.forget();
        assert!(seen.changed(b"v1", true));
    }

    #[test]
    fn a_changed_copy_is_pending_but_valid() {
        for pinned in [false, true] {
            assert_eq!(
                gauges::<()>(&Ok(Some("rows changed [base/FGI]".into())), pinned, true),
                Gauges {
                    pending_restart: true,
                    invalid: false,
                }
            );
        }
    }

    #[test]
    fn a_refused_latest_copy_is_invalid_only_without_a_pin() {
        assert_eq!(
            gauges(&Err(()), false, true),
            Gauges {
                pending_restart: true,
                invalid: true,
            }
        );
        assert_eq!(
            gauges(&Err(()), true, true),
            Gauges {
                pending_restart: true,
                invalid: false,
            }
        );
    }

    #[test]
    fn a_missing_pin_is_invalid_whatever_the_latest_copy_holds() {
        assert_eq!(
            gauges::<()>(&Ok(None), true, false),
            Gauges {
                pending_restart: false,
                invalid: true,
            }
        );
        assert_eq!(
            gauges(&Err(()), true, false),
            Gauges {
                pending_restart: true,
                invalid: true,
            }
        );
    }
}
