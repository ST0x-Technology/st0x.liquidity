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

use st0x_config::{RegistryLive, registry, registry_check};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const REFRESH: Duration = Duration::from_secs(60);

pub(crate) async fn watch(live: RegistryLive, shutdown: CancellationToken) {
    let http = match registry::http_client() {
        Ok(http) => http,
        Err(error) => {
            warn!(?error, "token file: refresh loop not started");
            return;
        }
    };
    let mut last: Option<Vec<u8>> = None;
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
                last = None;
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
                        last = None;
                        continue;
                    }
                }
            }
        };

        if last.as_deref() == Some(latest.as_slice()) && pinned_readable {
            continue;
        }
        let (pending_restart, invalid) = match registry_check(&live, &latest) {
            Ok(None) => (0, 0),
            Ok(Some(change)) => {
                info!(
                    url = %live.source.url,
                    %change,
                    "token file in the bucket differs from the running tables; a roll picks it up"
                );
                (1, 0)
            }
            Err(error) => {
                warn!(
                    url = %live.source.url,
                    ?error,
                    "the latest token file would be refused at boot"
                );
                // With a pin the next roll reads the pinned copy, not this
                // one; the latest copy only says a change is waiting.
                (1, u8::from(live.source.generation.is_none()))
            }
        };
        metrics::gauge!("registry_pending_restart").set(f64::from(pending_restart));
        metrics::gauge!("registry_invalid").set(f64::from(invalid.max(u8::from(!pinned_readable))));
        // A vanished pin is judged again next tick even when the latest
        // bytes have not moved, so a restored pin clears the gauge.
        last = pinned_readable.then_some(latest);
    }
}
