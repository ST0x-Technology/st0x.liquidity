//! Reports when the token file in the bucket differs from what this
//! instance runs. It never applies the change: a token change still takes
//! a roll, this only says one is due, or that the new copy would be refused.
//! With a pinned generation it reads that generation, so it reports whether
//! the copy the next roll needs is still readable, not what was published
//! since.

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
        // The copy the next roll would read: the pinned generation when
        // there is one, else the latest.
        let read = tokio::select! {
            () = shutdown.cancelled() => return,
            read = registry::fetch(&http, &live.source.url, live.source.generation) => read,
        };
        let bytes = match read {
            Ok(bytes) => bytes,
            Err(error) => {
                metrics::counter!("registry_fetch_errors_total").increment(1);
                warn!(?error, "token file: refresh read failed");
                continue;
            }
        };
        if last.as_deref() == Some(bytes.as_slice()) {
            continue;
        }
        match registry_check(&live, &bytes) {
            Ok(None) => report(0, 0),
            Ok(Some(change)) => {
                report(1, 0);
                info!(
                    url = %live.source.url,
                    %change,
                    "token file in the bucket differs from the running tables; a roll picks it up"
                );
            }
            Err(error) => {
                report(0, 1);
                warn!(
                    url = %live.source.url,
                    ?error,
                    "token file in the bucket would be refused at boot"
                );
            }
        }
        last = Some(bytes);
    }
}

fn report(pending_restart: u8, invalid: u8) {
    metrics::gauge!("registry_pending_restart").set(f64::from(pending_restart));
    metrics::gauge!("registry_invalid").set(f64::from(invalid));
}
