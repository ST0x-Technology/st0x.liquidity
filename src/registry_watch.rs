//! Reports when the token file in the bucket differs from what this
//! instance runs. It never applies the change: a token change still takes
//! a roll, this only says one is due, or that the new copy would be refused.

use std::time::Duration;

use st0x_config::{RegistryLive, registry, registry_check};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub(crate) async fn watch(live: RegistryLive, shutdown: CancellationToken) {
    let http = reqwest::Client::new();
    let every = Duration::from_secs(live.source.refresh_secs.max(5));
    let mut last: Option<Vec<u8>> = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(every) => {}
        }
        // The latest copy, not the pinned generation: the question is what
        // the next roll would pick up.
        let bytes = match registry::fetch(&http, &live.source.url, None).await {
            Ok(bytes) => bytes,
            Err(error) => {
                metrics::counter!("registry_fetch_errors_total").increment(1);
                warn!(%error, "token file: refresh read failed");
                continue;
            }
        };
        if last.as_deref() == Some(bytes.as_slice()) {
            continue;
        }
        match registry_check(&live, &bytes) {
            Ok(change) if change == "no difference" => report(0, 0),
            Ok(change) => {
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
                    %error,
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
