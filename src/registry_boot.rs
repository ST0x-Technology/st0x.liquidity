//! What a server does with the token-file record it booted: says which boot
//! case it took once logging is up, marks the record running when startup
//! completes, promotes it to last good after it stays up through the soak,
//! and records a clean stop so a deploy or operator restart inside a soak
//! does not count against a pending copy.

use num_traits::ToPrimitive;
use std::time::Duration;

use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info, warn};

use st0x_config::registry_state::{BootOutcome, Booted};

/// Logs the boot case [`st0x_config::claim_boot_tokens`] took. Called once
/// the subscriber exists: nothing can log while the config loads.
pub fn report_boot(outcome: &BootOutcome) {
    match outcome {
        BootOutcome::Inline | BootOutcome::LocalFile => {}
        BootOutcome::NoStateDir { generation } => info!(
            ?generation,
            "token file: read from the bucket; an in-memory database keeps no registry state"
        ),
        BootOutcome::Pinned { record, generation } => info!(
            %record,
            generation,
            "token file: booting the generation the config pins"
        ),
        BootOutcome::Seeded { record, generation } => info!(
            %record,
            generation,
            "token file: no registry state yet; booting the latest bucket copy"
        ),
        BootOutcome::Running {
            record,
            generation,
            discarded,
        } => {
            if let Some(discarded) = discarded {
                error!(
                    %discarded,
                    "token file: the pending record is missing or corrupt; it was dropped"
                );
            }
            info!(%record, generation, "token file: booting the record that ran before");
        }
        BootOutcome::Pending {
            record,
            generation,
            attempt,
        } => info!(
            %record,
            generation,
            attempt,
            "token file: booting an accepted copy"
        ),
        BootOutcome::Fallback {
            record,
            generation,
            failed,
            carried,
        } => error!(
            %record,
            generation,
            %failed,
            ?carried,
            "token file: the accepted copy failed to start twice; booting the last good tables \
             with the listings it added switched off"
        ),
        BootOutcome::FallbackAgain {
            record,
            generation,
            failed,
        } => warn!(
            %record,
            generation,
            %failed,
            "token file: booting the fallback until the failed copy is retried"
        ),
    }
}

/// Startup completed on `booted`: it is what runs now. The returned handle
/// promotes it to last good after `soak`, unless dropped first because the
/// session ended.
pub(crate) fn started(
    booted: &Booted,
    soak: Duration,
) -> Result<AbortOnDropHandle<()>, st0x_config::registry_state::RegistryStateError> {
    if let Some(value) = booted.generation.to_f64() {
        metrics::gauge!("registry_applied_generation").set(value);
    } else {
        error!("registry metric integer cannot be represented as f64");
    }
    if booted.fallback_created {
        metrics::counter!("registry_reloads_total", "result" => "fallback").increment(1);
        metrics::counter!("registry_reloads_total", "result" => "start_failed").increment(1);
        error!(record = %booted.record, "registry accepted copy failed startup; newly created fallback is running");
    }
    if let Ok(manifest) = booted.state.manifest()
        && let Some(outcome) = manifest.last_outcome
    {
        if booted.fallback_created
            && let Some(value) = outcome.at.to_f64()
        {
            metrics::gauge!("registry_last_reload_timestamp_seconds", "result" => "start_failed")
                .set(value);
        }
        if let Some(value) = outcome.at.to_f64() {
            metrics::gauge!("registry_last_reload_timestamp_seconds", "result" => outcome.result)
                .set(value);
        } else {
            error!("registry metric integer cannot be represented as f64");
        }
    }
    booted.state.mark_running(booted.record)?;
    let booted = booted.clone();
    Ok(AbortOnDropHandle::new(tokio::spawn(async move {
        tokio::time::sleep(soak).await;
        match booted.state.promote_running() {
            Ok(Some(running)) => {
                info!(record = %running, "registry running record passed its soak");
            }
            Ok(None) => warn!("registry soak found no running record"),
            Err(error) => error!(
                ?error,
                record = %booted.record,
                "token file: could not promote the booted record"
            ),
        }
    })))
}

/// The session stopped cleanly on `booted`.
pub(crate) fn stopped_cleanly(booted: &Booted, started: bool) {
    let result = if started {
        booted.state.mark_running_clean_exit()
    } else {
        booted.state.mark_clean_exit(booted.record)
    };
    if let Err(error) = result {
        warn!(?error, record = %booted.record, "token file: could not record the clean stop");
    }
}

#[cfg(test)]
mod tests {
    use st0x_config::registry_state::{RecordId, RegistryState};

    use super::*;

    fn booted() -> (tempfile::TempDir, Booted) {
        let dir = tempfile::tempdir().unwrap();
        let state = RegistryState::open(&dir.path().join("registry")).unwrap();
        let record = state.write_record(7, b"source", b"effective").unwrap();
        (
            dir,
            Booted {
                state,
                record,
                generation: 7,
                fallback_created: false,
            },
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_booted_record_runs_at_startup_and_is_promoted_after_its_soak() {
        let (_dir, booted) = booted();

        let soak = started(&booted, Duration::from_secs(600)).unwrap();
        let manifest = booted.state.manifest().unwrap();
        assert_eq!(manifest.running, Some(booted.record));
        assert_eq!(manifest.last_good, None);

        tokio::time::sleep(Duration::from_secs(601)).await;
        soak.await.unwrap();
        assert_eq!(
            booted.state.manifest().unwrap().last_good,
            Some(booted.record)
        );
    }

    /// A session that ends inside the soak drops the handle: a record that
    /// did not stay up is not last good.
    #[tokio::test(start_paused = true)]
    async fn a_session_ending_inside_the_soak_promotes_nothing() {
        let (_dir, booted) = booted();

        drop(started(&booted, Duration::from_secs(600)).unwrap());
        tokio::time::sleep(Duration::from_secs(601)).await;

        assert_eq!(booted.state.manifest().unwrap().last_good, None);
    }

    #[tokio::test(start_paused = true)]
    async fn soak_promotes_an_unchanged_generation_advance() {
        let (_dir, booted) = booted();
        let soak = started(&booted, Duration::from_secs(600)).unwrap();
        let advanced = booted
            .state
            .write_record(8, b"new source", b"effective")
            .unwrap();
        booted.state.mark_running(advanced).unwrap();
        tokio::time::sleep(Duration::from_secs(601)).await;
        soak.await.unwrap();
        assert_eq!(booted.state.manifest().unwrap().last_good, Some(advanced));
    }

    #[test]
    fn startup_cannot_reload_after_running_state_write_fails() {
        let (_dir, booted) = booted();
        std::fs::remove_dir_all(booted.state.dir()).unwrap();
        std::fs::write(booted.state.dir(), b"not a directory").unwrap();
        assert!(matches!(
            started(&booted, Duration::from_secs(600)).unwrap_err(),
            st0x_config::registry_state::RegistryStateError::Io { .. }
        ));
    }

    #[test]
    fn clean_signal_during_startup_marks_the_claimed_pending_copy() {
        let (_dir, booted) = booted();
        let old = booted
            .state
            .write_record(6, b"old", b"old effective")
            .unwrap();
        booted.state.mark_running(old).unwrap();
        stopped_cleanly(&booted, false);
        assert_eq!(
            booted.state.manifest().unwrap().clean_exit,
            Some(booted.record)
        );
    }

    #[test]
    fn a_clean_stop_is_recorded() {
        let (_dir, booted) = booted();
        booted.state.mark_running(booted.record).unwrap();

        stopped_cleanly(&booted, true);

        assert_eq!(
            booted.state.manifest().unwrap().clean_exit,
            Some(RecordId(1))
        );
    }
}
