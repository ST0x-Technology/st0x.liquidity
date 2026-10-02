//! Judges bucket publications before asking for a coordinated process restart.
use num_traits::ToPrimitive;
use st0x_config::{
    Ctx, registry,
    registry_state::{Booted, Outcome, Pending},
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const POLL: Duration = Duration::from_secs(10);
const DEBOUNCE: i64 = 120;
const HOLD_TTL: Duration = Duration::from_secs(15 * 60);

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn report(result: &'static str, at: i64) {
    metrics::counter!("registry_reloads_total", "result" => result).increment(1);
    if let Some(value) = at.to_f64() {
        metrics::gauge!("registry_last_reload_timestamp_seconds", "result" => result).set(value);
    } else {
        error!("registry metric integer cannot be represented as f64");
    }
}

fn held(booted: &Booted) -> bool {
    let path = booted.state.dir().join("hold");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            metrics::gauge!("registry_reload_held_seconds").set(0.0);
            return false;
        }
        Err(error) => {
            warn!(
                ?error,
                "registry deploy hold cannot be read; reload remains held"
            );
            return true;
        }
    };
    let age = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|hold| hold.get("created_at").and_then(serde_json::Value::as_i64))
        .map(|created_at| {
            Duration::from_secs(now().saturating_sub(created_at).max(0).cast_unsigned())
        })
        .or_else(|| {
            warn!("registry deploy hold is malformed; using its modification time");
            std::fs::metadata(&path)
                .ok()?
                .modified()
                .ok()?
                .elapsed()
                .ok()
        });
    metrics::gauge!("registry_reload_held_seconds").set(age.map_or(0.0, |age| age.as_secs_f64()));
    age.is_none_or(|age| age < HOLD_TTL)
}

fn projection(live: &registry::RegistryLive, bytes: &[u8]) -> Result<registry::Carried, String> {
    let fresh = registry::project(&registry::parse(bytes).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?
        .without_retired(&live.static_config);
    for (chain, rows) in &live.live.chain_rows {
        for (symbol, row) in rows {
            let Some(fresh_row) = fresh
                .chain_rows
                .get(chain)
                .and_then(|rows| rows.get(symbol))
            else {
                continue;
            };
            for key in ["tokenized_equity", "tokenized_equity_derivative"] {
                let address = |row: &toml::Table| {
                    row.get(key)
                        .and_then(toml::Value::as_str)
                        .map(str::to_ascii_lowercase)
                };
                if address(row) != address(fresh_row) {
                    return Err(format!("identity changed for {chain}/{symbol}: {key}"));
                }
            }
        }
    }
    Ok(fresh.carry_forward(&live.live))
}

pub(crate) async fn watch(
    ctx: Ctx,
    booted: Booted,
    reload: mpsc::Sender<()>,
    shutdown: CancellationToken,
) {
    let Some(_) = ctx.registry.as_ref() else {
        return;
    };
    let http = match registry::http_client() {
        Ok(http) => http,
        Err(error) => {
            warn!(?error, "registry reload client unavailable");
            return;
        }
    };
    let mut deferred: Option<(u64, i64, u32)> = None;
    loop {
        tokio::select! { () = shutdown.cancelled() => return, () = tokio::time::sleep(POLL) => {} }
        let tick = tokio::select! { () = shutdown.cancelled() => return, result = poll(&ctx, &booted, &http, &mut deferred) => result };
        match tick {
            Ok(true) => {
                if let Err(error) = reload.send(()).await {
                    metrics::counter!("registry_reload_request_errors_total").increment(1);
                    error!(
                        ?error,
                        "registry restart request failed; pending copy remains durable"
                    );
                }
                return;
            }
            Ok(false) => {}
            Err(error) => {
                metrics::counter!("registry_fetch_errors_total").increment(1);
                warn!(?error, "registry reload poll failed");
            }
        }
    }
}

async fn poll(
    ctx: &Ctx,
    booted: &Booted,
    http: &reqwest::Client,
    deferred: &mut Option<(u64, i64, u32)>,
) -> anyhow::Result<bool> {
    let Some(mut live) = ctx.registry.clone() else {
        return Ok(false);
    };
    let held_for_deploy = held(booted);
    let version = match registry::fetch_metadata(http, &live.source.url).await {
        Ok(version) => version,
        Err(error) => {
            if error.copy_is_unusable() {
                metrics::gauge!("registry_invalid").set(1.0);
            }
            return Err(error.into());
        }
    };
    let manifest = booted.state.manifest()?;
    let running = booted
        .state
        .record(manifest.running.unwrap_or(booted.record))?;
    live.live = registry::project(&registry::parse(&running.effective)?)?
        .without_retired(&live.static_config);
    if let Some(value) = running.meta.generation.to_f64() {
        metrics::gauge!("registry_applied_generation").set(value);
    } else {
        error!("registry metric integer cannot be represented as f64");
    }
    if version.generation == running.meta.generation {
        metrics::gauge!("registry_invalid").set(0.0);
        return Ok(false);
    }
    if manifest.refused.contains_key(&version.generation) {
        metrics::gauge!("registry_invalid").set(1.0);
        return Ok(false);
    }
    let timestamp = now();
    if deferred.as_ref().is_some_and(|(generation, until, _)| {
        *generation == version.generation && timestamp < *until
    }) {
        return Ok(false);
    }
    if let Some(pending) = &manifest.pending
        && Some(pending.record) != manifest.running
        && pending.retry_at.is_none_or(|until| timestamp < until)
        && booted.state.record(pending.record)?.meta.generation == version.generation
    {
        return Ok(false);
    }
    let copy = match registry::fetch_version(http, &live.source.url, &version).await {
        Ok(copy) => copy,
        Err(error) => {
            if error.copy_is_unusable() {
                metrics::gauge!("registry_invalid").set(1.0);
            }
            return Err(error.into());
        }
    };
    let Some((candidate, candidate_ctx, effective)) =
        validated_candidate(ctx, booted, &live, version.generation, &copy.bytes)?
    else {
        return Ok(false);
    };
    metrics::gauge!("registry_invalid").set(0.0);
    if let Some(value) = candidate.listings.len().to_f64() {
        metrics::gauge!("registry_carried_forward_symbols").set(value);
    } else {
        error!("registry metric integer cannot be represented as f64");
    }
    if held_for_deploy || live.source.generation.is_some() {
        return Ok(false);
    }
    let unchanged = registry::describe_change(&live.live, &candidate.projection).is_none();
    if !unchanged {
        // `accept` refuses a change until the first record soaks; checking
        // here first spares the external probes on every poll meanwhile.
        if manifest.last_good.is_none() {
            debug!(
                generation = version.generation,
                "registry change waits for the first record to pass its soak"
            );
            return Ok(false);
        }
        if manifest
            .last_applied_at
            .is_some_and(|at| timestamp - at < DEBOUNCE)
        {
            return Ok(false);
        }
        if let Err(reason) = judge_environment(ctx, &candidate_ctx, effective.as_bytes()).await {
            if let crate::conductor::CandidateContractError::Refused(error) = &reason {
                refuse(booted, version.generation, &error.to_string())?;
                return Ok(false);
            }
            defer(
                booted,
                version.generation,
                deferred,
                timestamp,
                &reason.to_string(),
            )?;
            return Ok(false);
        }
    }
    accept(
        booted,
        version.generation,
        &copy.bytes,
        effective.as_bytes(),
        unchanged,
        timestamp,
    )
}

fn validated_candidate(
    ctx: &Ctx,
    booted: &Booted,
    live: &registry::RegistryLive,
    generation: u64,
    bytes: &[u8],
) -> anyhow::Result<Option<(registry::Carried, Ctx, String)>> {
    let candidate = match projection(live, bytes) {
        Ok(candidate) => candidate,
        Err(reason) => {
            refuse(booted, generation, &reason)?;
            return Ok(None);
        }
    };
    let effective = candidate.projection.to_token_file();
    let candidate_ctx = match ctx.registry_candidate(effective.as_bytes()) {
        Ok(candidate) => candidate,
        Err(error) => {
            refuse(booted, generation, &error.to_string())?;
            return Ok(None);
        }
    };
    let chains = |ctx: &Ctx| {
        ctx.chains
            .hedged()
            .map(|chain| chain.chain)
            .collect::<std::collections::BTreeSet<_>>()
    };
    if chains(ctx) != chains(&candidate_ctx) {
        refuse(booted, generation, "hedged chain set changed")?;
        return Ok(None);
    }
    Ok(Some((candidate, candidate_ctx, effective)))
}

fn defer(
    booted: &Booted,
    generation: u64,
    deferred: &mut Option<(u64, i64, u32)>,
    timestamp: i64,
    reason: &str,
) -> anyhow::Result<()> {
    let attempt = deferred
        .as_ref()
        .filter(|(previous, _, _)| *previous == generation)
        .map_or(0, |(_, _, attempt)| attempt.saturating_add(1));
    *deferred = Some((
        generation,
        timestamp + (30_i64 * 2_i64.pow(attempt.min(4))).min(300),
        attempt,
    ));
    report("deferred", timestamp);
    booted.state.update(|manifest| {
        manifest.last_outcome = Some(Outcome {
            result: "deferred".into(),
            at: timestamp,
        });
        Ok(())
    })?;
    warn!(generation = generation, %reason, "registry candidate deferred");
    Ok(())
}

fn accept(
    booted: &Booted,
    generation: u64,
    source: &[u8],
    effective: &[u8],
    unchanged: bool,
    timestamp: i64,
) -> anyhow::Result<bool> {
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(booted.state.dir().join("apply.lock"))?;
    lock.lock()?;
    if held(booted) {
        return Ok(false);
    }
    if !unchanged && booted.state.manifest()?.last_good.is_none() {
        report("deferred", timestamp);
        booted.state.update(|manifest| {
            manifest.last_outcome = Some(Outcome {
                result: "deferred".into(),
                at: timestamp,
            });
            Ok(())
        })?;
        warn!(
            generation,
            "registry candidate deferred until the first record passes its soak"
        );
        return Ok(false);
    }
    let record = booted.state.write_record(generation, source, effective)?;
    booted.state.update(|manifest| {
        if unchanged {
            let soaked = manifest.last_good == manifest.running;
            let running = manifest.running;
            match manifest.pending.as_mut() {
                // The running candidate is still in its soak: it keeps
                // soaking under the newer generation.
                Some(pending) if Some(pending.record) == running => pending.record = record,
                // A failed or unbooted copy is superseded by this newer
                // generation; left behind, boot would revert to its fallback
                // and the deploy gates would keep judging it.
                Some(_) => manifest.pending = None,
                None => {}
            }
            manifest.running = Some(record);
            if soaked {
                manifest.last_good = Some(record);
            }
        } else {
            let fallbacks = manifest
                .pending
                .as_ref()
                .filter(|pending| {
                    booted
                        .state
                        .record(pending.record)
                        .is_ok_and(|record| record.meta.generation == generation)
                })
                .map_or(0, |pending| pending.fallbacks);
            manifest.last_applied_at = Some(timestamp);
            manifest.pending = Some(Pending {
                record,
                attempts: 0,
                last_failure: None,
                fallback: None,
                fallbacks,
                retry_at: None,
            });
        }
        manifest.last_outcome = Some(Outcome {
            result: if unchanged { "unchanged" } else { "applied" }.into(),
            at: timestamp,
        });
        Ok(())
    })?;
    metrics::gauge!("registry_invalid").set(0.0);
    report(if unchanged { "unchanged" } else { "applied" }, timestamp);
    info!(generation = generation, sha256 = %registry::sha256_hex(source), "registry candidate accepted");
    Ok(!unchanged)
}

fn refuse(booted: &Booted, generation: u64, reason: &str) -> anyhow::Result<()> {
    let timestamp = now();
    booted.state.update(|manifest| {
        manifest.refused.insert(generation, reason.into());
        manifest.last_outcome = Some(Outcome {
            result: "refused".into(),
            at: timestamp,
        });
        Ok(())
    })?;
    metrics::gauge!("registry_invalid").set(1.0);
    report("refused", timestamp);
    error!(
        generation,
        reason, "registry candidate refused; running tables retained"
    );
    Ok(())
}

async fn judge_environment(
    running: &Ctx,
    candidate: &Ctx,
    effective: &[u8],
) -> Result<(), crate::conductor::CandidateContractError> {
    crate::conductor::validate_registry_candidate_contracts(running, candidate).await?;
    verify_candidate_approvals(running, effective).await
}

/// Turnkey policies must cover every approval the candidate adds; a
/// private-key build has no policy to consult.
#[cfg(feature = "wallet-turnkey")]
async fn verify_candidate_approvals(
    running: &Ctx,
    effective: &[u8],
) -> Result<(), crate::conductor::CandidateContractError> {
    use crate::conductor::CandidateContractError;
    let inputs = running
        .registry_approval_inputs(effective)
        .map_err(|error| CandidateContractError::Refused(error.into()))?;
    let running_inputs = running
        .registry
        .as_ref()
        .map(|live| {
            running
                .registry_approval_inputs(live.live.to_token_file().as_bytes())
                .map_err(anyhow::Error::from)
        })
        .transpose()
        .map_err(CandidateContractError::Refused)?
        .flatten();
    crate::approval_policy::verify_registry_approval_inputs(running_inputs, inputs)
        .await
        .map_err(|error| CandidateContractError::Deferred(error.into()))?;
    Ok(())
}

#[cfg(not(feature = "wallet-turnkey"))]
fn verify_candidate_approvals(
    _running: &Ctx,
    _effective: &[u8],
) -> std::future::Ready<Result<(), crate::conductor::CandidateContractError>> {
    std::future::ready(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn booted() -> (tempfile::TempDir, Booted) {
        let dir = tempfile::tempdir().unwrap();
        let state =
            st0x_config::registry_state::RegistryState::open(&dir.path().join("registry")).unwrap();
        let record = state.write_record(1, b"source", b"effective").unwrap();
        state.mark_running(record).unwrap();
        state.promote(record).unwrap();
        (
            dir,
            Booted {
                state,
                record,
                generation: 1,
                fallback_created: false,
            },
        )
    }

    #[test]
    fn environmental_failures_back_off_per_generation() {
        let (_dir, booted) = booted();
        let mut deferred = None;
        defer(&booted, 2, &mut deferred, 100, "RPC unavailable").unwrap();
        assert_eq!(deferred, Some((2, 130, 0)));
        defer(&booted, 2, &mut deferred, 130, "RPC unavailable").unwrap();
        assert_eq!(deferred, Some((2, 190, 1)));
        defer(&booted, 3, &mut deferred, 140, "RPC unavailable").unwrap();
        assert_eq!(deferred, Some((3, 170, 0)));
        assert_eq!(
            booted.state.manifest().unwrap().running,
            Some(booted.record)
        );
    }

    #[test]
    fn changed_copy_waits_for_initial_last_good() {
        let (_dir, booted) = booted();
        booted
            .state
            .update(|manifest| {
                manifest.last_good = None;
                Ok(())
            })
            .unwrap();
        assert!(!accept(&booted, 2, b"new source", b"new effective", false, now()).unwrap());
        let manifest = booted.state.manifest().unwrap();
        assert_eq!(manifest.running, Some(booted.record));
        assert!(manifest.pending.is_none());
        assert_eq!(manifest.last_outcome.unwrap().result, "deferred");
    }

    #[test]
    fn accepted_change_is_durable_before_requesting_restart() {
        let (_dir, booted) = booted();
        assert!(accept(&booted, 2, b"new source", b"new effective", false, now()).unwrap());
        let manifest = booted.state.manifest().unwrap();
        assert_eq!(manifest.running, Some(booted.record));
        let pending = manifest.pending.unwrap();
        assert_eq!(
            booted.state.record(pending.record).unwrap().effective,
            b"new effective"
        );
    }

    #[test]
    fn retrying_a_failed_generation_preserves_its_fallback_backoff() {
        let (_dir, booted) = booted();
        let failed = booted
            .state
            .write_record(2, b"candidate", b"effective")
            .unwrap();
        booted
            .state
            .update(|manifest| {
                manifest.pending = Some(Pending {
                    record: failed,
                    attempts: 2,
                    last_failure: Some(st0x_config::registry_state::PendingFailure::StartFailed),
                    fallback: Some(booted.record),
                    fallbacks: 3,
                    retry_at: Some(0),
                });
                Ok(())
            })
            .unwrap();
        assert!(accept(&booted, 2, b"candidate", b"effective", false, now()).unwrap());
        let pending = booted.state.manifest().unwrap().pending.unwrap();
        assert_eq!(pending.attempts, 0);
        assert_eq!(pending.fallbacks, 3);
    }

    #[test]
    fn refused_change_keeps_running_record() {
        let (_dir, booted) = booted();
        refuse(&booted, 2, "invalid candidate").unwrap();
        let manifest = booted.state.manifest().unwrap();
        assert_eq!(manifest.running, Some(booted.record));
        assert!(manifest.pending.is_none());
        assert_eq!(manifest.refused[&2], "invalid candidate");
    }

    #[test]
    fn deploy_hold_prevents_acceptance() {
        let (_dir, booted) = booted();
        std::fs::write(
            booted.state.dir().join("hold"),
            format!(r#"{{"deploy_id":"test","created_at":{}}}"#, now()),
        )
        .unwrap();
        assert!(!accept(&booted, 2, b"new source", b"new effective", false, now()).unwrap());
        assert!(booted.state.manifest().unwrap().pending.is_none());
    }

    #[test]
    fn malformed_deploy_hold_blocks_acceptance() {
        let (_dir, booted) = booted();
        std::fs::write(booted.state.dir().join("hold"), b"incomplete JSON").unwrap();
        assert!(!accept(&booted, 2, b"new", b"changed", false, now()).unwrap());
        assert_eq!(booted.state.manifest().unwrap().pending, None);
    }

    #[test]
    fn unchanged_generation_during_soak_advances_pending_without_promoting() {
        let (_dir, booted) = booted();
        booted
            .state
            .update(|manifest| {
                manifest.last_good = None;
                manifest.pending = Some(Pending {
                    record: booted.record,
                    attempts: 1,
                    last_failure: None,
                    fallback: None,
                    fallbacks: 0,
                    retry_at: None,
                });
                Ok(())
            })
            .unwrap();
        assert!(!accept(&booted, 2, b"new source", b"effective", true, now()).unwrap());
        let manifest = booted.state.manifest().unwrap();
        assert_ne!(manifest.running, Some(booted.record));
        assert_eq!(manifest.pending.unwrap().record, manifest.running.unwrap());
        assert!(manifest.last_good.is_none());
    }

    /// After a fallback, a newer generation with the running tables
    /// supersedes the failed copy, so neither boot nor the deploy gates go
    /// back to the older generation.
    #[tokio::test]
    async fn newer_unchanged_generation_after_fallback_supersedes_the_failed_copy() {
        let (dir, booted) = booted();
        let failed = booted
            .state
            .write_record(2, b"candidate", b"failed effective")
            .unwrap();
        let fallback = booted
            .state
            .write_record(1, b"source", b"effective")
            .unwrap();
        booted
            .state
            .update(|manifest| {
                manifest.running = Some(fallback);
                manifest.pending = Some(Pending {
                    record: failed,
                    attempts: 2,
                    last_failure: Some(st0x_config::registry_state::PendingFailure::StartFailed),
                    fallback: Some(fallback),
                    fallbacks: 1,
                    retry_at: Some(now() + 1_800),
                });
                Ok(())
            })
            .unwrap();

        assert!(!accept(&booted, 3, b"newer source", b"effective", true, now()).unwrap());

        let manifest = booted.state.manifest().unwrap();
        assert_eq!(manifest.pending, None);
        let newer = manifest.running.unwrap();
        assert_eq!(booted.state.record(newer).unwrap().meta.generation, 3);
        assert_eq!(
            st0x_config::registry_state::gate_effective(booted.state.dir(), &toml::Table::new())
                .unwrap(),
            vec![b"effective".to_vec()]
        );
        let config: toml::Table = toml::from_str(&format!(
            "database_url = \"sqlite://{}/bot.db\"\n[registry]\nurl = \"gs://bucket/tokens.toml\"\n",
            dir.path().display()
        ))
        .unwrap();
        let claim = st0x_config::registry_state::claim_for_boot(&config, None, now())
            .await
            .unwrap();
        assert_eq!(claim.booted.unwrap().record, newer);
    }

    fn live() -> registry::RegistryLive {
        let bytes = include_bytes!("../tests/fixtures/tokens-staging.toml");
        registry::RegistryLive::for_test(
            registry::RegistrySource {
                url: "gs://bucket/tokens.toml".into(),
                generation: None,
            },
            toml::Table::new(),
            registry::project(&registry::parse(bytes).unwrap()).unwrap(),
        )
    }

    #[test]
    fn valid_publication_preserves_the_projected_tables() {
        let live = live();
        let accepted = projection(
            &live,
            include_bytes!("../tests/fixtures/tokens-staging.toml"),
        )
        .unwrap();
        assert!(registry::describe_change(&live.live, &accepted.projection).is_none());
    }

    #[test]
    fn invalid_publication_leaves_running_tables_untouched() {
        let live = live();
        let before = live.live.clone();
        assert!(projection(&live, b"schema_version = 999").is_err());
        assert_eq!(live.live, before);
    }

    #[test]
    fn removed_listings_are_retained_with_admission_disabled() {
        let live = live();
        let mut fresh = live.live.clone();
        let (chain, rows) = fresh.chain_rows.iter_mut().next().unwrap();
        let chain = chain.clone();
        let symbol = rows.keys().next().unwrap().clone();
        rows.remove(&symbol);
        let accepted = projection(&live, fresh.to_token_file().as_bytes()).unwrap();
        assert!(accepted.listings.contains(&format!("{chain}/{symbol}")));
        let row = &accepted.projection.chain_rows[&chain][&symbol];
        for switch in ["trading", "rebalancing", "wrapped_equity_recovery"] {
            assert_eq!(row[switch].as_str(), Some("disabled"));
        }
    }

    #[test]
    fn changing_an_existing_contract_is_refused() {
        let live = live();
        let mut fresh = live.live.clone();
        let row = fresh
            .chain_rows
            .values_mut()
            .next()
            .unwrap()
            .values_mut()
            .next()
            .unwrap();
        row.insert(
            "tokenized_equity".into(),
            toml::Value::String("0x0000000000000000000000000000000000000001".into()),
        );
        assert!(
            projection(&live, fresh.to_token_file().as_bytes())
                .unwrap_err()
                .contains("identity changed")
        );
    }
}
