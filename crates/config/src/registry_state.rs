//! The token-file copies this host has run, kept on its data disk.
//!
//! A server boots from a record here, not from whatever the bucket holds at
//! that moment, so a restart runs exactly what was judged and a bucket
//! outage or a bad publish cannot stop a restart. The state lives in
//! `<directory of the database file>/registry/`:
//!
//! - `records/<n>/`: one immutable record per accepted copy, `n` rising.
//!   `source.toml` holds the bucket bytes, `effective.toml` the per-symbol
//!   tables that run (a token file with only the bot's keys, carried rows
//!   included) and `meta.json` the generation and both SHA-256 digests.
//! - `state.json`: the manifest. `running` is what the live process booted,
//!   `last_good` the last record that stayed up through its soak, and
//!   `pending` an accepted copy waiting for its boot.
//!
//! Only the server writes here, and only through one [`RegistryState`]
//! handle, so writes cannot race. Every file is written to a temporary
//! name, synced and renamed, and a record is complete before the manifest
//! names it. The CLI and the deploy gates only read.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteConnectOptions;
use thiserror::Error;
use toml::Table;

use crate::registry::{self, RegistryError, sha256_hex};

/// How long a booted record must stay up before it becomes `last_good`. A
/// copy that starts and then crashes inside this window falls back.
pub const SOAK: Duration = Duration::from_secs(10 * 60);

/// Boots a pending record may take before the server falls back.
pub const MAX_BOOT_ATTEMPTS: u32 = 2;

/// How long after its second failed boot a pending record is tried again.
/// Each further fallback doubles it.
pub const FIRST_RETRY_BACKOFF: Duration = Duration::from_secs(30 * 60);

/// Records kept besides the ones the manifest names.
const KEPT_RECORDS: usize = 20;

/// Where the state for a database lives: `registry/` beside the database
/// file, named as SQLx decodes it. `None` for an in-memory database
/// (`:memory:` or `mode=memory`), which has no disk to keep it on.
pub fn state_dir(database_url: &str) -> Option<PathBuf> {
    let path = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite:"))
        .unwrap_or(database_url);
    let (path, params) = path
        .split_once('?')
        .map_or((path, None), |(path, params)| (path, Some(params)));
    let in_memory = params.is_some_and(|params| {
        url::form_urlencoded::parse(params.as_bytes())
            .any(|(key, value)| key == "mode" && value == "memory")
    });
    if path.is_empty() || path == ":memory:" || in_memory {
        return None;
    }
    let options = SqliteConnectOptions::from_str(database_url).ok()?;
    let parent = options.get_filename().parent()?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    Some(parent.join("registry"))
}

/// A record's number. Numbers only rise, so no record is ever rewritten.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordId(pub u64);

impl std::fmt::Display for RecordId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(id) = self;
        write!(formatter, "{id}")
    }
}

/// What `meta.json` says about a record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMeta {
    /// The bucket generation the source came from.
    pub generation: u64,
    pub source_sha256: String,
    pub effective_sha256: String,
    /// Unix seconds.
    pub created_at: i64,
}

/// One record, its digests checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub id: RecordId,
    pub meta: RecordMeta,
    pub source: Vec<u8>,
    pub effective: Vec<u8>,
}

/// Why a pending record is not booting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingFailure {
    /// It used every boot attempt without staying up through its soak.
    StartFailed,
}

/// An accepted copy waiting for its boot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub record: RecordId,
    /// Boots claimed for it that did not end in a clean exit.
    pub attempts: u32,
    pub last_failure: Option<PendingFailure>,
    /// The record booted instead once the attempts ran out.
    pub fallback: Option<RecordId>,
    /// How many times it fell back; sets the retry backoff.
    pub fallbacks: u32,
    /// Unix seconds before which it is not tried again.
    pub retry_at: Option<i64>,
}

/// The result of the latest reload decision, for the metrics a restart
/// restores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub result: String,
    /// Unix seconds.
    pub at: i64,
}

/// `state.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Raised on every write.
    pub version: u64,
    pub running: Option<RecordId>,
    pub last_good: Option<RecordId>,
    pub pending: Option<Pending>,
    /// Generations the judge refused, with the reason.
    #[serde(default)]
    pub refused: BTreeMap<u64, String>,
    pub last_outcome: Option<Outcome>,
    #[serde(default)]
    pub last_applied_at: Option<i64>,
    /// The record a process stopped cleanly on (a signal or a reload), so
    /// the next boot does not count it as a failed attempt.
    pub clean_exit: Option<RecordId>,
}

#[derive(Debug, Error)]
pub enum RegistryStateError {
    #[error("{}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} is not valid JSON of its kind", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "registry record {record}: {file} is missing, unreadable or does not match the SHA-256 \
         in meta.json; the record is corrupt. Move the registry state directory aside to boot \
         from the bucket"
    )]
    Corrupt {
        record: RecordId,
        file: &'static str,
    },
    #[error("the manifest names record {record}, which is not on disk")]
    MissingRecord { record: RecordId },
    #[error(
        "pending record {pending} failed to boot and there is no running or last good record \
         to fall back to"
    )]
    NoFallbackBase { pending: RecordId },
    #[error("the registry state lock is poisoned")]
    Poisoned,
    #[error(transparent)]
    Registry(#[from] RegistryError),
}

/// The one handle through which a server reads and writes the state.
/// Clones share its lock.
#[derive(Debug, Clone)]
pub struct RegistryState {
    dir: PathBuf,
    writer: Arc<Mutex<()>>,
}

impl RegistryState {
    /// Opens the state in `dir`, creating the directory when absent.
    pub fn open(dir: &Path) -> Result<Self, RegistryStateError> {
        let records = dir.join("records");
        std::fs::create_dir_all(&records).map_err(|source| io(&records, source))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            writer: Arc::new(Mutex::new(())),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The manifest; a default one when none was written yet.
    pub fn manifest(&self) -> Result<Manifest, RegistryStateError> {
        read_manifest(&self.dir)
    }

    /// One record, its digests checked.
    pub fn record(&self, id: RecordId) -> Result<Record, RegistryStateError> {
        read_record(&self.dir, id)
    }

    /// Reads, changes and writes the manifest under the writer lock. The
    /// version rises on every write.
    pub fn update<Changed>(
        &self,
        change: impl FnOnce(&mut Manifest) -> Result<Changed, RegistryStateError>,
    ) -> Result<Changed, RegistryStateError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RegistryStateError::Poisoned)?;
        let mut manifest = read_manifest(&self.dir)?;
        let changed = change(&mut manifest)?;
        manifest.version += 1;
        write_manifest(&self.dir, &manifest)?;
        Ok(changed)
    }

    /// Writes a new record and returns its number. The record is complete
    /// and synced before this returns, so a manifest may then name it.
    pub fn write_record(
        &self,
        generation: u64,
        source: &[u8],
        effective: &[u8],
    ) -> Result<RecordId, RegistryStateError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RegistryStateError::Poisoned)?;
        write_record(&self.dir, generation, source, effective)
    }

    /// The booted record reached startup: it is what runs now.
    pub fn mark_running(&self, record: RecordId) -> Result<(), RegistryStateError> {
        self.update(|manifest| {
            manifest.running = Some(record);
            Ok(())
        })
    }

    /// The booted record stayed up through its soak. It becomes `last_good`
    /// when it still runs, and a pending copy it was clears.
    pub fn promote(&self, record: RecordId) -> Result<Promotion, RegistryStateError> {
        let promotion = self.update(|manifest| {
            if manifest.running != Some(record) {
                return Ok(Promotion::Superseded {
                    running: manifest.running,
                });
            }
            manifest.last_good = Some(record);
            if manifest
                .pending
                .as_ref()
                .is_some_and(|pending| pending.record == record)
            {
                manifest.pending = None;
            }
            Ok(Promotion::LastGood)
        })?;
        self.collect_garbage()?;
        Ok(promotion)
    }

    /// Promotes the current running record in the same transaction that
    /// reads its identity, including an unchanged generation advance.
    pub fn promote_running(&self) -> Result<Option<RecordId>, RegistryStateError> {
        let promoted = self.update(|manifest| {
            let Some(running) = manifest.running else {
                return Ok(None);
            };
            manifest.last_good = Some(running);
            if manifest
                .pending
                .as_ref()
                .is_some_and(|pending| pending.record == running)
            {
                manifest.pending = None;
            }
            Ok(Some(running))
        })?;
        self.collect_garbage()?;
        Ok(promoted)
    }

    /// Marks the current running record clean in one writer transaction.
    pub fn mark_running_clean_exit(&self) -> Result<(), RegistryStateError> {
        self.update(|manifest| {
            manifest.clean_exit = manifest.running;
            Ok(())
        })
    }

    /// The process is stopping cleanly on `record`: a deploy or operator
    /// restart inside a soak must not count against a pending copy.
    pub fn mark_clean_exit(&self, record: RecordId) -> Result<(), RegistryStateError> {
        self.update(|manifest| {
            manifest.clean_exit = Some(record);
            Ok(())
        })
    }

    /// Removes the oldest records the manifest does not name, keeping
    /// [`KEPT_RECORDS`] besides.
    fn collect_garbage(&self) -> Result<(), RegistryStateError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RegistryStateError::Poisoned)?;
        let manifest = read_manifest(&self.dir)?;
        let named: BTreeSet<RecordId> = [
            manifest.running,
            manifest.last_good,
            manifest.pending.as_ref().map(|pending| pending.record),
            manifest
                .pending
                .as_ref()
                .and_then(|pending| pending.fallback),
            manifest.clean_exit,
        ]
        .into_iter()
        .flatten()
        .collect();
        let ids = record_ids(&self.dir)?;
        let unnamed: Vec<RecordId> = ids
            .into_iter()
            .rev()
            .filter(|id| !named.contains(id))
            .skip(KEPT_RECORDS)
            .collect();
        for id in unnamed {
            let path = record_dir(&self.dir, id);
            std::fs::remove_dir_all(&path).map_err(|source| io(&path, source))?;
        }
        Ok(())
    }
}

/// What [`RegistryState::promote`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Promotion {
    LastGood,
    /// Another record runs now; nothing moved.
    Superseded {
        running: Option<RecordId>,
    },
}

/// The per-symbol tables the running server uses, from the state in `dir`,
/// without writing anything. `None` when no record runs yet.
pub fn running_effective(dir: &Path) -> Result<Option<Vec<u8>>, RegistryStateError> {
    if !dir.join(MANIFEST).is_file() {
        return Ok(None);
    }
    let manifest = read_manifest(dir)?;
    let Some(record) = manifest.running.or(manifest.last_good) else {
        return Ok(None);
    };
    read_record(dir, record).map(|record| Some(record.effective))
}

/// Reads every copy a deploy must validate, without changing the manifest.
/// Pending is checked alongside the fallback that would keep its added listings.
pub fn gate_effective(dir: &Path, config: &Table) -> Result<Vec<Vec<u8>>, RegistryStateError> {
    if !dir.join(MANIFEST).is_file() {
        return Ok(Vec::new());
    }
    let manifest = read_manifest(dir)?;
    let Some(pending) = manifest.pending.clone() else {
        return running_effective(dir).map(|bytes| bytes.into_iter().collect());
    };
    let candidate = read_record(dir, pending.record)?;
    let fallback = match pending.fallback.map(|id| read_record(dir, id)) {
        Some(Ok(fallback)) => fallback.effective,
        None => fallback_tables(dir, &manifest, &candidate, None, config)?.effective,
        Some(Err(
            RegistryStateError::Corrupt { record, .. }
            | RegistryStateError::MissingRecord { record },
        )) => fallback_tables(dir, &manifest, &candidate, Some(record), config)?.effective,
        Some(Err(error)) => return Err(error),
    };
    Ok(vec![candidate.effective, fallback])
}

/// How the server boots its per-symbol tables, from [`claim_for_boot`].
#[derive(Debug)]
pub struct BootClaim {
    /// The token file to boot, `None` when the config carries its tables
    /// inline.
    pub tokens: Option<Vec<u8>>,
    /// The record booted, for promotion. `None` when no state is kept.
    pub booted: Option<Booted>,
    pub outcome: BootOutcome,
}

/// The record a server booted, and the state it came from.
#[derive(Debug, Clone)]
pub struct Booted {
    pub state: RegistryState,
    pub record: RecordId,
    pub generation: u64,
    /// This boot created a fallback, rather than reusing an existing one.
    pub fallback_created: bool,
}

/// Which case boot took. Logged once logging is up: nothing can log while
/// the config loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootOutcome {
    /// The config carries its per-symbol tables inline.
    Inline,
    /// `--registry-file`: a local copy, no state.
    LocalFile,
    /// An in-memory database: the bucket copy, no state.
    NoStateDir { generation: Option<u64> },
    /// No state yet: the latest bucket copy, now recorded.
    Seeded { record: RecordId, generation: u64 },
    /// The record that ran before. `discarded` names a pending record
    /// dropped because it was missing or corrupt.
    Running {
        record: RecordId,
        generation: u64,
        discarded: Option<RecordId>,
    },
    /// An accepted copy's boot.
    Pending {
        record: RecordId,
        generation: u64,
        attempt: u32,
    },
    /// A pending copy used its attempts; this boots the last good tables
    /// with the listings only it added switched off.
    Fallback {
        record: RecordId,
        generation: u64,
        failed: RecordId,
        /// `chain/SYMBOL` kept switched off from the failed copy.
        carried: BTreeSet<String>,
    },
    /// The fallback an earlier boot built, until the pending copy is
    /// retried.
    FallbackAgain {
        record: RecordId,
        generation: u64,
        failed: RecordId,
    },
}

/// Chooses and claims the token file a server boots. Only the server calls
/// this; it is the one boot step that writes state.
///
/// `--registry-file` and an in-memory database bypass the state. Otherwise,
/// in order: a pending copy with attempts left (one more attempt charged,
/// unless the last process exited cleanly on it); a pending copy out of
/// attempts, which boots a fallback built from the last good tables; the
/// record that ran before; and, with no state at all, the latest bucket
/// copy. A deploy hold is left in place: the activation that wrote it
/// removes it.
pub async fn claim_for_boot(
    config: &Table,
    registry_file: Option<&Path>,
    now: i64,
) -> Result<BootClaim, RegistryStateError> {
    let Some(source) = registry::source_of(config)? else {
        return Ok(BootClaim {
            tokens: None,
            booted: None,
            outcome: BootOutcome::Inline,
        });
    };
    if let Some(path) = registry_file {
        let bytes = registry::load_bytes(&source, Some(path)).await?;
        return Ok(BootClaim {
            tokens: Some(bytes),
            booted: None,
            outcome: BootOutcome::LocalFile,
        });
    }
    let dir = config
        .get("database_url")
        .and_then(toml::Value::as_str)
        .and_then(state_dir);
    let Some(dir) = dir else {
        let copy = registry::load_copy(&source).await?;
        return Ok(BootClaim {
            tokens: Some(copy.bytes),
            booted: None,
            outcome: BootOutcome::NoStateDir {
                generation: Some(copy.generation),
            },
        });
    };
    let state = RegistryState::open(&dir)?;
    claim_with(&state, config, now, || registry::load_copy(&source)).await
}

/// [`claim_for_boot`] once the state is open, with the bucket read
/// injected so tests can see whether boot touched the bucket.
async fn claim_with<Load, Loading>(
    state: &RegistryState,
    config: &Table,
    now: i64,
    load: Load,
) -> Result<BootClaim, RegistryStateError>
where
    Load: FnOnce() -> Loading,
    Loading: Future<Output = Result<registry::TokenCopy, RegistryError>>,
{
    let choice = state.update(|manifest| choose(state, manifest, config, now))?;
    let (record, outcome) = if let Some(chosen) = choice {
        chosen
    } else {
        let copy = load().await?;
        let record = record_for_copy(state, &copy)?;
        let outcome = BootOutcome::Seeded {
            record: record.id,
            generation: copy.generation,
        };
        (record, outcome)
    };
    Ok(boot(state, &record, outcome))
}

fn boot(state: &RegistryState, record: &Record, outcome: BootOutcome) -> BootClaim {
    BootClaim {
        tokens: Some(record.effective.clone()),
        booted: Some(Booted {
            state: state.clone(),
            record: record.id,
            generation: record.meta.generation,
            fallback_created: matches!(outcome, BootOutcome::Fallback { .. }),
        }),
        outcome,
    }
}

/// The boot case for the manifest as it stands, charging the attempt or
/// persisting the fallback it takes. `None` when there is no state.
fn choose(
    state: &RegistryState,
    manifest: &mut Manifest,
    config: &Table,
    now: i64,
) -> Result<Option<(Record, BootOutcome)>, RegistryStateError> {
    let clean_exit = manifest.clean_exit.take();
    let Some(mut pending) = manifest.pending.clone() else {
        return boot_running(state, manifest, None);
    };
    let failed = match read_record(&state.dir, pending.record) {
        Ok(failed) => failed,
        Err(RegistryStateError::Corrupt { .. } | RegistryStateError::MissingRecord { .. }) => {
            manifest.pending = None;
            return boot_running(state, manifest, Some(pending.record));
        }
        Err(error) => return Err(error),
    };
    if pending.fallback.is_some() && pending.retry_at.is_some_and(|until| now >= until) {
        pending = Pending {
            attempts: 0,
            fallback: None,
            retry_at: None,
            ..pending
        };
        manifest.pending = Some(pending.clone());
    }
    if let Some(fallback) = pending.fallback {
        match read_record(&state.dir, fallback) {
            Ok(record) => {
                let outcome = BootOutcome::FallbackAgain {
                    record: record.id,
                    generation: record.meta.generation,
                    failed: pending.record,
                };
                return Ok(Some((record, outcome)));
            }
            Err(RegistryStateError::Corrupt { .. } | RegistryStateError::MissingRecord { .. }) => {
                let (record, carried) =
                    write_fallback(state, manifest, &failed, Some(fallback), config)?;
                manifest.pending = Some(Pending {
                    fallback: Some(record.id),
                    ..pending
                });
                manifest.running = Some(record.id);
                let outcome = BootOutcome::Fallback {
                    record: record.id,
                    generation: record.meta.generation,
                    failed: failed.id,
                    carried,
                };
                return Ok(Some((record, outcome)));
            }
            Err(error) => return Err(error),
        }
    }
    if pending.attempts < MAX_BOOT_ATTEMPTS || clean_exit == Some(pending.record) {
        let attempts = if clean_exit == Some(pending.record) {
            pending.attempts
        } else {
            pending.attempts + 1
        };
        manifest.pending = Some(Pending {
            attempts,
            ..pending
        });
        let outcome = BootOutcome::Pending {
            record: failed.id,
            generation: failed.meta.generation,
            attempt: attempts,
        };
        return Ok(Some((failed, outcome)));
    }

    let (record, carried) = write_fallback(state, manifest, &failed, None, config)?;
    let backoff = (FIRST_RETRY_BACKOFF.as_secs() << pending.fallbacks.min(8)).cast_signed();
    manifest.pending = Some(Pending {
        last_failure: Some(PendingFailure::StartFailed),
        fallback: Some(record.id),
        fallbacks: pending.fallbacks + 1,
        retry_at: Some(now + backoff),
        ..pending
    });
    manifest.running = Some(record.id);
    manifest.last_outcome = Some(Outcome {
        result: "fallback".into(),
        at: now,
    });
    let outcome = BootOutcome::Fallback {
        record: record.id,
        generation: record.meta.generation,
        failed: failed.id,
        carried,
    };
    Ok(Some((record, outcome)))
}

/// Writes the fallback for a failed pending record: the last good (or
/// running) tables with the listings only the failed copy added carried in
/// switched off, so durable references to them still resolve. `damaged`
/// names an earlier fallback that no longer reads back, which cannot serve
/// as the base.
fn write_fallback(
    state: &RegistryState,
    manifest: &Manifest,
    failed: &Record,
    damaged: Option<RecordId>,
    config: &Table,
) -> Result<(Record, BTreeSet<String>), RegistryStateError> {
    let FallbackTables {
        base,
        effective,
        carried,
    } = fallback_tables(&state.dir, manifest, failed, damaged, config)?;
    let id = write_record(&state.dir, base.meta.generation, &base.source, &effective)?;
    Ok((read_record(&state.dir, id)?, carried))
}

/// The tables a fallback for `failed` holds, computed without writing, so
/// boot and the deploy gates build the same fallback.
struct FallbackTables {
    base: Record,
    effective: Vec<u8>,
    /// `chain/SYMBOL` carried in switched off from the failed copy.
    carried: BTreeSet<String>,
}

fn fallback_tables(
    dir: &Path,
    manifest: &Manifest,
    failed: &Record,
    damaged: Option<RecordId>,
    config: &Table,
) -> Result<FallbackTables, RegistryStateError> {
    let Some(base) = [manifest.last_good, manifest.running]
        .into_iter()
        .flatten()
        .find(|id| *id != failed.id && Some(*id) != damaged)
    else {
        return Err(RegistryStateError::NoFallbackBase { pending: failed.id });
    };
    let base = read_record(dir, base)?;
    let carried = registry::project(&registry::parse(&base.effective)?)?
        .carry_forward(&registry::project(&registry::parse(&failed.effective)?)?);
    let effective = carried
        .projection
        .without_retired(config)
        .to_token_file()
        .into_bytes();
    Ok(FallbackTables {
        base,
        effective,
        carried: carried.listings,
    })
}

/// Boots the record that ran before, `None` when there is none.
fn boot_running(
    state: &RegistryState,
    manifest: &Manifest,
    discarded: Option<RecordId>,
) -> Result<Option<(Record, BootOutcome)>, RegistryStateError> {
    let mut corrupt = None;
    for id in [manifest.running, manifest.last_good]
        .into_iter()
        .flatten()
        .filter(|id| Some(*id) != discarded)
    {
        let record = match read_record(&state.dir, id) {
            Ok(record) => record,
            Err(
                error @ (RegistryStateError::Corrupt { .. }
                | RegistryStateError::MissingRecord { .. }),
            ) => {
                corrupt = Some(error);
                continue;
            }
            Err(error) => return Err(error),
        };
        let outcome = BootOutcome::Running {
            record: record.id,
            generation: record.meta.generation,
            discarded,
        };
        return Ok(Some((record, outcome)));
    }
    if let Some(error) = corrupt {
        return Err(error);
    }
    if let Some(pending) = discarded {
        return Err(RegistryStateError::NoFallbackBase { pending });
    }
    Ok(None)
}

/// The record for a bucket copy: the running, last good or newest one when
/// it holds exactly these bytes of this generation, a new one otherwise.
/// Reusing the newest keeps a crash loop before `mark_running` from adding
/// a record per boot, and a new write collects garbage at once since such a
/// process never reaches the soak that otherwise does.
fn record_for_copy(
    state: &RegistryState,
    copy: &registry::TokenCopy,
) -> Result<Record, RegistryStateError> {
    let manifest = state.manifest()?;
    let source_sha256 = sha256_hex(&copy.bytes);
    let newest = record_ids(&state.dir)?.last().copied();
    for id in [manifest.running, manifest.last_good, newest]
        .into_iter()
        .flatten()
    {
        let record = match read_record(&state.dir, id) {
            Ok(record) => record,
            Err(RegistryStateError::Corrupt { .. } | RegistryStateError::MissingRecord { .. })
                if Some(id) == newest
                    && Some(id) != manifest.running
                    && Some(id) != manifest.last_good =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if record.meta.generation == copy.generation && record.meta.source_sha256 == source_sha256 {
            return Ok(record);
        }
    }
    let effective = registry::project(&registry::parse(&copy.bytes)?)?.to_token_file();
    let id = state.write_record(copy.generation, &copy.bytes, effective.as_bytes())?;
    state.collect_garbage()?;
    read_record(&state.dir, id)
}

const MANIFEST: &str = "state.json";
const SOURCE: &str = "source.toml";
const EFFECTIVE: &str = "effective.toml";
const META: &str = "meta.json";

fn io(path: &Path, source: std::io::Error) -> RegistryStateError {
    RegistryStateError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn record_dir(dir: &Path, RecordId(id): RecordId) -> PathBuf {
    dir.join("records").join(id.to_string())
}

fn read_manifest(dir: &Path) -> Result<Manifest, RegistryStateError> {
    let path = dir.join(MANIFEST);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|source| RegistryStateError::Json { path, source }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
        Err(source) => Err(io(&path, source)),
    }
}

fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<(), RegistryStateError> {
    let path = dir.join(MANIFEST);
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|source| RegistryStateError::Json {
        path: path.clone(),
        source,
    })?;
    let temporary = dir.join(format!(".{MANIFEST}.tmp"));
    write_synced(&temporary, &bytes)?;
    std::fs::rename(&temporary, &path).map_err(|source| io(&path, source))?;
    sync_dir(dir)
}

fn read_record(dir: &Path, id: RecordId) -> Result<Record, RegistryStateError> {
    let path = record_dir(dir, id);
    if !path.is_dir() {
        return Err(RegistryStateError::MissingRecord { record: id });
    }
    let read = |name: &'static str| {
        let file = path.join(name);
        std::fs::read(&file).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound => RegistryStateError::Corrupt {
                record: id,
                file: name,
            },
            _ => io(&file, source),
        })
    };
    let meta: RecordMeta =
        serde_json::from_slice(&read(META)?).map_err(|_| RegistryStateError::Corrupt {
            record: id,
            file: META,
        })?;
    let source = read(SOURCE)?;
    let effective = read(EFFECTIVE)?;
    if sha256_hex(&source) != meta.source_sha256 {
        return Err(RegistryStateError::Corrupt {
            record: id,
            file: SOURCE,
        });
    }
    if sha256_hex(&effective) != meta.effective_sha256 {
        return Err(RegistryStateError::Corrupt {
            record: id,
            file: EFFECTIVE,
        });
    }
    Ok(Record {
        id,
        meta,
        source,
        effective,
    })
}

/// Every record number on disk, rising. Temporary directories of an
/// interrupted write are not records.
fn record_ids(dir: &Path) -> Result<Vec<RecordId>, RegistryStateError> {
    let records = dir.join("records");
    let entries = std::fs::read_dir(&records).map_err(|source| io(&records, source))?;
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| io(&records, source))?;
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
        {
            ids.push(RecordId(id));
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn write_record(
    dir: &Path,
    generation: u64,
    source: &[u8],
    effective: &[u8],
) -> Result<RecordId, RegistryStateError> {
    let records = dir.join("records");
    let id = RecordId(record_ids(dir)?.last().map_or(1, |RecordId(last)| last + 1));
    let meta = RecordMeta {
        generation,
        source_sha256: sha256_hex(source),
        effective_sha256: sha256_hex(effective),
        created_at: chrono::Utc::now().timestamp(),
    };
    let temporary = records.join(format!(".{id}.tmp"));
    if temporary.exists() {
        std::fs::remove_dir_all(&temporary).map_err(|source| io(&temporary, source))?;
    }
    std::fs::create_dir(&temporary).map_err(|error| io(&temporary, error))?;
    let meta_bytes =
        serde_json::to_vec_pretty(&meta).map_err(|source| RegistryStateError::Json {
            path: temporary.join(META),
            source,
        })?;
    write_synced(&temporary.join(SOURCE), source)?;
    write_synced(&temporary.join(EFFECTIVE), effective)?;
    write_synced(&temporary.join(META), &meta_bytes)?;
    sync_dir(&temporary)?;
    let path = record_dir(dir, id);
    std::fs::rename(&temporary, &path).map_err(|source| io(&path, source))?;
    sync_dir(&records)?;
    Ok(id)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), RegistryStateError> {
    let mut file = File::create(path).map_err(|source| io(path, source))?;
    file.write_all(bytes).map_err(|source| io(path, source))?;
    file.sync_all().map_err(|source| io(path, source))
}

fn sync_dir(dir: &Path) -> Result<(), RegistryStateError> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| io(dir, source))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use toml::Value;

    use super::*;
    use crate::registry::{Projection, TokenCopy, fixtures};

    fn staging() -> Vec<u8> {
        fixtures::read("tokens-staging.toml")
    }

    fn projection(bytes: &[u8]) -> Projection {
        registry::project(&registry::parse(bytes).unwrap()).unwrap()
    }

    fn effective(bytes: &[u8]) -> Vec<u8> {
        projection(bytes).to_token_file().into_bytes()
    }

    /// The staging file with FGI's rows and policy removed.
    fn staging_without_fgi() -> Vec<u8> {
        let mut file = registry::parse(&staging()).unwrap();
        for chain in ["base", "robinhood"] {
            if let Some(equities) = file["chains"][chain]["assets"]["equities"].as_table_mut() {
                equities.remove("FGI");
            }
        }
        file["assets"]["equities"]
            .as_table_mut()
            .unwrap()
            .remove("FGI");
        file.to_string().into_bytes()
    }

    fn config() -> Table {
        toml::from_str("[chains.base.trading]\n[chains.robinhood.trading]\n").unwrap()
    }

    fn open() -> (tempfile::TempDir, RegistryState) {
        let dir = tempfile::tempdir().unwrap();
        let state = RegistryState::open(&dir.path().join("registry")).unwrap();
        (dir, state)
    }

    /// Claims a boot with a bucket read that counts its calls and serves
    /// `bytes` as `generation`.
    async fn claim_boot(
        state: &RegistryState,
        bytes: &[u8],
        generation: u64,
        reads: &AtomicU32,
    ) -> BootClaim {
        claim_with(state, &config(), 1_000, || {
            reads.fetch_add(1, Ordering::Relaxed);
            let copy = TokenCopy {
                generation,
                bytes: bytes.to_vec(),
            };
            async move { Ok(copy) }
        })
        .await
        .unwrap()
    }

    fn set_pending(state: &RegistryState, record: RecordId) {
        state
            .update(|manifest| {
                manifest.pending = Some(Pending {
                    record,
                    attempts: 0,
                    last_failure: None,
                    fallback: None,
                    fallbacks: 0,
                    retry_at: None,
                });
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn the_state_lives_beside_the_database_file() {
        for (url, want) in [
            (
                "sqlite:///mnt/data/st0x-hedge.db",
                Some("/mnt/data/registry"),
            ),
            ("sqlite:dev.db", Some("./registry")),
            ("sqlite://data/bot.db?mode=rwc", Some("data/registry")),
            ("/var/lib/bot.db", Some("/var/lib/registry")),
            (":memory:", None),
            ("sqlite::memory:", None),
            ("sqlite:///mnt/data%2Fbot.db", Some("/mnt/data/registry")),
            ("sqlite://scratch.db?mode=memory", None),
        ] {
            assert_eq!(state_dir(url), want.map(PathBuf::from), "{url}");
        }
    }

    /// Numbers only rise and a written record never changes, so what a
    /// manifest names is always what was judged.
    #[test]
    fn records_are_numbered_upwards_and_never_rewritten() {
        let (_dir, state) = open();
        let first = state.write_record(7, b"source", b"effective").unwrap();
        let before = state.record(first).unwrap();
        let second = state.write_record(7, b"source", b"effective").unwrap();

        assert_eq!((first, second), (RecordId(1), RecordId(2)));
        assert_eq!(state.record(first).unwrap(), before);
        assert_eq!(before.meta.generation, 7);
        assert_eq!(before.meta.source_sha256, sha256_hex(b"source"));
        assert_eq!(before.effective, b"effective");
    }

    /// A write cut short leaves a temporary directory or manifest behind:
    /// neither is read as state, and the next write replaces them.
    #[test]
    fn an_interrupted_write_leaves_no_state() {
        let (_dir, state) = open();
        std::fs::create_dir(state.dir().join("records/.1.tmp")).unwrap();
        std::fs::write(state.dir().join("records/.1.tmp/source.toml"), b"half").unwrap();
        std::fs::write(state.dir().join(".state.json.tmp"), b"{\"running\":").unwrap();

        assert_eq!(state.manifest().unwrap(), Manifest::default());
        assert_eq!(record_ids(state.dir()).unwrap(), Vec::<RecordId>::new());
        assert_eq!(
            running_effective(state.dir()).unwrap(),
            None,
            "a leftover temporary manifest is not a manifest"
        );

        let id = state.write_record(1, b"source", b"effective").unwrap();
        assert_eq!(id, RecordId(1));
        assert_eq!(state.record(id).unwrap().source, b"source");
        state.mark_running(id).unwrap();
        assert_eq!(state.manifest().unwrap().running, Some(id));
        assert_eq!(state.manifest().unwrap().version, 1);
    }

    #[test]
    fn a_record_that_does_not_match_its_digests_is_refused() {
        let (_dir, state) = open();
        let id = state.write_record(1, b"source", b"effective").unwrap();
        std::fs::write(state.dir().join("records/1/effective.toml"), b"tampered").unwrap();

        assert!(matches!(
            state.record(id).unwrap_err(),
            RegistryStateError::Corrupt {
                record: RecordId(1),
                file: "effective.toml"
            }
        ));
    }

    /// With no state, boot reads the latest bucket copy and records it.
    #[tokio::test]
    async fn with_no_state_boot_records_the_latest_copy() {
        let (_dir, state) = open();
        let reads = AtomicU32::new(0);

        let claim = claim_boot(&state, &staging(), 11, &reads).await;

        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(
            claim.outcome,
            BootOutcome::Seeded {
                record: RecordId(1),
                generation: 11
            }
        );
        let record = state.record(RecordId(1)).unwrap();
        assert_eq!(record.source, staging());
        assert_eq!(claim.tokens.unwrap(), record.effective);
        assert_eq!(projection(&record.effective), projection(&staging()));
    }

    /// Boot never removes a deploy hold: a service that restarts while an
    /// activation runs its gates must not let a publication in behind
    /// them. The activation removes its own hold; a stale one expires.
    #[tokio::test]
    async fn a_successful_boot_leaves_the_deploy_hold() {
        let (_dir, state) = open();
        let hold = state.dir().join("hold");
        std::fs::write(&hold, b"deploy").unwrap();
        let reads = AtomicU32::new(0);
        for _ in 0..2 {
            let claim = claim_boot(&state, &staging(), 11, &reads).await;
            assert_eq!(std::fs::read(&hold).unwrap(), b"deploy");
            state.mark_running(claim.booted.unwrap().record).unwrap();
        }
    }

    #[tokio::test]
    async fn a_failed_boot_leaves_the_deploy_hold() {
        let (_dir, state) = open();
        let hold = state.dir().join("hold");
        std::fs::write(&hold, b"deploy").unwrap();
        let claim = claim_with(&state, &config(), 1_000, || async {
            Ok(TokenCopy {
                generation: 11,
                bytes: b"invalid toml [".to_vec(),
            })
        })
        .await;
        assert!(claim.is_err());
        assert_eq!(std::fs::read(&hold).unwrap(), b"deploy");
    }

    /// Once a record runs, boot does not read the bucket: an outage or a
    /// bad publish cannot stop a restart.
    #[tokio::test]
    async fn a_restart_boots_the_running_record_without_the_bucket() {
        let (_dir, state) = open();
        let id = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        state.mark_running(id).unwrap();
        let reads = AtomicU32::new(0);

        let claim = claim_boot(&state, b"not read", 12, &reads).await;

        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert_eq!(
            claim.outcome,
            BootOutcome::Running {
                record: id,
                generation: 11,
                discarded: None
            }
        );
        assert_eq!(claim.booted.unwrap().record, id);
    }

    /// A pending copy gets two boots; out of attempts, boot persists and
    /// runs a fallback: the last good tables plus the listings only the
    /// pending copy had, switched off. The pending copy stays, marked, for
    /// a later retry.
    #[tokio::test]
    async fn a_pending_copy_that_fails_twice_falls_back() {
        let (_dir, state) = open();
        let without_fgi = staging_without_fgi();
        let good = state
            .write_record(10, &without_fgi, &effective(&without_fgi))
            .unwrap();
        state.mark_running(good).unwrap();
        state.promote(good).unwrap();
        let pending = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        set_pending(&state, pending);
        let reads = AtomicU32::new(0);

        for attempt in 1..=2 {
            let claim = claim_boot(&state, b"", 0, &reads).await;
            assert_eq!(
                claim.outcome,
                BootOutcome::Pending {
                    record: pending,
                    generation: 11,
                    attempt
                }
            );
        }
        let claim = claim_boot(&state, b"", 0, &reads).await;
        let BootOutcome::Fallback {
            record: fallback,
            generation: 10,
            failed,
            carried,
        } = claim.outcome
        else {
            panic!("expected a fallback, got {:?}", claim.outcome);
        };
        assert_eq!(failed, pending);
        assert_eq!(
            carried,
            BTreeSet::from(["base/FGI".to_string(), "robinhood/FGI".to_string()])
        );

        let manifest = state.manifest().unwrap();
        assert_eq!(manifest.running, Some(fallback));
        assert_eq!(manifest.last_good, Some(good));
        let kept = manifest.pending.unwrap();
        assert_eq!(kept.record, pending);
        assert_eq!(kept.last_failure, Some(PendingFailure::StartFailed));
        assert_eq!(kept.retry_at, Some(1_000 + 30 * 60));

        let tables = projection(&state.record(fallback).unwrap().effective);
        for chain in ["base", "robinhood"] {
            let row = &tables.chain_rows[chain]["FGI"];
            for key in ["trading", "rebalancing", "wrapped_equity_recovery"] {
                assert_eq!(row[key], Value::String("disabled".into()), "{chain} {key}");
            }
        }
        assert_eq!(
            tables.policies["FGI"]["extended_hours_counter_trading"],
            Value::String("disabled".into())
        );
        assert_eq!(
            tables.chain_rows["base"]["RKLB"],
            projection(&without_fgi).chain_rows["base"]["RKLB"]
        );

        let again = claim_boot(&state, b"", 0, &reads).await;
        assert_eq!(
            again.outcome,
            BootOutcome::FallbackAgain {
                record: fallback,
                generation: 10,
                failed: pending
            }
        );
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }

    /// Once its backoff ends, a boot tries the pending copy again with
    /// fresh attempts; the fallback count stays so the next backoff doubles.
    #[tokio::test]
    async fn a_pending_copy_is_retried_after_its_backoff() {
        let (_dir, state) = open();
        let good = state
            .write_record(10, &staging(), &effective(&staging()))
            .unwrap();
        state.mark_running(good).unwrap();
        state.promote(good).unwrap();
        let pending = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        set_pending(&state, pending);
        let reads = AtomicU32::new(0);
        for _ in 0..=MAX_BOOT_ATTEMPTS {
            claim_boot(&state, b"", 0, &reads).await;
        }
        let retry_at = state.manifest().unwrap().pending.unwrap().retry_at.unwrap();

        let before = state
            .update(|manifest| choose(&state, manifest, &config(), retry_at - 1))
            .unwrap()
            .unwrap();
        assert!(matches!(before.1, BootOutcome::FallbackAgain { .. }));

        let (record, outcome) = state
            .update(|manifest| choose(&state, manifest, &config(), retry_at))
            .unwrap()
            .unwrap();
        assert_eq!(record.id, pending);
        assert_eq!(
            outcome,
            BootOutcome::Pending {
                record: pending,
                generation: 11,
                attempt: 1
            }
        );
        let kept = state.manifest().unwrap().pending.unwrap();
        assert_eq!(
            (kept.attempts, kept.fallback, kept.fallbacks, kept.retry_at),
            (1, None, 1, None)
        );
    }

    /// A deploy or operator restart inside the soak stops the process
    /// cleanly; that boot does not count against the pending copy.
    #[tokio::test]
    async fn a_clean_stop_does_not_use_an_attempt() {
        let (_dir, state) = open();
        let good = state
            .write_record(10, &staging(), &effective(&staging()))
            .unwrap();
        state.mark_running(good).unwrap();
        let pending = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        set_pending(&state, pending);
        let reads = AtomicU32::new(0);

        for _ in 0..3 {
            let claim = claim_boot(&state, b"", 0, &reads).await;
            assert_eq!(
                claim.outcome,
                BootOutcome::Pending {
                    record: pending,
                    generation: 11,
                    attempt: 1
                }
            );
            state.mark_running(pending).unwrap();
            state.mark_clean_exit(pending).unwrap();
        }
        assert_eq!(state.manifest().unwrap().pending.unwrap().attempts, 1);
    }

    #[tokio::test]
    async fn a_corrupt_pending_record_is_dropped_and_the_running_one_boots() {
        let (_dir, state) = open();
        let good = state
            .write_record(10, &staging(), &effective(&staging()))
            .unwrap();
        state.mark_running(good).unwrap();
        let pending = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        set_pending(&state, pending);
        std::fs::write(state.dir().join("records/2/source.toml"), b"tampered").unwrap();
        let reads = AtomicU32::new(0);

        let claim = claim_boot(&state, b"", 0, &reads).await;

        assert_eq!(
            claim.outcome,
            BootOutcome::Running {
                record: good,
                generation: 10,
                discarded: Some(pending)
            }
        );
        assert_eq!(state.manifest().unwrap().pending, None);
    }

    /// A config that still pins a generation stops boot before it reads
    /// the bucket or touches the state.
    #[tokio::test]
    async fn a_pinned_config_is_refused_before_boot_reads_anything() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("hedge.db");
        let config: Table = toml::from_str(&format!(
            "database_url = \"sqlite://{}\"\n\
             [registry]\nurl = \"gs://t0-artifacts-tokens/production/tokens.toml\"\n\
             generation = 11\n",
            database.display()
        ))
        .unwrap();

        let error = claim_for_boot(&config, None, 1_000).await.unwrap_err();

        assert!(
            matches!(error, RegistryStateError::Registry(RegistryError::Pinned)),
            "{error:?}"
        );
        assert!(!dir.path().join("registry").exists());
    }

    /// A record becomes last good only while it still runs, and a pending
    /// copy clears when it does.
    #[test]
    fn promotion_moves_last_good_only_for_the_running_record() {
        let (_dir, state) = open();
        let first = state.write_record(1, b"a", b"a").unwrap();
        let second = state.write_record(2, b"b", b"b").unwrap();
        set_pending(&state, second);
        state.mark_running(second).unwrap();

        assert_eq!(
            state.promote(first).unwrap(),
            Promotion::Superseded {
                running: Some(second)
            }
        );
        assert_eq!(state.manifest().unwrap().last_good, None);

        assert_eq!(state.promote(second).unwrap(), Promotion::LastGood);
        let manifest = state.manifest().unwrap();
        assert_eq!(manifest.last_good, Some(second));
        assert_eq!(manifest.pending, None);
    }

    #[test]
    fn garbage_collection_keeps_every_named_record() {
        let (_dir, state) = open();
        let ids: Vec<RecordId> = (0..30)
            .map(|generation| state.write_record(generation, b"s", b"e").unwrap())
            .collect();
        state.mark_running(ids[0]).unwrap();
        state.promote(ids[0]).unwrap();

        let kept = record_ids(state.dir()).unwrap();
        assert!(kept.contains(&ids[0]), "the running record is kept");
        assert_eq!(kept.len(), KEPT_RECORDS + 1);
        assert_eq!(kept.last(), ids.last());
    }

    /// The CLI and the gates read what runs without touching the state.
    #[test]
    fn reading_the_running_tables_writes_nothing() {
        let (_dir, state) = open();
        assert_eq!(running_effective(state.dir()).unwrap(), None);
        let id = state.write_record(1, b"source", b"effective").unwrap();
        state.mark_running(id).unwrap();
        let manifest = std::fs::read(state.dir().join("state.json")).unwrap();
        let hold = state.dir().join("hold");
        std::fs::write(&hold, b"deploy").unwrap();

        assert_eq!(
            running_effective(state.dir()).unwrap(),
            Some(b"effective".to_vec())
        );
        assert_eq!(
            std::fs::read(state.dir().join("state.json")).unwrap(),
            manifest
        );
        assert_eq!(std::fs::read(&hold).unwrap(), b"deploy");
    }
    #[test]
    fn gates_read_pending_and_disabled_union_without_writing_state() {
        let (_dir, state) = open();
        let source = staging();
        let base = state.write_record(1, &source, &effective(&source)).unwrap();
        let changed = staging_without_fgi();
        let pending = state
            .write_record(2, &changed, &effective(&changed))
            .unwrap();
        state.mark_running(base).unwrap();
        state.promote(base).unwrap();
        set_pending(&state, pending);
        let hold = state.dir().join("hold");
        std::fs::write(&hold, b"deploy").unwrap();
        let before = std::fs::read(state.dir().join(MANIFEST)).unwrap();
        let copies = gate_effective(state.dir(), &config()).unwrap();
        assert_eq!(copies.len(), 2);
        assert_eq!(copies[0], effective(&changed));
        assert!(
            projection(&copies[1])
                .slots()
                .iter()
                .any(|slot| slot.contains("FGI"))
        );
        assert_eq!(std::fs::read(state.dir().join(MANIFEST)).unwrap(), before);
        assert_eq!(std::fs::read(&hold).unwrap(), b"deploy");
    }

    #[test]
    fn gates_read_running_and_absent_state_without_writing() {
        let (_dir, state) = open();
        assert!(gate_effective(state.dir(), &config()).unwrap().is_empty());
        let source = staging();
        let bytes = effective(&source);
        let record = state.write_record(1, &source, &bytes).unwrap();
        state.mark_running(record).unwrap();
        let hold = state.dir().join("hold");
        std::fs::write(&hold, b"deploy").unwrap();
        let before = std::fs::read(state.dir().join(MANIFEST)).unwrap();
        assert_eq!(gate_effective(state.dir(), &config()).unwrap(), vec![bytes]);
        assert_eq!(std::fs::read(state.dir().join(MANIFEST)).unwrap(), before);
        assert_eq!(std::fs::read(&hold).unwrap(), b"deploy");
    }
    #[tokio::test]
    async fn a_clean_second_attempt_still_boots_pending() {
        let (_dir, state) = open();
        let source = staging();
        let base = state.write_record(1, &source, &effective(&source)).unwrap();
        state.mark_running(base).unwrap();
        state.promote(base).unwrap();
        let pending = state.write_record(2, &source, &effective(&source)).unwrap();
        set_pending(&state, pending);
        state
            .update(|manifest| {
                manifest.pending.as_mut().unwrap().attempts = MAX_BOOT_ATTEMPTS;
                manifest.clean_exit = Some(pending);
                Ok(())
            })
            .unwrap();
        let reads = AtomicU32::new(0);
        let claim = claim_boot(&state, &source, 3, &reads).await;
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert_eq!(claim.booted.unwrap().record, pending);
        assert!(
            state
                .manifest()
                .unwrap()
                .pending
                .unwrap()
                .fallback
                .is_none()
        );
    }
    #[test]
    fn promotion_and_clean_exit_follow_an_unchanged_advance_atomically() {
        let (_dir, state) = open();
        let first = state.write_record(1, b"source", b"effective").unwrap();
        state.mark_running(first).unwrap();
        set_pending(&state, first);
        let latest = state.write_record(2, b"new source", b"effective").unwrap();
        state
            .update(|manifest| {
                manifest.running = Some(latest);
                manifest.pending.as_mut().unwrap().record = latest;
                Ok(())
            })
            .unwrap();
        state.mark_running_clean_exit().unwrap();
        assert_eq!(state.manifest().unwrap().clean_exit, Some(latest));
        assert_eq!(state.promote_running().unwrap(), Some(latest));
        let manifest = state.manifest().unwrap();
        assert_eq!(manifest.last_good, Some(latest));
        assert!(manifest.pending.is_none());
    }

    #[test]
    fn gate_fallback_matches_boot_when_a_symbol_is_retired() {
        let (_dir, state) = open();
        let source = staging();
        let first = state.write_record(1, &source, &effective(&source)).unwrap();
        state.mark_running(first).unwrap();
        state.promote(first).unwrap();
        let changed = staging_without_fgi();
        let pending = state
            .write_record(2, &changed, &effective(&changed))
            .unwrap();
        set_pending(&state, pending);
        let mut config = config();
        let assets: Table =
            toml::from_str("[assets.equities]\nretired_symbols = [\"FGI\"]\n").unwrap();
        config.extend(assets);
        let preview = gate_effective(state.dir(), &config).unwrap();
        state
            .update(|manifest| {
                manifest.pending.as_mut().unwrap().attempts = MAX_BOOT_ATTEMPTS;
                Ok(())
            })
            .unwrap();
        let boot = state
            .update(|manifest| choose(&state, manifest, &config, 1_000))
            .unwrap()
            .unwrap();
        assert_eq!(preview[1], boot.0.effective);
        assert!(
            !projection(&preview[1])
                .slots()
                .iter()
                .any(|slot| slot.contains("FGI"))
        );
    }
    #[test]
    fn failed_pending_record_cannot_be_its_own_fallback() {
        let (_dir, state) = open();
        let source = staging();
        let pending = state.write_record(1, &source, &effective(&source)).unwrap();
        state.mark_running(pending).unwrap();
        set_pending(&state, pending);
        state
            .update(|manifest| {
                manifest.pending.as_mut().unwrap().attempts = MAX_BOOT_ATTEMPTS;
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            gate_effective(state.dir(), &config()),
            Err(RegistryStateError::NoFallbackBase { .. })
        ));
        assert!(matches!(
            state.update(|manifest| choose(&state, manifest, &config(), 1_000)),
            Err(RegistryStateError::NoFallbackBase { .. })
        ));
    }
    #[tokio::test]
    async fn a_corrupt_pending_that_ran_recovers_to_last_good() {
        let (_dir, state) = open();
        let source = staging();
        let good = state.write_record(1, &source, &effective(&source)).unwrap();
        state.mark_running(good).unwrap();
        state.promote(good).unwrap();
        let changed = staging_without_fgi();
        let pending = state
            .write_record(2, &changed, &effective(&changed))
            .unwrap();
        set_pending(&state, pending);
        state.mark_running(pending).unwrap();
        std::fs::write(state.dir().join("records/2/source.toml"), b"tampered").unwrap();
        let reads = AtomicU32::new(0);
        let claim = claim_boot(&state, b"", 0, &reads).await;
        assert!(
            matches!(claim.outcome, BootOutcome::Running { record, discarded: Some(discarded), .. } if record == good && discarded == pending)
        );
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert!(state.manifest().unwrap().pending.is_none());
        assert_eq!(claim.tokens.unwrap(), state.record(good).unwrap().effective);
    }

    /// A record directory missing a file or holding unreadable metadata is
    /// corrupt, so boot drops it like a digest mismatch rather than failing.
    #[tokio::test]
    async fn a_pending_record_missing_a_file_or_metadata_is_dropped() {
        for damage in ["missing effective", "malformed meta"] {
            let (_dir, state) = open();
            let good = state
                .write_record(10, &staging(), &effective(&staging()))
                .unwrap();
            state.mark_running(good).unwrap();
            let pending = state
                .write_record(11, &staging(), &effective(&staging()))
                .unwrap();
            set_pending(&state, pending);
            let file = if damage == "missing effective" {
                std::fs::remove_file(state.dir().join("records/2/effective.toml")).unwrap();
                EFFECTIVE
            } else {
                std::fs::write(state.dir().join("records/2/meta.json"), b"{\"generation\":")
                    .unwrap();
                META
            };
            assert!(
                matches!(
                    state.record(pending).unwrap_err(),
                    RegistryStateError::Corrupt { record, file: damaged }
                        if record == pending && damaged == file
                ),
                "{damage}"
            );

            let reads = AtomicU32::new(0);
            let claim = claim_boot(&state, b"", 0, &reads).await;

            assert_eq!(
                claim.outcome,
                BootOutcome::Running {
                    record: good,
                    generation: 10,
                    discarded: Some(pending)
                },
                "{damage}"
            );
        }
    }

    /// A process that crashes before `mark_running` reboots the same copy
    /// without adding a record per boot.
    #[tokio::test]
    async fn a_crash_loop_before_running_reuses_its_record() {
        let (_dir, state) = open();
        let reads = AtomicU32::new(0);

        for _ in 0..3 {
            let claim = claim_boot(&state, &staging(), 11, &reads).await;
            assert_eq!(
                claim.outcome,
                BootOutcome::Seeded {
                    record: RecordId(1),
                    generation: 11
                }
            );
        }

        assert_eq!(record_ids(state.dir()).unwrap(), vec![RecordId(1)]);
    }

    /// A fallback that no longer reads back is rebuilt from last good and
    /// the failed copy, by boot and the deploy gate alike.
    #[tokio::test]
    async fn a_damaged_fallback_is_rebuilt() {
        let (_dir, state) = open();
        let without_fgi = staging_without_fgi();
        let good = state
            .write_record(10, &without_fgi, &effective(&without_fgi))
            .unwrap();
        state.mark_running(good).unwrap();
        state.promote(good).unwrap();
        let pending = state
            .write_record(11, &staging(), &effective(&staging()))
            .unwrap();
        set_pending(&state, pending);
        let reads = AtomicU32::new(0);
        for _ in 0..3 {
            claim_boot(&state, b"", 0, &reads).await;
        }
        let first = state.manifest().unwrap().pending.unwrap();
        let damaged = first.fallback.unwrap();
        let tables = state.record(damaged).unwrap().effective;
        std::fs::write(
            state
                .dir()
                .join(format!("records/{damaged}/effective.toml")),
            b"tampered",
        )
        .unwrap();

        let gate = gate_effective(state.dir(), &config()).unwrap();
        assert_eq!(gate, vec![effective(&staging()), tables.clone()]);

        let claim = claim_boot(&state, b"", 0, &reads).await;
        let BootOutcome::Fallback {
            record: rebuilt,
            generation: 10,
            failed,
            carried,
        } = claim.outcome
        else {
            panic!("expected a rebuilt fallback, got {:?}", claim.outcome);
        };
        assert_ne!(rebuilt, damaged);
        assert_eq!(failed, pending);
        assert_eq!(
            carried,
            BTreeSet::from(["base/FGI".to_string(), "robinhood/FGI".to_string()])
        );
        assert_eq!(state.record(rebuilt).unwrap().effective, tables);
        let manifest = state.manifest().unwrap();
        assert_eq!(manifest.running, Some(rebuilt));
        let kept = manifest.pending.unwrap();
        assert_eq!(kept.fallback, Some(rebuilt));
        assert_eq!(
            (kept.fallbacks, kept.retry_at),
            (first.fallbacks, first.retry_at)
        );
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
}
