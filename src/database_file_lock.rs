//! Advisory file locks next to the SQLite database. The daemon and operator CLI
//! processes attached to the same database contend on the same lock file, so
//! each lock serializes its critical section across processes as well as
//! across tasks within one process. The kernel releases a lock when its process
//! exits, so a crash cannot strand a lease.
//!
//! Fill accounting takes one process wide mutex before its lock file, so callers
//! inside the bot (the live accounting jobs and the process-tx route) queue in
//! order instead of polling the file, and the file lock only arbitrates between
//! processes. An in memory database has no file to lock beside, and only the
//! process that created it can attach to it, so there the mutex alone is the
//! lock. The submission lock needs no mutex here: every in process placement
//! already holds the shared counter trade submission mutex.

use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{Mutex, MutexGuard};

use crate::conductor::job::DEFAULT_PERFORM_TIMEOUT;

const RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// The critical sections guarded by a database file lock. Each has its own
/// lock file, so holding one never waits on the other.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DatabaseFileLock {
    /// The account wide broker submission window: the position claim through
    /// the broker placement.
    CounterTradeSubmission,
    /// One fill's durable dedup check and its `Position` acknowledge.
    FillAccounting,
}

impl DatabaseFileLock {
    const fn file_suffix(self) -> &'static str {
        match self {
            Self::CounterTradeSubmission => ".counter-trade.lock",
            Self::FillAccounting => ".fill-accounting.lock",
        }
    }
}

/// Serializes fill accounting within this process; taken before the lock file.
static FILL_ACCOUNTING_IN_PROCESS: Mutex<()> = Mutex::const_new(());

/// Held for the critical section; dropping it releases the lock.
pub(crate) struct DatabaseFileGuard {
    _held: Held,
}

/// Each variant holds its lock only to release it on drop. Fields drop in
/// declaration order, so `File` releases the lock file before the in process
/// mutex: the next in process waiter then finds the file already free.
enum Held {
    File {
        _file: File,
        _in_process: Option<MutexGuard<'static, ()>>,
    },
    InMemory {
        _in_process: MutexGuard<'static, ()>,
    },
    Nothing,
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseFileLockError {
    #[error("Failed to resolve the SQLite database path for a lock file")]
    ResolveDatabasePath(#[source] sqlx::Error),
    #[error("Failed to join the lock file task")]
    Join(#[source] tokio::task::JoinError),
    #[error("Failed to open lock file {path}")]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to acquire lock file {path}")]
    Acquire {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Timed out acquiring lock file {path}")]
    TimedOut { path: PathBuf },
}

/// Acquires `lock` for the database behind `pool`. Fill accounting takes the
/// process wide mutex first, then the lock file; on an in memory database the
/// mutex alone, and the submission lock nothing (see the module docs).
pub(crate) async fn acquire_database_file_lock(
    pool: &SqlitePool,
    lock: DatabaseFileLock,
) -> Result<DatabaseFileGuard, DatabaseFileLockError> {
    acquire_database_file_lock_with_timeout(pool, lock, DEFAULT_PERFORM_TIMEOUT, RETRY_INTERVAL)
        .await
}

async fn acquire_database_file_lock_with_timeout(
    pool: &SqlitePool,
    lock: DatabaseFileLock,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<DatabaseFileGuard, DatabaseFileLockError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let database_path: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(pool)
            .await
            .map_err(DatabaseFileLockError::ResolveDatabasePath)?;
    let mut lock_path = PathBuf::from(&database_path).into_os_string();
    lock_path.push(lock.file_suffix());
    let lock_path = PathBuf::from(lock_path);

    let in_process = match lock {
        DatabaseFileLock::FillAccounting => Some(
            tokio::time::timeout_at(deadline, FILL_ACCOUNTING_IN_PROCESS.lock())
                .await
                .map_err(|_| DatabaseFileLockError::TimedOut {
                    path: lock_path.clone(),
                })?,
        ),
        DatabaseFileLock::CounterTradeSubmission => None,
    };

    if database_path.is_empty() {
        let held = in_process.map_or(Held::Nothing, |guard| Held::InMemory { _in_process: guard });
        return Ok(DatabaseFileGuard { _held: held });
    }

    let file = tokio::task::spawn_blocking({
        let lock_path = lock_path.clone();
        move || {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(|source| DatabaseFileLockError::Open {
                    path: lock_path.clone(),
                    source,
                })?;
            Ok::<File, DatabaseFileLockError>(file)
        }
    })
    .await
    .map_err(DatabaseFileLockError::Join)??;

    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(DatabaseFileLockError::TimedOut { path: lock_path });
                }
                tokio::time::sleep(retry_interval.min(deadline - now)).await;
            }
            Err(source) => {
                return Err(DatabaseFileLockError::Acquire {
                    path: lock_path,
                    source: source.into(),
                });
            }
        }
    }

    Ok(DatabaseFileGuard {
        _held: Held::File {
            _file: file,
            _in_process: in_process,
        },
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    async fn two_pools_on_one_file(name: &str) -> (tempfile::TempDir, SqlitePool, SqlitePool) {
        let directory = tempfile::tempdir().unwrap();
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(directory.path().join(name))
            .create_if_missing(true);
        let first_pool = SqlitePool::connect_with(options.clone()).await.unwrap();
        let second_pool = SqlitePool::connect_with(options).await.unwrap();
        (directory, first_pool, second_pool)
    }

    #[tokio::test]
    async fn file_lock_serializes_independent_database_pools() {
        let (_directory, first_pool, second_pool) = two_pools_on_one_file("lock.sqlite").await;

        let first_guard =
            acquire_database_file_lock(&first_pool, DatabaseFileLock::CounterTradeSubmission)
                .await
                .unwrap();
        let waiter = tokio::spawn(async move {
            drop(
                acquire_database_file_lock(&second_pool, DatabaseFileLock::CounterTradeSubmission)
                    .await
                    .unwrap(),
            );
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "a second process-equivalent pool must wait for the lock"
        );

        drop(first_guard);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter must acquire after release")
            .unwrap();
    }

    /// Fill accounting must never wait on a broker submission window (or the
    /// reverse): each lock has its own file.
    #[tokio::test]
    async fn file_locks_are_independent_of_each_other() {
        let (_directory, first_pool, second_pool) = two_pools_on_one_file("locks.sqlite").await;

        let _submission =
            acquire_database_file_lock(&first_pool, DatabaseFileLock::CounterTradeSubmission)
                .await
                .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire_database_file_lock_with_timeout(
                &second_pool,
                DatabaseFileLock::FillAccounting,
                Duration::from_millis(25),
                Duration::from_millis(5),
            ),
        )
        .await
        .expect("the fill accounting lock must not wait on the submission lock")
        .expect("the fill accounting lock is free while only the submission lock is held");
    }

    #[tokio::test]
    async fn file_lock_timeout_does_not_strand_later_acquisitions() {
        let (_directory, first_pool, second_pool) =
            two_pools_on_one_file("lock-timeout.sqlite").await;

        let first_guard =
            acquire_database_file_lock(&first_pool, DatabaseFileLock::CounterTradeSubmission)
                .await
                .unwrap();
        let Err(error) = acquire_database_file_lock_with_timeout(
            &second_pool,
            DatabaseFileLock::CounterTradeSubmission,
            Duration::from_millis(25),
            Duration::from_millis(5),
        )
        .await
        else {
            panic!("the second pool must time out while the lock is held");
        };
        assert!(matches!(error, DatabaseFileLockError::TimedOut { .. }));

        drop(first_guard);
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire_database_file_lock(&second_pool, DatabaseFileLock::CounterTradeSubmission),
        )
        .await
        .expect("a timed-out waiter must not strand the file lock")
        .unwrap();
    }

    #[tokio::test]
    async fn file_lock_skips_shared_in_memory_databases() {
        let database_name = format!("lock-memory-{}", Uuid::new_v4());
        let database_url = format!("file:{database_name}?mode=memory&cache=shared");
        let lock_path = PathBuf::from(format!("file:{database_name}.counter-trade.lock"));
        let pool = SqlitePool::connect(&database_url).await.unwrap();

        let _guard = acquire_database_file_lock(&pool, DatabaseFileLock::CounterTradeSubmission)
            .await
            .unwrap();

        assert!(!lock_path.exists());
    }

    /// On a shared in memory database there is no lock file, yet the live
    /// accounting job and the process-tx route still run in one process. Fill
    /// accounting must still exclude a second holder, and must not leave a lock
    /// file behind.
    #[tokio::test]
    async fn fill_accounting_lock_serializes_on_a_shared_in_memory_database() {
        let database_name = format!("fill-lock-memory-{}", Uuid::new_v4());
        let database_url = format!("file:{database_name}?mode=memory&cache=shared");
        let first_pool = SqlitePool::connect(&database_url).await.unwrap();
        let second_pool = SqlitePool::connect(&database_url).await.unwrap();

        let first_guard = acquire_database_file_lock(&first_pool, DatabaseFileLock::FillAccounting)
            .await
            .unwrap();
        let Err(error) = acquire_database_file_lock_with_timeout(
            &second_pool,
            DatabaseFileLock::FillAccounting,
            Duration::from_millis(25),
            Duration::from_millis(5),
        )
        .await
        else {
            panic!("a second fill accounting holder must wait while the first holds it");
        };
        assert!(matches!(error, DatabaseFileLockError::TimedOut { .. }));

        drop(first_guard);
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire_database_file_lock(&second_pool, DatabaseFileLock::FillAccounting),
        )
        .await
        .expect("the lock must be free once the first holder drops it")
        .unwrap();
        assert!(!PathBuf::from(format!("file:{database_name}.fill-accounting.lock")).exists());
    }
}
