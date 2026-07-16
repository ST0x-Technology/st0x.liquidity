//! Lightweight operational telemetry store.
//!
//! Persists high-frequency operational samples -- chain-tip block lag,
//! poll-cycle duration and skipped ticks, external dependency call
//! latency/errors -- to plain SQLite tables, outside the CQRS event store:
//! these are operational observations, not domain events. Writes are
//! best-effort from the caller's perspective -- a failed sample must never
//! fail the monitored operation -- and rows older than [`RETENTION`] are
//! pruned opportunistically.
//!
//! Monitor samples are written inline (one row per poll interval).
//! Dependency-call samples come from hot paths (every RPC and broker call),
//! so they flow through a bounded mpsc channel ([`TelemetrySender`]) into a
//! background writer task that batches inserts and never blocks the caller.

use std::borrow::Cow;
use std::fmt::Display;
use std::num::TryFromIntError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::Address;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub(crate) mod broker;
pub(crate) mod executor;
pub(crate) mod rpc;

/// How long telemetry samples are retained. Telemetry is an operational
/// debugging aid, not an audit trail; two weeks comfortably covers "what
/// changed since last week" investigations.
const RETENTION: chrono::Duration = chrono::Duration::days(14);

/// One observation of how far fill detection trails the chain.
#[derive(Debug, Clone)]
pub(crate) struct BlockLagSample {
    pub(crate) sampled_at: DateTime<Utc>,
    pub(crate) orderbook: Address,
    pub(crate) chain_tip: u64,
    /// `None` when the configured ingestion cutoff tag is unavailable.
    pub(crate) cutoff_block: Option<u64>,
    /// `None` before the first backfill checkpoint exists.
    pub(crate) last_processed_block: Option<u64>,
}

impl BlockLagSample {
    /// Cutoff blocks not yet processed: `cutoff_block - last_processed_block`.
    /// Measured against the cutoff block (the ingestion boundary) rather than
    /// the raw tip because the checkpoint can only ever advance to the cutoff
    /// block -- against the raw tip a fully caught-up system would read a
    /// permanent floor of the finality lag instead of zero. Saturates at zero:
    /// a load-balanced RPC can briefly report a cutoff block behind the
    /// checkpoint, which is staleness noise, not negative lag.
    fn lag_blocks(&self) -> Option<u64> {
        self.cutoff_block
            .zip(self.last_processed_block)
            .map(|(cutoff_block, checkpoint)| cutoff_block.saturating_sub(checkpoint))
    }
}

/// Monitor a poll-cycle sample belongs to. An enum (not a free string) so
/// writers and the read path cannot drift on the label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Monitor {
    OrderFill,
}

impl Monitor {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OrderFill => "order_fill",
        }
    }
}

/// Whether a poll cycle succeeded or produced an error. An enum (not a raw
/// string) so the writer and reader share a single source of truth for the
/// discriminator stored in the `outcome` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    Ok,
    Error,
}

impl PollOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum TelemetryError {
    #[error("failed to write telemetry sample")]
    Database(#[from] sqlx::Error),
    #[error("telemetry value out of range for storage")]
    IntConversion(#[from] TryFromIntError),
}

/// Format a timestamp exactly like SQLite's
/// `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` so stored text timestamps
/// compare lexicographically as times.
pub(crate) fn sqlite_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Persist one block-lag sample.
pub(crate) async fn record_block_lag(
    pool: &SqlitePool,
    sample: &BlockLagSample,
) -> Result<(), TelemetryError> {
    let lag_blocks = sample.lag_blocks().map(i64::try_from).transpose()?;
    let cutoff_block = sample.cutoff_block.map(i64::try_from).transpose()?;
    let last_processed_block = sample.last_processed_block.map(i64::try_from).transpose()?;

    sqlx::query(
        "INSERT INTO block_lag_samples \
         (sampled_at, orderbook, chain_tip, cutoff_block, last_processed_block, \
          lag_blocks) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(sqlite_timestamp(sample.sampled_at))
    .bind(sample.orderbook.to_string())
    .bind(i64::try_from(sample.chain_tip)?)
    .bind(cutoff_block)
    .bind(last_processed_block)
    .bind(lag_blocks)
    .execute(pool)
    .await?;

    Ok(())
}

/// Persist one poll-cycle sample. The outcome keeps the caller's structured
/// error until this storage boundary, where it is rendered once into the
/// operator-facing `error` text column.
pub(crate) async fn record_poll_cycle(
    pool: &SqlitePool,
    monitor: Monitor,
    orderbook: Address,
    sampled_at: DateTime<Utc>,
    duration: Duration,
    skipped_ticks: u64,
    // `Sync` so `&error` is `Send` and the returned future stays `Send` for
    // the supervised monitor task.
    outcome: Result<(), &(impl Display + Sync)>,
) -> Result<(), TelemetryError> {
    let (outcome_label, error) = match outcome {
        Ok(()) => (PollOutcome::Ok, None),
        Err(error) => (PollOutcome::Error, Some(error.to_string())),
    };

    sqlx::query(
        "INSERT INTO poll_cycle_samples \
         (sampled_at, monitor, orderbook, duration_ms, skipped_ticks, outcome, error) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(sqlite_timestamp(sampled_at))
    .bind(monitor.as_str())
    .bind(orderbook.to_string())
    .bind(i64::try_from(duration.as_millis())?)
    .bind(i64::try_from(skipped_ticks)?)
    .bind(outcome_label.as_str())
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

/// Delete telemetry samples older than the retention window. Cheap when
/// there is nothing to delete (indexed seek), so callers run it on their
/// own cadence without coordination.
pub(crate) async fn prune_expired(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<(), TelemetryError> {
    let cutoff = sqlite_timestamp(now - RETENTION);

    sqlx::query("DELETE FROM block_lag_samples WHERE sampled_at < $1")
        .bind(&cutoff)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM poll_cycle_samples WHERE sampled_at < $1")
        .bind(&cutoff)
        .execute(pool)
        .await?;

    Ok(())
}

/// External dependency a call sample belongs to. `Broker` covers whatever
/// `Executor` implementation is configured, not just Alpaca.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dependency {
    Rpc,
    Broker,
}

impl Dependency {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::Broker => "broker",
        }
    }
}

/// One observed call to an external dependency.
#[derive(Debug, Clone)]
pub(crate) struct DependencyCallSample {
    pub(crate) recorded_at: DateTime<Utc>,
    pub(crate) dependency: Dependency,
    pub(crate) operation: Cow<'static, str>,
    pub(crate) duration: Duration,
    /// `Some` when the call failed, rendered for operators.
    pub(crate) error: Option<String>,
}

/// Capacity of the sample channel. At one slow batch insert per
/// `recv_many`, thousands of in-flight samples means the writer is wedged,
/// not busy -- dropping is the correct behavior on hot paths.
const DEPENDENCY_CHANNEL_CAPACITY: usize = 4_096;

/// Log every Nth dropped sample so a wedged writer is visible without
/// turning the hot path into a log storm.
const DROP_LOG_EVERY: u64 = 1_000;

/// State present only when the sender is connected to a live writer task.
#[derive(Debug, Clone)]
struct Connected {
    channel: mpsc::Sender<DependencyCallSample>,
    dropped: Arc<AtomicU64>,
}

/// Non-blocking handle hot paths use to emit [`DependencyCallSample`]s.
///
/// `record` never blocks and never fails the caller: when the channel is
/// full or closed the sample is counted and dropped. A disabled sender
/// (`connected` is `None`) swallows samples outright -- used where a handle
/// is structurally required but no writer exists.
#[derive(Debug, Clone)]
pub(crate) struct TelemetrySender {
    connected: Option<Connected>,
}

impl TelemetrySender {
    /// A connected sender plus the receiver to hand to
    /// [`spawn_dependency_call_writer`].
    pub(crate) fn channel() -> (Self, mpsc::Receiver<DependencyCallSample>) {
        let (sender, receiver) = mpsc::channel(DEPENDENCY_CHANNEL_CAPACITY);
        (
            Self {
                connected: Some(Connected {
                    channel: sender,
                    dropped: Arc::new(AtomicU64::new(0)),
                }),
            },
            receiver,
        )
    }

    /// A sender that swallows every sample.
    pub(crate) fn disabled() -> Self {
        Self { connected: None }
    }

    /// Emit one sample, never blocking the caller.
    pub(crate) fn record(&self, sample: DependencyCallSample) {
        let Some(connected) = &self.connected else {
            return;
        };

        if connected.channel.try_send(sample).is_err() {
            let count = connected.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if count == 1 || count.is_multiple_of(DROP_LOG_EVERY) {
                warn!(
                    dropped = count,
                    "Telemetry channel full or closed; dropping dependency call samples"
                );
            }
        }
    }

    /// Total samples dropped so far (channel full or closed). Test-only.
    #[cfg(test)]
    pub(crate) fn dropped_count(&self) -> u64 {
        self.connected
            .as_ref()
            .map_or(0, |connected| connected.dropped.load(Ordering::Relaxed))
    }
}

/// Redact credential-bearing parts of any URL embedded in a dependency error
/// before it is persisted. Production RPC URLs carry the API key in the path
/// or query string (e.g. dRPC `?dkey=...`, Alchemy `/v2/<key>`), and a
/// transport failure surfaces the full URL through the error's `Display`.
/// Keeping only `scheme://host` preserves the operator-useful "which provider
/// failed" signal while dropping the secret-bearing remainder.
///
/// Trailing non-URL characters (e.g. a closing `)` from a surrounding message)
/// are detected by trimming URL-legal chars from the right and re-appended
/// after the redacted output so the surrounding sentence stays readable.
pub(crate) fn scrub_secrets(message: &str) -> String {
    message
        .split(' ')
        .map(|token| {
            let Some(scheme_end) = token.find("://") else {
                return Cow::Borrowed(token);
            };
            let rest = &token[scheme_end + 3..];
            let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
            let authority = &rest[..authority_end];
            // Drop userinfo (`user:pass@`); keep only the host.
            let host = authority
                .rfind('@')
                .map_or(authority, |at| &authority[at + 1..]);
            if authority_end == rest.len() && host.len() == authority.len() {
                return Cow::Borrowed(token);
            }
            // Preserve any non-URL trailing characters (e.g. `)`) that were
            // part of the surrounding message text, not the URL itself.
            // Find the last char that is a legal URL character; everything
            // after it is the trailing suffix to re-append.
            let url_char_end = rest
                .rfind(|ch: char| {
                    ch.is_alphanumeric()
                        || matches!(
                            ch,
                            '-' | '.'
                                | '_'
                                | '~'
                                | ':'
                                | '/'
                                | '?'
                                | '#'
                                | '['
                                | ']'
                                | '@'
                                | '!'
                                | '$'
                                | '&'
                                | '\''
                                | '('
                                | '*'
                                | '+'
                                | ','
                                | ';'
                                | '='
                                | '%'
                        )
                })
                .map_or(0, |pos| pos + 1);
            let trailing = &rest[url_char_end..];
            let suffix = if authority_end == rest.len() {
                ""
            } else {
                "/<redacted>"
            };
            Cow::Owned(format!(
                "{}{host}{suffix}{trailing}",
                &token[..scheme_end + 3]
            ))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Samples drained per write batch.
const WRITE_BATCH: usize = 256;

/// How often the writer prunes expired dependency-call rows.
const DEPENDENCY_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Spawn the background task that drains the sample channel into
/// `dependency_call_samples` in batched transactions and prunes expired
/// rows hourly. Exits when every [`TelemetrySender`] clone is dropped.
pub(crate) fn spawn_dependency_call_writer(
    pool: SqlitePool,
    mut receiver: mpsc::Receiver<DependencyCallSample>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // `interval_at` (not `interval`) so the first prune fires one full
        // interval in, not immediately at startup before any rows exist.
        let mut prune_timer = tokio::time::interval_at(
            tokio::time::Instant::now() + DEPENDENCY_PRUNE_INTERVAL,
            DEPENDENCY_PRUNE_INTERVAL,
        );
        let mut batch = Vec::with_capacity(WRITE_BATCH);

        loop {
            tokio::select! {
                received_count = receiver.recv_many(&mut batch, WRITE_BATCH) => {
                    if received_count == 0 {
                        info!("Telemetry channel closed; dependency call writer stopping");
                        return;
                    }
                    if let Err(error) = write_dependency_calls(&pool, &batch).await {
                        // Telemetry is best-effort, but report the magnitude so
                        // a persistent write failure (disk full, lock) is not an
                        // invisible blackhole.
                        warn!(?error, lost = batch.len(), "Failed to write dependency call samples");
                    }
                    batch.clear();
                }
                _ = prune_timer.tick() => {
                    if let Err(error) = prune_expired_dependency_calls(&pool, Utc::now()).await {
                        warn!(?error, "Failed to prune expired dependency call samples");
                    }
                }
            }
        }
    })
}

async fn write_dependency_calls(
    pool: &SqlitePool,
    samples: &[DependencyCallSample],
) -> Result<(), TelemetryError> {
    let mut transaction = pool.begin().await?;

    for sample in samples {
        // One unrepresentable duration (>292 million years) must not poison
        // the whole batch: skip the sample, keep the rest.
        let duration_ms = match i64::try_from(sample.duration.as_millis()) {
            Ok(duration_ms) => duration_ms,
            Err(error) => {
                warn!(
                    ?error,
                    operation = %sample.operation,
                    "Skipping dependency sample with unrepresentable duration"
                );
                continue;
            }
        };

        let (outcome, error) = sample
            .error
            .as_ref()
            .map_or(("ok", None), |message| ("error", Some(message.as_str())));

        sqlx::query(
            "INSERT INTO dependency_call_samples \
             (recorded_at, dependency, operation, duration_ms, outcome, error) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(sqlite_timestamp(sample.recorded_at))
        .bind(sample.dependency.as_str())
        .bind(sample.operation.as_ref())
        .bind(duration_ms)
        .bind(outcome)
        .bind(error)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}

async fn prune_expired_dependency_calls(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<(), TelemetryError> {
    sqlx::query("DELETE FROM dependency_call_samples WHERE recorded_at < $1")
        .bind(sqlite_timestamp(now - RETENTION))
        .execute(pool)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use alloy::primitives::address;
    use chrono::TimeZone;

    use crate::test_utils::setup_test_db;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    fn sample(seconds: i64, chain_tip: u64, checkpoint: Option<u64>) -> BlockLagSample {
        BlockLagSample {
            sampled_at: timestamp(seconds),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            chain_tip,
            // Cutoff trails the tip by a few blocks in this mock.
            cutoff_block: Some(chain_tip.saturating_sub(3)),
            last_processed_block: checkpoint,
        }
    }

    #[tokio::test]
    async fn record_block_lag_computes_lag_from_checkpoint() {
        let pool = setup_test_db().await;

        record_block_lag(&pool, &sample(0, 105, Some(100)))
            .await
            .unwrap();

        let (chain_tip, lag_blocks): (i64, Option<i64>) =
            sqlx::query_as("SELECT chain_tip, lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(chain_tip, 105);
        // Lag measures cutoff_block (105 - 3 = 102) minus the checkpoint
        // (100): a caught-up system reads zero, not the finality lag.
        assert_eq!(lag_blocks, Some(2));
    }

    #[tokio::test]
    async fn record_block_lag_stores_null_lag_without_checkpoint() {
        let pool = setup_test_db().await;

        record_block_lag(&pool, &sample(0, 105, None))
            .await
            .unwrap();

        let (last_processed_block, lag_blocks): (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT last_processed_block, lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(last_processed_block, None);
        assert_eq!(lag_blocks, None);
    }

    #[tokio::test]
    async fn record_block_lag_saturates_stale_cutoff_to_zero() {
        let pool = setup_test_db().await;

        // A load-balanced RPC reported a cutoff block behind the checkpoint.
        record_block_lag(&pool, &sample(0, 99, Some(100)))
            .await
            .unwrap();

        let lag_blocks: Option<i64> =
            sqlx::query_scalar("SELECT lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(lag_blocks, Some(0));
    }

    /// Orderbook all test samples are recorded against.
    const ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");

    #[tokio::test]
    async fn record_poll_cycle_stores_outcome_and_error() {
        let pool = setup_test_db().await;

        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            ORDERBOOK,
            timestamp(0),
            Duration::from_millis(250),
            2,
            Err(&"rpc unreachable"),
        )
        .await
        .unwrap();

        let row: (String, String, i64, i64, String, Option<String>) = sqlx::query_as(
            "SELECT monitor, orderbook, duration_ms, skipped_ticks, outcome, error \
             FROM poll_cycle_samples",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, "order_fill");
        assert_eq!(row.1, ORDERBOOK.to_string());
        assert_eq!(row.2, 250);
        assert_eq!(row.3, 2);
        assert_eq!(row.4, PollOutcome::Error.as_str());
        assert_eq!(row.5, Some("rpc unreachable".to_string()));
    }

    #[tokio::test]
    async fn record_poll_cycle_stores_ok_outcome_with_null_error() {
        let pool = setup_test_db().await;
        use std::convert::Infallible;

        record_poll_cycle(
            &pool,
            Monitor::OrderFill,
            ORDERBOOK,
            timestamp(0),
            Duration::from_millis(100),
            0,
            Ok::<(), &Infallible>(()),
        )
        .await
        .unwrap();

        let (outcome, error): (String, Option<String>) =
            sqlx::query_as("SELECT outcome, error FROM poll_cycle_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            outcome,
            PollOutcome::Ok.as_str(),
            "a successful cycle must be stored with the ok discriminator"
        );
        assert_eq!(
            error, None,
            "a successful cycle must have a null error column"
        );
    }

    #[tokio::test]
    async fn prune_expired_keeps_samples_inside_retention() {
        let pool = setup_test_db().await;
        let now = timestamp(0);
        let expired = now - RETENTION - chrono::Duration::hours(1);
        // Exactly at the cutoff: the predicate is strictly `<`, so a sample
        // precisely RETENTION old must survive.
        let at_cutoff = now - RETENTION;
        let retained = now - RETENTION + chrono::Duration::hours(1);

        for sampled_at in [expired, at_cutoff, retained] {
            record_block_lag(
                &pool,
                &BlockLagSample {
                    sampled_at,
                    ..sample(0, 105, Some(100))
                },
            )
            .await
            .unwrap();
            record_poll_cycle(
                &pool,
                Monitor::OrderFill,
                ORDERBOOK,
                sampled_at,
                Duration::ZERO,
                0,
                Ok::<(), &Infallible>(()),
            )
            .await
            .unwrap();
        }

        prune_expired(&pool, now).await.unwrap();

        // Both tables share the cutoff computation; assert each strictly-`<`
        // predicate keeps the at-cutoff row by listing the survivors exactly.
        let lag_rows: Vec<String> =
            sqlx::query_scalar("SELECT sampled_at FROM block_lag_samples ORDER BY sampled_at")
                .fetch_all(&pool)
                .await
                .unwrap();
        let cycle_rows: Vec<String> =
            sqlx::query_scalar("SELECT sampled_at FROM poll_cycle_samples ORDER BY sampled_at")
                .fetch_all(&pool)
                .await
                .unwrap();
        let survivors = vec![sqlite_timestamp(at_cutoff), sqlite_timestamp(retained)];
        assert_eq!(
            lag_rows, survivors,
            "the at-cutoff and in-retention lag samples survive; only the \
             expired one is pruned"
        );
        assert_eq!(
            cycle_rows, survivors,
            "the at-cutoff and in-retention poll cycles survive; only the \
             expired one is pruned"
        );
    }

    #[test]
    fn sqlite_timestamp_matches_sqlite_strftime_format() {
        let formatted = sqlite_timestamp(Utc.timestamp_opt(1_750_000_000, 123_000_000).unwrap());
        assert_eq!(formatted, "2025-06-15T15:06:40.123Z");
    }

    /// Samples are stamped relative to now: the writer task prunes expired
    /// rows on its first timer tick, so historic timestamps would be
    /// deleted right after insertion.
    fn call_sample(
        seconds: i64,
        dependency: Dependency,
        error: Option<&str>,
    ) -> DependencyCallSample {
        DependencyCallSample {
            recorded_at: Utc::now() + chrono::Duration::seconds(seconds),
            dependency,
            operation: Cow::Borrowed("eth_blockNumber"),
            duration: Duration::from_millis(42),
            error: error.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn writer_persists_batched_samples_and_exits_on_close() {
        let pool = setup_test_db().await;
        let (sender, receiver) = TelemetrySender::channel();
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);

        sender.record(call_sample(0, Dependency::Rpc, None));
        sender.record(call_sample(1, Dependency::Broker, Some("rejected")));
        drop(sender);
        writer.await.unwrap();

        let rows: Vec<(String, String, i64, String, Option<String>)> = sqlx::query_as(
            "SELECT dependency, operation, duration_ms, outcome, error \
             FROM dependency_call_samples ORDER BY recorded_at",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "rpc");
        assert_eq!(rows[0].1, "eth_blockNumber");
        assert_eq!(rows[0].2, 42);
        assert_eq!(rows[0].3, "ok");
        assert_eq!(rows[0].4, None);
        assert_eq!(rows[1].0, "broker");
        assert_eq!(rows[1].3, "error");
        assert_eq!(rows[1].4, Some("rejected".to_string()));
    }

    #[test]
    fn scrub_secrets_redacts_url_credentials() {
        // dRPC-style key in the query string. The closing `)` is surrounding
        // message punctuation, not part of the URL, and must be preserved.
        assert_eq!(
            scrub_secrets("error sending request for url (https://lb.drpc.org/ogrpc?dkey=SECRET)"),
            "error sending request for url (https://lb.drpc.org/<redacted>)"
        );
        // Alchemy-style key in the path.
        assert_eq!(
            scrub_secrets("connect error: https://base-mainnet.g.alchemy.com/v2/SECRET_KEY"),
            "connect error: https://base-mainnet.g.alchemy.com/<redacted>"
        );
        // userinfo credentials.
        assert_eq!(
            scrub_secrets("https://user:pass@node.example.com"),
            "https://node.example.com"
        );
        // A plain message with no URL is untouched.
        assert_eq!(
            scrub_secrets("broker rejected order: insufficient funds"),
            "broker rejected order: insufficient funds"
        );
    }

    #[test]
    fn scrub_secrets_bare_host_is_returned_verbatim() {
        // A bare-host URL with no path or query has no credentials to redact;
        // it must pass through unchanged.
        assert_eq!(
            scrub_secrets("https://eth-mainnet.example.com"),
            "https://eth-mainnet.example.com"
        );
    }

    #[tokio::test]
    async fn record_never_panics_on_closed_or_disabled_channels() {
        let (sender, receiver) = TelemetrySender::channel();
        drop(receiver);
        sender.record(call_sample(0, Dependency::Rpc, None));
        assert_eq!(
            sender.dropped_count(),
            1,
            "a record on a closed channel must increment the drop counter"
        );

        let disabled = TelemetrySender::disabled();
        disabled.record(call_sample(0, Dependency::Broker, None));
        assert_eq!(
            disabled.dropped_count(),
            0,
            "a disabled sender has nothing to drop; its count must stay zero"
        );
    }

    #[tokio::test]
    async fn prune_expired_dependency_calls_respects_retention() {
        let pool = setup_test_db().await;
        let now = timestamp(0);
        let expired = call_sample(0, Dependency::Rpc, None);
        let expired = DependencyCallSample {
            recorded_at: now - RETENTION - chrono::Duration::hours(1),
            ..expired
        };
        let retained = DependencyCallSample {
            recorded_at: now - RETENTION + chrono::Duration::hours(1),
            ..call_sample(0, Dependency::Rpc, None)
        };
        write_dependency_calls(&pool, &[expired, retained])
            .await
            .unwrap();

        prune_expired_dependency_calls(&pool, now).await.unwrap();

        // Assert the surviving row is the retained one, not merely that one
        // row remains: a reversed predicate would keep the expired row and
        // still leave a count of 1.
        let survivors: Vec<String> =
            sqlx::query_scalar("SELECT recorded_at FROM dependency_call_samples")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            survivors,
            vec![sqlite_timestamp(
                now - RETENTION + chrono::Duration::hours(1)
            )],
            "only the in-retention dependency sample survives"
        );
    }

    #[tokio::test]
    async fn record_drops_and_counts_samples_when_the_channel_is_full() {
        // No writer drains the channel, so it fills at capacity. record() must
        // never block: the test completing is itself the non-blocking proof.
        let (sender, mut receiver) = TelemetrySender::channel();
        let overflow = 10;
        for index in 0..DEPENDENCY_CHANNEL_CAPACITY + overflow {
            sender.record(call_sample(
                i64::try_from(index).unwrap(),
                Dependency::Rpc,
                None,
            ));
        }

        assert_eq!(
            sender.dropped_count(),
            u64::try_from(overflow).unwrap(),
            "every sample past capacity is dropped and counted"
        );

        let mut drained = Vec::new();
        let drained_count = receiver.recv_many(&mut drained, usize::MAX).await;
        assert_eq!(
            drained_count, DEPENDENCY_CHANNEL_CAPACITY,
            "exactly the channel capacity is buffered; the overflow was dropped"
        );
    }
}
