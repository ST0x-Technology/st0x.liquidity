//! Lightweight operational telemetry store.
//!
//! Persists high-frequency ingestion-health samples (chain-tip block lag,
//! poll-cycle duration and skipped ticks) to plain SQLite tables, outside
//! the CQRS event store: these are operational observations, not domain
//! events. Writes are best-effort from the caller's perspective -- a failed
//! sample must never fail the monitored operation -- and rows older than
//! [`RETENTION`] are pruned opportunistically.

use std::fmt::Display;
use std::num::TryFromIntError;
use std::time::Duration;

use alloy::primitives::Address;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::SqlitePool;
use thiserror::Error;

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
    pub(crate) finalized_block: u64,
    /// `None` before the first backfill checkpoint exists.
    pub(crate) last_processed_block: Option<u64>,
}

impl BlockLagSample {
    /// Finalized blocks not yet processed: `finalized_block -
    /// last_processed_block`. Measured against the finalized block (the
    /// ingestion cutoff) rather than the raw tip because the checkpoint can
    /// only ever advance to the finalized block -- against the raw tip a fully
    /// caught-up system would read a permanent floor of the finality lag
    /// instead of zero. Saturates at zero: a load-balanced RPC can briefly
    /// report a finalized block behind the checkpoint, which is staleness
    /// noise, not negative lag.
    fn lag_blocks(&self) -> Option<u64> {
        self.last_processed_block
            .map(|checkpoint| self.finalized_block.saturating_sub(checkpoint))
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
    let last_processed_block = sample.last_processed_block.map(i64::try_from).transpose()?;

    sqlx::query(
        "INSERT INTO block_lag_samples \
         (sampled_at, orderbook, chain_tip, finalized_block, last_processed_block, \
          lag_blocks) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(sqlite_timestamp(sample.sampled_at))
    .bind(sample.orderbook.to_string())
    .bind(i64::try_from(sample.chain_tip)?)
    .bind(i64::try_from(sample.finalized_block)?)
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
            // Finality trails the tip by a few blocks in this mock.
            finalized_block: chain_tip.saturating_sub(3),
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
        // Lag measures finalized_block (105 - 3 = 102) minus the checkpoint
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
    async fn record_block_lag_saturates_stale_finalized_to_zero() {
        let pool = setup_test_db().await;

        // A load-balanced RPC reported a finalized block behind the checkpoint.
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
}
