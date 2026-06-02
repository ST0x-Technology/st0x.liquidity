//! Bot lifecycle helpers and event polling for e2e tests.
//!
//! Provides `spawn_bot`, `wait_for_processing`, `sleep_or_crash` for
//! managing the bot task, and `poll_for_events*` for waiting on CQRS
//! events to appear in the database.

use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use st0x_dto::Statement;
use st0x_execution::FractionalShares;
use st0x_execution::alpaca_broker_api::OrderStatus;
use st0x_hedge::config::Ctx;
use st0x_hedge::{run_bot_session, run_bot_session_with_event_channel};

/// Spawns the full bot as a background task.
pub fn spawn_bot(ctx: Ctx) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run_bot_session(ctx))
}

/// Spawns the full bot with an externally-provided event channel,
/// allowing tests to inspect `receiver_count()` for dashboard auto-detect.
pub fn spawn_bot_with_event_channel(
    ctx: Ctx,
    event_sender: broadcast::Sender<Statement>,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run_bot_session_with_event_channel(ctx, event_sender))
}

/// Polls the bot's health endpoint until it responds, panicking if the
/// bot crashes or the timeout (30s) expires before it becomes ready.
pub async fn poll_for_ready(bot: &mut JoinHandle<anyhow::Result<()>>, port: u16) {
    let url = format!("http://localhost:{port}/health");
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        sleep_or_crash(bot, "health endpoint").await;

        let is_ready = client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());

        if is_ready {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for bot health endpoint at {url}",
        );
    }
}

/// Sleeps for `seconds`, then panics if the bot task has already finished
/// (indicating it crashed during processing).
pub async fn wait_for_processing(bot: &mut JoinHandle<anyhow::Result<()>>, seconds: u64) {
    tokio::select! {
        result = &mut *bot => {
            match result {
                Ok(Ok(())) => panic!("Bot exited cleanly before processing completed ({seconds}s)"),
                Ok(Err(error)) => panic!("Bot crashed during wait_for_processing: {error:#}"),
                Err(join_error) => panic!("Bot task panicked: {join_error}"),
            }
        }
        () = tokio::time::sleep(Duration::from_secs(seconds)) => {}
    }
}

pub const POLL_INTERVAL: Duration = Duration::from_millis(200);
pub const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

/// Sleeps for [`POLL_INTERVAL`], panicking immediately if the bot task
/// exits (crash or clean shutdown) during the sleep.
pub async fn sleep_or_crash(bot: &mut JoinHandle<anyhow::Result<()>>, context: &str) {
    tokio::select! {
        result = &mut *bot => {
            match result {
                Ok(Ok(())) => panic!("Bot exited cleanly while polling for: {context}"),
                Ok(Err(error)) => panic!("Bot crashed while polling for: {context}: {error:#}"),
                Err(join_error) => panic!("Bot panicked while polling for: {context}: {join_error}"),
            }
        }
        () = tokio::time::sleep(POLL_INTERVAL) => {}
    }
}

/// Polls the CQRS events table until at least `expected_count` events of
/// the given `event_type` exist, using the default 30s timeout.
pub async fn poll_for_events(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    event_type: &str,
    expected_count: i64,
) {
    poll_for_events_with_timeout(
        bot,
        db_path,
        event_type,
        expected_count,
        Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS),
    )
    .await;
}

/// Polls the CQRS events table until at least `expected_count` events of
/// the given `event_type` exist, with an explicit timeout.
///
/// Tolerates the database not existing yet (the bot creates it on
/// startup via migrations), retrying the connection each poll cycle.
pub async fn poll_for_events_with_timeout(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    event_type: &str,
    expected_count: i64,
    timeout: Duration,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_count}x {event_type}");

    loop {
        sleep_or_crash(bot, &context).await;

        // The DB file may not exist yet if the bot is still starting up.
        let Ok(pool) = SqlitePool::connect_with(connect_opts.clone()).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let query_result =
            sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind(event_type)
                .fetch_one(&pool)
                .await;

        pool.close().await;

        match query_result {
            Ok((count,)) if count >= expected_count => return,
            Ok((count,)) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (found {count})",
            ),
            Err(query_error) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} \
                 (query failed: {query_error})",
            ),
        }
    }
}

/// Polls for events matching an `aggregate_type` whose `event_type`
/// contains `type_substring` (e.g. "Failed"). Uses the default 30s
/// timeout.
///
/// Tolerates the database not existing yet (see
/// [`poll_for_events_with_timeout`]).
pub async fn poll_for_aggregate_events_containing(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    aggregate_type: &str,
    type_substring: &str,
    expected_count: i64,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_count}x {aggregate_type}/*{type_substring}*");

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect_with(connect_opts.clone()).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let query_result = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = ? AND event_type LIKE '%' || ? || '%'",
        )
        .bind(aggregate_type)
        .bind(type_substring)
        .fetch_one(&pool)
        .await;

        pool.close().await;

        match query_result {
            Ok((count,)) if count >= expected_count => return,
            Ok((count,)) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (found {count})",
            ),
            Err(query_error) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} \
                 (query failed: {query_error})",
            ),
        }
    }
}

/// Polls the `snapshots` table until a snapshot for the given aggregate
/// contains a non-empty value for the given JSON field.
///
/// Useful for compactable aggregates where events may be deleted before
/// a test can observe them. The snapshot is the durable record.
pub async fn poll_for_snapshot_field(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    aggregate_type: &str,
    field_name: &str,
    timeout: Duration,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{aggregate_type} snapshot with non-empty {field_name}");

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect_with(connect_opts.clone()).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let query_result = sqlx::query_as::<_, (String,)>(
            "SELECT payload FROM snapshots WHERE aggregate_type = ? LIMIT 1",
        )
        .bind(aggregate_type)
        .fetch_optional(&pool)
        .await;

        pool.close().await;

        match query_result {
            Ok(Some((payload,))) => {
                if let Ok(snapshot) = serde_json::from_str::<serde_json::Value>(&payload) {
                    // Snapshot payload is wrapped in Lifecycle state (e.g. {"Live": {...}})
                    let inner = snapshot.get("Live").and_then(|live| live.get(field_name));
                    match inner {
                        Some(value) if !value.is_null() => {
                            let is_non_empty = match value {
                                serde_json::Value::Object(map) => !map.is_empty(),
                                serde_json::Value::Array(arr) => !arr.is_empty(),
                                _ => true,
                            };
                            if is_non_empty {
                                return;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(None) => {}
            Err(query_error) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} \
                 (query failed: {query_error})",
            ),
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context}",
        );
    }
}

/// Polls the Position projection view until the given symbol has no
/// pending offchain order (hedge cycle completed).
///
/// Uses `pending_offchain_order_id == null` instead of `net == 0`
/// because Float precision truncation at the broker API boundary
/// can leave a tiny non-zero residual that never reaches exact zero.
pub async fn poll_for_hedge_completion(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
    timeout: Duration,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("Position({symbol}) hedge completed");

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect_with(connect_opts.clone()).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let query_result =
            sqlx::query_as::<_, (String,)>("SELECT payload FROM position_view WHERE view_id = ?")
                .bind(symbol)
                .fetch_optional(&pool)
                .await;

        pool.close().await;

        match query_result {
            Ok(Some((payload,))) => {
                if let Ok(lifecycle) = serde_json::from_str::<serde_json::Value>(&payload)
                    && let Some(live) = lifecycle.get("Live")
                {
                    let pending = live
                        .get("pending_offchain_order_id")
                        .and_then(|value| value.as_str());

                    if pending.is_none() {
                        return;
                    }
                }

                assert!(
                    tokio::time::Instant::now() < deadline,
                    "Timed out after {timeout:?} waiting for {context}",
                );
            }

            Ok(None) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (position not found)",
            ),

            Err(query_error) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} \
                 (query failed: {query_error})",
            ),
        }
    }
}

/// Polls until at least `expected_total` jobs exist and all have status 'Done'.
///
/// Jobs that retry with exponential backoff (e.g., due to CQRS aggregate
/// conflicts) may still be in-flight after higher-level conditions are met.
/// Use this instead of an immediate `count_done_jobs` assertion.
///
/// The self-rescheduling `CheckPositions` job is excluded from both totals,
/// since it always leaves exactly one `Pending` row in the queue waiting for
/// the next tick and is not "in-flight work".
pub async fn poll_for_all_jobs_done(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    expected_total: i64,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_total} jobs all done");

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect_with(connect_opts.clone()).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let check_positions = st0x_hedge::check_positions_job_type();
        let total = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM Jobs WHERE job_type != ?")
            .bind(check_positions)
            .fetch_one(&pool)
            .await;

        let done = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Done' AND job_type != ?",
        )
        .bind(check_positions)
        .fetch_one(&pool)
        .await;

        pool.close().await;

        match (total, done) {
            (Ok((total,)), Ok((done,))) if total >= expected_total && done == total => {
                return;
            }
            (Ok((total,)), Ok((done,))) => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} \
                 (total={total}, done={done})",
            ),
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (query failed)",
            ),
        }
    }
}

/// Opens a SQLite connection to the test database.
pub async fn connect_db(db_path: &std::path::Path) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new().filename(db_path);
    Ok(SqlitePool::connect_with(options).await?)
}

/// Counts CQRS events for a specific aggregate type.
pub async fn count_events(pool: &SqlitePool, aggregate_type: &str) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE aggregate_type = ?")
        .bind(aggregate_type)
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Counts total apalis jobs enqueued, excluding the periodic
/// `CheckPositions` job. CheckPositions reschedules itself on a
/// wall-clock interval, so its count is a function of test duration
/// rather than pipeline work and would dwarf the signal in
/// crash-recovery comparisons.
pub async fn count_jobs(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type != ?")
        .bind(st0x_hedge::check_positions_job_type())
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Counts apalis jobs that have been processed (status = 'Done'),
/// excluding the periodic `CheckPositions` job (see `count_jobs`).
pub async fn count_done_jobs(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM Jobs WHERE status = 'Done' AND job_type != ?")
            .bind(st0x_hedge::check_positions_job_type())
            .fetch_one(pool)
            .await?;

    Ok(row.0)
}

/// Fetches all domain events ordered by insertion.
pub async fn fetch_all_domain_events(
    pool: &SqlitePool,
) -> anyhow::Result<Vec<crate::assert::StoredEvent>> {
    let events: Vec<crate::assert::StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type != 'SchemaRegistry' \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(events)
}

/// Polls the broker mock until the total filled quantity for a symbol/side
/// reaches the expected amount. This asserts on the actual external
/// interaction (broker orders) rather than internal event counts.
pub async fn poll_for_broker_fills(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    broker: &st0x_execution::alpaca_broker_api::AlpacaBrokerMock,
    symbol: &str,
    side: st0x_execution::alpaca_broker_api::OrderSide,
    expected_total: FractionalShares,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_total} {symbol} {side}");

    loop {
        sleep_or_crash(bot, &context).await;

        let filled_total: FractionalShares = broker
            .orders()
            .iter()
            .filter(|order| {
                order.symbol == symbol && order.side == side && order.status == OrderStatus::Filled
            })
            .fold(FractionalShares::ZERO, |acc, order| {
                (acc + FractionalShares::new(order.quantity)).unwrap()
            });

        if filled_total == expected_total {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context}. \
             Current filled total: {filled_total}",
        );
    }
}

/// Fetches events for a specific aggregate type, ordered by insertion.
pub async fn fetch_events_by_type(
    pool: &SqlitePool,
    aggregate_type: &str,
) -> anyhow::Result<Vec<crate::assert::StoredEvent>> {
    let events: Vec<crate::assert::StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type = ? \
         ORDER BY rowid ASC",
    )
    .bind(aggregate_type)
    .fetch_all(pool)
    .await?;

    Ok(events)
}
