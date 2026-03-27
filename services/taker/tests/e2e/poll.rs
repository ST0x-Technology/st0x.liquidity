//! Bot lifecycle helpers and event polling for e2e tests.

use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use std::time::Duration;
use tokio::task::JoinHandle;

use st0x_execution::MarketDataProvider;
use st0x_taker::Ctx;

/// Spawns the full taker bot as a background task.
pub fn spawn_bot(
    ctx: Ctx,
    market_data: impl MarketDataProvider + 'static,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(st0x_taker::launch(ctx, market_data))
}

pub const POLL_INTERVAL: Duration = Duration::from_millis(200);
pub const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

/// Sleeps for [`POLL_INTERVAL`], panicking immediately if the bot task
/// exits during the sleep.
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
/// the given `event_type` exist.
pub async fn poll_for_events(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    event_type: &str,
    expected_count: i64,
) {
    let connect_opts = SqliteConnectOptions::new().filename(db_path);
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("{expected_count}x {event_type}");

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

/// Opens a SQLite connection to the test database.
pub async fn connect_db(db_path: &std::path::Path) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new().filename(db_path);
    Ok(SqlitePool::connect_with(options).await?)
}

/// Fetches all domain events ordered by insertion.
#[derive(Debug, sqlx::FromRow)]
pub struct StoredEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

pub async fn fetch_all_events(pool: &SqlitePool) -> anyhow::Result<Vec<StoredEvent>> {
    let events: Vec<StoredEvent> = sqlx::query_as(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
         FROM events \
         WHERE aggregate_type != 'SchemaRegistry' \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(events)
}
