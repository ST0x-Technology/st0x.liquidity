use serde_json::Value;
use sqlx::SqlitePool;

mod arbitrage;
mod rebalancing;

#[derive(Debug, sqlx::FromRow)]
struct StoredEvent {
    aggregate_type: String,
    aggregate_id: String,
    #[allow(dead_code)]
    sequence: i64,
    event_type: String,
    #[allow(dead_code)]
    event_version: String,
    payload: Value,
    #[allow(dead_code)]
    metadata: Value,
}

/// Fetches all domain events, filtering out internal event-sorcery
/// infrastructure events (e.g. SchemaRegistry reconciliation).
async fn fetch_events(pool: &SqlitePool) -> Vec<StoredEvent> {
    sqlx::query_as::<_, StoredEvent>(
        "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
         FROM events \
         WHERE aggregate_type != 'SchemaRegistry' \
         ORDER BY rowid ASC",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[derive(Debug, PartialEq)]
struct ExpectedEvent {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
}

impl ExpectedEvent {
    fn new(aggregate_type: &str, aggregate_id: &str, event_type: &str) -> Self {
        Self {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
        }
    }
}

/// Fetches all events and asserts that the `(aggregate_type, aggregate_id, event_type)` triples
/// match the expected sequence exactly.
async fn assert_events(pool: &SqlitePool, expected: &[ExpectedEvent]) -> Vec<StoredEvent> {
    let events = fetch_events(pool).await;
    let actual: Vec<ExpectedEvent> = events
        .iter()
        .map(|e| ExpectedEvent::new(&e.aggregate_type, &e.aggregate_id, &e.event_type))
        .collect();
    assert_eq!(actual, expected);
    events
}

/// Finds the first event matching the given `aggregate_type` and `event_type`.
/// Panics with a diagnostic message listing all events if no match is found.
#[allow(dead_code)]
fn find_event<'a>(
    events: &'a [StoredEvent],
    aggregate_type: &str,
    event_type: &str,
) -> &'a StoredEvent {
    events
        .iter()
        .find(|e| e.aggregate_type == aggregate_type && e.event_type == event_type)
        .unwrap_or_else(|| {
            let available: Vec<String> = events
                .iter()
                .map(|e| format!("  {}::{} (agg={})", e.aggregate_type, e.event_type, e.aggregate_id))
                .collect();
            panic!(
                "No event with aggregate_type={aggregate_type:?}, event_type={event_type:?}.\nAvailable events:\n{}",
                available.join("\n")
            )
        })
}
