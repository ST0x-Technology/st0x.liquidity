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

async fn fetch_events(pool: &SqlitePool) -> Vec<StoredEvent> {
    sqlx::query_as::<_, StoredEvent>(
        "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
         FROM events \
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
