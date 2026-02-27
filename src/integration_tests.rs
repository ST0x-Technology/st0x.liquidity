use serde_json::Value;
use sqlx::SqlitePool;

mod arbitrage;
mod rebalancing;

#[derive(Debug, sqlx::FromRow)]
struct StoredEvent {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Value,
}

/// Fetches all domain events, filtering out internal event-sorcery
/// infrastructure events (e.g. SchemaRegistry reconciliation).
async fn fetch_events(pool: &SqlitePool) -> Vec<StoredEvent> {
    sqlx::query_as::<_, StoredEvent>(
        "SELECT aggregate_type, aggregate_id, event_type, payload \
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
        .map(|event| {
            ExpectedEvent::new(
                &event.aggregate_type,
                &event.aggregate_id,
                &event.event_type,
            )
        })
        .collect();
    assert_eq!(actual, expected);
    events
}
