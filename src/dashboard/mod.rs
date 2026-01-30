use futures_util::SinkExt;
use rocket::serde::json::Json;
use rocket::{Route, State, get, routes};
use rocket_ws::{Channel, Message, WebSocket};
use serde::Serialize;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::warn;
use ts_rs::TS;

mod auth;
mod circuit_breaker;
mod event;
mod inventory;
mod performance;
mod position;
mod rebalance;
mod spread;
mod trade;
mod ts;

pub(crate) use event::EventBroadcaster;
pub use ts::export_bindings;

use auth::AuthStatus;
use circuit_breaker::CircuitBreakerStatus;
use event::{EventStoreEntry, load_events_before, load_recent_events};
use inventory::Inventory;
use performance::PerformanceMetrics;
use rebalance::RebalanceOperation;
use spread::SpreadSummary;
use trade::Trade;

/// Messages sent from the server to WebSocket clients.
///
/// Note: Variants for future message types (trades, positions, metrics, etc.)
/// will be added as their respective dashboard panels are implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    Initial(Box<InitialState>),
    Event(EventStoreEntry),
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct InitialState {
    recent_trades: Vec<Trade>,
    inventory: Inventory,
    metrics: PerformanceMetrics,
    spreads: Vec<SpreadSummary>,
    active_rebalances: Vec<RebalanceOperation>,
    recent_rebalances: Vec<RebalanceOperation>,
    auth_status: AuthStatus,
    circuit_breaker: CircuitBreakerStatus,
    recent_events: Vec<EventStoreEntry>,
}

impl InitialState {
    #[cfg(test)]
    fn stub() -> Self {
        Self {
            recent_trades: Vec::new(),
            inventory: Inventory::empty(),
            metrics: PerformanceMetrics::zero(),
            spreads: Vec::new(),
            active_rebalances: Vec::new(),
            recent_rebalances: Vec::new(),
            auth_status: AuthStatus::NotConfigured,
            circuit_breaker: CircuitBreakerStatus::Active,
            recent_events: Vec::new(),
        }
    }
}

/// Paginated response for the events HTTP endpoint.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventsPage {
    events: Vec<EventStoreEntry>,
    has_more: bool,
}

pub(crate) struct Broadcast {
    pub(crate) sender: broadcast::Sender<ServerMessage>,
}

const DEFAULT_PAGE_SIZE: i64 = 50;

/// HTTP endpoint for paginated events.
///
/// - `GET /events` - Returns the first page (newest events)
/// - `GET /events?before=123` - Returns events with rowid < 123 (older events)
/// - `GET /events?limit=25` - Customize page size (default 50)
#[get("/events?<limit>&<before>")]
async fn get_events(
    pool: &State<SqlitePool>,
    limit: Option<i64>,
    before: Option<i64>,
) -> Json<EventsPage> {
    let page_size = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    // Request one extra to determine if there are more pages
    let fetch_limit = page_size + 1;

    let mut events = match before {
        Some(cursor) => match load_events_before(pool.inner(), cursor, fetch_limit).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to load_events_before: {e}");
                Vec::new()
            }
        },
        None => match load_recent_events(pool.inner(), fetch_limit).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to load_recent_events: {e}");
                Vec::new()
            }
        },
    };

    let page_size_usize = usize::try_from(page_size).unwrap_or(usize::MAX);
    let has_more = events.len() > page_size_usize;
    if has_more {
        events.pop();
    }

    Json(EventsPage { events, has_more })
}

#[get("/ws")]
fn ws_endpoint<'a>(
    ws: WebSocket,
    broadcast: &'a State<Broadcast>,
    pool: &'a State<SqlitePool>,
) -> Channel<'a> {
    let mut receiver = broadcast.sender.subscribe();
    let pool = pool.inner().clone();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            let recent_events = match event::load_recent_events(&pool, 100).await {
                Ok(events) => events,
                Err(e) => {
                    warn!("Failed to load recent events: {e}");
                    Vec::new()
                }
            };

            let initial = ServerMessage::Initial(Box::new(InitialState {
                recent_trades: Vec::new(),
                inventory: Inventory::empty(),
                metrics: PerformanceMetrics::zero(),
                spreads: Vec::new(),
                active_rebalances: Vec::new(),
                recent_rebalances: Vec::new(),
                auth_status: AuthStatus::NotConfigured,
                circuit_breaker: CircuitBreakerStatus::Active,
                recent_events,
            }));

            let json = match serde_json::to_string(&initial) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to serialize initial state: {e}");
                    return Ok(());
                }
            };

            if let Err(e) = stream.send(Message::Text(json)).await {
                warn!("Failed to send initial state: {e}");
                return Ok(());
            }

            loop {
                match receiver.recv().await {
                    Ok(msg) => {
                        let json = match serde_json::to_string(&msg) {
                            Ok(j) => j,
                            Err(e) => {
                                warn!("Failed to serialize message: {e}");
                                continue;
                            }
                        };

                        if let Err(e) = stream.send(Message::Text(json)).await {
                            warn!("Failed to send message to client: {e}");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client lagged, skipped {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            Ok(())
        })
    })
}

pub(crate) fn routes() -> Vec<Route> {
    routes![get_events, ws_endpoint]
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use rocket::config::Config;
    use rocket::fairing::AdHoc;
    use rocket::local::asynchronous::Client;
    use tokio::sync::oneshot;
    use tokio_tungstenite::connect_async;

    fn create_test_broadcast() -> Broadcast {
        let (sender, _) = broadcast::channel(256);
        Broadcast { sender }
    }

    #[tokio::test]
    async fn initial_state_stub_serializes_correctly() {
        let initial = InitialState::stub();
        let json = serde_json::to_string(&initial).expect("serialization should succeed");
        assert!(json.contains("recentTrades"));
        assert!(json.contains("inventory"));
        assert!(json.contains("metrics"));
        assert!(json.contains("authStatus"));
        assert!(json.contains("circuitBreaker"));
        assert!(
            json.contains("recentEvents"),
            "InitialState should include recentEvents field"
        );
    }

    #[tokio::test]
    async fn server_message_initial_serializes_with_type_tag() {
        let msg = ServerMessage::Initial(Box::new(InitialState::stub()));
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(json.contains(r#""type":"initial""#));
        assert!(json.contains(r#""data":"#));
    }

    #[tokio::test]
    async fn broadcast_channel_delivers_messages_to_subscribers() {
        let broadcast = create_test_broadcast();
        let mut rx = broadcast.sender.subscribe();

        let sent_msg = ServerMessage::Initial(Box::new(InitialState::stub()));
        broadcast
            .sender
            .send(sent_msg.clone())
            .expect("send should succeed");

        let recv_msg = rx.recv().await.expect("receive should succeed");
        let original_json = serde_json::to_string(&sent_msg).expect("serialization should succeed");
        let received_json = serde_json::to_string(&recv_msg).expect("serialization should succeed");
        assert_eq!(original_json, received_json);
    }

    #[tokio::test]
    async fn broadcast_supports_multiple_subscribers() {
        let broadcast = create_test_broadcast();
        let mut receiver1 = broadcast.sender.subscribe();
        let mut receiver2 = broadcast.sender.subscribe();

        let msg = ServerMessage::Initial(Box::new(InitialState::stub()));
        broadcast.sender.send(msg).expect("send should succeed");

        receiver1
            .recv()
            .await
            .expect("receiver1 should get message");
        receiver2
            .recv()
            .await
            .expect("receiver2 should get message");
    }

    #[tokio::test]
    async fn routes_includes_events_and_websocket() {
        let route_list = routes();
        assert_eq!(route_list.len(), 2);
    }

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:")
            .await
            .expect("create in-memory db");
        sqlx::migrate!().run(&pool).await.expect("run migrations");
        pool
    }

    #[tokio::test]
    async fn websocket_endpoint_sends_initial_message() {
        let broadcast = create_test_broadcast();
        let pool = create_test_pool().await;

        let config = Config {
            port: 0, // Let OS assign a random available port
            log_level: rocket::config::LogLevel::Off,
            ..Config::debug_default()
        };

        let (port_tx, port_rx) = oneshot::channel::<u16>();
        let port_tx = std::sync::Mutex::new(Some(port_tx));

        let rocket = rocket::build()
            .configure(config)
            .mount("/api", routes())
            .manage(broadcast)
            .manage(pool)
            .attach(AdHoc::on_liftoff("Port Sender", move |rocket| {
                Box::pin(async move {
                    let maybe_tx = port_tx.lock().unwrap().take();
                    if let Some(tx) = maybe_tx {
                        let _ = tx.send(rocket.config().port);
                    }
                })
            }));

        let rocket = rocket.ignite().await.expect("ignite failed");
        let shutdown_handle = rocket.shutdown();

        tokio::spawn(async move {
            let _ = rocket.launch().await;
        });

        let port = port_rx.await.expect("failed to receive port");

        let url = format!("ws://127.0.0.1:{port}/api/ws");
        let (mut ws_stream, _response) = connect_async(&url)
            .await
            .expect("WebSocket connection failed");

        let msg = ws_stream
            .next()
            .await
            .expect("stream closed")
            .expect("message error");

        let text = msg.into_text().expect("expected text message");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["type"], "initial");
        assert!(parsed["data"]["recentTrades"].is_array());
        assert!(parsed["data"]["inventory"].is_object());

        shutdown_handle.notify();
    }

    async fn start_test_server() -> (u16, rocket::Shutdown, Broadcast) {
        let broadcast = create_test_broadcast();
        let broadcast_clone = Broadcast {
            sender: broadcast.sender.clone(),
        };
        let pool = create_test_pool().await;

        let config = Config {
            port: 0,
            log_level: rocket::config::LogLevel::Off,
            ..Config::debug_default()
        };

        let (port_tx, port_rx) = oneshot::channel::<u16>();
        let port_tx = std::sync::Mutex::new(Some(port_tx));

        let rocket = rocket::build()
            .configure(config)
            .mount("/api", routes())
            .manage(broadcast_clone)
            .manage(pool)
            .attach(AdHoc::on_liftoff("Port Sender", move |rocket| {
                Box::pin(async move {
                    let maybe_tx = port_tx.lock().unwrap().take();
                    if let Some(tx) = maybe_tx {
                        let _ = tx.send(rocket.config().port);
                    }
                })
            }));

        let rocket = rocket.ignite().await.expect("ignite failed");
        let shutdown_handle = rocket.shutdown();

        tokio::spawn(async move {
            let _ = rocket.launch().await;
        });

        let port = port_rx.await.expect("failed to receive port");

        (port, shutdown_handle, broadcast)
    }

    #[tokio::test]
    async fn multiple_concurrent_clients_receive_initial_message() {
        let (port, shutdown_handle, _broadcast) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");
        let (mut client3, _) = connect_async(&url)
            .await
            .expect("client3 connection failed");

        for (i, client) in [&mut client1, &mut client2, &mut client3]
            .iter_mut()
            .enumerate()
        {
            let msg = client
                .next()
                .await
                .unwrap_or_else(|| panic!("client{} stream closed", i + 1))
                .unwrap_or_else(|e| panic!("client{} message error: {}", i + 1, e));

            let text = msg.into_text().expect("expected text message");
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

            assert_eq!(
                parsed["type"],
                "initial",
                "client{} should receive initial message",
                i + 1
            );
        }

        shutdown_handle.notify();
    }

    #[tokio::test]
    async fn broadcast_message_reaches_connected_clients() {
        let (port, shutdown_handle, broadcast) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        // Consume initial messages
        client1.next().await.expect("client1 initial").unwrap();
        client2.next().await.expect("client2 initial").unwrap();

        // Broadcast an event message
        let event = EventStoreEntry {
            aggregate_type: "TestAggregate".to_string(),
            aggregate_id: "test-123".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            rowid: 0,
        };
        let broadcast_msg = ServerMessage::Event(event);
        broadcast
            .sender
            .send(broadcast_msg)
            .expect("broadcast send");

        // Both clients should receive the broadcast
        let results = join_all([client1.next(), client2.next()]).await;

        for (i, result) in results.into_iter().enumerate() {
            let msg = result
                .unwrap_or_else(|| panic!("client{} stream closed", i + 1))
                .unwrap_or_else(|e| panic!("client{} error: {}", i + 1, e));

            let text = msg.into_text().expect("expected text");
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

            assert_eq!(
                parsed["type"],
                "event",
                "client{} should receive event message",
                i + 1
            );
            assert_eq!(parsed["data"]["aggregate_type"], "TestAggregate");
            assert_eq!(parsed["data"]["aggregate_id"], "test-123");
        }

        shutdown_handle.notify();
    }

    #[tokio::test]
    async fn client_disconnect_does_not_affect_other_clients() {
        let (port, shutdown_handle, broadcast) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        // Consume initial messages
        client1.next().await.expect("client1 initial").unwrap();

        // Drop client2 to simulate disconnect
        drop(client2);

        // Give the server a moment to process the disconnect
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Broadcast a message - should still reach client1
        let event = EventStoreEntry {
            aggregate_type: "StillWorking".to_string(),
            aggregate_id: "after-disconnect".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            rowid: 0,
        };
        broadcast
            .sender
            .send(ServerMessage::Event(event))
            .expect("broadcast send");

        // client1 should still receive messages
        let msg = client1
            .next()
            .await
            .expect("client1 stream closed")
            .expect("client1 error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["type"], "event");
        assert_eq!(parsed["data"]["aggregate_type"], "StillWorking");

        shutdown_handle.notify();
    }

    #[tokio::test]
    async fn new_client_receives_initial_not_previous_broadcasts() {
        let (port, shutdown_handle, broadcast) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        // Connect first client to have a receiver
        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");

        // Consume initial message for client1
        client1.next().await.expect("client1 initial").unwrap();

        // Broadcast a message (client1 will receive it)
        let event = EventStoreEntry {
            aggregate_type: "OldEvent".to_string(),
            aggregate_id: "before-client2".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            rowid: 0,
        };
        broadcast
            .sender
            .send(ServerMessage::Event(event))
            .expect("broadcast send");

        // Consume the broadcast on client1
        client1.next().await.expect("client1 broadcast").unwrap();

        // Now connect a second client - should get initial, not the old broadcast
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        let msg = client2
            .next()
            .await
            .expect("stream closed")
            .expect("message error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(
            parsed["type"], "initial",
            "new client should receive initial, not previous broadcast"
        );

        shutdown_handle.notify();
    }

    #[tokio::test]
    async fn get_events_returns_empty_page_when_no_events() {
        let pool = create_test_pool().await;
        let broadcast = create_test_broadcast();

        let rocket = rocket::build()
            .mount("/api", routes())
            .manage(broadcast)
            .manage(pool);

        let client = Client::tracked(rocket).await.expect("valid rocket");
        let response = client.get("/api/events").dispatch().await;

        assert_eq!(response.status(), rocket::http::Status::Ok);

        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        assert_eq!(parsed["events"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["hasMore"], false);
    }

    #[tokio::test]
    async fn get_events_returns_events_in_descending_rowid_order() {
        let pool = create_test_pool().await;
        let broadcast = create_test_broadcast();

        // Insert some test events
        for i in 1..=5 {
            sqlx::query(
                r"INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                   VALUES ('TestAggregate', 'agg', ?, ?, '1.0', '{}', '{}')",
            )
            .bind(i)
            .bind(format!("Event{i}"))
            .execute(&pool)
            .await
            .expect("insert event");
        }

        let rocket = rocket::build()
            .mount("/api", routes())
            .manage(broadcast)
            .manage(pool);

        let client = Client::tracked(rocket).await.expect("valid rocket");
        let response = client.get("/api/events").dispatch().await;

        assert_eq!(response.status(), rocket::http::Status::Ok);

        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        let events = parsed["events"].as_array().expect("events array");
        assert_eq!(events.len(), 5);
        assert_eq!(events[0]["event_type"], "Event5", "newest event first");
        assert_eq!(events[4]["event_type"], "Event1", "oldest event last");

        // Verify rowids are in descending order
        let rowid0 = events[0]["rowid"].as_i64().unwrap();
        let rowid4 = events[4]["rowid"].as_i64().unwrap();
        assert!(rowid0 > rowid4, "rowids should be descending");
    }

    #[tokio::test]
    async fn get_events_with_before_cursor_returns_older_events() {
        let pool = create_test_pool().await;
        let broadcast = create_test_broadcast();

        // Insert 10 test events
        for i in 1..=10 {
            sqlx::query(
                r"INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                   VALUES ('TestAggregate', 'agg', ?, ?, '1.0', '{}', '{}')",
            )
            .bind(i)
            .bind(format!("Event{i}"))
            .execute(&pool)
            .await
            .expect("insert event");
        }

        let rocket = rocket::build()
            .mount("/api", routes())
            .manage(broadcast)
            .manage(pool);

        let client = Client::tracked(rocket).await.expect("valid rocket");

        // First request: get all events to find a cursor
        let response = client.get("/api/events?limit=5").dispatch().await;
        let body = response.into_string().await.expect("response body");
        let first_page: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        let events = first_page["events"].as_array().expect("events array");
        assert_eq!(events.len(), 5);
        assert!(first_page["hasMore"].as_bool().unwrap());

        // Get the last event's rowid as cursor for next page
        let cursor = events[4]["rowid"].as_i64().unwrap();

        // Second request: get events before cursor
        let response = client
            .get(format!("/api/events?before={cursor}"))
            .dispatch()
            .await;
        let body = response.into_string().await.expect("response body");
        let second_page: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        let events = second_page["events"].as_array().expect("events array");
        assert_eq!(events.len(), 5, "should return remaining 5 events");
        assert!(!second_page["hasMore"].as_bool().unwrap());

        // All events should have rowid < cursor
        for event in events {
            let rowid = event["rowid"].as_i64().unwrap();
            assert!(rowid < cursor, "all events should have rowid < {cursor}");
        }
    }

    #[tokio::test]
    async fn get_events_has_more_indicates_additional_pages() {
        let pool = create_test_pool().await;
        let broadcast = create_test_broadcast();

        // Insert exactly 3 events
        for i in 1..=3 {
            sqlx::query(
                r"INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
                   VALUES ('TestAggregate', 'agg', ?, ?, '1.0', '{}', '{}')",
            )
            .bind(i)
            .bind(format!("Event{i}"))
            .execute(&pool)
            .await
            .expect("insert event");
        }

        let rocket = rocket::build()
            .mount("/api", routes())
            .manage(broadcast)
            .manage(pool);

        let client = Client::tracked(rocket).await.expect("valid rocket");

        // Request with limit=3, should get all events and hasMore=false
        let response = client.get("/api/events?limit=3").dispatch().await;
        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        assert_eq!(parsed["events"].as_array().unwrap().len(), 3);
        assert!(!parsed["hasMore"].as_bool().unwrap());

        // Request with limit=2, should get 2 events and hasMore=true
        let response = client.get("/api/events?limit=2").dispatch().await;
        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        assert_eq!(parsed["events"].as_array().unwrap().len(), 2);
        assert!(parsed["hasMore"].as_bool().unwrap());
    }
}
