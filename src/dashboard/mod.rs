//! WebSocket-based dashboard for real-time server state streaming.

use futures_util::SinkExt;
use rocket::{Route, State, get, routes};
use rocket_ws::{Channel, Message, WebSocket};
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{InitialState, ServerMessage};

mod event;

pub(crate) use event::EventBroadcaster;

pub(crate) struct Broadcast {
    pub(crate) sender: broadcast::Sender<ServerMessage>,
}

#[get("/ws")]
fn ws_endpoint(ws: WebSocket, broadcast: &State<Broadcast>) -> Channel<'_> {
    let mut receiver = broadcast.sender.subscribe();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            // Stub initial state for dashboard skeleton. Real data will be populated as
            // each panel is implemented: #178 (metrics), #179 (inventory), #180 (spreads),
            // #181 (trades), #182 (rebalances), #183 (circuit breaker), #184 (auth).
            let initial = ServerMessage::Initial(Box::new(InitialState::stub()));
            let json = match serde_json::to_string(&initial) {
                Ok(serialized) => serialized,
                Err(error) => {
                    warn!("Failed to serialize initial state: {error}");
                    return Ok(());
                }
            };

            if let Err(error) = stream.send(Message::Text(json)).await {
                warn!("Failed to send initial state: {error}");
                return Ok(());
            }

            loop {
                match receiver.recv().await {
                    Ok(msg) => {
                        let json = match serde_json::to_string(&msg) {
                            Ok(serialized) => serialized,
                            Err(error) => {
                                warn!("Failed to serialize message: {error}");
                                continue;
                            }
                        };

                        if let Err(error) = stream.send(Message::Text(json)).await {
                            warn!("Failed to send message to client: {error}");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("Client lagged, skipped {skipped} messages");
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
    routes![ws_endpoint]
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use rocket::config::Config;
    use rocket::fairing::AdHoc;
    use st0x_dto::EventStoreEntry;
    use tokio::sync::oneshot;
    use tokio_tungstenite::connect_async;

    use super::*;

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
    async fn websocket_routes_returns_one_route() {
        let route_list = routes();
        assert_eq!(route_list.len(), 1);
    }

    #[tokio::test]
    async fn websocket_endpoint_sends_initial_message() {
        let broadcast = create_test_broadcast();

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
                .unwrap_or_else(|error| panic!("client{} message error: {}", i + 1, error));

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
            timestamp: chrono::Utc::now(),
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
                .unwrap_or_else(|error| panic!("client{} error: {}", i + 1, error));

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
            timestamp: chrono::Utc::now(),
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
            timestamp: chrono::Utc::now(),
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
}
