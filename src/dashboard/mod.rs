//! WebSocket-based dashboard for real-time server state streaming.

use futures_util::SinkExt;
use rocket::{Route, State, get, routes};
use rocket_ws::{Channel, Message, WebSocket};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

use st0x_dto::Statement;

use crate::inventory::BroadcastingInventory;

mod event;
mod transfer_loader;
pub(crate) use event::Broadcaster;

pub(crate) struct Broadcast {
    pub(crate) sender: broadcast::Sender<Statement>,
}

pub(crate) struct DashboardCtx {
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) pool: SqlitePool,
}

#[get("/ws")]
fn ws_endpoint<'r>(
    ws: WebSocket,
    broadcast: &'r State<Broadcast>,
    _dashboard: &'r State<DashboardCtx>,
) -> Channel<'r> {
    let mut receiver = broadcast.sender.subscribe();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            loop {
                match receiver.recv().await {
                    Ok(msg) => {
                        info!(?msg, "Broadcasting to dashboard client");
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
    use st0x_dto::Concern;
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio_tungstenite::connect_async;

    use super::*;

    fn test_statement() -> Statement {
        Statement {
            id: "test-123".to_string(),
            statement: Concern::Transfer,
        }
    }

    fn create_test_broadcast() -> Broadcast {
        let (sender, _) = broadcast::channel(256);
        Broadcast { sender }
    }

    async fn create_test_dashboard_state() -> DashboardCtx {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        DashboardCtx {
            inventory: Arc::new(BroadcastingInventory::new_without_broadcast(
                crate::inventory::InventoryView::default(),
            )),
            pool,
        }
    }

    #[tokio::test]
    async fn statement_serializes_with_type_tag() {
        let msg = test_statement();
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(json.contains(r#""id":"test-123""#));
        assert!(json.contains(r#""statement""#));
    }

    #[tokio::test]
    async fn broadcast_channel_delivers_messages_to_subscribers() {
        let broadcast = create_test_broadcast();
        let mut rx = broadcast.sender.subscribe();

        let sent_msg = test_statement();
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

        let msg = test_statement();
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

    async fn start_test_server() -> (u16, rocket::Shutdown, Broadcast) {
        let broadcast = create_test_broadcast();
        let broadcast_clone = Broadcast {
            sender: broadcast.sender.clone(),
        };
        let dashboard_state = create_test_dashboard_state().await;

        let config = Config {
            port: 0,
            log_level: rocket::config::LogLevel::Off,
            ..Config::debug_default()
        };

        let (port_tx, port_rx) = oneshot::channel::<u16>();
        let port_tx = Mutex::new(Some(port_tx));

        let rocket = rocket::build()
            .configure(config)
            .mount("/api", routes())
            .manage(broadcast_clone)
            .manage(dashboard_state)
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
    async fn broadcast_message_reaches_connected_clients() {
        let (port, shutdown_handle, broadcast) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        let broadcast_msg = Statement {
            id: "test-broadcast".to_string(),
            statement: Concern::Transfer,
        };
        broadcast
            .sender
            .send(broadcast_msg)
            .expect("broadcast send");

        let results = join_all([client1.next(), client2.next()]).await;

        for (idx, result) in results.into_iter().enumerate() {
            let msg = result
                .unwrap_or_else(|| panic!("client{} stream closed", idx + 1))
                .unwrap_or_else(|error| panic!("client{} error: {}", idx + 1, error));

            let text = msg.into_text().expect("expected text");
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

            assert_eq!(
                parsed["id"],
                "test-broadcast",
                "client{} should receive broadcast",
                idx + 1
            );
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

        drop(client2);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let broadcast_msg = Statement {
            id: "after-disconnect".to_string(),
            statement: Concern::Transfer,
        };
        broadcast
            .sender
            .send(broadcast_msg)
            .expect("broadcast send");

        let msg = client1
            .next()
            .await
            .expect("client1 stream closed")
            .expect("client1 error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["id"], "after-disconnect");

        shutdown_handle.notify();
    }
}
