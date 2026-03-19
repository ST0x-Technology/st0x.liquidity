//! WebSocket-based dashboard for real-time server state streaming.

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::sync::broadcast;
use tracing::{info, warn};

use st0x_dto::Statement;

mod event;
mod transfer_loader;
pub(crate) use event::Broadcaster;

async fn ws_endpoint(
    ws: WebSocketUpgrade,
    State(state): State<super::AppState>,
) -> impl IntoResponse {
    let receiver = state.event_sender.subscribe();

    ws.on_upgrade(move |socket| handle_ws(socket, receiver))
}

async fn handle_ws(mut socket: WebSocket, mut receiver: broadcast::Receiver<Statement>) {
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

                if let Err(error) = socket.send(Message::Text(json.into())).await {
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
}

pub(crate) fn routes() -> Router<super::AppState> {
    Router::new().route("/ws", get(ws_endpoint))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_tungstenite::connect_async;

    use st0x_dto::Concern;

    use super::*;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::inventory::{self, BroadcastingInventory};

    fn test_statement(id: &str) -> Statement {
        Statement {
            id: id.to_string(),
            statement: Concern::Inventory,
        }
    }

    async fn create_test_state() -> (super::super::AppState, broadcast::Sender<Statement>) {
        let (sender, _) = broadcast::channel(256);
        let sender_clone = sender.clone();

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let state = super::super::AppState {
            pool,
            ctx: create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            event_sender: sender,
            inventory: Arc::new(BroadcastingInventory::new(
                inventory::InventoryView::default(),
            )),
        };

        (state, sender_clone)
    }

    async fn start_test_server() -> (u16, tokio::task::AbortHandle, broadcast::Sender<Statement>) {
        let (state, sender) = create_test_state().await;

        let app = Router::new().nest("/api", routes()).with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (port, handle.abort_handle(), sender)
    }

    #[tokio::test]
    async fn broadcast_channel_delivers_messages_to_subscribers() {
        let (sender, _) = broadcast::channel(256);
        let mut receiver = sender.subscribe();

        let sent_msg = test_statement("test-1");

        sender.send(sent_msg.clone()).expect("send should succeed");

        let recv_msg = receiver.recv().await.expect("receive should succeed");
        let original_json = serde_json::to_string(&sent_msg).expect("serialization should succeed");
        let received_json = serde_json::to_string(&recv_msg).expect("serialization should succeed");
        assert_eq!(original_json, received_json);
    }

    #[tokio::test]
    async fn broadcast_supports_multiple_subscribers() {
        let (sender, _) = broadcast::channel::<Statement>(256);
        let mut receiver1 = sender.subscribe();
        let mut receiver2 = sender.subscribe();

        sender
            .send(test_statement("test-1"))
            .expect("send should succeed");

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
    async fn broadcast_message_reaches_connected_clients() {
        let (port, abort_handle, sender) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        sender
            .send(test_statement("test-123"))
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
                "test-123",
                "client{} should receive the broadcast message",
                idx + 1
            );
            assert_eq!(parsed["statement"]["type"], "inventory");
        }

        abort_handle.abort();
    }

    #[tokio::test]
    async fn client_disconnect_does_not_affect_other_clients() {
        let (port, abort_handle, sender) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        // Drop client2 to simulate disconnect
        drop(client2);

        // Give the server a moment to process the disconnect
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Broadcast a message - should still reach client1
        sender
            .send(test_statement("still-working"))
            .expect("broadcast send");

        let msg = client1
            .next()
            .await
            .expect("client1 stream closed")
            .expect("client1 error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["id"], "still-working");

        abort_handle.abort();
    }

    #[tokio::test]
    async fn new_client_does_not_receive_previous_broadcasts() {
        let (port, abort_handle, sender) = start_test_server().await;
        let url = format!("ws://127.0.0.1:{port}/api/ws");

        // Connect first client
        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");

        // Broadcast a message (client1 will receive it)
        sender
            .send(test_statement("old-event"))
            .expect("broadcast send");

        // Consume the broadcast on client1
        client1.next().await.expect("client1 broadcast").unwrap();

        // Connect a second client - should NOT receive the old broadcast
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        // Send a new event so client2 has something to receive
        sender
            .send(test_statement("new-event"))
            .expect("broadcast send");

        let msg = client2
            .next()
            .await
            .expect("stream closed")
            .expect("message error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(
            parsed["id"], "new-event",
            "new client should receive new event, not previous broadcast"
        );

        abort_handle.abort();
    }
}
