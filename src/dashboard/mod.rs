//! WebSocket-based dashboard for real-time server state streaming.

use futures_util::SinkExt;
use rocket::{Route, State, get, routes};
use rocket_ws::{Channel, Message, WebSocket};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

use st0x_dto::Statement;

use crate::inventory::BroadcastingInventory;

mod event;
pub(crate) use event::Broadcaster;

pub(crate) struct Broadcast {
    pub(crate) sender: broadcast::Sender<Statement>,
}

pub(crate) struct DashboardState {
    pub(crate) inventory: Arc<BroadcastingInventory>,
}

#[get("/ws")]
fn ws_endpoint<'r>(
    ws: WebSocket,
    broadcast: &'r State<Broadcast>,
    _dashboard: &'r State<DashboardState>,
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
    use st0x_dto::Concern;

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
}
