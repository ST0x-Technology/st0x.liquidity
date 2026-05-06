//! WebSocket-based dashboard for real-time server state streaming.

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{info, trace, warn};

use st0x_dto::{CurrentState, Statement};
use st0x_finance::Positive;

use crate::AppState;
use st0x_config::ExecutionThreshold;
use st0x_config::OperationMode;

mod event;
mod trade_loader;
pub(crate) mod transfer_loader;
pub(crate) use event::Broadcaster;

async fn ws_endpoint(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Outcome of attempting to send a serialized message over the socket.
enum SendOutcome {
    Sent,
    SerializeFailed,
    SocketClosed,
}

async fn send_json(socket: &mut WebSocket, value: &Statement) -> SendOutcome {
    let json = match serde_json::to_string(value) {
        Ok(serialized) => serialized,
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to serialize message");
            return SendOutcome::SerializeFailed;
        }
    };

    if let Err(error) = socket.send(Message::Text(json.into())).await {
        warn!(target: "dashboard", %error, "Failed to send message to client");
        return SendOutcome::SocketClosed;
    }

    SendOutcome::Sent
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut receiver = state.event_sender.subscribe();

    if !send_initial_state(&mut socket, &state).await {
        return;
    }

    stream_broadcasts(&mut socket, &mut receiver).await;
}

async fn send_initial_state(socket: &mut WebSocket, state: &AppState) -> bool {
    let inventory_dto = state.inventory.read().await.to_dto();
    let transfers = transfer_loader::load_transfers(&state.pool).await;
    let trades = trade_loader::load_trades(&state.pool).await;
    let positions = load_positions(&state.pool).await;

    let initial = Statement::CurrentState(Box::new(CurrentState {
        trades,
        inventory: inventory_dto,
        positions,
        settings: state.settings.clone(),
        active_transfers: transfers.active,
        recent_transfers: transfers.recent,
        warnings: transfers.warnings,
    }));

    match send_json(socket, &initial).await {
        SendOutcome::Sent => {
            info!(target: "dashboard", "Sent initial state to dashboard client");
            true
        }
        SendOutcome::SerializeFailed | SendOutcome::SocketClosed => false,
    }
}

async fn stream_broadcasts(socket: &mut WebSocket, receiver: &mut broadcast::Receiver<Statement>) {
    loop {
        match receiver.recv().await {
            Ok(msg) => {
                trace!(target: "dashboard", ?msg, "Broadcasting to dashboard client");
                match send_json(socket, &msg).await {
                    SendOutcome::Sent | SendOutcome::SerializeFailed => {}
                    SendOutcome::SocketClosed => break,
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(target: "dashboard", skipped, "Client lagged, skipped messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/ws", get(ws_endpoint))
}

pub(crate) fn settings_from_ctx(ctx: &st0x_config::Ctx) -> st0x_dto::Settings {
    let (equity_target, equity_deviation, usdc_target, usdc_deviation) =
        ctx.rebalancing_ctx().map_or_else(
            |_| (0.5, 0.2, None, None),
            |rebalancing| {
                let (ut, ud) = rebalancing.usdc.as_ref().map_or((None, None), |threshold| {
                    (
                        Some(float_to_f64(threshold.target, 0.5)),
                        Some(float_to_f64(threshold.deviation, 0.3)),
                    )
                });

                (
                    float_to_f64(rebalancing.equity.target, 0.5),
                    float_to_f64(rebalancing.equity.deviation, 0.2),
                    ut,
                    ud,
                )
            },
        );

    let execution_threshold = match &ctx.execution_threshold {
        ExecutionThreshold::Shares(shares) => {
            let formatted = shares
                .inner()
                .inner()
                .format()
                .unwrap_or_else(|_| "?".to_string());

            format!("{formatted} shares")
        }
        ExecutionThreshold::DollarValue(usd) => {
            let formatted = usd.inner().format().unwrap_or_else(|_| "?".to_string());

            format!("${formatted}")
        }
    };

    let assets = ctx
        .assets
        .equities
        .symbols
        .iter()
        .map(|(symbol, config)| {
            let limit = config.operational_limit.map(|limit| {
                limit
                    .inner()
                    .inner()
                    .format()
                    .unwrap_or_else(|_| "?".to_string())
            });

            st0x_dto::AssetSettings {
                symbol: symbol.clone(),
                trading: config.trading == OperationMode::Enabled,
                rebalancing: config.rebalancing == OperationMode::Enabled,
                operational_limit: limit,
            }
        })
        .collect();

    let trading_mode = match &ctx.trading_mode {
        st0x_config::TradingMode::Standalone => "standalone",
        st0x_config::TradingMode::Rebalancing(_) => "rebalancing",
    };

    let broker = match &ctx.broker {
        st0x_config::BrokerCtx::AlpacaBrokerApi(_) => "alpaca",
        st0x_config::BrokerCtx::DryRun => "dry_run",
    };

    let cash_reserved = ctx
        .assets
        .cash
        .as_ref()
        .and_then(|cash| cash.reserved)
        .map(Positive::inner);

    let wallet = ctx
        .wallet_meta
        .as_ref()
        .map(|meta| st0x_dto::WalletSettings {
            kind: meta.kind.clone(),
            address: format!("{:#x}", meta.address),
            organization_id: meta.organization_id.clone(),
        });

    st0x_dto::Settings {
        equity_target,
        equity_deviation,
        usdc_target,
        usdc_deviation,
        cash_reserved,
        execution_threshold,
        assets,
        wallet,
        log_level: format!("{:?}", ctx.log_level),
        server_port: ctx.server_port,
        orderbook: format!("{:#x}", ctx.evm.orderbook),
        deployment_block: ctx.evm.deployment_block,
        trading_mode: trading_mode.to_string(),
        broker: broker.to_string(),
        order_polling_interval: ctx.order_polling_interval,
        inventory_poll_interval: ctx.inventory_poll_interval,
    }
}

fn float_to_f64(value: rain_math_float::Float, fallback: f64) -> f64 {
    let result = value
        .format()
        .ok()
        .and_then(|formatted| formatted.parse::<f64>().ok());

    if result.is_none() {
        warn!(target: "dashboard", %fallback, "Float conversion failed for dashboard settings, using fallback");
    }

    result.unwrap_or(fallback)
}

async fn load_positions(pool: &SqlitePool) -> Vec<st0x_dto::Position> {
    let rows: Vec<(String, Option<String>, Option<String>)> = match sqlx::query_as(
        "SELECT symbol, net_position, \
         json_extract(payload, '$.Live.last_price_usdc') \
         FROM position_view WHERE symbol IS NOT NULL",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to load positions for dashboard");
            return Vec::new();
        }
    };

    rows.into_iter()
        .filter_map(|(raw_symbol, net_str, price_str)| {
            let symbol = st0x_execution::Symbol::new(&raw_symbol)
                .inspect_err(|error| {
                    warn!(target: "dashboard", %error, %raw_symbol, "Invalid symbol in position view, skipping");
                })
                .ok()?;

            let net = net_str
                .and_then(|value| match rain_math_float::Float::parse(value) {
                    Ok(float) => Some(float),
                    Err(error) => {
                        warn!(target: "dashboard", %error, %raw_symbol, "Unparseable net_position, skipping");
                        None
                    }
                })
                .unwrap_or_else(|| st0x_float_macro::float!(0));

            let last_price_usdc = price_str.and_then(|value| {
                rain_math_float::Float::parse(value)
                    .inspect_err(|error| {
                        warn!(target: "dashboard", %error, %raw_symbol, "Unparseable last_price_usdc, ignoring");
                    })
                    .ok()
            });

            Some(st0x_dto::Position {
                symbol,
                net,
                last_price_usdc,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::task::AbortHandle;
    use tokio_tungstenite::connect_async;

    use st0x_dto::{Direction, Trade, TradingVenue};

    use super::*;
    use crate::inventory::{self, BroadcastingInventory};
    use st0x_config::create_test_ctx_with_order_owner;

    fn dummy_fill(symbol: &str) -> Statement {
        Statement::TradeFill(Trade {
            id: format!("test-fill-{symbol}"),
            filled_at: chrono::Utc::now(),
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: st0x_finance::Symbol::new(symbol).unwrap(),
            shares: st0x_finance::FractionalShares::new(st0x_float_macro::float!(1)),
        })
    }

    fn empty_settings() -> st0x_dto::Settings {
        st0x_dto::Settings {
            equity_target: 0.5,
            equity_deviation: 0.2,
            usdc_target: None,
            usdc_deviation: None,
            cash_reserved: None,
            execution_threshold: "$2".to_string(),
            assets: Vec::new(),
            wallet: None,
            log_level: "Debug".to_string(),
            server_port: 8001,
            orderbook: "0x0".to_string(),
            deployment_block: 0,
            trading_mode: "standalone".to_string(),
            broker: "dry_run".to_string(),
            order_polling_interval: 5,
            inventory_poll_interval: 15,
        }
    }

    fn empty_current_state() -> Box<CurrentState> {
        Box::new(CurrentState {
            trades: Vec::new(),
            inventory: st0x_dto::Inventory::empty(),
            positions: Vec::new(),
            settings: empty_settings(),
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn create_test_state() -> AppState {
        let (sender, _) = broadcast::channel(256);
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        AppState {
            ctx: create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            pool,
            event_sender: sender.clone(),
            inventory: Arc::new(BroadcastingInventory::new(
                inventory::InventoryView::default(),
                sender,
            )),
            settings: empty_settings(),
        }
    }

    struct TestServer {
        port: u16,
        abort: AbortHandle,
        event_sender: broadcast::Sender<Statement>,
        inventory: Arc<BroadcastingInventory>,
    }

    impl TestServer {
        fn shutdown(&self) {
            self.abort.abort();
        }
    }

    async fn start_test_server() -> TestServer {
        let state = create_test_state().await;
        let event_sender = state.event_sender.clone();
        let inventory = Arc::clone(&state.inventory);

        let app = Router::new().nest("/api", routes()).with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        TestServer {
            port,
            abort: handle.abort_handle(),
            event_sender,
            inventory,
        }
    }

    #[tokio::test]
    async fn current_state_serializes_all_fields() {
        let state = CurrentState {
            trades: Vec::new(),
            inventory: st0x_dto::Inventory::empty(),
            positions: Vec::new(),
            settings: empty_settings(),
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
            warnings: Vec::new(),
        };
        let json = serde_json::to_value(&state).expect("serialization should succeed");
        assert_eq!(json["trades"], json!([]));
        assert_eq!(json["positions"], json!([]));
        assert_eq!(json["activeTransfers"], json!([]));
        assert_eq!(json["recentTransfers"], json!([]));
        assert_eq!(json["warnings"], json!([]));
        assert_eq!(json["settings"]["equityTarget"], json!(0.5));
        assert_eq!(json["settings"]["executionThreshold"], json!("$2"));
        assert!(json["inventory"].is_object());
    }

    #[tokio::test]
    async fn server_message_initial_serializes_with_type_tag() {
        let msg = Statement::CurrentState(empty_current_state());
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(json.contains(r#""type":"current_state""#));
        assert!(json.contains(r#""data":"#));
    }

    #[tokio::test]
    async fn broadcast_channel_delivers_messages_to_subscribers() {
        let (sender, _) = broadcast::channel::<Statement>(256);
        let mut rx = sender.subscribe();

        let sent_msg = Statement::CurrentState(empty_current_state());
        sender.send(sent_msg.clone()).expect("send should succeed");

        let recv_msg = rx.recv().await.expect("receive should succeed");
        let original_json = serde_json::to_string(&sent_msg).expect("serialization should succeed");
        let received_json = serde_json::to_string(&recv_msg).expect("serialization should succeed");
        assert_eq!(original_json, received_json);
    }

    #[tokio::test]
    async fn broadcast_supports_multiple_subscribers() {
        let (sender, _) = broadcast::channel::<Statement>(256);
        let mut receiver1 = sender.subscribe();
        let mut receiver2 = sender.subscribe();

        let msg = Statement::CurrentState(empty_current_state());
        sender.send(msg).expect("send should succeed");

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
    async fn websocket_endpoint_sends_initial_message() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

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

        assert_eq!(parsed["type"], "current_state");
        assert!(parsed["data"]["trades"].is_array());
        assert!(parsed["data"]["inventory"].is_object());

        server.shutdown();
    }

    #[tokio::test]
    async fn multiple_concurrent_clients_receive_initial_message() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

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
                "current_state",
                "client{} should receive current_state message",
                i + 1
            );
        }

        server.shutdown();
    }

    #[tokio::test]
    async fn broadcast_message_reaches_connected_clients() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (mut client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        client1.next().await.expect("client1 initial").unwrap();
        client2.next().await.expect("client2 initial").unwrap();

        server
            .event_sender
            .send(dummy_fill("AAPL"))
            .expect("broadcast send");

        let results = join_all([client1.next(), client2.next()]).await;

        for (i, result) in results.into_iter().enumerate() {
            let msg = result
                .unwrap_or_else(|| panic!("client{} stream closed", i + 1))
                .unwrap_or_else(|error| panic!("client{} error: {}", i + 1, error));

            let text = msg.into_text().expect("expected text");
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

            assert_eq!(
                parsed["type"],
                "trade_fill",
                "client{} should receive trade_fill message",
                i + 1
            );
            assert_eq!(parsed["data"]["symbol"], "AAPL");
        }

        server.shutdown();
    }

    #[tokio::test]
    async fn client_disconnect_does_not_affect_other_clients() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");
        let (client2, _) = connect_async(&url)
            .await
            .expect("client2 connection failed");

        client1.next().await.expect("client1 initial").unwrap();

        drop(client2);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        server
            .event_sender
            .send(dummy_fill("TSLA"))
            .expect("broadcast send");

        let msg = client1
            .next()
            .await
            .expect("client1 stream closed")
            .expect("client1 error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["type"], "trade_fill");
        assert_eq!(parsed["data"]["symbol"], "TSLA");

        server.shutdown();
    }

    #[tokio::test]
    async fn new_client_receives_initial_not_previous_broadcasts() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");

        client1.next().await.expect("client1 initial").unwrap();

        server
            .event_sender
            .send(dummy_fill("MSFT"))
            .expect("broadcast send");

        client1.next().await.expect("client1 broadcast").unwrap();

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
            parsed["type"], "current_state",
            "new client should receive current_state, not previous broadcast"
        );

        server.shutdown();
    }

    #[tokio::test]
    async fn inventory_mutation_sends_snapshot_to_websocket_client() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        let (mut client, _) = connect_async(&url)
            .await
            .expect("WebSocket connection failed");

        client.next().await.expect("initial").unwrap();

        {
            let mut guard = server.inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(
                st0x_execution::Symbol::new("AAPL").unwrap(),
                st0x_finance::FractionalShares::new(st0x_float_macro::float!(10)),
                st0x_finance::FractionalShares::new(st0x_float_macro::float!(5)),
            );
        }

        let msg = client
            .next()
            .await
            .expect("stream closed")
            .expect("message error");

        let text = msg.into_text().expect("expected text message");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(
            parsed["type"], "inventory_snapshot",
            "expected inventory_snapshot message after inventory mutation, got: {parsed}"
        );
        let aapl = &parsed["data"]["inventory"]["perSymbol"][0];
        assert_eq!(aapl["symbol"], json!("AAPL"));
        assert_eq!(aapl["onchainAvailable"], json!("10"));
        assert_eq!(aapl["onchainInflight"], json!("0"));
        assert_eq!(aapl["offchainAvailable"], json!("5"));
        assert_eq!(aapl["offchainInflight"], json!("0"));

        server.shutdown();
    }
}
