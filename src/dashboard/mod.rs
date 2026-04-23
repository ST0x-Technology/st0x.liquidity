//! WebSocket-based dashboard for real-time server state streaming.

use futures_util::SinkExt;
use rocket::{Route, State, get, routes};
use rocket_ws::{Channel, Message, WebSocket};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use st0x_dto::{CurrentState, Statement};

use crate::config::OperationMode;
use crate::inventory::BroadcastingInventory;
use crate::threshold::ExecutionThreshold;

mod event;
mod trade_loader;
mod transfer_loader;
pub(crate) use event::Broadcaster;

pub(crate) struct Broadcast {
    pub(crate) sender: broadcast::Sender<Statement>,
}

pub(crate) struct DashboardState {
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) pool: SqlitePool,
    pub(crate) settings: st0x_dto::Settings,
}

#[get("/ws")]
fn ws_endpoint<'r>(
    ws: WebSocket,
    broadcast: &'r State<Broadcast>,
    dashboard: &'r State<DashboardState>,
) -> Channel<'r> {
    let mut receiver = broadcast.sender.subscribe();
    let inventory = Arc::clone(&dashboard.inventory);
    let pool = dashboard.pool.clone();
    let settings = dashboard.settings.clone();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            let inventory_dto = inventory.read().await.to_dto();
            let transfers = transfer_loader::load_transfers(&pool).await;
            let trades = trade_loader::load_trades(&pool).await;
            let positions = load_positions(&pool).await;

            let state = CurrentState {
                trades,
                inventory: inventory_dto,
                positions,
                settings,
                active_transfers: transfers.active,
                recent_transfers: transfers.recent,
                warnings: transfers.warnings,
            };

            let initial = Statement::CurrentState(Box::new(state));
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
            info!("Sent initial state to dashboard client");

            loop {
                match receiver.recv().await {
                    Ok(msg) => {
                        debug!(?msg, "Broadcasting to dashboard client");
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

pub(crate) fn settings_from_ctx(ctx: &crate::config::Ctx) -> st0x_dto::Settings {
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
        crate::config::TradingMode::Standalone { .. } => "standalone",
        crate::config::TradingMode::Rebalancing(_) => "rebalancing",
    };

    let broker = match &ctx.broker {
        crate::config::BrokerCtx::AlpacaBrokerApi(_) => "alpaca",
        crate::config::BrokerCtx::DryRun => "dry_run",
    };

    st0x_dto::Settings {
        equity_target,
        equity_deviation,
        usdc_target,
        usdc_deviation,
        execution_threshold,
        assets,
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
        warn!(%fallback, "Float conversion failed for dashboard settings, using fallback");
    }

    result.unwrap_or(fallback)
}

async fn load_positions(pool: &SqlitePool) -> Vec<st0x_dto::Position> {
    let rows: Vec<(String, Option<String>)> = match sqlx::query_as(
        "SELECT symbol, net_position FROM position_view WHERE symbol IS NOT NULL",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            warn!(%error, "Failed to load positions for dashboard");
            return Vec::new();
        }
    };

    rows.into_iter()
        .filter_map(|(raw_symbol, net_str)| {
            let symbol = st0x_execution::Symbol::new(&raw_symbol)
                .inspect_err(|error| {
                    warn!(%error, %raw_symbol, "Invalid symbol in position view, skipping");
                })
                .ok()?;

            let net = net_str
                .and_then(|value| match rain_math_float::Float::parse(value) {
                    Ok(float) => Some(float),
                    Err(error) => {
                        warn!(%error, %raw_symbol, "Unparseable net_position, skipping");
                        None
                    }
                })
                .unwrap_or_else(|| st0x_float_macro::float!(0));

            Some(st0x_dto::Position { symbol, net })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use rocket::config::Config;
    use rocket::fairing::AdHoc;
    use serde_json::json;
    use st0x_dto::{Trade, TradeDirection, TradingVenue};
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio_tungstenite::connect_async;

    use super::*;

    fn dummy_fill(symbol: &str) -> Statement {
        Statement::TradeFill(Trade {
            id: format!("test-fill-{symbol}"),
            filled_at: chrono::Utc::now(),
            venue: TradingVenue::Raindex,
            direction: TradeDirection::Buy,
            symbol: st0x_finance::Symbol::new(symbol).unwrap(),
            shares: st0x_finance::FractionalShares::new(st0x_float_macro::float!(1)),
        })
    }

    fn empty_current_state() -> Box<CurrentState> {
        Box::new(CurrentState {
            trades: Vec::new(),
            inventory: st0x_dto::Inventory::empty(),
            positions: Vec::new(),
            settings: st0x_dto::Settings {
                equity_target: 0.5,
                equity_deviation: 0.2,
                usdc_target: None,
                usdc_deviation: None,
                execution_threshold: "$2".to_string(),
                assets: Vec::new(),
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0".to_string(),
                deployment_block: 0,
                trading_mode: "standalone".to_string(),
                broker: "dry_run".to_string(),
                order_polling_interval: 5,
                inventory_poll_interval: 15,
            },
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn create_test_broadcast() -> Broadcast {
        let (sender, _) = broadcast::channel(256);
        Broadcast { sender }
    }

    async fn create_test_dashboard_state(
        event_sender: broadcast::Sender<Statement>,
    ) -> DashboardState {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        DashboardState {
            inventory: Arc::new(BroadcastingInventory::new(
                crate::inventory::InventoryView::default(),
                event_sender,
            )),
            pool,
            settings: st0x_dto::Settings {
                equity_target: 0.5,
                equity_deviation: 0.2,
                usdc_target: None,
                usdc_deviation: None,
                execution_threshold: "$2".to_string(),
                assets: Vec::new(),
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0".to_string(),
                deployment_block: 0,
                trading_mode: "standalone".to_string(),
                broker: "dry_run".to_string(),
                order_polling_interval: 5,
                inventory_poll_interval: 15,
            },
        }
    }

    #[tokio::test]
    async fn current_state_serializes_all_fields() {
        let state = CurrentState {
            trades: Vec::new(),
            inventory: st0x_dto::Inventory::empty(),
            positions: Vec::new(),
            settings: st0x_dto::Settings {
                equity_target: 0.5,
                equity_deviation: 0.2,
                usdc_target: None,
                usdc_deviation: None,
                execution_threshold: "$2".to_string(),
                assets: Vec::new(),
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0".to_string(),
                deployment_block: 0,
                trading_mode: "standalone".to_string(),
                broker: "dry_run".to_string(),
                order_polling_interval: 5,
                inventory_poll_interval: 15,
            },
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
        let broadcast = create_test_broadcast();
        let mut rx = broadcast.sender.subscribe();

        let sent_msg = Statement::CurrentState(empty_current_state());
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

        let msg = Statement::CurrentState(empty_current_state());
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
        let dashboard_state = create_test_dashboard_state(broadcast.sender.clone()).await;

        let config = Config {
            port: 0, // Let OS assign a random available port
            log_level: rocket::config::LogLevel::Off,
            ..Config::debug_default()
        };

        let (port_tx, port_rx) = oneshot::channel::<u16>();
        let port_tx = Mutex::new(Some(port_tx));

        let rocket = rocket::build()
            .configure(config)
            .mount("/api", routes())
            .manage(broadcast)
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

        assert_eq!(parsed["type"], "current_state");
        assert!(parsed["data"]["trades"].is_array());
        assert!(parsed["data"]["inventory"].is_object());

        shutdown_handle.notify();
    }

    struct TestServer {
        port: u16,
        shutdown: rocket::Shutdown,
        broadcast: Broadcast,
        inventory: Arc<BroadcastingInventory>,
    }

    async fn start_test_server() -> TestServer {
        let broadcast = create_test_broadcast();
        let broadcast_clone = Broadcast {
            sender: broadcast.sender.clone(),
        };
        let dashboard_state = create_test_dashboard_state(broadcast.sender.clone()).await;
        let inventory = Arc::clone(&dashboard_state.inventory);

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
        let shutdown = rocket.shutdown();

        tokio::spawn(async move {
            let _ = rocket.launch().await;
        });

        let port = port_rx.await.expect("failed to receive port");

        TestServer {
            port,
            shutdown,
            broadcast,
            inventory,
        }
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

        server.shutdown.notify();
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

        // Consume initial messages
        client1.next().await.expect("client1 initial").unwrap();
        client2.next().await.expect("client2 initial").unwrap();

        // Broadcast a fill message
        server
            .broadcast
            .sender
            .send(dummy_fill("AAPL"))
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
                "trade_fill",
                "client{} should receive trade_fill message",
                i + 1
            );
            assert_eq!(parsed["data"]["symbol"], "AAPL");
        }

        server.shutdown.notify();
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

        // Consume initial messages
        client1.next().await.expect("client1 initial").unwrap();

        // Drop client2 to simulate disconnect
        drop(client2);

        // Give the server a moment to process the disconnect
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Broadcast a message - should still reach client1
        server
            .broadcast
            .sender
            .send(dummy_fill("TSLA"))
            .expect("broadcast send");

        // client1 should still receive messages
        let msg = client1
            .next()
            .await
            .expect("client1 stream closed")
            .expect("client1 error");

        let text = msg.into_text().expect("expected text");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("invalid JSON");

        assert_eq!(parsed["type"], "trade_fill");
        assert_eq!(parsed["data"]["symbol"], "TSLA");

        server.shutdown.notify();
    }

    #[tokio::test]
    async fn new_client_receives_initial_not_previous_broadcasts() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        // Connect first client to have a receiver
        let (mut client1, _) = connect_async(&url)
            .await
            .expect("client1 connection failed");

        // Consume initial message for client1
        client1.next().await.expect("client1 initial").unwrap();

        // Broadcast a message (client1 will receive it)
        server
            .broadcast
            .sender
            .send(dummy_fill("MSFT"))
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
            parsed["type"], "current_state",
            "new client should receive current_state, not previous broadcast"
        );

        server.shutdown.notify();
    }

    #[tokio::test]
    async fn inventory_mutation_sends_snapshot_to_websocket_client() {
        let server = start_test_server().await;
        let url = format!("ws://127.0.0.1:{}/api/ws", server.port);

        let (mut client, _) = connect_async(&url)
            .await
            .expect("WebSocket connection failed");

        // Consume initial message
        client.next().await.expect("initial").unwrap();

        // Mutate inventory through the shared BroadcastingInventory
        {
            let mut guard = server.inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(
                st0x_execution::Symbol::new("AAPL").unwrap(),
                st0x_finance::FractionalShares::new(st0x_float_macro::float!(10)),
                st0x_finance::FractionalShares::new(st0x_float_macro::float!(5)),
            );
        }

        // Client should receive the snapshot broadcast
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

        server.shutdown.notify();
    }
}
