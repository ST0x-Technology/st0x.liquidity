//! WebSocket-based dashboard for real-time server state streaming.

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::sink::SinkExt;
use futures_util::stream::{SplitSink, StreamExt};
use serde::Deserialize;
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{info, trace, warn};

use st0x_config::{ExecutionThreshold, OperationMode};
use st0x_dto::{CurrentState, Statement, Trade, TradeOutcome};
use st0x_event_sorcery::{load_all_ids, load_entity};
use st0x_finance::Positive;

use crate::AppState;
use crate::position::Position;

mod event;
pub(crate) mod pnl;
mod trade_loader;
pub(crate) mod transfer_loader;
pub(crate) use event::{
    Broadcaster, DashboardTradeDelivery, DashboardTradeDeliveryCtx, DashboardTradeDeliveryJobQueue,
    DashboardTradeHandoffMonitor, DeliverDashboardTrade,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TradeProtocol {
    #[default]
    LegacyFills,
    TerminalOutcomesV1,
    TerminalOutcomesV2,
}

impl TradeProtocol {
    fn includes_trade(self, trade: &Trade) -> bool {
        match self {
            Self::LegacyFills => match trade.outcome {
                TradeOutcome::Filled => true,
                TradeOutcome::Failed { .. } => false,
            },
            Self::TerminalOutcomesV1 | Self::TerminalOutcomesV2 => match trade.outcome {
                TradeOutcome::Filled | TradeOutcome::Failed { .. } => true,
            },
        }
    }

    fn includes_statement(self, statement: &Statement) -> bool {
        match self {
            Self::LegacyFills => match statement {
                Statement::TradeUpdate(_) => false,
                Statement::CurrentState(_)
                | Statement::TradeFill(_)
                | Statement::PositionUpdate(_)
                | Statement::InventorySnapshot(_)
                | Statement::TransferUpdate(_) => true,
            },
            Self::TerminalOutcomesV1 | Self::TerminalOutcomesV2 => match statement {
                Statement::TradeFill(_) => false,
                Statement::CurrentState(_)
                | Statement::TradeUpdate(_)
                | Statement::PositionUpdate(_)
                | Statement::InventorySnapshot(_)
                | Statement::TransferUpdate(_) => true,
            },
        }
    }
}

#[derive(Default, Deserialize)]
struct WebSocketQuery {
    #[serde(default)]
    trade_protocol: TradeProtocol,
}

async fn ws_endpoint(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<WebSocketQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.trade_protocol))
}

/// Outcome of attempting to send a serialized message over the socket.
enum SendOutcome {
    Sent,
    SerializeFailed,
    SocketClosed,
}

fn serialize_statement(
    statement: &Statement,
    trade_protocol: TradeProtocol,
) -> Result<String, serde_json::Error> {
    if trade_protocol != TradeProtocol::TerminalOutcomesV1 {
        return serde_json::to_string(statement);
    }

    let mut wire = serde_json::to_value(statement)?;
    match statement {
        Statement::CurrentState(state) => {
            wire["data"]["trades"] = serde_json::Value::Array(
                state
                    .trades
                    .iter()
                    .map(|trade| serde_json::to_value(trade.terminal_outcomes_v1()))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Statement::TradeUpdate(trade) => {
            wire["data"] = serde_json::to_value(trade.terminal_outcomes_v1())?;
        }
        Statement::TradeFill(_)
        | Statement::PositionUpdate(_)
        | Statement::InventorySnapshot(_)
        | Statement::TransferUpdate(_) => {}
    }

    serde_json::to_string(&wire)
}

async fn send_json(
    sink: &mut SplitSink<WebSocket, Message>,
    value: &Statement,
    trade_protocol: TradeProtocol,
) -> SendOutcome {
    let json = match serialize_statement(value, trade_protocol) {
        Ok(serialized) => serialized,
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to serialize message");
            return SendOutcome::SerializeFailed;
        }
    };

    if let Err(error) = sink.send(Message::Text(json.into())).await {
        warn!(target: "dashboard", %error, "Failed to send message to client");
        return SendOutcome::SocketClosed;
    }

    SendOutcome::Sent
}

async fn handle_socket(socket: WebSocket, state: AppState, trade_protocol: TradeProtocol) {
    let mut receiver = state.event_sender.subscribe();
    let (mut sink, mut stream) = socket.split();

    if !send_initial_state(&mut sink, &state, trade_protocol).await {
        return;
    }

    tokio::select! {
        () = stream_broadcasts(&mut sink, &mut receiver, trade_protocol) => {}
        () = drain_client_messages(&mut stream) => {}
    }
}

/// Polls the client side of the socket so that Close / Ping / Pong are
/// observed promptly. Exits when the client closes the socket or sends
/// nothing more — at which point [`handle_socket`] tears down the send half.
async fn drain_client_messages(stream: &mut futures_util::stream::SplitStream<WebSocket>) {
    while let Some(result) = stream.next().await {
        match result {
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(error) => {
                warn!(target: "dashboard", %error, "WebSocket receive error");
                break;
            }
        }
    }
}

async fn send_initial_state(
    sink: &mut SplitSink<WebSocket, Message>,
    state: &AppState,
    trade_protocol: TradeProtocol,
) -> bool {
    let inventory_dto = state.inventory.read().await.to_dto();
    let transfers = transfer_loader::load_transfers(&state.pool).await;
    let mut trades = match trade_loader::load_trades(&state.pool).await {
        Ok(trades) => trades,
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to load initial trade history");
            return false;
        }
    };
    trades.retain(|trade| trade_protocol.includes_trade(trade));
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

    match send_json(sink, &initial, trade_protocol).await {
        SendOutcome::Sent => {
            info!(target: "dashboard", "Sent initial state to dashboard client");
            true
        }
        SendOutcome::SerializeFailed | SendOutcome::SocketClosed => false,
    }
}

async fn stream_broadcasts(
    sink: &mut SplitSink<WebSocket, Message>,
    receiver: &mut broadcast::Receiver<Statement>,
    trade_protocol: TradeProtocol,
) {
    loop {
        match receiver.recv().await {
            Ok(msg) => {
                if !trade_protocol.includes_statement(&msg) {
                    continue;
                }
                trace!(target: "dashboard", ?msg, "Broadcasting to dashboard client");
                match send_json(sink, &msg, trade_protocol).await {
                    SendOutcome::Sent => {}
                    SendOutcome::SerializeFailed | SendOutcome::SocketClosed => break,
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    target: "dashboard",
                    skipped,
                    "Client lagged, closing socket so the client reconnects and refetches state"
                );
                break;
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

            // Extended hours lives under `Enabled` only. Config validation
            // rejects `extended_hours_counter_trading = enabled` with
            // `trading = disabled`, so a disabled asset never carries a live
            // extended-hours flag -- this maps valid config, it does not
            // coerce a contradictory pair.
            let counter_trading = match config.trading {
                OperationMode::Enabled => st0x_dto::CounterTrading::Enabled {
                    extended_hours: config.extended_hours_counter_trading == OperationMode::Enabled,
                },
                OperationMode::Disabled => st0x_dto::CounterTrading::Disabled,
            };

            st0x_dto::AssetSettings {
                symbol: symbol.clone(),
                counter_trading,
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
    let ids = match load_all_ids::<Position>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(target: "dashboard", %error, "Failed to load positions for dashboard");
            return Vec::new();
        }
    };

    let mut positions = Vec::with_capacity(ids.len());

    for id in ids {
        match load_entity::<Position>(pool, &id).await {
            Ok(Some(position)) => positions.push(st0x_dto::Position {
                symbol: position.symbol,
                net: position.net.inner(),
                last_price_usdc: position.last_price_usdc,
            }),
            Ok(None) => {
                warn!(target: "dashboard", %id, "Position disappeared while loading dashboard state");
            }
            Err(error) => {
                warn!(target: "dashboard", %id, ?error, "Failed to load position for dashboard");
            }
        }
    }

    positions
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use futures_util::StreamExt;
    use futures_util::future::join_all;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_tungstenite::connect_async;

    use st0x_config::{EquityAssetConfig, create_test_ctx_with_order_owner};
    use st0x_dto::{Direction, Trade, TradingVenue};
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, ExecutorOrderId, FractionalShares, MarketSession, Positive,
        SupportedExecutor, Symbol, Usd,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::{self, BroadcastingInventory};
    use crate::offchain::order::{
        CounterTradeOrderKind, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
        noop_order_placer,
    };

    fn dummy_fill(symbol: &str) -> Statement {
        Statement::TradeUpdate(Trade {
            id: format!("test-fill-{symbol}"),
            occurred_at: chrono::Utc::now(),
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: st0x_finance::Symbol::new(symbol).unwrap(),
            shares: Positive::new(st0x_finance::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
            outcome: st0x_dto::TradeOutcome::Filled,
        })
    }

    #[test]
    fn trade_protocol_routes_only_its_trade_statement_version() {
        let Statement::TradeUpdate(trade) = dummy_fill("AAPL") else {
            panic!("dummy fill should be a trade update")
        };
        let legacy_fill = Statement::TradeFill(
            trade
                .legacy_fill()
                .expect("filled trade should have a legacy representation"),
        );
        let trade_update = Statement::TradeUpdate(trade);

        assert!(TradeProtocol::LegacyFills.includes_statement(&legacy_fill));
        assert!(!TradeProtocol::LegacyFills.includes_statement(&trade_update));
        assert!(TradeProtocol::TerminalOutcomesV1.includes_statement(&trade_update));
        assert!(!TradeProtocol::TerminalOutcomesV1.includes_statement(&legacy_fill));
        assert!(TradeProtocol::TerminalOutcomesV2.includes_statement(&trade_update));
        assert!(!TradeProtocol::TerminalOutcomesV2.includes_statement(&legacy_fill));
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

    #[test]
    fn settings_from_ctx_includes_asset_operation_flags() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let symbol = st0x_finance::Symbol::new("RKLB").unwrap();
        ctx.assets.equities.symbols.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: address!("0x1111111111111111111111111111111111111111"),
                tokenized_equity_derivative: address!("0x2222222222222222222222222222222222222222"),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let settings = settings_from_ctx(&ctx);
        let asset = settings
            .assets
            .iter()
            .find(|asset| asset.symbol == symbol)
            .expect("RKLB settings should be present");

        assert!(matches!(
            asset.counter_trading,
            st0x_dto::CounterTrading::Enabled {
                extended_hours: true
            }
        ));
        assert!(!asset.rebalancing);
    }

    #[test]
    fn settings_from_ctx_maps_disabled_trading_to_counter_trading_disabled() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let symbol = st0x_finance::Symbol::new("RKLB").unwrap();
        ctx.assets.equities.symbols.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: address!("0x1111111111111111111111111111111111111111"),
                tokenized_equity_derivative: address!("0x2222222222222222222222222222222222222222"),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let settings = settings_from_ctx(&ctx);
        let asset = settings
            .assets
            .iter()
            .find(|asset| asset.symbol == symbol)
            .expect("RKLB settings should be present");

        assert!(matches!(
            asset.counter_trading,
            st0x_dto::CounterTrading::Disabled
        ));
        assert!(asset.rebalancing);
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
            recovery: Arc::new(tokio::sync::OnceCell::new()),
            resume_lock: Arc::new(crate::api::ResumeLock(tokio::sync::Mutex::new(()))),
            metrics_handle: crate::metrics::setup().expect("metrics setup"),
        }
    }

    struct TestServer {
        port: u16,
        handle: Option<tokio::task::JoinHandle<()>>,
        event_sender: broadcast::Sender<Statement>,
        inventory: Arc<BroadcastingInventory>,
    }

    impl TestServer {
        async fn shutdown(&mut self) {
            let Some(handle) = self.handle.take() else {
                return;
            };
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        }
    }

    async fn start_test_server() -> TestServer {
        start_test_server_with_state(create_test_state().await).await
    }

    async fn start_test_server_with_state(state: AppState) -> TestServer {
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
            handle: Some(handle),
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
    async fn websocket_endpoint_sends_initial_message() {
        let mut server = start_test_server().await;
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

        server.shutdown().await;
    }

    fn assert_terminal_outcomes_v1_failure(trade: &serde_json::Value) {
        assert_eq!(trade["outcome"]["status"], "failed");
        assert!(trade["outcome"].get("acceptedShares").is_none());
        assert_eq!(trade["outcome"]["filledShares"], "0");
        assert_eq!(trade["outcome"]["remainingShares"], "1");
        assert_eq!(trade["outcome"]["excessShares"], "0");
    }

    #[tokio::test]
    async fn websocket_initial_state_includes_failed_counter_trades() {
        let state = create_test_state().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(state.pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        let id = OffchainOrderId::new();
        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("SPCX").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Buy,
                    executor: SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        let filled_id = OffchainOrderId::new();
        store
            .send(
                &filled_id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(filled_id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &filled_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("broker-fill"),
                    placed_shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
                    submitted_at: chrono::Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &filled_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(100)),
                    filled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "asset is not tradable".to_string(),
                },
            )
            .await
            .unwrap();

        let mut server = start_test_server_with_state(state).await;
        let legacy_url = format!("ws://127.0.0.1:{}/api/ws", server.port);
        let (mut legacy_client, _) = connect_async(&legacy_url).await.unwrap();
        let legacy_message = legacy_client
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        let legacy_state: serde_json::Value = serde_json::from_str(&legacy_message).unwrap();
        assert_eq!(legacy_state["data"]["trades"].as_array().unwrap().len(), 1);
        let legacy_trade = &legacy_state["data"]["trades"][0];
        assert_eq!(legacy_trade["id"], filled_id.to_string());
        assert!(legacy_trade["filledAt"].as_str().is_some());
        assert_eq!(legacy_trade["outcome"]["status"], "filled");

        let v1_url = format!(
            "ws://127.0.0.1:{}/api/ws?trade_protocol=terminal_outcomes_v1",
            server.port
        );
        let (mut v1_client, _) = connect_async(&v1_url).await.unwrap();
        let v1_message = v1_client
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        let v1_state: serde_json::Value = serde_json::from_str(&v1_message).unwrap();
        let v1_trade = &v1_state["data"]["trades"][0];
        assert_terminal_outcomes_v1_failure(v1_trade);

        let url = format!(
            "ws://127.0.0.1:{}/api/ws?trade_protocol=terminal_outcomes_v2",
            server.port
        );
        let (mut client, _) = connect_async(&url).await.unwrap();
        let message = client.next().await.unwrap().unwrap().into_text().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
        let trade = &parsed["data"]["trades"][0];

        assert_eq!(trade["id"], id.to_string());
        assert!(trade["occurredAt"].as_str().is_some());
        assert_eq!(trade["outcome"]["status"], "failed");
        assert_eq!(trade["outcome"]["error"], "asset is not tradable");
        assert_eq!(trade["outcome"]["acceptedShares"], serde_json::Value::Null);
        assert_eq!(trade["outcome"]["filledShares"], serde_json::Value::Null);
        assert_eq!(trade["outcome"]["remainingShares"], serde_json::Value::Null);
        assert_eq!(trade["outcome"]["excessShares"], serde_json::Value::Null);

        let live_trade = Trade {
            id: "live-fill".to_string(),
            occurred_at: chrono::Utc::now(),
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            outcome: TradeOutcome::Filled,
        };
        server
            .event_sender
            .send(Statement::TradeUpdate(live_trade.clone()))
            .unwrap();
        server
            .event_sender
            .send(Statement::TradeFill(live_trade.legacy_fill().unwrap()))
            .unwrap();

        let legacy_live: serde_json::Value = serde_json::from_str(
            &legacy_client
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(legacy_live["type"], "trade_fill");
        assert_eq!(legacy_live["data"]["id"], "live-fill");
        assert!(legacy_live["data"]["filledAt"].as_str().is_some());
        assert!(legacy_live["data"].get("outcome").is_none());

        let canonical_live: serde_json::Value =
            serde_json::from_str(&client.next().await.unwrap().unwrap().into_text().unwrap())
                .unwrap();
        assert_eq!(canonical_live["type"], "trade_update");
        assert_eq!(canonical_live["data"]["id"], "live-fill");
        assert_eq!(canonical_live["data"]["outcome"]["status"], "filled");

        let v1_live_fill: serde_json::Value = serde_json::from_str(
            &v1_client
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v1_live_fill["data"]["outcome"]["status"], "filled");

        let failed_live_trade = Trade {
            id: "live-failure".to_string(),
            occurred_at: chrono::Utc::now(),
            venue: TradingVenue::Alpaca,
            direction: Direction::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            outcome: TradeOutcome::Failed {
                error: "broker rejected order".to_string(),
                accepted_shares: None,
                filled_shares: None,
                remaining_shares: None,
                excess_shares: None,
            },
        };
        server
            .event_sender
            .send(Statement::TradeUpdate(failed_live_trade))
            .unwrap();

        let v1_live_failure: serde_json::Value = serde_json::from_str(
            &v1_client
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v1_live_failure["type"], "trade_update");
        assert_eq!(v1_live_failure["data"]["id"], "live-failure");
        assert_terminal_outcomes_v1_failure(&v1_live_failure["data"]);

        server.shutdown().await;
    }

    #[tokio::test]
    async fn multiple_concurrent_clients_receive_initial_message() {
        let mut server = start_test_server().await;
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

        server.shutdown().await;
    }

    #[tokio::test]
    async fn broadcast_message_reaches_connected_clients() {
        let mut server = start_test_server().await;
        let url = format!(
            "ws://127.0.0.1:{}/api/ws?trade_protocol=terminal_outcomes_v1",
            server.port
        );

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
                "trade_update",
                "client{} should receive trade_update message",
                i + 1
            );
            assert_eq!(parsed["data"]["symbol"], "AAPL");
        }

        server.shutdown().await;
    }

    #[tokio::test]
    async fn client_disconnect_does_not_affect_other_clients() {
        let mut server = start_test_server().await;
        let url = format!(
            "ws://127.0.0.1:{}/api/ws?trade_protocol=terminal_outcomes_v1",
            server.port
        );

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

        assert_eq!(parsed["type"], "trade_update");
        assert_eq!(parsed["data"]["symbol"], "TSLA");

        server.shutdown().await;
    }

    #[tokio::test]
    async fn new_client_receives_initial_not_previous_broadcasts() {
        let mut server = start_test_server().await;
        let url = format!(
            "ws://127.0.0.1:{}/api/ws?trade_protocol=terminal_outcomes_v1",
            server.port
        );

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

        server.shutdown().await;
    }

    #[tokio::test]
    async fn inventory_mutation_sends_snapshot_to_websocket_client() {
        let mut server = start_test_server().await;
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

        server.shutdown().await;
    }
}
