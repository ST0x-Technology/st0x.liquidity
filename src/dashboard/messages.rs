use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub(super) enum ServerMessage {
    Initial(InitialState),
    Event(EventStoreEntry),
    #[serde(rename = "trade:onchain")]
    TradeOnchain(OnchainTrade),
    #[serde(rename = "trade:offchain")]
    TradeOffchain(OffchainTrade),
    #[serde(rename = "position:updated")]
    PositionUpdated(Position),
    #[serde(rename = "inventory:updated")]
    InventoryUpdated(Inventory),
    #[serde(rename = "metrics:updated")]
    MetricsUpdated(PerformanceMetrics),
    #[serde(rename = "spread:updated")]
    SpreadUpdated(SpreadUpdate),
    #[serde(rename = "rebalance:updated")]
    RebalanceUpdated(RebalanceOperation),
    #[serde(rename = "circuit_breaker:changed")]
    CircuitBreakerChanged(CircuitBreakerStatus),
    #[serde(rename = "auth:status")]
    AuthStatus(AuthStatus),
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct InitialState {
    recent_trades: Vec<Trade>,
    inventory: Inventory,
    metrics: PerformanceMetrics,
    spreads: Vec<SpreadSummary>,
    active_rebalances: Vec<RebalanceOperation>,
    recent_rebalances: Vec<RebalanceOperation>,
    auth_status: AuthStatus,
    circuit_breaker: CircuitBreakerStatus,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(super) struct EventStoreEntry {
    aggregate_type: String,
    aggregate_id: String,
    #[ts(type = "number")]
    sequence: u64,
    event_type: String,
    timestamp: String,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "lowercase")]
pub(super) enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct OnchainTrade {
    tx_hash: String,
    #[ts(type = "number")]
    log_index: u64,
    symbol: String,
    amount: f64,
    direction: Direction,
    price_usdc: f64,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct OffchainTrade {
    #[ts(type = "number")]
    id: i64,
    symbol: String,
    #[ts(type = "number")]
    shares: i64,
    direction: Direction,
    order_id: Option<String>,
    #[ts(type = "number | null")]
    price_cents: Option<i64>,
    status: OffchainTradeStatus,
    executed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "lowercase")]
pub(super) enum OffchainTradeStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub(super) enum Trade {
    Onchain(OnchainTrade),
    Offchain(OffchainTrade),
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "lowercase")]
pub(super) enum Position {
    Empty {
        symbol: String,
    },
    Active {
        symbol: String,
        net: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pending_execution_id: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(super) struct SymbolInventory {
    symbol: String,
    onchain: f64,
    offchain: f64,
    net: f64,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct Inventory {
    per_symbol: Vec<SymbolInventory>,
    usdc: UsdcInventory,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(super) struct UsdcInventory {
    onchain: f64,
    offchain: f64,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "lowercase")]
pub(super) enum Timeframe {
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "1d")]
    OneDay,
    #[serde(rename = "1w")]
    OneWeek,
    #[serde(rename = "1m")]
    OneMonth,
    All,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct TimeframeMetrics {
    aum: f64,
    pnl: PnL,
    volume: f64,
    #[ts(type = "number")]
    trade_count: u64,
    sharpe_ratio: Option<f64>,
    sortino_ratio: Option<f64>,
    max_drawdown: f64,
    hedge_lag_ms: Option<f64>,
    uptime_percent: f64,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(super) struct PnL {
    absolute: f64,
    percent: f64,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(super) struct PerformanceMetrics {
    #[serde(rename = "1h")]
    one_hour: TimeframeMetrics,
    #[serde(rename = "1d")]
    one_day: TimeframeMetrics,
    #[serde(rename = "1w")]
    one_week: TimeframeMetrics,
    #[serde(rename = "1m")]
    one_month: TimeframeMetrics,
    all: TimeframeMetrics,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct SpreadSummary {
    symbol: String,
    last_buy_price: f64,
    last_sell_price: f64,
    pyth_price: f64,
    spread_bps: f64,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct SpreadUpdate {
    symbol: String,
    timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    buy_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sell_price: Option<f64>,
    pyth_price: f64,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum RebalanceStatus {
    InProgress {
        started_at: String,
    },
    Completed {
        started_at: String,
        completed_at: String,
    },
    Failed {
        started_at: String,
        failed_at: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(super) struct RebalanceOperation {
    #[serde(flatten)]
    operation_type: RebalanceOperationType,
    #[serde(flatten)]
    status: RebalanceStatus,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", rename_all = "lowercase")]
pub(super) enum RebalanceOperationType {
    Mint {
        id: String,
        symbol: String,
        amount: f64,
    },
    Redeem {
        id: String,
        symbol: String,
        amount: f64,
    },
    Usdc {
        id: String,
        direction: UsdcDirection,
        amount: f64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "snake_case")]
pub(super) enum UsdcDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum CircuitBreakerStatus {
    Active,
    Tripped { reason: String, tripped_at: String },
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum AuthStatus {
    Valid { expires_at: String },
    ExpiringSoon { expires_at: String },
    Expired,
    NotConfigured,
}
