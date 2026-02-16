//! Dashboard DTO types for TypeScript codegen.
//!
//! This crate contains all types that derive `TS` for TypeScript type generation.
//! Keeping these in a separate crate allows the dashboard to build without waiting
//! for the full workspace.

use std::path::{Path, PathBuf};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

/// Absolute path to the dashboard TypeScript bindings directory.
///
/// Resolved from CARGO_MANIFEST_DIR at compile time so it works
/// regardless of the working directory (e.g. git worktrees).
fn dashboard_bindings_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dashboard/src/lib/api")
}

/// Messages sent from the server to WebSocket clients.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ServerMessage {
    Initial(Box<InitialState>),
    Event(EventStoreEntry),
}

/// Full dashboard snapshot sent to the frontend on connection.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct InitialState {
    pub recent_trades: Vec<Trade>,
    pub inventory: Inventory,
    pub metrics: PerformanceMetrics,
    pub spreads: Vec<SpreadSummary>,
    pub active_rebalances: Vec<RebalanceOperation>,
    pub recent_rebalances: Vec<RebalanceOperation>,
    pub auth_status: AuthStatus,
    pub circuit_breaker: CircuitBreakerStatus,
}

impl InitialState {
    pub fn stub() -> Self {
        Self {
            recent_trades: Vec::new(),
            inventory: Inventory::empty(),
            metrics: PerformanceMetrics::zero(),
            spreads: Vec::new(),
            active_rebalances: Vec::new(),
            recent_rebalances: Vec::new(),
            auth_status: AuthStatus::NotConfigured,
            circuit_breaker: CircuitBreakerStatus::Active,
        }
    }
}

/// Single event from the event store for live updates.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
pub struct EventStoreEntry {
    pub aggregate_type: String,
    pub aggregate_id: String,
    #[ts(type = "number")]
    pub sequence: u64,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
}

/// Completed trade record.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub id: String,
}

/// Per-symbol net position.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub symbol: String,
    #[ts(type = "string")]
    pub net: Decimal,
}

/// Per-symbol onchain/offchain/net balances.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct SymbolInventory {
    pub symbol: String,
    #[ts(type = "string")]
    pub onchain: Decimal,
    #[ts(type = "string")]
    pub offchain: Decimal,
    #[ts(type = "string")]
    pub net: Decimal,
}

/// Onchain and offchain USDC balances.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct UsdcInventory {
    #[ts(type = "string")]
    pub onchain: Decimal,
    #[ts(type = "string")]
    pub offchain: Decimal,
}

/// Full inventory snapshot across all symbols and USDC.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct Inventory {
    pub per_symbol: Vec<SymbolInventory>,
    pub usdc: UsdcInventory,
}

impl Inventory {
    pub fn empty() -> Self {
        Self {
            per_symbol: Vec::new(),
            usdc: UsdcInventory {
                onchain: Decimal::ZERO,
                offchain: Decimal::ZERO,
            },
        }
    }
}

/// Absolute and percentage profit/loss.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
pub struct PnL {
    #[ts(type = "string")]
    pub absolute: Decimal,
    #[ts(type = "string")]
    pub percent: Decimal,
}

/// Performance metrics for a single time window.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct TimeframeMetrics {
    #[ts(type = "string")]
    pub aum: Decimal,
    pub pnl: PnL,
    #[ts(type = "string")]
    pub volume: Decimal,
    #[ts(type = "number")]
    pub trade_count: u64,
    #[ts(type = "string | null")]
    pub sharpe_ratio: Option<Decimal>,
    #[ts(type = "string | null")]
    pub sortino_ratio: Option<Decimal>,
    #[ts(type = "string")]
    pub max_drawdown: Decimal,
    #[ts(type = "number | null")]
    pub hedge_lag_ms: Option<u64>,
    #[ts(optional, type = "string")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_percent: Option<Decimal>,
}

impl TimeframeMetrics {
    pub fn zero() -> Self {
        Self {
            aum: Decimal::ZERO,
            pnl: PnL {
                absolute: Decimal::ZERO,
                percent: Decimal::ZERO,
            },
            volume: Decimal::ZERO,
            trade_count: 0,
            sharpe_ratio: None,
            sortino_ratio: None,
            max_drawdown: Decimal::ZERO,
            hedge_lag_ms: None,
            uptime_percent: None,
        }
    }
}

/// Metrics across all tracked timeframes.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
pub struct PerformanceMetrics {
    #[serde(rename = "1h")]
    pub one_hour: TimeframeMetrics,
    #[serde(rename = "1d")]
    pub one_day: TimeframeMetrics,
    #[serde(rename = "1w")]
    pub one_week: TimeframeMetrics,
    #[serde(rename = "1m")]
    pub one_month: TimeframeMetrics,
    pub all: TimeframeMetrics,
}

impl PerformanceMetrics {
    pub fn zero() -> Self {
        let zero = TimeframeMetrics::zero();
        Self {
            one_hour: zero,
            one_day: zero,
            one_week: zero,
            one_month: zero,
            all: zero,
        }
    }
}

/// Current bid/ask spread for a symbol.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct SpreadSummary {
    pub symbol: String,
    #[ts(type = "string")]
    pub last_buy_price: Decimal,
    #[ts(type = "string")]
    pub last_sell_price: Decimal,
    #[ts(type = "string")]
    pub pyth_price: Decimal,
    #[ts(type = "string")]
    pub spread_bps: Decimal,
    pub updated_at: DateTime<Utc>,
}

/// Incremental spread change for a symbol.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct SpreadUpdate {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    #[ts(optional, type = "string")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buy_price: Option<Decimal>,
    #[ts(optional, type = "string")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sell_price: Option<Decimal>,
    #[ts(type = "string")]
    pub pyth_price: Decimal,
}

/// Active or completed rebalance operation.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub struct RebalanceOperation {
    pub id: String,
}

/// Whether the trading circuit breaker is active.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CircuitBreakerStatus {
    Active,
}

/// Broker authentication status.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthStatus {
    NotConfigured,
}

/// Export all TypeScript bindings to the dashboard directory.
pub fn export_bindings() -> Result<(), ts_rs::ExportError> {
    let dir = dashboard_bindings_dir();

    ServerMessage::export_all_to(&dir)?;
    InitialState::export_all_to(&dir)?;
    EventStoreEntry::export_all_to(&dir)?;
    Trade::export_all_to(&dir)?;
    Position::export_all_to(&dir)?;
    SymbolInventory::export_all_to(&dir)?;
    Inventory::export_all_to(&dir)?;
    UsdcInventory::export_all_to(&dir)?;
    TimeframeMetrics::export_all_to(&dir)?;
    PnL::export_all_to(&dir)?;
    PerformanceMetrics::export_all_to(&dir)?;
    SpreadSummary::export_all_to(&dir)?;
    SpreadUpdate::export_all_to(&dir)?;
    RebalanceOperation::export_all_to(&dir)?;
    CircuitBreakerStatus::export_all_to(&dir)?;
    AuthStatus::export_all_to(&dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_stub_serializes_correctly() {
        let initial = InitialState::stub();
        let json = serde_json::to_string(&initial).expect("serialization should succeed");
        assert!(json.contains("recentTrades"));
        assert!(json.contains("inventory"));
        assert!(json.contains("metrics"));
        assert!(json.contains("authStatus"));
        assert!(json.contains("circuitBreaker"));
    }

    #[derive(TS)]
    #[ts(export, export_to = "../../dashboard/src/lib/api/")]
    struct TestOnlyBinding {
        _canary: bool,
    }

    #[test]
    fn export_bindings_generates_files_in_dashboard_directory() {
        let export_dir = dashboard_bindings_dir();

        export_bindings().unwrap();

        assert!(
            export_dir.join("ServerMessage.ts").exists(),
            "ServerMessage.ts should exist in {}/",
            export_dir.display()
        );
        assert!(
            export_dir.join("InitialState.ts").exists(),
            "InitialState.ts should exist in {}/",
            export_dir.display()
        );

        // Export a test-only type and verify it lands in the same directory with correct contents
        TestOnlyBinding::export_all_to(&export_dir).unwrap();
        let test_file = export_dir.join("TestOnlyBinding.ts");
        let contents = std::fs::read_to_string(&test_file).unwrap_or_else(|e| {
            panic!(
                "test-only binding should be readable at {}: {e}",
                test_file.display()
            )
        });
        assert!(
            contents.contains("TestOnlyBinding"),
            "generated file should contain the type name, got: {contents}"
        );
        assert!(
            contents.contains("_canary"),
            "generated file should contain the field name, got: {contents}"
        );
        std::fs::remove_file(&test_file).unwrap();
    }

    #[test]
    fn server_message_initial_serializes_with_type_tag() {
        let msg = ServerMessage::Initial(Box::new(InitialState::stub()));
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(json.contains(r#""type":"initial""#));
        assert!(json.contains(r#""data":"#));
    }
}
