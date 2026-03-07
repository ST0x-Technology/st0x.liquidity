//! Dashboard DTO types for TypeScript codegen.
//!
//! This crate contains all types that derive `TS` for TypeScript type generation.
//! Keeping these in a separate crate allows the dashboard to build without waiting
//! for the full workspace. Domain types from `st0x-finance` are used for type
//! safety, with `#[ts(type = "string")]` overrides for TypeScript compatibility.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::path::Path;
use ts_rs::TS;

use st0x_finance::{FractionalShares, HasZero, Id, Symbol, Usdc};

/// Messages sent from the server to WebSocket clients.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ServerMessage {
    Initial(Box<InitialState>),
    Event(EventStoreEntry),
    InventoryUpdate(Box<InventorySnapshot>),
    TransferUpdate(TransferOperation),
}

/// Full dashboard snapshot sent to the frontend on connection.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InitialState {
    pub recent_trades: Vec<Trade>,
    pub inventory: Inventory,
    pub metrics: PerformanceMetrics,
    pub spreads: Vec<SpreadSummary>,
    pub active_transfers: Vec<TransferOperation>,
    pub recent_transfers: Vec<TransferOperation>,
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
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
            auth_status: AuthStatus::NotConfigured,
            circuit_breaker: CircuitBreakerStatus::Active,
        }
    }
}

/// Single event from the event store for live updates.
#[derive(Debug, Clone, Serialize, TS)]
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
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub id: String,
}

/// Per-symbol net position.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub net: Decimal,
}

/// Per-symbol equity balances split by venue and availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInventory {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub onchain_available: FractionalShares,
    #[ts(type = "string")]
    pub onchain_inflight: FractionalShares,
    #[ts(type = "string")]
    pub offchain_available: FractionalShares,
    #[ts(type = "string")]
    pub offchain_inflight: FractionalShares,
}

/// Onchain and offchain USDC balances split by availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcInventory {
    #[ts(type = "string")]
    pub onchain_available: Usdc,
    #[ts(type = "string")]
    pub onchain_inflight: Usdc,
    #[ts(type = "string")]
    pub offchain_available: Usdc,
    #[ts(type = "string")]
    pub offchain_inflight: Usdc,
}

/// Full inventory snapshot across all symbols and USDC.
#[derive(Debug, Clone, Serialize, TS)]
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
                onchain_available: Usdc::ZERO,
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::ZERO,
                offchain_inflight: Usdc::ZERO,
            },
        }
    }
}

/// Timestamped inventory snapshot for history queries.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InventorySnapshot {
    pub inventory: Inventory,
    pub fetched_at: DateTime<Utc>,
}

/// Tag type for equity mint operation IDs.
pub enum EquityMintTag {}

/// Tag type for equity redemption operation IDs.
pub enum EquityRedemptionTag {}

/// Tag type for USDC bridge operation IDs.
pub enum UsdcBridgeTag {}

/// Transfer operation: a tagged union per operation type.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransferOperation {
    EquityMint(EquityMintOperation),
    EquityRedemption(EquityRedemptionOperation),
    UsdcBridge(UsdcBridgeOperation),
}

/// Minting tokenized equity from real shares.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EquityMintOperation {
    #[ts(type = "string")]
    pub id: Id<EquityMintTag>,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub quantity: FractionalShares,
    pub status: EquityMintStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for an equity mint.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EquityMintStatus {
    Minting,
    Wrapping,
    Depositing,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

/// Redeeming tokenized equity back to real shares.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EquityRedemptionOperation {
    #[ts(type = "string")]
    pub id: Id<EquityRedemptionTag>,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub quantity: FractionalShares,
    pub status: EquityRedemptionStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for an equity redemption.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EquityRedemptionStatus {
    Withdrawing,
    Unwrapping,
    Sending,
    PendingConfirmation,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

/// Direction for USDC bridge transfers.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum UsdcBridgeDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

/// Bridging USDC between venues (onchain <-> offchain).
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcBridgeOperation {
    #[ts(type = "string")]
    pub id: Id<UsdcBridgeTag>,
    pub direction: UsdcBridgeDirection,
    #[ts(type = "string")]
    pub amount: Usdc,
    pub status: UsdcBridgeStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for a USDC bridge transfer.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum UsdcBridgeStatus {
    Converting,
    Withdrawing,
    Bridging,
    Depositing,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

impl TransferOperation {
    /// Whether this transfer is in a terminal state (completed or failed).
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::EquityMint(op) => {
                use EquityMintStatus::*;
                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Minting | Wrapping | Depositing => false,
                }
            }
            Self::EquityRedemption(op) => {
                use EquityRedemptionStatus::*;
                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Withdrawing | Unwrapping | Sending | PendingConfirmation => false,
                }
            }
            Self::UsdcBridge(op) => {
                use UsdcBridgeStatus::*;
                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Converting | Withdrawing | Bridging | Depositing => false,
                }
            }
        }
    }

    /// The last time this transfer was updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Self::EquityMint(op) => op.updated_at,
            Self::EquityRedemption(op) => op.updated_at,
            Self::UsdcBridge(op) => op.updated_at,
        }
    }
}

impl TransferOperation {
    /// Whether this transfer is in a terminal state (completed or failed).
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::EquityMint(op) => matches!(
                op.status,
                EquityMintStatus::Completed { .. } | EquityMintStatus::Failed { .. }
            ),
            Self::EquityRedemption(op) => matches!(
                op.status,
                EquityRedemptionStatus::Completed { .. } | EquityRedemptionStatus::Failed { .. }
            ),
            Self::UsdcBridge(op) => matches!(
                op.status,
                UsdcBridgeStatus::Completed { .. } | UsdcBridgeStatus::Failed { .. }
            ),
        }
    }

    /// The last time this transfer was updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Self::EquityMint(op) => op.updated_at,
            Self::EquityRedemption(op) => op.updated_at,
            Self::UsdcBridge(op) => op.updated_at,
        }
    }
}

/// Absolute and percentage profit/loss.
#[derive(Debug, Clone, Copy, Serialize, TS)]
pub struct PnL {
    #[ts(type = "string")]
    pub absolute: Decimal,
    #[ts(type = "string")]
    pub percent: Decimal,
}

/// Performance metrics for a single time window.
#[derive(Debug, Clone, Copy, Serialize, TS)]
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
#[serde(rename_all = "camelCase")]
pub struct SpreadSummary {
    #[ts(type = "string")]
    pub symbol: Symbol,
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
#[serde(rename_all = "camelCase")]
pub struct SpreadUpdate {
    #[ts(type = "string")]
    pub symbol: Symbol,
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

/// Whether the trading circuit breaker is active.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CircuitBreakerStatus {
    Active,
}

/// Broker authentication status.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthStatus {
    NotConfigured,
}

/// Export all TypeScript bindings into `out_dir`.
///
/// Each type is written to `out_dir/TypeName.ts`. The caller controls
/// the output location explicitly -- no `TS_RS_EXPORT_DIR` magic.
///
/// # Errors
///
/// Returns [`ts_rs::ExportError`] if any type's binding file fails to
/// write (e.g., `out_dir` does not exist or is not writable).
pub fn export_bindings(out_dir: &Path) -> Result<(), ts_rs::ExportError> {
    ServerMessage::export_all_to(out_dir)?;
    InitialState::export_all_to(out_dir)?;
    EventStoreEntry::export_all_to(out_dir)?;
    Trade::export_all_to(out_dir)?;
    Position::export_all_to(out_dir)?;
    SymbolInventory::export_all_to(out_dir)?;
    Inventory::export_all_to(out_dir)?;
    InventorySnapshot::export_all_to(out_dir)?;
    UsdcInventory::export_all_to(out_dir)?;
    TransferOperation::export_all_to(out_dir)?;
    EquityMintOperation::export_all_to(out_dir)?;
    EquityMintStatus::export_all_to(out_dir)?;
    EquityRedemptionOperation::export_all_to(out_dir)?;
    EquityRedemptionStatus::export_all_to(out_dir)?;
    UsdcBridgeDirection::export_all_to(out_dir)?;
    UsdcBridgeOperation::export_all_to(out_dir)?;
    UsdcBridgeStatus::export_all_to(out_dir)?;
    TimeframeMetrics::export_all_to(out_dir)?;
    PnL::export_all_to(out_dir)?;
    PerformanceMetrics::export_all_to(out_dir)?;
    SpreadSummary::export_all_to(out_dir)?;
    SpreadUpdate::export_all_to(out_dir)?;
    CircuitBreakerStatus::export_all_to(out_dir)?;
    AuthStatus::export_all_to(out_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn default_bindings_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dashboard/src/lib/api")
    }

    #[test]
    fn initial_state_stub_serializes_correctly() {
        let initial = InitialState::stub();
        let json = serde_json::to_string(&initial).expect("serialization should succeed");
        assert!(json.contains("recentTrades"));
        assert!(json.contains("inventory"));
        assert!(json.contains("metrics"));
        assert!(json.contains("authStatus"));
        assert!(json.contains("circuitBreaker"));
        assert!(json.contains("activeTransfers"));
        assert!(json.contains("recentTransfers"));
    }

    #[derive(TS)]
    struct TestOnlyBinding {
        _canary: bool,
    }

    #[test]
    fn export_bindings_generates_files_in_dashboard_directory() {
        let out_dir = default_bindings_dir();

        export_bindings(&out_dir).unwrap();

        assert!(
            out_dir.join("ServerMessage.ts").exists(),
            "ServerMessage.ts should exist in {}/",
            out_dir.display()
        );
        assert!(
            out_dir.join("InitialState.ts").exists(),
            "InitialState.ts should exist in {}/",
            out_dir.display()
        );

        TestOnlyBinding::export_all_to(&out_dir).unwrap();
        let test_file = out_dir.join("TestOnlyBinding.ts");
        let contents = std::fs::read_to_string(&test_file).unwrap_or_else(|error| {
            panic!(
                "test-only binding should be readable at {}: {error}",
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

    #[test]
    fn transfer_operation_equity_mint_serializes_with_kind_tag() {
        let operation = TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(Decimal::new(10, 0)),
            status: EquityMintStatus::Minting,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let json = serde_json::to_string(&operation).expect("serialization should succeed");
        assert!(json.contains(r#""kind":"equity_mint""#));
        assert!(json.contains(r#""status":"minting""#));
        assert!(json.contains(r#""symbol":"AAPL""#));
    }

    #[test]
    fn transfer_operation_usdc_bridge_serializes_with_kind_tag() {
        let operation = TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(Decimal::new(1000, 0)),
            status: UsdcBridgeStatus::Completed {
                completed_at: Utc::now(),
            },
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let json = serde_json::to_string(&operation).expect("serialization should succeed");
        assert!(json.contains(r#""kind":"usdc_bridge""#));
        assert!(json.contains(r#""status":"completed""#));
        assert!(json.contains(r#""direction":"alpaca_to_base""#));
    }

    #[test]
    fn inventory_with_inflight_serializes_correctly() {
        let inventory = Inventory {
            per_symbol: vec![SymbolInventory {
                symbol: Symbol::new("TSLA").unwrap(),
                onchain_available: FractionalShares::new(Decimal::new(50, 0)),
                onchain_inflight: FractionalShares::new(Decimal::new(5, 0)),
                offchain_available: FractionalShares::new(Decimal::new(45, 0)),
                offchain_inflight: FractionalShares::ZERO,
            }],
            usdc: UsdcInventory {
                onchain_available: Usdc::new(Decimal::new(10000, 0)),
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::new(Decimal::new(5000, 0)),
                offchain_inflight: Usdc::new(Decimal::new(500, 0)),
            },
        };

        let json = serde_json::to_string(&inventory).expect("serialization should succeed");
        assert!(json.contains("onchainAvailable"));
        assert!(json.contains("onchainInflight"));
        assert!(json.contains("offchainAvailable"));
        assert!(json.contains("offchainInflight"));
    }

    #[test]
    fn failed_equity_mint_serializes_with_failed_at() {
        let status = EquityMintStatus::Failed {
            failed_at: Utc::now(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(
            json.contains(r#""status":"failed""#),
            "should include failed status tag, got: {json}"
        );
        assert!(
            json.contains("\"failedAt\""),
            "should include failedAt, got: {json}"
        );
    }

    #[test]
    fn completed_equity_mint_serializes_with_completed_at() {
        let status = EquityMintStatus::Completed {
            completed_at: Utc::now(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(
            json.contains(r#""status":"completed""#),
            "should include completed status tag, got: {json}"
        );
        assert!(
            json.contains("\"completedAt\""),
            "should include completedAt, got: {json}"
        );
    }

    #[test]
    fn failed_usdc_bridge_serializes_with_failed_at() {
        let status = UsdcBridgeStatus::Failed {
            failed_at: Utc::now(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(
            json.contains(r#""status":"failed""#),
            "should include failed status tag, got: {json}"
        );
        assert!(
            json.contains("\"failedAt\""),
            "should include failedAt, got: {json}"
        );
    }

    #[test]
    fn completed_usdc_bridge_serializes_with_completed_at() {
        let status = UsdcBridgeStatus::Completed {
            completed_at: Utc::now(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(
            json.contains(r#""status":"completed""#),
            "should include completed status tag, got: {json}"
        );
        assert!(
            json.contains("\"completedAt\""),
            "should include completedAt, got: {json}"
        );
    }

    #[test]
    fn equity_mint_wrapping_status_serializes() {
        let operation = TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-cs"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(Decimal::new(10, 0)),
            status: EquityMintStatus::Wrapping,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let json = serde_json::to_string(&operation).unwrap();
        assert!(
            json.contains(r#""status":"wrapping""#),
            "should contain wrapping status, got: {json}"
        );
        assert!(
            json.contains(r#""kind":"equity_mint""#),
            "should contain equity_mint kind, got: {json}"
        );
    }

    #[test]
    fn failed_equity_redemption_serializes_with_failed_at() {
        let status = EquityRedemptionStatus::Failed {
            failed_at: Utc::now(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(
            json.contains(r#""status":"failed""#),
            "should include failed status tag, got: {json}"
        );
        assert!(
            json.contains("\"failedAt\""),
            "should include failedAt, got: {json}"
        );
    }
}
