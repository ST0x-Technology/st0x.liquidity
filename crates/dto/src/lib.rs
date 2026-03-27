//! Dashboard DTO types for TypeScript codegen.
//!
//! This crate contains all types that derive `TS` for TypeScript type generation.
//! Keeping these in a separate crate allows the dashboard to build without waiting
//! for the full workspace. Domain types from `st0x-finance` are used for type
//! safety, with `#[ts(type = "string")]` overrides for TypeScript compatibility.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use ts_rs::TS;

use rain_math_float::Float;
use st0x_float_macro::float;
use st0x_float_serde::{float_string_serde, option_float_string_serde};

use st0x_finance::{FractionalShares, HasZero, Id, Symbol, Usdc};

fn zero_float() -> Float {
    float!(0)
}

/// Converts a domain entity into a [`Statement`] for broadcasting to
/// dashboard clients.
pub trait Reportable {
    fn report(&self, id: &str) -> Statement;
}

/// Notification sent from the server to WebSocket clients.
///
/// A Statement announces that something changed about a specific entity.
/// The client uses the concern type to know what to refetch/invalidate.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    pub id: String,
    pub statement: Concern,
}

/// Domain concern category for a [`Statement`].
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Concern {
    Inventory,
    Transfer,
    Trading,
}

/// Messages sent from the server to WebSocket clients.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ServerMessage {
    Initial(Box<InitialState>),
    Fill(Trade),
    Snapshot(Box<InventorySnapshot>),
    Transfer(TransferOperation),
    Statement(Statement),
}

impl ServerMessage {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Initial(_) => "Initial",
            Self::Fill(_) => "Fill",
            Self::Snapshot(_) => "Snapshot",
            Self::Transfer(_) => "Transfer",
            Self::Statement(_) => "Statement",
        }
    }
}

/// Dashboard snapshot sent to the frontend on connection.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InitialState {
    pub trades: Vec<Trade>,
    pub inventory: Inventory,
    pub active_transfers: Vec<TransferOperation>,
    pub recent_transfers: Vec<TransferOperation>,
}

impl Default for InitialState {
    fn default() -> Self {
        Self {
            trades: Vec::new(),
            inventory: Inventory::empty(),
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
        }
    }
}

/// Where a trade was executed.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    Raindex,
    Alpaca,
    Schwab,
    DryRun,
}

/// Whether the trade was a buy or sell.
#[derive(Debug, Clone, Copy, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TradeDirection {
    Buy,
    Sell,
}

/// A completed trade fill.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub filled_at: DateTime<Utc>,
    pub venue: TradingVenue,
    pub direction: TradeDirection,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub shares: FractionalShares,
}

/// Per-symbol net position.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Position {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub net: Float,
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Self::EquityMint(op) => op.updated_at,
            Self::EquityRedemption(op) => op.updated_at,
            Self::UsdcBridge(op) => op.updated_at,
        }
    }
}

/// Absolute and percentage profit/loss.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
pub struct PnL {
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub absolute: Float,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub percent: Float,
}

/// Performance metrics for a single time window.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TimeframeMetrics {
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub aum: Float,
    pub pnl: PnL,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub volume: Float,
    #[ts(type = "number")]
    pub trade_count: u64,
    #[serde(default, with = "option_float_string_serde")]
    #[ts(type = "string | null")]
    pub sharpe_ratio: Option<Float>,
    #[serde(default, with = "option_float_string_serde")]
    #[ts(type = "string | null")]
    pub sortino_ratio: Option<Float>,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub max_drawdown: Float,
    #[ts(type = "number | null")]
    pub hedge_lag_ms: Option<u64>,
    #[serde(
        default,
        with = "option_float_string_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "string")]
    pub uptime_percent: Option<Float>,
}

impl TimeframeMetrics {
    #[must_use]
    pub fn zero() -> Self {
        Self {
            aum: zero_float(),
            pnl: PnL {
                absolute: zero_float(),
                percent: zero_float(),
            },
            volume: zero_float(),
            trade_count: 0,
            sharpe_ratio: None,
            sortino_ratio: None,
            max_drawdown: zero_float(),
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
    #[must_use]
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
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SpreadSummary {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub last_buy_price: Float,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub last_sell_price: Float,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub pyth_price: Float,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub spread_bps: Float,
    pub updated_at: DateTime<Utc>,
}

/// Incremental spread change for a symbol.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SpreadUpdate {
    #[ts(type = "string")]
    pub symbol: Symbol,
    pub timestamp: DateTime<Utc>,
    #[serde(
        default,
        with = "option_float_string_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "string")]
    pub buy_price: Option<Float>,
    #[serde(
        default,
        with = "option_float_string_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "string")]
    pub sell_price: Option<Float>,
    #[serde(with = "float_string_serde")]
    #[ts(type = "string")]
    pub pyth_price: Float,
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
    Statement::export_all_to(out_dir)?;
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;

    fn default_bindings_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dashboard/src/lib/api")
    }

    fn float(value: &str) -> Float {
        Float::parse(value.to_string()).unwrap()
    }

    #[test]
    fn statement_serializes_correctly() {
        let statement = Statement {
            id: "test-123".to_string(),
            statement: Concern::Transfer,
        };
        let json = serde_json::to_string(&statement).expect("serialization should succeed");
        assert!(json.contains(r#""id":"test-123""#));
        assert!(json.contains(r#""statement""#));
    }

    #[test]
    fn server_message_fill_serializes_with_type_tag() {
        let trade = Trade {
            filled_at: Utc::now(),
            venue: TradingVenue::Raindex,
            direction: TradeDirection::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares::new(float!(10)),
        };
        let msg = ServerMessage::Fill(trade);
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(
            json.contains(r#""type":"fill""#),
            "expected fill tag, got: {json}"
        );
        assert!(json.contains(r#""venue":"raindex""#));
        assert!(json.contains(r#""direction":"buy""#));
        assert!(json.contains(r#""symbol":"AAPL""#));
    }

    #[test]
    fn server_message_initial_serializes_with_type_tag() {
        let msg = ServerMessage::Initial(Box::default());
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(json.contains(r#""type":"initial""#));
        assert!(json.contains(r#""data":"#));
    }

    #[test]
    fn server_message_snapshot_serializes_with_type_tag() {
        let msg = ServerMessage::Snapshot(Box::new(InventorySnapshot {
            inventory: Inventory::empty(),
            fetched_at: Utc::now(),
        }));
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(
            json.contains(r#""type":"snapshot""#),
            "expected snapshot tag, got: {json}"
        );
    }

    #[test]
    fn server_message_statement_serializes_with_type_tag() {
        let msg = ServerMessage::Statement(Statement {
            id: "mint-001".to_string(),
            statement: Concern::Transfer,
        });
        let json = serde_json::to_string(&msg).expect("serialization should succeed");
        assert!(
            json.contains(r#""type":"statement""#),
            "expected statement tag, got: {json}"
        );
    }

    #[test]
    fn trade_serializes_all_fields() {
        let trade = Trade {
            filled_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Alpaca,
            direction: TradeDirection::Sell,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: FractionalShares::new(float!(5.5)),
        };
        let json = serde_json::to_value(&trade).expect("serialization should succeed");
        assert_eq!(json["venue"], json!("alpaca"));
        assert_eq!(json["direction"], json!("sell"));
        assert_eq!(json["symbol"], json!("TSLA"));
        assert_eq!(json["shares"], json!("5.5"));
        assert!(json["filledAt"].is_string());
    }

    #[test]
    fn initial_state_default_serializes_correctly() {
        let initial = InitialState::default();
        let json = serde_json::to_string(&initial).expect("serialization should succeed");
        assert!(json.contains("trades"));
        assert!(json.contains("inventory"));
        assert!(json.contains("activeTransfers"));
        assert!(json.contains("recentTransfers"));
    }

    #[test]
    fn position_serializes_float_as_decimal_string() {
        let position = Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: float("1.25"),
        };

        let json = serde_json::to_value(position).expect("serialization should succeed");
        assert_eq!(
            json,
            json!({
                "symbol": "AAPL",
                "net": "1.25"
            })
        );
    }

    #[test]
    fn timeframe_metrics_serializes_decimal_strings_and_optional_fields() {
        let metrics = TimeframeMetrics {
            aum: float("100.5"),
            pnl: PnL {
                absolute: float("12.25"),
                percent: float("3.5"),
            },
            volume: float("2500.75"),
            trade_count: 4,
            sharpe_ratio: Some(float("1.2")),
            sortino_ratio: None,
            max_drawdown: float("0.15"),
            hedge_lag_ms: None,
            uptime_percent: None,
        };

        let json = serde_json::to_value(metrics).expect("serialization should succeed");

        assert_eq!(json["aum"], json!("100.5"));
        assert_eq!(json["pnl"]["absolute"], json!("12.25"));
        assert_eq!(json["pnl"]["percent"], json!("3.5"));
        assert_eq!(json["volume"], json!("2500.75"));
        assert_eq!(json["sharpeRatio"], json!("1.2"));
        assert_eq!(json["sortinoRatio"], serde_json::Value::Null);
        assert_eq!(json["maxDrawdown"], json!("0.15"));
        assert_eq!(json["hedgeLagMs"], serde_json::Value::Null);
        assert!(
            json.get("uptimePercent").is_none(),
            "uptimePercent should be omitted when None, got: {json}"
        );
    }

    #[test]
    fn spread_update_skips_missing_optional_prices_and_formats_present_values() {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let update = SpreadUpdate {
            symbol: Symbol::new("NVDA").unwrap(),
            timestamp,
            buy_price: None,
            sell_price: Some(float("125.75")),
            pyth_price: float("125.5"),
        };

        let json = serde_json::to_value(update).expect("serialization should succeed");

        assert_eq!(json["symbol"], json!("NVDA"));
        assert_eq!(json["sellPrice"], json!("125.75"));
        assert_eq!(json["pythPrice"], json!("125.5"));
        assert!(
            json.get("buyPrice").is_none(),
            "buyPrice should be omitted when None, got: {json}"
        );
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
        assert!(
            out_dir.join("Statement.ts").exists(),
            "Statement.ts should exist in {}/",
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
    fn transfer_operation_equity_mint_serializes_with_kind_tag() {
        let operation = TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
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
            amount: Usdc::new(float!(1000)),
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

    fn mint_operation(status: EquityMintStatus, updated_at: DateTime<Utc>) -> TransferOperation {
        TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    fn redemption_operation(
        status: EquityRedemptionStatus,
        updated_at: DateTime<Utc>,
    ) -> TransferOperation {
        TransferOperation::EquityRedemption(EquityRedemptionOperation {
            id: Id::new("redeem-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    fn bridge_operation(status: UsdcBridgeStatus, updated_at: DateTime<Utc>) -> TransferOperation {
        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    #[test]
    fn is_terminal_equity_mint() {
        let now = Utc::now();

        assert!(
            mint_operation(EquityMintStatus::Completed { completed_at: now }, now).is_terminal()
        );
        assert!(mint_operation(EquityMintStatus::Failed { failed_at: now }, now).is_terminal());

        assert!(!mint_operation(EquityMintStatus::Minting, now).is_terminal());
        assert!(!mint_operation(EquityMintStatus::Wrapping, now).is_terminal());
        assert!(!mint_operation(EquityMintStatus::Depositing, now).is_terminal());
    }

    #[test]
    fn is_terminal_equity_redemption() {
        let now = Utc::now();

        assert!(
            redemption_operation(EquityRedemptionStatus::Completed { completed_at: now }, now)
                .is_terminal()
        );
        assert!(
            redemption_operation(EquityRedemptionStatus::Failed { failed_at: now }, now)
                .is_terminal()
        );

        assert!(!redemption_operation(EquityRedemptionStatus::Withdrawing, now).is_terminal());
        assert!(!redemption_operation(EquityRedemptionStatus::Unwrapping, now).is_terminal());
        assert!(!redemption_operation(EquityRedemptionStatus::Sending, now).is_terminal());
        assert!(
            !redemption_operation(EquityRedemptionStatus::PendingConfirmation, now).is_terminal()
        );
    }

    #[test]
    fn is_terminal_usdc_bridge() {
        let now = Utc::now();

        assert!(
            bridge_operation(UsdcBridgeStatus::Completed { completed_at: now }, now).is_terminal()
        );
        assert!(bridge_operation(UsdcBridgeStatus::Failed { failed_at: now }, now).is_terminal());

        assert!(!bridge_operation(UsdcBridgeStatus::Converting, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Withdrawing, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Bridging, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Depositing, now).is_terminal());
    }

    #[test]
    fn updated_at_returns_inner_value() {
        let timestamp = Utc::now();

        assert_eq!(
            mint_operation(EquityMintStatus::Minting, timestamp).updated_at(),
            timestamp
        );
        assert_eq!(
            redemption_operation(EquityRedemptionStatus::Withdrawing, timestamp).updated_at(),
            timestamp
        );
        assert_eq!(
            bridge_operation(UsdcBridgeStatus::Converting, timestamp).updated_at(),
            timestamp
        );
    }

    #[test]
    fn inventory_with_inflight_serializes_correctly() {
        let inventory = Inventory {
            per_symbol: vec![SymbolInventory {
                symbol: Symbol::new("TSLA").unwrap(),
                onchain_available: FractionalShares::new(float!(50)),
                onchain_inflight: FractionalShares::new(float!(5)),
                offchain_available: FractionalShares::new(float!(45)),
                offchain_inflight: FractionalShares::ZERO,
            }],
            usdc: UsdcInventory {
                onchain_available: Usdc::new(float!(10000)),
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::new(float!(5000)),
                offchain_inflight: Usdc::new(float!(500)),
            },
        };

        let json = serde_json::to_string(&inventory).expect("serialization should succeed");
        assert!(json.contains("onchainAvailable"));
        assert!(json.contains("onchainInflight"));
        assert!(json.contains("offchainAvailable"));
        assert!(json.contains("offchainInflight"));
    }
}
