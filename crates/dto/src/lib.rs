//! Dashboard DTO types for TypeScript codegen.
//!
//! This crate contains all types that derive `TS` for TypeScript type generation.
//! Keeping these in a separate crate allows the dashboard to build without waiting
//! for the full workspace. Domain types from `st0x-finance` are used for type
//! safety, with `#[ts(type = "string")]` overrides for TypeScript compatibility.

use serde::Serialize;
use std::path::Path;
use ts_rs::TS;

mod inventory;
mod position;
mod settings;
mod statement;
mod trade;
mod transfer;

pub use inventory::*;
pub use position::*;
pub use settings::*;
pub use statement::*;
pub use trade::*;
pub use transfer::*;

/// Dashboard snapshot sent to the frontend on connection.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CurrentState {
    pub trades: Vec<Trade>,
    pub inventory: Inventory,
    pub positions: Vec<Position>,
    pub settings: Settings,
    pub active_transfers: Vec<TransferOperation>,
    pub recent_transfers: Vec<TransferOperation>,
    pub warnings: Vec<TransferWarning>,
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
    Statement::export_all_to(out_dir)?;
    CurrentState::export_all_to(out_dir)?;
    Settings::export_all_to(out_dir)?;
    AssetSettings::export_all_to(out_dir)?;
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
    TransferWarning::export_all_to(out_dir)?;
    std::fs::write(out_dir.join("StatementGuard.ts"), Statement::guard_ts())
        .map_err(ts_rs::ExportError::Io)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::path::PathBuf;
    use ts_rs::TS;

    use super::*;

    fn default_bindings_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dashboard/src/lib/api")
    }

    #[test]
    fn current_state_serializes_all_fields() {
        let state = CurrentState {
            trades: Vec::new(),
            inventory: Inventory::empty(),
            positions: Vec::new(),
            settings: Settings {
                equity_target: 0.5,
                equity_deviation: 0.2,
                usdc_target: None,
                usdc_deviation: None,
                cash_reserved: None,
                execution_threshold: "$2".to_string(),
                assets: Vec::new(),
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0000000000000000000000000000000000000000".to_string(),
                deployment_block: 12345,
                trading_mode: "rebalancing".to_string(),
                broker: "alpaca".to_string(),
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
        assert_eq!(json["settings"]["equityDeviation"], json!(0.2));
        assert_eq!(json["settings"]["usdcTarget"], json!(null));
        assert_eq!(json["settings"]["executionThreshold"], json!("$2"));
        assert!(json["inventory"].is_object());
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
            out_dir.join("Statement.ts").exists(),
            "Statement.ts should exist in {}/",
            out_dir.display()
        );
        assert!(
            out_dir.join("CurrentState.ts").exists(),
            "CurrentState.ts should exist in {}/",
            out_dir.display()
        );
        assert!(
            out_dir.join("StatementGuard.ts").exists(),
            "StatementGuard.ts should exist in {}/",
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
}
