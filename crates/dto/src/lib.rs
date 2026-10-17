//! Data transfer objects for the bot's read API: the REST handlers, the
//! websocket stream and its `current_state` seed, and the liq_* metrics read
//! them. Domain types from `st0x-finance` keep the wire values typed.

use serde::Serialize;

mod equity_price;
mod inventory;
mod performance;
mod position;
mod settings;
mod statement;
mod trade;
mod transfer;

pub use equity_price::*;
pub use inventory::*;
pub use performance::*;
pub use position::*;
pub use settings::*;
pub use statement::*;
pub use trade::*;
pub use transfer::*;

/// The state snapshot `/api/ws` sends first on connection (the exporter reads it as a seed).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentState {
    pub trades: Vec<Trade>,
    pub inventory: Inventory,
    pub positions: Vec<Position>,
    pub equity_prices: Vec<EquityPrice>,
    pub settings: Settings,
    pub active_transfers: Vec<TransferOperation>,
    pub recent_transfers: Vec<TransferOperation>,
    pub warnings: Vec<TransferWarning>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn current_state_serializes_all_fields() {
        let state = CurrentState {
            trades: Vec::new(),
            inventory: Inventory::empty("USDC"),
            positions: Vec::new(),
            equity_prices: Vec::new(),
            settings: Settings {
                equity_target: Some(0.5),
                equity_deviation: 0.2,
                usdc_target: None,
                usdc_deviation: None,
                cash_reserved: None,
                execution_threshold: "$2".to_string(),
                assets: Vec::new(),
                wallet: None,
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0000000000000000000000000000000000000000".to_string(),
                deployment_block: 12345,
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
        assert_eq!(json["equityPrices"], json!([]));
        assert_eq!(json["activeTransfers"], json!([]));
        assert_eq!(json["recentTransfers"], json!([]));
        assert_eq!(json["warnings"], json!([]));
        assert_eq!(json["settings"]["equityTarget"], json!(0.5));
        assert_eq!(json["settings"]["equityDeviation"], json!(0.2));
        assert_eq!(json["settings"]["usdcTarget"], json!(null));
        assert_eq!(json["settings"]["executionThreshold"], json!("$2"));
        assert!(json["inventory"].is_object());
    }
}
