//! Server-to-client WebSocket statement envelope.
//!
//! [`Statement`] is the top-level tagged union pushed over WebSocket.
//! Each variant wraps a domain-specific payload.

use serde::Serialize;
use strum::VariantNames;

use crate::inventory::InventorySnapshot;
use crate::transfer::TransferOperation;
use crate::{CurrentState, EquityPrice, LegacyTrade, Position, Trade};

/// Server-to-client WebSocket statements.
///
/// Each variant represents a domain-specific statement the server
/// pushes to connected clients. The `type` tag is serialized as
/// snake_case.
///
/// [`VariantNames`] derives `Statement::VARIANTS`: a compile-time
/// slice of all variant names in snake_case. The serialization tests
/// check the serde `type` tags themselves.
#[derive(Debug, Clone, Serialize, VariantNames)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Statement {
    CurrentState(Box<CurrentState>),
    TradeUpdate(Trade),
    TradeFill(LegacyTrade),
    PositionUpdate(Position),
    EquityPriceUpdate(EquityPrice),
    InventorySnapshot(Box<InventorySnapshot>),
    TransferUpdate(TransferOperation),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use st0x_finance::{FractionalShares, Positive, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::{Direction, Inventory, Settings, TradingVenue};

    #[test]
    fn statement_variants_matches_serde_wire_format() {
        assert_eq!(
            Statement::VARIANTS,
            &[
                "current_state",
                "trade_update",
                "trade_fill",
                "position_update",
                "equity_price_update",
                "inventory_snapshot",
                "transfer_update"
            ]
        );
    }

    #[test]
    fn statement_trade_update_serializes_with_type_tag() {
        let trade = Trade {
            id: "0x0000000000000000000000000000000000000000000000000000000000000000:0".to_string(),
            occurred_at: Utc::now(),
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            price: None,
            outcome: crate::TradeOutcome::Filled,
        };
        let msg = Statement::TradeUpdate(trade);
        let json = serde_json::to_value(&msg).expect("serialization should succeed");
        assert_eq!(json["type"], json!("trade_update"));
        assert_eq!(json["data"]["venue"], json!("raindex"));
        assert_eq!(json["data"]["direction"], json!("buy"));
        assert_eq!(json["data"]["symbol"], json!("AAPL"));
    }

    #[test]
    fn statement_current_state_serializes_with_type_tag() {
        let msg = Statement::CurrentState(Box::new(CurrentState {
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
                execution_threshold: "1 share".to_string(),
                assets: Vec::new(),
                wallet: None,
                log_level: "Debug".to_string(),
                server_port: 8001,
                orderbook: "0x0".to_string(),
                deployment_block: 0,
                broker: "dry_run".to_string(),
                order_polling_interval: 5,
                inventory_poll_interval: 15,
            },
            active_transfers: Vec::new(),
            recent_transfers: Vec::new(),
            warnings: Vec::new(),
        }));
        let json = serde_json::to_value(&msg).expect("serialization should succeed");
        assert_eq!(json["type"], json!("current_state"));
        assert_eq!(json["data"]["trades"], json!([]));
        assert_eq!(json["data"]["positions"], json!([]));
    }

    #[test]
    fn statement_position_update_serializes_with_type_tag() {
        let msg = Statement::PositionUpdate(Position {
            symbol: Symbol::new("AAPL").unwrap(),
            net: rain_math_float::Float::parse("5.5".to_string()).unwrap(),
        });
        let json = serde_json::to_value(&msg).expect("serialization should succeed");
        assert_eq!(json["type"], json!("position_update"));
        assert_eq!(json["data"]["symbol"], json!("AAPL"));
        assert_eq!(json["data"]["net"], json!("5.5"));
    }

    #[test]
    fn statement_inventory_snapshot_serializes_with_type_tag() {
        let msg = Statement::InventorySnapshot(Box::new(InventorySnapshot {
            inventory: Inventory::empty("USDC"),
            fetched_at: Utc::now(),
        }));
        let json = serde_json::to_value(&msg).expect("serialization should succeed");
        assert_eq!(json["type"], json!("inventory_snapshot"));
    }
}
