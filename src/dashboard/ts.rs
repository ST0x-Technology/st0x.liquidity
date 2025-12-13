use ts_rs::TS;

use super::auth::AuthStatus;
use super::circuit_breaker::CircuitBreakerStatus;
use super::event::EventStoreEntry;
use super::inventory::{Inventory, SymbolInventory, UsdcInventory};
use super::metrics::{PerformanceMetrics, PnL, Timeframe, TimeframeMetrics};
use super::position::Position;
use super::rebalance::{
    RebalanceOperation, RebalanceOperationType, RebalanceStatus, UsdcDirection,
};
use super::spread::{SpreadSummary, SpreadUpdate};
use super::trade::{Direction, OffchainTrade, OffchainTradeStatus, OnchainTrade, Trade};
use super::{InitialState, ServerMessage};

pub fn export_bindings() -> Result<(), ts_rs::ExportError> {
    ServerMessage::export_all()?;
    InitialState::export_all()?;
    EventStoreEntry::export_all()?;
    Direction::export_all()?;
    OnchainTrade::export_all()?;
    OffchainTrade::export_all()?;
    OffchainTradeStatus::export_all()?;
    Trade::export_all()?;
    Position::export_all()?;
    SymbolInventory::export_all()?;
    Inventory::export_all()?;
    UsdcInventory::export_all()?;
    Timeframe::export_all()?;
    TimeframeMetrics::export_all()?;
    PnL::export_all()?;
    PerformanceMetrics::export_all()?;
    SpreadSummary::export_all()?;
    SpreadUpdate::export_all()?;
    RebalanceStatus::export_all()?;
    RebalanceOperation::export_all()?;
    RebalanceOperationType::export_all()?;
    UsdcDirection::export_all()?;
    CircuitBreakerStatus::export_all()?;
    AuthStatus::export_all()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_bindings_succeeds() {
        export_bindings().expect("Failed to export TypeScript bindings");
    }
}
