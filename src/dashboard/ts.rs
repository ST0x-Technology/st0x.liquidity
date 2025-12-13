use ts_rs::TS;

use super::auth::AuthStatus;
use super::circuit_breaker::CircuitBreakerStatus;
use super::event::EventStoreEntry;
use super::inventory::{Inventory, SymbolInventory, UsdcInventory};
use super::performance::{PerformanceMetrics, PnL, TimeframeMetrics};
use super::position::Position;
use super::rebalance::RebalanceOperation;
use super::spread::{SpreadSummary, SpreadUpdate};
use super::trade::Trade;
use super::{InitialState, ServerMessage};

pub fn export_bindings() -> Result<(), ts_rs::ExportError> {
    ServerMessage::export_all()?;
    InitialState::export_all()?;
    EventStoreEntry::export_all()?;
    Trade::export_all()?;
    Position::export_all()?;
    SymbolInventory::export_all()?;
    Inventory::export_all()?;
    UsdcInventory::export_all()?;
    TimeframeMetrics::export_all()?;
    PnL::export_all()?;
    PerformanceMetrics::export_all()?;
    SpreadSummary::export_all()?;
    SpreadUpdate::export_all()?;
    RebalanceOperation::export_all()?;
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
