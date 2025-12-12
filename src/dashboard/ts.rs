use ts_rs::TS;

use super::messages::*;

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
