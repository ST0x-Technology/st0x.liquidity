//! Inventory view for tracking cross-venue asset positions.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;
use std::collections::HashMap;

use super::venue_balance::VenueBalance;
use crate::shares::FractionalShares;
use crate::threshold::Usdc;

/// Imbalance requiring rebalancing action.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Imbalance<T> {
    /// Too much onchain - triggers movement to offchain.
    TooMuchOnchain { excess: T },
    /// Too much offchain - triggers movement to onchain.
    TooMuchOffchain { excess: T },
}

/// Threshold configuration for imbalance detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    target: Decimal,
    /// Deviation from target that triggers rebalancing.
    deviation: Decimal,
}

/// Inventory at a pair of venues (onchain/offchain).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Inventory<T> {
    onchain: VenueBalance<T>,
    offchain: VenueBalance<T>,
    last_rebalancing: Option<DateTime<Utc>>,
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
}
