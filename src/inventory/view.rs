//! Inventory view for tracking cross-venue asset positions.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Add, Sub};

use super::venue_balance::VenueBalance;
use crate::shares::{ArithmeticError, FractionalShares, HasZero};
use crate::threshold::Usdc;
use st0x_broker::Symbol;

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

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + std::ops::Mul<Decimal, Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + Into<Decimal>,
{
    /// Returns the ratio of onchain to total inventory.
    /// Returns `None` if total is zero.
    fn ratio(&self) -> Option<Decimal> {
        let onchain: Decimal = self.onchain.total().ok()?.into();
        let offchain: Decimal = self.offchain.total().ok()?.into();
        let total = onchain + offchain;

        if total.is_zero() {
            return None;
        }

        Some(onchain / total)
    }

    fn has_inflight(&self) -> bool {
        self.onchain.has_inflight() || self.offchain.has_inflight()
    }

    /// Detects imbalance based on threshold configuration.
    /// Returns `None` if balanced, has inflight operations, or total is zero.
    fn detect_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<Imbalance<T>> {
        if self.has_inflight() {
            return None;
        }

        let ratio = self.ratio()?;
        let lower = threshold.target - threshold.deviation;
        let upper = threshold.target + threshold.deviation;

        if ratio < lower {
            let onchain = self.onchain.total().ok()?;
            let offchain = self.offchain.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (target_onchain - onchain).ok()?;

            Some(Imbalance::TooMuchOffchain { excess })
        } else if ratio > upper {
            let onchain = self.onchain.total().ok()?;
            let offchain = self.offchain.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (onchain - target_onchain).ok()?;

            Some(Imbalance::TooMuchOnchain { excess })
        } else {
            None
        }
    }
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn shares(n: i64) -> FractionalShares {
        FractionalShares(Decimal::from(n))
    }

    fn venue(available: i64, inflight: i64) -> VenueBalance<FractionalShares> {
        VenueBalance::new(shares(available), shares(inflight))
    }

    fn inventory(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> Inventory<FractionalShares> {
        Inventory {
            onchain: venue(onchain_available, onchain_inflight),
            offchain: venue(offchain_available, offchain_inflight),
            last_rebalancing: None,
        }
    }

    fn threshold(target: &str, deviation: &str) -> ImbalanceThreshold {
        ImbalanceThreshold {
            target: target.parse().unwrap(),
            deviation: deviation.parse().unwrap(),
        }
    }

    #[test]
    fn ratio_returns_none_when_total_is_zero() {
        let inv = inventory(0, 0, 0, 0);
        assert!(inv.ratio().is_none());
    }

    #[test]
    fn ratio_returns_half_for_equal_split() {
        let inv = inventory(50, 0, 50, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn ratio_returns_one_when_all_onchain() {
        let inv = inventory(100, 0, 0, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::ONE);
    }

    #[test]
    fn ratio_returns_zero_when_all_offchain() {
        let inv = inventory(0, 0, 100, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::ZERO);
    }

    #[test]
    fn ratio_includes_inflight_in_total() {
        let inv = inventory(25, 25, 25, 25);
        assert_eq!(inv.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn has_inflight_false_when_no_inflight() {
        let inv = inventory(50, 0, 50, 0);
        assert!(!inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_onchain_inflight() {
        let inv = inventory(50, 10, 50, 0);
        assert!(inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_offchain_inflight() {
        let inv = inventory(50, 0, 50, 10);
        assert!(inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_both_inflight() {
        let inv = inventory(50, 10, 50, 10);
        assert!(inv.has_inflight());
    }

    #[test]
    fn detect_imbalance_returns_none_when_balanced() {
        let inv = inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_has_inflight() {
        let inv = inventory(80, 10, 20, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_total_is_zero() {
        let inv = inventory(0, 0, 0, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_too_much_onchain() {
        // 80 onchain, 20 offchain = 80% ratio, threshold is 50% +- 20%
        let inv = inventory(80, 0, 20, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inv.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 80, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOnchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_returns_too_much_offchain() {
        // 20 onchain, 80 offchain = 20% ratio, threshold is 50% +- 20%
        let inv = inventory(20, 0, 80, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inv.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 20, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOffchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_at_upper_boundary_is_balanced() {
        // 70% ratio exactly at upper threshold (50% +- 20%)
        let inv = inventory(70, 0, 30, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_at_lower_boundary_is_balanced() {
        // 30% ratio exactly at lower threshold (50% +- 20%)
        let inv = inventory(30, 0, 70, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }
}
