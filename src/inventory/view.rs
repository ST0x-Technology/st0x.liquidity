//! Inventory view for tracking cross-venue asset positions.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Add, Sub};

use st0x_execution::{ArithmeticError, Direction, FractionalShares, HasZero, Symbol};

use super::venue_balance::{InventoryError, VenueBalance};
use crate::threshold::Usdc;
use crate::wrapper::UnderlyingPerWrapped;

/// Error type for inventory view operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub(crate) enum InventoryViewError {
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
    #[error(transparent)]
    Usdc(#[from] InventoryError<Usdc>),
    #[error("failed to convert cash balance cents {0} to USDC")]
    CashBalanceConversion(i64),
}

/// Imbalance requiring rebalancing action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Imbalance<T> {
    /// Too much onchain - triggers movement to offchain.
    TooMuchOnchain { excess: T },
    /// Too much offchain - triggers movement to onchain.
    TooMuchOffchain { excess: T },
}

/// Threshold configuration for imbalance detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    pub(crate) target: Decimal,
    /// Deviation from target that triggers rebalancing.
    pub(crate) deviation: Decimal,
}

/// Discriminant for the two venues tracked by an [`Inventory`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Venue {
    /// Onchain venue (Raindex) -- where market making happens.
    MarketMaking,
    /// Offchain venue (brokerage) -- where hedging happens.
    Hedging,
}

impl Venue {
    fn other(self) -> Self {
        match self {
            Self::MarketMaking => Self::Hedging,
            Self::Hedging => Self::MarketMaking,
        }
    }
}

/// Add or remove from a venue's available balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operator {
    Add,
    Remove,
}

impl From<Direction> for Operator {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Buy => Self::Add,
            Direction::Sell => Self::Remove,
        }
    }
}

/// Stage of an inflight transfer between venues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferOp {
    /// Move available to inflight (assets leaving this venue).
    Start,
    /// Confirm inflight at source and add available at destination.
    Complete,
    /// Cancel inflight back to available at source.
    Cancel,
}

/// Inventory at a pair of venues (onchain/offchain).
///
/// Venues are `Option` to distinguish "not yet polled" from "polled with zero balance".
/// Imbalance detection requires both venues to have been initialized by snapshot events.
///
/// Fields are private — mutation is only possible through the closure-returning
/// factory methods, which are designed to be passed to
/// [`InventoryView::update_equity`] or [`InventoryView::update_usdc`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Inventory<T> {
    onchain: Option<VenueBalance<T>>,
    offchain: Option<VenueBalance<T>>,
    last_rebalancing: Option<DateTime<Utc>>,
}

/// Impl block with minimal bounds for `has_inflight` - shared by all other impl blocks.
impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + std::fmt::Debug,
{
    fn has_inflight(&self) -> bool {
        self.onchain.as_ref().is_some_and(|v| v.has_inflight())
            || self.offchain.as_ref().is_some_and(|v| v.has_inflight())
    }

    fn get_venue(&self, venue: Venue) -> Option<VenueBalance<T>> {
        match venue {
            Venue::MarketMaking => self.onchain,
            Venue::Hedging => self.offchain,
        }
    }

    fn set_venue(self, venue: Venue, balance: Option<VenueBalance<T>>) -> Self {
        match venue {
            Venue::MarketMaking => Self {
                onchain: balance,
                ..self
            },
            Venue::Hedging => Self {
                offchain: balance,
                ..self
            },
        }
    }
}

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + std::ops::Mul<Decimal, Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + Into<Decimal>
        + std::fmt::Debug,
{
    /// Returns the ratio of onchain to total inventory.
    /// Returns `None` if either venue is uninitialized or total is zero.
    fn ratio(&self) -> Option<Decimal> {
        let onchain: Decimal = self.onchain.as_ref()?.total().ok()?.into();
        let offchain: Decimal = self.offchain.as_ref()?.total().ok()?.into();
        let total = onchain + offchain;

        if total.is_zero() {
            return None;
        }

        Some(onchain / total)
    }

    /// Detects imbalance based on threshold configuration.
    /// Returns `None` if either venue is uninitialized, balanced, has inflight operations,
    /// or total is zero.
    fn detect_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<Imbalance<T>> {
        // Require both venues to be initialized before detecting imbalance.
        // This prevents triggering rebalancing when only one venue has been polled.
        let onchain_venue = self.onchain.as_ref()?;
        let offchain_venue = self.offchain.as_ref()?;

        if onchain_venue.has_inflight() || offchain_venue.has_inflight() {
            return None;
        }

        let ratio = self.ratio()?;
        let lower = threshold.target - threshold.deviation;
        let upper = threshold.target + threshold.deviation;

        if ratio < lower {
            let onchain = onchain_venue.total().ok()?;
            let offchain = offchain_venue.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (target_onchain - onchain).ok()?;

            Some(Imbalance::TooMuchOffchain { excess })
        } else if ratio > upper {
            let onchain = onchain_venue.total().ok()?;
            let offchain = offchain_venue.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (onchain - target_onchain).ok()?;

            Some(Imbalance::TooMuchOnchain { excess })
        } else {
            None
        }
    }

    /// Detects imbalance using a normalized onchain value.
    ///
    /// This is used when onchain balance is in wrapped tokens and needs to be
    /// converted to unwrapped-equivalent before comparison with offchain balance.
    ///
    /// # Arguments
    ///
    /// * `threshold` - The imbalance threshold configuration
    /// * `normalized_onchain` - The onchain balance converted to unwrapped-equivalent
    ///
    /// Returns `None` if balanced, has inflight operations, or total is zero.
    fn detect_imbalance_normalized(
        &self,
        threshold: &ImbalanceThreshold,
        normalized_onchain: T,
    ) -> Option<Imbalance<T>> {
        if self.has_inflight() {
            return None;
        }

        let onchain_decimal: Decimal = normalized_onchain.into();
        let offchain: Decimal = self.offchain.as_ref()?.total().ok()?.into();
        let total = onchain_decimal + offchain;

        if total.is_zero() {
            return None;
        }

        let ratio = onchain_decimal / total;
        let lower = threshold.target - threshold.deviation;
        let upper = threshold.target + threshold.deviation;

        if ratio < lower {
            let offchain_val = self.offchain.as_ref()?.total().ok()?;
            let total_val = (normalized_onchain + offchain_val).ok()?;
            let target = (total_val * threshold.target).ok()?;
            let excess = (target - normalized_onchain).ok()?;

            Some(Imbalance::TooMuchOffchain { excess })
        } else if ratio > upper {
            let offchain_val = self.offchain.as_ref()?.total().ok()?;
            let total_val = (normalized_onchain + offchain_val).ok()?;
            let target = (total_val * threshold.target).ok()?;
            let excess = (normalized_onchain - target).ok()?;

            Some(Imbalance::TooMuchOnchain { excess })
        } else {
            None
        }
    }
}

impl<T> Default for Inventory<T> {
    fn default() -> Self {
        Self {
            onchain: None,
            offchain: None,
            last_rebalancing: None,
        }
    }
}

/// Closure-returning factory methods for inventory mutations.
///
/// Each method captures its parameters and returns a boxed closure that
/// performs the mutation when called with an `Inventory`. This pattern
/// keeps the `Inventory` fields and `VenueBalance` methods private while
/// allowing callers in other modules to compose operations and pass them
/// to [`InventoryView::update_equity`] or [`InventoryView::update_usdc`].
impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + PartialOrd
        + std::fmt::Debug
        + Send
        + 'static,
{
    /// Add or remove from a venue's available balance.
    pub(crate) fn available(
        venue: Venue,
        op: Operator,
        amount: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            let balance = match op {
                Operator::Add => match inventory.get_venue(venue) {
                    Some(v) => v.add_available(amount)?,
                    None => VenueBalance::new(amount, T::ZERO),
                },
                Operator::Remove => inventory
                    .get_venue(venue)
                    .unwrap_or_default()
                    .remove_available(amount)?,
            };

            Ok(inventory.set_venue(venue, Some(balance)))
        })
    }

    /// Perform a transfer lifecycle operation at a venue.
    ///
    /// - [`TransferOp::Start`]: move available to inflight (assets leaving).
    /// - [`TransferOp::Complete`]: confirm inflight at `from` and add
    ///   available at the other venue.
    /// - [`TransferOp::Cancel`]: return inflight back to available.
    pub(crate) fn transfer(
        from: Venue,
        op: TransferOp,
        amount: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| match op {
            TransferOp::Start => {
                let balance = inventory
                    .get_venue(from)
                    .unwrap_or_default()
                    .move_to_inflight(amount)?;

                Ok(inventory.set_venue(from, Some(balance)))
            }

            TransferOp::Complete => {
                let source = inventory
                    .get_venue(from)
                    .unwrap_or_default()
                    .confirm_inflight(amount)?;

                let dest = match inventory.get_venue(from.other()) {
                    Some(v) => v.add_available(amount)?,
                    None => VenueBalance::new(amount, T::ZERO),
                };

                Ok(inventory
                    .set_venue(from, Some(source))
                    .set_venue(from.other(), Some(dest)))
            }

            TransferOp::Cancel => {
                let balance = inventory
                    .get_venue(from)
                    .unwrap_or_default()
                    .cancel_inflight(amount)?;

                Ok(inventory.set_venue(from, Some(balance)))
            }
        })
    }

    pub(crate) fn with_last_rebalancing(
        timestamp: DateTime<Utc>,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            Ok(Self {
                last_rebalancing: Some(timestamp),
                ..inventory
            })
        })
    }

    /// Apply a fetched venue snapshot.
    ///
    /// Skips if ANY venue has inflight operations, because we cannot
    /// distinguish "transfer completed but not confirmed" from
    /// "unrelated inventory change".
    pub(crate) fn on_snapshot(
        venue: Venue,
        snapshot_balance: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            if inventory.has_inflight() {
                return Ok(inventory);
            }

            let balance = inventory
                .get_venue(venue)
                .unwrap_or_default()
                .apply_snapshot(snapshot_balance);

            Ok(inventory.set_venue(venue, Some(balance)))
        })
    }
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
}

impl InventoryView {
    /// Checks a single equity for imbalance against the threshold.
    ///
    /// The onchain balance is converted from wrapped to unwrapped-equivalent using
    /// the vault ratio before comparison with offchain balance. This ensures correct
    /// imbalance detection when onchain tokens have accrued value through stock
    /// splits or dividends.
    ///
    /// Returns the imbalance if one exists, or None if balanced or symbol not tracked.
    pub(crate) fn check_equity_imbalance(
        &self,
        symbol: &Symbol,
        threshold: &ImbalanceThreshold,
        vault_ratio: &UnderlyingPerWrapped,
    ) -> Option<Imbalance<FractionalShares>> {
        let inventory = self.equities.get(symbol)?;

        // Convert onchain (wrapped) to unwrapped-equivalent
        let onchain_wrapped = inventory.onchain.as_ref()?.total().ok()?;
        let onchain_equivalent = vault_ratio.to_underlying_fractional(onchain_wrapped).ok()?;

        inventory.detect_imbalance_normalized(threshold, onchain_equivalent)
    }

    /// Checks USDC inventory for imbalance against the threshold.
    /// Returns the imbalance if one exists.
    pub(crate) fn check_usdc_imbalance(
        &self,
        threshold: &ImbalanceThreshold,
    ) -> Option<Imbalance<Usdc>> {
        self.usdc.detect_imbalance(threshold)
    }
}

impl Default for InventoryView {
    fn default() -> Self {
        Self {
            usdc: Inventory::default(),
            equities: HashMap::new(),
            last_updated: Utc::now(),
        }
    }
}

impl InventoryView {
    /// Registers a symbol with zeroed inventory.
    #[cfg(test)]
    pub(crate) fn with_equity(mut self, symbol: Symbol) -> Self {
        self.equities.insert(symbol, Inventory::default());
        self
    }

    /// Sets USDC inventory with specified available balances (zero inflight).
    #[cfg(test)]
    pub(crate) fn with_usdc(self, onchain_available: Usdc, offchain_available: Usdc) -> Self {
        Self {
            usdc: Inventory {
                onchain: Some(VenueBalance::new(onchain_available, Usdc(Decimal::ZERO))),
                offchain: Some(VenueBalance::new(offchain_available, Usdc(Decimal::ZERO))),
                last_rebalancing: None,
            },
            ..self
        }
    }

    pub(crate) fn update_equity(
        self,
        symbol: &Symbol,
        update: impl FnOnce(
            Inventory<FractionalShares>,
        )
            -> Result<Inventory<FractionalShares>, InventoryError<FractionalShares>>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let inventory = self.equities.get(symbol).cloned().unwrap_or_default();

        let updated = update(inventory)?;

        let mut equities = self.equities;
        equities.insert(symbol.clone(), updated);

        Ok(Self {
            equities,
            last_updated: now,
            usdc: self.usdc,
        })
    }

    pub(crate) fn update_usdc(
        self,
        update: impl FnOnce(Inventory<Usdc>) -> Result<Inventory<Usdc>, InventoryError<Usdc>>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let updated = update(self.usdc)?;

        Ok(Self {
            usdc: updated,
            last_updated: now,
            equities: self.equities,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    use super::*;
    use crate::threshold::Usdc;
    use crate::wrapper::RATIO_ONE;

    fn shares(amount: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(amount))
    }

    fn one_to_one_ratio() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
    }

    fn venue(available: i64, inflight: i64) -> VenueBalance<FractionalShares> {
        VenueBalance::new(shares(available), shares(inflight))
    }

    fn make_inventory(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> Inventory<FractionalShares> {
        Inventory {
            onchain: Some(venue(onchain_available, onchain_inflight)),
            offchain: Some(venue(offchain_available, offchain_inflight)),
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
        let inventory = make_inventory(0, 0, 0, 0);
        assert!(inventory.ratio().is_none());
    }

    #[test]
    fn ratio_returns_half_for_equal_split() {
        let inventory = make_inventory(50, 0, 50, 0);
        assert_eq!(inventory.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn ratio_returns_one_when_all_onchain() {
        let inventory = make_inventory(100, 0, 0, 0);
        assert_eq!(inventory.ratio().unwrap(), Decimal::ONE);
    }

    #[test]
    fn ratio_returns_zero_when_all_offchain() {
        let inventory = make_inventory(0, 0, 100, 0);
        assert_eq!(inventory.ratio().unwrap(), Decimal::ZERO);
    }

    #[test]
    fn ratio_includes_inflight_in_total() {
        let inventory = make_inventory(25, 25, 25, 25);
        assert_eq!(inventory.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn ratio_returns_none_when_onchain_uninitialized() {
        let inventory = Inventory {
            onchain: None,
            offchain: Some(venue(100, 0)),
            last_rebalancing: None,
        };
        assert!(inventory.ratio().is_none());
    }

    #[test]
    fn ratio_returns_none_when_offchain_uninitialized() {
        let inventory = Inventory {
            onchain: Some(venue(100, 0)),
            offchain: None,
            last_rebalancing: None,
        };
        assert!(inventory.ratio().is_none());
    }

    #[test]
    fn has_inflight_false_when_no_inflight() {
        let inventory = make_inventory(50, 0, 50, 0);
        assert!(!inventory.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_onchain_inflight() {
        let inventory = make_inventory(50, 10, 50, 0);
        assert!(inventory.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_offchain_inflight() {
        let inventory = make_inventory(50, 0, 50, 10);
        assert!(inventory.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_both_inflight() {
        let inventory = make_inventory(50, 10, 50, 10);
        assert!(inventory.has_inflight());
    }

    #[test]
    fn detect_imbalance_returns_none_when_balanced() {
        let inventory = make_inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_has_inflight() {
        let inventory = make_inventory(80, 10, 20, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_total_is_zero() {
        let inventory = make_inventory(0, 0, 0, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_too_much_onchain() {
        // 80 onchain, 20 offchain = 80% ratio, threshold is 50% +- 20%
        let inventory = make_inventory(80, 0, 20, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inventory.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 80, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOnchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_returns_too_much_offchain() {
        // 20 onchain, 80 offchain = 20% ratio, threshold is 50% +- 20%
        let inventory = make_inventory(20, 0, 80, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inventory.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 20, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOffchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_at_upper_boundary_is_balanced() {
        // 70% ratio exactly at upper threshold (50% +- 20%)
        let inventory = make_inventory(70, 0, 30, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_at_lower_boundary_is_balanced() {
        // 30% ratio exactly at lower threshold (50% +- 20%)
        let inventory = make_inventory(30, 0, 70, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_onchain_not_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: None,
            offchain: Some(venue(50, 0)),
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_offchain_not_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: Some(venue(50, 0)),
            offchain: None,
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_neither_venue_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: None,
            offchain: None,
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert!(inventory.detect_imbalance(&thresh).is_none());
    }

    fn usdc_venue(available: i64, inflight: i64) -> VenueBalance<Usdc> {
        VenueBalance::new(
            Usdc(Decimal::from(available)),
            Usdc(Decimal::from(inflight)),
        )
    }

    fn usdc_make_inventory(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> Inventory<Usdc> {
        Inventory {
            onchain: Some(usdc_venue(onchain_available, onchain_inflight)),
            offchain: Some(usdc_venue(offchain_available, offchain_inflight)),
            last_rebalancing: None,
        }
    }

    fn make_view(equities: Vec<(Symbol, Inventory<FractionalShares>)>) -> InventoryView {
        InventoryView {
            usdc: usdc_make_inventory(1000, 0, 1000, 0),
            equities: equities.into_iter().collect(),
            last_updated: Utc::now(),
        }
    }

    fn make_usdc_view(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> InventoryView {
        InventoryView {
            usdc: usdc_make_inventory(
                onchain_available,
                onchain_inflight,
                offchain_available,
                offchain_inflight,
            ),
            equities: HashMap::new(),
            last_updated: Utc::now(),
        }
    }

    #[test]
    fn usdc_inflight_blocks_imbalance_detection() {
        let inventory = usdc_make_inventory(800, 0, 200, 0);
        let thresh = threshold("0.5", "0.2");
        assert!(inventory.detect_imbalance(&thresh).is_some());

        let inventory_with_inflight = usdc_make_inventory(700, 100, 200, 0);
        assert!(inventory_with_inflight.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_balanced() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(50, 0, 50, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio)
                .is_none()
        );
    }

    #[test]
    fn check_equity_imbalance_detects_too_much_onchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio);

        assert!(matches!(imbalance, Some(Imbalance::TooMuchOnchain { .. })));
    }

    #[test]
    fn check_equity_imbalance_detects_too_much_offchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(20, 0, 80, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio);

        assert!(matches!(imbalance, Some(Imbalance::TooMuchOffchain { .. })));
    }

    #[test]
    fn check_equity_imbalance_returns_none_for_unknown_symbol() {
        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();
        let view = make_view(vec![(aapl, make_inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        assert!(
            view.check_equity_imbalance(&msft, &thresh, &ratio)
                .is_none()
        );
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_inflight() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(60, 20, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio)
                .is_none()
        );
    }

    #[test]
    fn check_equity_imbalance_with_one_to_one_ratio_detects_imbalance() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio);

        assert!(matches!(imbalance, Some(Imbalance::TooMuchOnchain { .. })));
    }

    #[test]
    fn check_equity_imbalance_with_1_05_ratio_converts_onchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        // 50 wrapped onchain, 50 offchain
        // With 1:1 ratio: 50/100 = 0.5 (balanced)
        // With 1.05 ratio: 50 wrapped = 52.5 unwrapped-equivalent
        // Total = 52.5 + 50 = 102.5
        // Ratio = 52.5 / 102.5 = 0.512 (still within 50% +/- 20% threshold)
        let view = make_view(vec![(aapl.clone(), make_inventory(50, 0, 50, 0))]);
        let thresh = threshold("0.5", "0.2");

        // 1:1 ratio - balanced
        let one_to_one = one_to_one_ratio();
        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &one_to_one)
                .is_none()
        );

        // 1.05 ratio - still balanced (small appreciation doesn't change outcome)
        let ratio_1_05 =
            UnderlyingPerWrapped::new(U256::from(1_050_000_000_000_000_000u64)).unwrap();
        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio_1_05)
                .is_none()
        );
    }

    #[test]
    fn check_equity_imbalance_with_high_ratio_changes_detection() {
        let aapl = Symbol::new("AAPL").unwrap();
        // 65 wrapped onchain, 35 offchain
        // With 1:1 ratio: 65/100 = 0.65 (within 50% +/- 20% = 30%-70%)
        // With 1.5 ratio: 65 wrapped = 97.5 unwrapped-equivalent
        // Total = 97.5 + 35 = 132.5
        // Ratio = 97.5 / 132.5 = 0.736 (above 70% upper threshold!)
        let view = make_view(vec![(aapl.clone(), make_inventory(65, 0, 35, 0))]);
        let thresh = threshold("0.5", "0.2");

        // 1:1 ratio - balanced (65% within threshold)
        let one_to_one = one_to_one_ratio();
        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &one_to_one)
                .is_none()
        );

        // 1.5 ratio - triggers imbalance (73.6% exceeds 70% upper bound)
        let ratio_1_5 =
            UnderlyingPerWrapped::new(U256::from(1_500_000_000_000_000_000u64)).unwrap();
        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio_1_5);
        assert!(
            matches!(imbalance, Some(Imbalance::TooMuchOnchain { .. })),
            "Expected TooMuchOnchain, got: {imbalance:?}"
        );
    }

    #[test]
    fn detect_imbalance_normalized_returns_none_when_balanced() {
        let inventory = make_inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        // Normalized onchain = 50 (same as raw)
        let normalized = shares(50);
        let result = inventory.detect_imbalance_normalized(&thresh, normalized);

        assert!(result.is_none());
    }

    #[test]
    fn detect_imbalance_normalized_detects_too_much_onchain() {
        let inventory = make_inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        // Normalized onchain = 100 (double the raw wrapped amount)
        // Total = 100 + 50 = 150, ratio = 100/150 ~= 0.67 (within threshold)
        // But if normalized = 120, ratio = 120/170 ~= 0.71 (above 70%)
        let normalized = shares(120);
        let result = inventory.detect_imbalance_normalized(&thresh, normalized);

        assert!(matches!(result, Some(Imbalance::TooMuchOnchain { .. })));
    }

    #[test]
    fn detect_imbalance_normalized_returns_none_when_inflight() {
        let inventory = make_inventory(50, 10, 50, 0);
        let thresh = threshold("0.5", "0.2");

        let normalized = shares(120);
        let result = inventory.detect_imbalance_normalized(&thresh, normalized);

        // Even with high normalized value, inflight blocks detection
        assert!(result.is_none());
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_balanced() {
        let view = make_usdc_view(500, 0, 500, 0);

        assert!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3"))
                .is_none()
        );
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_onchain() {
        let view = make_usdc_view(900, 0, 100, 0);

        let imbalance = view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOnchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_offchain() {
        let view = make_usdc_view(100, 0, 900, 0);

        let imbalance = view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOffchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_inflight() {
        let view = make_usdc_view(700, 200, 100, 0);

        assert!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3"))
                .is_none()
        );
    }
}
