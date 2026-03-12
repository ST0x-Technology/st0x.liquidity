//! Inventory view for tracking cross-venue asset positions.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Add, Sub};
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::debug;

use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};

use super::venue_balance::{InventoryError, VenueBalance};
use crate::threshold::Usdc;
use crate::wrapper::{RatioError, UnderlyingPerWrapped};

static EXACT_ONE: LazyLock<Float> =
    LazyLock::new(|| Float::parse("1".to_string()).unwrap_or_else(|_| unreachable!()));

/// Error type for inventory view operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryViewError {
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
    #[error(transparent)]
    Usdc(#[from] InventoryError<Usdc>),
    #[error("failed to convert cash balance cents {0} to USDC")]
    CashBalanceConversion(i64),
}

/// Why an equity imbalance check failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityImbalanceError {
    #[error("symbol {0} not tracked in inventory")]
    SymbolNotTracked(Symbol),
    #[error("arithmetic error: {0}")]
    Float(#[from] FloatError),
    #[error(transparent)]
    Ratio(#[from] RatioError),
}

/// Imbalance requiring rebalancing action.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Imbalance<T> {
    /// Too much onchain - triggers movement to offchain.
    TooMuchOnchain { excess: T },
    /// Too much offchain - triggers movement to onchain.
    TooMuchOffchain { excess: T },
}

/// Threshold configuration for imbalance detection.
///
/// Invariants enforced by [`ImbalanceThreshold::new`]:
/// - `target` must be in `[0.0, 1.0]`
/// - `deviation` must be `>= 0`
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "RawImbalanceThreshold", deny_unknown_fields)]
pub struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub(crate) target: Float,
    /// Deviation from target that triggers rebalancing.
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub(crate) deviation: Float,
}

/// Error returned when [`ImbalanceThreshold`] is constructed with
/// out-of-range values.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InvalidImbalanceThreshold {
    #[error(
        "target must be between 0.0 and 1.0 inclusive, \
         got {target:?}"
    )]
    TargetOutOfRange { target: Float },
    #[error("deviation must be >= 0, got {deviation:?}")]
    NegativeDeviation { deviation: Float },
}

impl ImbalanceThreshold {
    /// Creates a new threshold with validated parameters.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidImbalanceThreshold`] if `target` is not in
    /// `[0.0, 1.0]` or `deviation` is negative.
    pub fn new(target: Float, deviation: Float) -> Result<Self, InvalidImbalanceThreshold> {
        let zero =
            Float::zero().map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?;

        if target
            .lt(zero)
            .map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?
            || target
                .gt(*EXACT_ONE)
                .map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?
        {
            return Err(InvalidImbalanceThreshold::TargetOutOfRange { target });
        }

        if deviation
            .lt(zero)
            .map_err(|_| InvalidImbalanceThreshold::NegativeDeviation { deviation })?
        {
            return Err(InvalidImbalanceThreshold::NegativeDeviation { deviation });
        }

        Ok(Self { target, deviation })
    }
}

/// Private helper for serde deserialization with validation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawImbalanceThreshold {
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    target: Float,
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    deviation: Float,
}

impl TryFrom<RawImbalanceThreshold> for ImbalanceThreshold {
    type Error = InvalidImbalanceThreshold;

    fn try_from(raw: RawImbalanceThreshold) -> Result<Self, Self::Error> {
        Self::new(raw.target, raw.deviation)
    }
}

/// Discriminant for the two venues tracked by an [`Inventory`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl Operator {
    /// Returns the opposite operator: Add becomes Remove, Remove becomes Add.
    ///
    /// Used when a fill event affects two asset types in opposite directions
    /// (e.g., buying equity removes USDC, selling equity adds USDC).
    pub(crate) fn inverse(self) -> Self {
        match self {
            Self::Add => Self::Remove,
            Self::Remove => Self::Add,
        }
    }
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
    T: Add<Output = Result<T, FloatError>>
        + Sub<Output = Result<T, FloatError>>
        + Copy
        + HasZero
        + std::fmt::Debug,
{
    fn has_inflight(&self) -> Result<bool, FloatError> {
        let onchain_inflight = self
            .onchain
            .as_ref()
            .map(|v| v.has_inflight())
            .transpose()?
            .unwrap_or(false);

        let offchain_inflight = self
            .offchain
            .as_ref()
            .map(|v| v.has_inflight())
            .transpose()?
            .unwrap_or(false);

        Ok(onchain_inflight || offchain_inflight)
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
    T: Add<Output = Result<T, FloatError>>
        + Sub<Output = Result<T, FloatError>>
        + std::ops::Mul<Float, Output = Result<T, FloatError>>
        + Copy
        + HasZero
        + Into<Float>
        + std::fmt::Debug,
{
    /// Returns the ratio of onchain to total inventory.
    /// Returns `Ok(None)` if either venue is uninitialized or total is zero.
    fn ratio(&self) -> Result<Option<Float>, FloatError> {
        let Some(onchain_ref) = self.onchain.as_ref() else {
            return Ok(None);
        };
        let Some(offchain_ref) = self.offchain.as_ref() else {
            return Ok(None);
        };

        let onchain: Float = onchain_ref.total()?.into();
        let offchain: Float = offchain_ref.total()?.into();
        let total = (onchain + offchain)?;

        if total.is_zero()? {
            return Ok(None);
        }

        Ok(Some((onchain / total)?))
    }

    /// Detects imbalance based on threshold configuration.
    /// Returns `Ok(None)` if either venue is uninitialized, balanced, has inflight
    /// operations, or total is zero.
    fn detect_imbalance(
        &self,
        threshold: &ImbalanceThreshold,
    ) -> Result<Option<Imbalance<T>>, FloatError> {
        // Require both venues to be initialized before detecting imbalance.
        // This prevents triggering rebalancing when only one venue has been polled.
        let Some(onchain_venue) = self.onchain.as_ref() else {
            return Ok(None);
        };
        let Some(offchain_venue) = self.offchain.as_ref() else {
            return Ok(None);
        };

        if onchain_venue.has_inflight()? || offchain_venue.has_inflight()? {
            return Ok(None);
        }

        let Some(ratio) = self.ratio()? else {
            return Ok(None);
        };

        let lower = (threshold.target - threshold.deviation)?;
        let upper = (threshold.target + threshold.deviation)?;

        if ratio.lt(lower)? {
            let onchain = onchain_venue.total()?;
            let offchain = offchain_venue.total()?;
            let total = (onchain + offchain)?;
            let target_onchain = (total * threshold.target)?;
            let excess = (target_onchain - onchain)?;

            Ok(Some(Imbalance::TooMuchOffchain { excess }))
        } else if ratio.gt(upper)? {
            let onchain = onchain_venue.total()?;
            let offchain = offchain_venue.total()?;
            let total = (onchain + offchain)?;
            let target_onchain = (total * threshold.target)?;
            let excess = (onchain - target_onchain)?;

            Ok(Some(Imbalance::TooMuchOnchain { excess }))
        } else {
            Ok(None)
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
    ) -> Result<Option<Imbalance<T>>, FloatError> {
        if self.has_inflight()? {
            return Ok(None);
        }

        let Some(offchain_venue) = self.offchain.as_ref() else {
            return Ok(None);
        };

        let onchain_decimal: Float = normalized_onchain.into();
        let offchain: Float = offchain_venue.total()?.into();
        let total = (onchain_decimal + offchain)?;

        if total.is_zero()? {
            return Ok(None);
        }

        let ratio = (onchain_decimal / total)?;
        let lower = (threshold.target - threshold.deviation)?;
        let upper = (threshold.target + threshold.deviation)?;

        if ratio.lt(lower)? {
            let offchain_val = offchain_venue.total()?;
            let total_val = (normalized_onchain + offchain_val)?;
            let target = (total_val * threshold.target)?;
            let excess = (target - normalized_onchain)?;

            Ok(Some(Imbalance::TooMuchOffchain { excess }))
        } else if ratio.gt(upper)? {
            let offchain_val = offchain_venue.total()?;
            let total_val = (normalized_onchain + offchain_val)?;
            let target = (total_val * threshold.target)?;
            let excess = (normalized_onchain - target)?;

            Ok(Some(Imbalance::TooMuchOnchain { excess }))
        } else {
            Ok(None)
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
    T: Add<Output = Result<T, FloatError>>
        + Sub<Output = Result<T, FloatError>>
        + Copy
        + HasZero
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

    pub(crate) fn last_rebalancing(&self) -> Option<DateTime<Utc>> {
        self.last_rebalancing
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

    /// Replace the inflight balance at a venue with a polled value
    /// from an external system (Alpaca's tokenization API).
    ///
    /// Unlike `transfer(TransferOp::Start)` which moves from available to
    /// inflight, this directly sets inflight without touching available.
    /// The available balance is already correct from a separate snapshot.
    pub(crate) fn set_inflight(
        venue: Venue,
        amount: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            let existing = inventory.get_venue(venue);

            // Don't initialize a venue that doesn't exist yet when the
            // inflight amount is zero — that would create a spurious
            // Some(0, 0) balance for an uninitialized venue.
            if existing.is_none() && amount.is_zero()? {
                return Ok(inventory);
            }

            let balance = existing.unwrap_or_default().set_inflight(amount);

            Ok(inventory.set_venue(venue, Some(balance)))
        })
    }

    /// Apply a fetched venue snapshot.
    ///
    /// Skips if ANY venue has inflight operations, because we cannot
    /// distinguish "transfer completed but not confirmed" from
    /// "unrelated inventory change".
    ///
    /// Also skips if the snapshot predates the last rebalancing operation,
    /// because a stale snapshot could overwrite post-rebalancing inventory
    /// and trigger duplicate operations.
    pub(crate) fn on_snapshot(
        venue: Venue,
        snapshot_balance: T,
        fetched_at: DateTime<Utc>,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            if inventory.has_inflight()? {
                return Ok(inventory);
            }

            if let Some(last_rebalancing) = inventory.last_rebalancing
                && fetched_at < last_rebalancing
            {
                debug!(
                    ?fetched_at,
                    ?last_rebalancing,
                    "Rejecting stale snapshot that predates last rebalancing"
                );
                return Ok(inventory);
            }

            let balance = inventory
                .get_venue(venue)
                .unwrap_or_default()
                .apply_snapshot(snapshot_balance)?;

            Ok(inventory.set_venue(venue, Some(balance)))
        })
    }

    /// Force-apply a venue snapshot, clearing inflight and ignoring
    /// the normal inflight guard.
    ///
    /// Used for recovery when reactor state is corrupted. The
    /// snapshot represents actual venue reality, so we trust it
    /// unconditionally and discard any tracked inflight.
    ///
    /// Takes the triggering error as a witness to prevent blind
    /// usage — callers must have an error in hand.
    pub(crate) fn force_on_snapshot<E: std::fmt::Debug + Send + 'static>(
        venue: Venue,
        snapshot_balance: T,
        recovering_from: E,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            let balance = inventory
                .get_venue(venue)
                .unwrap_or_default()
                .force_apply_snapshot(snapshot_balance, &recovering_from);

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
    /// Symbols with sticky inflight that should not be zeroed by
    /// `apply_inflight_snapshot`. Set on terminal failure states where
    /// tokens are physically in transit and no snapshot source tracks them.
    #[serde(default)]
    sticky_inflight: HashSet<(Symbol, Venue)>,
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
    ) -> Result<Option<Imbalance<FractionalShares>>, EquityImbalanceError> {
        let inventory = self
            .equities
            .get(symbol)
            .ok_or_else(|| EquityImbalanceError::SymbolNotTracked(symbol.clone()))?;

        let Some(onchain_venue) = inventory.onchain.as_ref() else {
            return Ok(None);
        };

        let onchain_wrapped = onchain_venue.total()?;
        let onchain_equivalent = vault_ratio.to_underlying_fractional(onchain_wrapped)?;

        Ok(inventory.detect_imbalance_normalized(threshold, onchain_equivalent)?)
    }

    /// Checks USDC inventory for imbalance against the threshold.
    /// Returns the imbalance if one exists.
    pub(crate) fn check_usdc_imbalance(
        &self,
        threshold: &ImbalanceThreshold,
    ) -> Result<Option<Imbalance<Usdc>>, FloatError> {
        self.usdc.detect_imbalance(threshold)
    }
}

impl Default for InventoryView {
    fn default() -> Self {
        Self {
            usdc: Inventory::default(),
            equities: HashMap::new(),
            last_updated: Utc::now(),
            sticky_inflight: HashSet::new(),
        }
    }
}

impl InventoryView {
    /// Registers a symbol with specified available balances (zero inflight).
    #[cfg(test)]
    pub(crate) fn with_equity(
        mut self,
        symbol: Symbol,
        onchain_available: FractionalShares,
        offchain_available: FractionalShares,
    ) -> Self {
        self.equities.insert(
            symbol,
            Inventory {
                onchain: Some(VenueBalance::new(onchain_available, FractionalShares::ZERO)),
                offchain: Some(VenueBalance::new(
                    offchain_available,
                    FractionalShares::ZERO,
                )),
                last_rebalancing: None,
            },
        );
        self
    }

    /// Returns the equity available balance at the given venue for a symbol.
    #[cfg(test)]
    pub(crate) fn equity_available(
        &self,
        symbol: &Symbol,
        venue: Venue,
    ) -> Option<FractionalShares> {
        let inventory = self.equities.get(symbol)?;
        inventory.get_venue(venue).map(VenueBalance::available)
    }

    /// Returns the equity inflight balance at the given venue for a symbol.
    #[cfg(test)]
    pub(crate) fn equity_inflight(
        &self,
        symbol: &Symbol,
        venue: Venue,
    ) -> Option<FractionalShares> {
        let inventory = self.equities.get(symbol)?;
        inventory.get_venue(venue).map(VenueBalance::inflight)
    }

    /// Returns the USDC available balance at the given venue.
    #[cfg(test)]
    pub(crate) fn usdc_available(&self, venue: Venue) -> Option<Usdc> {
        match venue {
            Venue::MarketMaking => self.usdc.onchain.map(VenueBalance::available),
            Venue::Hedging => self.usdc.offchain.map(VenueBalance::available),
        }
    }

    /// Sets USDC inventory with specified available balances (zero inflight).
    #[cfg(test)]
    pub(crate) fn with_usdc(self, onchain_available: Usdc, offchain_available: Usdc) -> Self {
        Self {
            usdc: Inventory {
                onchain: Some(VenueBalance::new(onchain_available, Usdc::ZERO)),
                offchain: Some(VenueBalance::new(offchain_available, Usdc::ZERO)),
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
            sticky_inflight: self.sticky_inflight,
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
            sticky_inflight: self.sticky_inflight,
        })
    }

    pub(crate) fn mark_sticky_inflight(&mut self, symbol: Symbol, venue: Venue) {
        self.sticky_inflight.insert((symbol, venue));
    }

    pub(crate) fn clear_sticky_inflight(&mut self, symbol: &Symbol, venue: Venue) {
        self.sticky_inflight.remove(&(symbol.clone(), venue));
    }

    /// Returns the set of symbols that currently have inflight balances
    /// at any venue.
    #[cfg(test)]
    pub(crate) fn symbols_with_inflight(&self) -> HashSet<Symbol> {
        self.equities
            .iter()
            .filter_map(|(symbol, inventory)| {
                inventory
                    .has_inflight()
                    .ok()
                    .filter(|has| *has)
                    .map(|_| symbol.clone())
            })
            .collect()
    }

    /// Whether a symbol's inflight snapshot should be skipped at a venue.
    ///
    /// Returns `true` if the symbol is absent from the pending map AND is
    /// marked sticky at that venue, meaning the inflight value should be
    /// preserved rather than zeroed.
    fn is_sticky_at(
        &self,
        symbol: &Symbol,
        venue: Venue,
        pending: &BTreeMap<Symbol, FractionalShares>,
    ) -> bool {
        let absent = !pending.contains_key(symbol);
        absent && self.sticky_inflight.contains(&(symbol.clone(), venue))
    }

    /// Whether a symbol's inflight snapshot predates its last rebalancing.
    fn is_stale_for_symbol(&self, symbol: &Symbol, fetched_at: DateTime<Utc>) -> bool {
        self.equities
            .get(symbol)
            .and_then(Inventory::last_rebalancing)
            .is_some_and(|last_rebalancing| fetched_at < last_rebalancing)
    }

    /// Apply an inflight equity snapshot with full replacement semantics.
    ///
    /// Sets inflight for symbols present in the maps and clears inflight to
    /// zero for any tracked symbol absent from the maps. This prevents stale
    /// inflight values from persisting when pending requests complete.
    ///
    /// Skips symbols whose `last_rebalancing` is more recent than `fetched_at`,
    /// because a stale poll could otherwise re-introduce inflight that was
    /// already cleared by a completed transfer.
    ///
    /// Preserves inflight for sticky entries (terminal failure states where
    /// tokens are in transit but no snapshot source tracks them).
    ///
    /// Mints are inflight at Hedging (shares leaving offchain broker toward
    /// onchain). Redemptions are inflight at MarketMaking (shares leaving
    /// onchain toward offchain broker).
    pub(crate) fn apply_inflight_snapshot(
        self,
        mints: &BTreeMap<Symbol, FractionalShares>,
        redemptions: &BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let tracked_symbols: HashSet<Symbol> = self.equities.keys().cloned().collect();
        let mut view = self;

        for symbol in &tracked_symbols {
            if view.is_stale_for_symbol(symbol, fetched_at) {
                debug!(
                    %symbol,
                    ?fetched_at,
                    "Skipping inflight snapshot for symbol: \
                     fetched before last rebalancing"
                );
                continue;
            }

            if !view.is_sticky_at(symbol, Venue::Hedging, mints) {
                let mint_amount = mints.get(symbol).copied().unwrap_or(FractionalShares::ZERO);
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::Hedging, mint_amount),
                    now,
                )?;
            }

            if !view.is_sticky_at(symbol, Venue::MarketMaking, redemptions) {
                let redemption_amount = redemptions
                    .get(symbol)
                    .copied()
                    .unwrap_or(FractionalShares::ZERO);
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::MarketMaking, redemption_amount),
                    now,
                )?;
            }
        }

        // Also set inflight for symbols in the maps that aren't tracked yet
        for (symbol, quantity) in mints {
            if !tracked_symbols.contains(symbol) {
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::Hedging, *quantity),
                    now,
                )?;
            }
        }

        for (symbol, quantity) in redemptions {
            if !tracked_symbols.contains(symbol) {
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::MarketMaking, *quantity),
                    now,
                )?;
            }
        }

        Ok(view)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use rain_math_float::Float;
    use std::collections::HashMap;

    use super::*;
    use crate::threshold::Usdc;
    use crate::wrapper::RATIO_ONE;

    fn shares(amount: i64) -> FractionalShares {
        FractionalShares::new(float!(&amount.to_string()))
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
            target: Float::parse(target.to_string()).unwrap(),
            deviation: Float::parse(deviation.to_string()).unwrap(),
        }
    }

    #[test]
    fn ratio_returns_none_when_total_is_zero() {
        let inventory = make_inventory(0, 0, 0, 0);
        assert!(inventory.ratio().unwrap().is_none());
    }

    #[test]
    fn ratio_returns_half_for_equal_split() {
        let inventory = make_inventory(50, 0, 50, 0);
        assert!(
            inventory
                .ratio()
                .unwrap()
                .unwrap()
                .eq(float!("0.5"))
                .unwrap()
        );
    }

    #[test]
    fn ratio_returns_one_when_all_onchain() {
        let inventory = make_inventory(100, 0, 0, 0);
        assert!(inventory.ratio().unwrap().unwrap().eq(float!("1")).unwrap());
    }

    #[test]
    fn ratio_returns_zero_when_all_offchain() {
        let inventory = make_inventory(0, 0, 100, 0);
        assert!(
            inventory
                .ratio()
                .unwrap()
                .unwrap()
                .eq(Float::zero().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn ratio_includes_inflight_in_total() {
        let inventory = make_inventory(25, 25, 25, 25);
        assert!(
            inventory
                .ratio()
                .unwrap()
                .unwrap()
                .eq(float!("0.5"))
                .unwrap()
        );
    }

    #[test]
    fn ratio_returns_none_when_onchain_uninitialized() {
        let inventory = Inventory {
            onchain: None,
            offchain: Some(venue(100, 0)),
            last_rebalancing: None,
        };
        assert!(inventory.ratio().unwrap().is_none());
    }

    #[test]
    fn ratio_returns_none_when_offchain_uninitialized() {
        let inventory = Inventory {
            onchain: Some(venue(100, 0)),
            offchain: None,
            last_rebalancing: None,
        };
        assert!(inventory.ratio().unwrap().is_none());
    }

    #[test]
    fn has_inflight_false_when_no_inflight() {
        let inventory = make_inventory(50, 0, 50, 0);
        assert!(!inventory.has_inflight().unwrap());
    }

    #[test]
    fn has_inflight_true_when_onchain_inflight() {
        let inventory = make_inventory(50, 10, 50, 0);
        assert!(inventory.has_inflight().unwrap());
    }

    #[test]
    fn has_inflight_true_when_offchain_inflight() {
        let inventory = make_inventory(50, 0, 50, 10);
        assert!(inventory.has_inflight().unwrap());
    }

    #[test]
    fn has_inflight_true_when_both_inflight() {
        let inventory = make_inventory(50, 10, 50, 10);
        assert!(inventory.has_inflight().unwrap());
    }

    #[test]
    fn detect_imbalance_returns_none_when_balanced() {
        let inventory = make_inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_none_when_has_inflight() {
        let inventory = make_inventory(80, 10, 20, 0);
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_none_when_total_is_zero() {
        let inventory = make_inventory(0, 0, 0, 0);
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_too_much_onchain() {
        // 80 onchain, 20 offchain = 80% ratio, threshold is 50% +- 20%
        let inventory = make_inventory(80, 0, 20, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inventory.detect_imbalance(&thresh).unwrap().unwrap();

        // Target is 50 onchain, current is 80, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOnchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_returns_too_much_offchain() {
        // 20 onchain, 80 offchain = 20% ratio, threshold is 50% +- 20%
        let inventory = make_inventory(20, 0, 80, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inventory.detect_imbalance(&thresh).unwrap().unwrap();

        // Target is 50 onchain, current is 20, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOffchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_at_upper_boundary_is_balanced() {
        // 70% ratio exactly at upper threshold (50% +- 20%)
        let inventory = make_inventory(70, 0, 30, 0);
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_at_lower_boundary_is_balanced() {
        // 30% ratio exactly at lower threshold (50% +- 20%)
        let inventory = make_inventory(30, 0, 70, 0);
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_none_when_onchain_not_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: None,
            offchain: Some(venue(50, 0)),
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_none_when_offchain_not_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: Some(venue(50, 0)),
            offchain: None,
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    #[test]
    fn detect_imbalance_returns_none_when_neither_venue_initialized() {
        let inventory = Inventory::<FractionalShares> {
            onchain: None,
            offchain: None,
            last_rebalancing: None,
        };
        let thresh = threshold("0.5", "0.2");

        assert_eq!(inventory.detect_imbalance(&thresh).unwrap(), None);
    }

    fn usdc_venue(available: i64, inflight: i64) -> VenueBalance<Usdc> {
        VenueBalance::new(
            Usdc(float!(&available.to_string())),
            Usdc(float!(&inflight.to_string())),
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
            sticky_inflight: HashSet::new(),
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
            sticky_inflight: HashSet::new(),
        }
    }

    #[test]
    fn usdc_inflight_blocks_imbalance_detection() {
        let inventory = usdc_make_inventory(800, 0, 200, 0);
        let thresh = threshold("0.5", "0.2");
        assert!(inventory.detect_imbalance(&thresh).unwrap().is_some());

        let inventory_with_inflight = usdc_make_inventory(700, 100, 200, 0);
        assert_eq!(
            inventory_with_inflight.detect_imbalance(&thresh).unwrap(),
            None
        );
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_balanced() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(50, 0, 50, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio)
                .unwrap()
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

        assert!(matches!(
            imbalance,
            Ok(Some(Imbalance::TooMuchOnchain { .. }))
        ));
    }

    #[test]
    fn check_equity_imbalance_detects_too_much_offchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(20, 0, 80, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio);

        assert!(matches!(
            imbalance,
            Ok(Some(Imbalance::TooMuchOffchain { .. }))
        ));
    }

    #[test]
    fn check_equity_imbalance_errors_for_unknown_symbol() {
        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();
        let view = make_view(vec![(aapl, make_inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        let error = view
            .check_equity_imbalance(&msft, &thresh, &ratio)
            .unwrap_err();
        assert!(matches!(error, EquityImbalanceError::SymbolNotTracked(symbol) if symbol == msft));
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_inflight() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), make_inventory(60, 20, 20, 0))]);
        let thresh = threshold("0.5", "0.2");
        let ratio = one_to_one_ratio();

        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio)
                .unwrap()
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

        assert!(matches!(
            imbalance,
            Ok(Some(Imbalance::TooMuchOnchain { .. }))
        ));
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
                .unwrap()
                .is_none()
        );

        // 1.05 ratio - still balanced (small appreciation doesn't change outcome)
        let ratio_1_05 =
            UnderlyingPerWrapped::new(U256::from(1_050_000_000_000_000_000u64)).unwrap();
        assert!(
            view.check_equity_imbalance(&aapl, &thresh, &ratio_1_05)
                .unwrap()
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
                .unwrap()
                .is_none()
        );

        // 1.5 ratio - triggers imbalance (73.6% exceeds 70% upper bound)
        let ratio_1_5 =
            UnderlyingPerWrapped::new(U256::from(1_500_000_000_000_000_000u64)).unwrap();
        let imbalance = view.check_equity_imbalance(&aapl, &thresh, &ratio_1_5);
        assert!(
            matches!(imbalance, Ok(Some(Imbalance::TooMuchOnchain { .. }))),
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

        assert!(result.unwrap().is_none());
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

        assert!(matches!(result, Ok(Some(Imbalance::TooMuchOnchain { .. }))));
    }

    #[test]
    fn detect_imbalance_normalized_returns_none_when_inflight() {
        let inventory = make_inventory(50, 10, 50, 0);
        let thresh = threshold("0.5", "0.2");

        let normalized = shares(120);
        let result = inventory.detect_imbalance_normalized(&thresh, normalized);

        // Even with high normalized value, inflight blocks detection
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_balanced() {
        let view = make_usdc_view(500, 0, 500, 0);

        assert_eq!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap(),
            None
        );
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_onchain() {
        let view = make_usdc_view(900, 0, 100, 0);

        let imbalance = view
            .check_usdc_imbalance(&threshold("0.5", "0.3"))
            .unwrap()
            .unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOnchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_offchain() {
        let view = make_usdc_view(100, 0, 900, 0);

        let imbalance = view
            .check_usdc_imbalance(&threshold("0.5", "0.3"))
            .unwrap()
            .unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOffchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_inflight() {
        let view = make_usdc_view(700, 200, 100, 0);

        assert_eq!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap(),
            None
        );
    }

    #[test]
    fn on_snapshot_rejects_stale_snapshot_predating_last_rebalancing() {
        let last_rebalancing = Utc::now();
        let stale_fetched_at = last_rebalancing - Duration::seconds(10);

        let inventory = Inventory {
            onchain: Some(venue(50, 0)),
            offchain: Some(venue(50, 0)),
            last_rebalancing: Some(last_rebalancing),
        };

        // Stale snapshot should be rejected — inventory unchanged
        let update_fn = Inventory::on_snapshot(Venue::MarketMaking, shares(999), stale_fetched_at);
        let result = update_fn(inventory.clone()).unwrap();
        assert_eq!(result, inventory);
    }

    #[test]
    fn on_snapshot_applies_when_fetched_at_equals_last_rebalancing() {
        let last_rebalancing = Utc::now();

        let inventory = Inventory {
            onchain: Some(venue(50, 0)),
            offchain: Some(venue(50, 0)),
            last_rebalancing: Some(last_rebalancing),
        };

        // fetched_at == last_rebalancing should apply
        let update_fn = Inventory::on_snapshot(Venue::MarketMaking, shares(999), last_rebalancing);
        let result = update_fn(inventory.clone()).unwrap();
        assert_ne!(result, inventory);

        let onchain = result.onchain.unwrap();
        assert_eq!(onchain.total().unwrap(), shares(999));
    }

    #[test]
    fn on_snapshot_applies_when_fetched_at_after_last_rebalancing() {
        let last_rebalancing = Utc::now();
        let fresh_fetched_at = last_rebalancing + Duration::seconds(10);

        let inventory = Inventory {
            onchain: Some(venue(50, 0)),
            offchain: Some(venue(50, 0)),
            last_rebalancing: Some(last_rebalancing),
        };

        let update_fn = Inventory::on_snapshot(Venue::MarketMaking, shares(999), fresh_fetched_at);
        let result = update_fn(inventory.clone()).unwrap();
        assert_ne!(result, inventory);

        let onchain = result.onchain.unwrap();
        assert_eq!(onchain.total().unwrap(), shares(999));
    }

    #[test]
    fn on_snapshot_applies_when_no_last_rebalancing() {
        let inventory = Inventory {
            onchain: Some(venue(50, 0)),
            offchain: Some(venue(50, 0)),
            last_rebalancing: None,
        };

        let update_fn = Inventory::on_snapshot(Venue::MarketMaking, shares(999), Utc::now());
        let result = update_fn(inventory.clone()).unwrap();
        assert_ne!(result, inventory);

        let onchain = result.onchain.unwrap();
        assert_eq!(onchain.total().unwrap(), shares(999));
    }

    #[test]
    fn inflight_snapshot_skipped_when_fetched_before_last_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let last_rebalancing = Utc::now();
        let stale_fetched_at = last_rebalancing - Duration::seconds(5);

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::with_last_rebalancing(last_rebalancing),
                Utc::now(),
            )
            .unwrap();

        // Apply an empty snapshot with stale fetched_at -- should NOT zero
        // the inflight because the snapshot predates last_rebalancing.
        let result = view
            .apply_inflight_snapshot(
                &BTreeMap::new(),
                &BTreeMap::new(),
                stale_fetched_at,
                Utc::now(),
            )
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            inventory.has_inflight().unwrap(),
            "Stale snapshot should not clear inflight"
        );
    }

    #[test]
    fn inflight_snapshot_applied_when_fetched_after_last_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let last_rebalancing = Utc::now();
        let fresh_fetched_at = last_rebalancing + Duration::seconds(5);

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::with_last_rebalancing(last_rebalancing),
                Utc::now(),
            )
            .unwrap();

        // Apply an empty snapshot with fresh fetched_at -- should zero inflight.
        let result = view
            .apply_inflight_snapshot(
                &BTreeMap::new(),
                &BTreeMap::new(),
                fresh_fetched_at,
                Utc::now(),
            )
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            !inventory.has_inflight().unwrap(),
            "Fresh snapshot should clear inflight"
        );
    }

    #[test]
    fn inflight_snapshot_applied_when_no_last_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        // No last_rebalancing -> always accept
        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            !inventory.has_inflight().unwrap(),
            "Should clear inflight when no last_rebalancing"
        );
    }

    #[test]
    fn non_sticky_symbol_absent_from_pending_is_zeroed() {
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            !inventory.has_inflight().unwrap(),
            "Non-sticky symbol absent from pending should have inflight zeroed"
        );
    }

    #[test]
    fn sticky_symbol_absent_from_pending_is_preserved() {
        let symbol = Symbol::new("AAPL").unwrap();

        let mut view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        view.mark_sticky_inflight(symbol.clone(), Venue::MarketMaking);

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            inventory.has_inflight().unwrap(),
            "Sticky symbol absent from pending should preserve inflight"
        );
    }

    #[test]
    fn apply_inflight_snapshot_does_not_initialize_missing_venue() {
        // When a symbol has only one venue initialized (e.g. offchain only),
        // applying an empty inflight snapshot should NOT conjure a
        // Some(0, 0) VenueBalance for the missing venue.
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView {
            equities: std::iter::once((
                symbol.clone(),
                Inventory {
                    onchain: None,
                    offchain: Some(VenueBalance::new(shares(100), FractionalShares::ZERO)),
                    last_rebalancing: None,
                },
            ))
            .collect(),
            ..InventoryView::default()
        };

        // Onchain (MarketMaking) is None before the snapshot
        let pre = view.equities.get(&symbol).unwrap();
        assert!(
            pre.onchain.is_none(),
            "Precondition: onchain should be None"
        );

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();

        // The bug: set_inflight calls unwrap_or_default() which creates
        // Some(available=0, inflight=0) for the missing venue.
        // After fix, the missing venue should remain None.
        assert!(
            inventory.onchain.is_none(),
            "Empty inflight snapshot should not initialize a missing venue to Some(0, 0)"
        );

        // Imbalance detection should still return None since one venue is uninitialized
        let thresh = threshold("0.5", "0.2");
        assert_eq!(
            inventory.detect_imbalance(&thresh).unwrap(),
            None,
            "Imbalance detection should return None when a venue is uninitialized"
        );
    }

    #[test]
    fn cleared_sticky_allows_subsequent_snapshot_to_zero() {
        let symbol = Symbol::new("AAPL").unwrap();

        let mut view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        view.mark_sticky_inflight(symbol.clone(), Venue::MarketMaking);
        view.clear_sticky_inflight(&symbol, Venue::MarketMaking);

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert!(
            !inventory.has_inflight().unwrap(),
            "After clearing sticky, snapshot should zero inflight"
        );
    }
}
