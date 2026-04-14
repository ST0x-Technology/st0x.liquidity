//! Inventory view for tracking cross-venue asset positions.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Add, Sub};
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};
use itertools::Itertools;
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::debug;

use st0x_dto::{SymbolInventory, UsdcInventory};
use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};
use st0x_finance::Usdc;
use st0x_float_macro::float;

use super::snapshot::InventorySnapshotEvent;
use super::venue_balance::{InventoryError, VenueBalance};
use crate::wrapper::{RatioError, UnderlyingPerWrapped};

static EXACT_ONE: LazyLock<Float> = LazyLock::new(|| float!(1));

/// Error type for inventory view operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryViewError {
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
    #[error(transparent)]
    Usdc(#[from] InventoryError<Usdc>),
    #[error("failed to convert USD balance cents {0} to USDC")]
    UsdBalanceConversion(i64),
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
/// Fields are private - mutation is only possible through the closure-returning
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

    /// Confirm an inflight transfer at the source venue and add the
    /// actual settled amount at the destination venue.
    ///
    /// Unlike [`Self::transfer`] with [`TransferOp::Complete`], this allows
    /// the amount leaving the source venue to differ from the amount credited
    /// at the destination venue. USDC rebalancing needs this because bridge
    /// fees and conversion slippage mean the settled amount can be smaller
    /// than the amount that originally left the source venue.
    pub(crate) fn settle_transfer(
        from: Venue,
        sent_amount: T,
        received_amount: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            let source = inventory
                .get_venue(from)
                .unwrap_or_default()
                .confirm_inflight(sent_amount)?;

            let dest = match inventory.get_venue(from.other()) {
                Some(balance) => balance.add_available(received_amount)?,
                None => VenueBalance::new(received_amount, T::ZERO),
            };

            Ok(inventory
                .set_venue(from, Some(source))
                .set_venue(from.other(), Some(dest)))
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

            let balance = existing.unwrap_or_default().set_inflight(amount)?;

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
    /// usage - callers must have an error in hand.
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

    /// Converts the in-memory inventory view to a DTO for dashboard serialization.
    pub(crate) fn to_dto(&self) -> st0x_dto::Inventory {
        let per_symbol = self
            .equities
            .iter()
            .map(|(symbol, inventory)| {
                let (onchain_available, onchain_inflight) = venue_balances(inventory.onchain);

                let (offchain_available, offchain_inflight) = venue_balances(inventory.offchain);

                SymbolInventory {
                    symbol: symbol.clone(),
                    onchain_available,
                    onchain_inflight,
                    offchain_available,
                    offchain_inflight,
                }
            })
            .sorted_by(|left, right| left.symbol.cmp(&right.symbol))
            .collect();

        let (usdc_onchain_available, usdc_onchain_inflight) = venue_balances(self.usdc.onchain);

        let (usdc_offchain_available, usdc_offchain_inflight) = venue_balances(self.usdc.offchain);

        st0x_dto::Inventory {
            per_symbol,
            usdc: UsdcInventory {
                onchain_available: usdc_onchain_available,
                onchain_inflight: usdc_onchain_inflight,
                offchain_available: usdc_offchain_available,
                offchain_inflight: usdc_offchain_inflight,
            },
        }
    }
}

fn venue_balances<T>(venue: Option<VenueBalance<T>>) -> (T, T)
where
    T: Add<Output = Result<T, FloatError>> + Sub<Output = Result<T, FloatError>> + Copy + HasZero,
{
    venue.map_or((T::ZERO, T::ZERO), |balance| {
        (balance.available(), balance.inflight())
    })
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

    /// Returns the USDC inflight balance at the given venue.
    #[cfg(test)]
    pub(crate) fn usdc_inflight(&self, venue: Venue) -> Option<Usdc> {
        match venue {
            Venue::MarketMaking => self.usdc.onchain.map(VenueBalance::inflight),
            Venue::Hedging => self.usdc.offchain.map(VenueBalance::inflight),
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

    /// Returns the set of symbols that currently have inflight balances
    /// at any venue.
    #[cfg(test)]
    pub(crate) fn symbols_with_inflight(&self) -> std::collections::HashSet<Symbol> {
        self.equities
            .iter()
            .filter(|(_, inventory)| {
                inventory
                    .has_inflight()
                    .expect("has_inflight should not fail on valid inventory")
            })
            .map(|(symbol, _)| symbol.clone())
            .collect()
    }

    /// Whether a symbol's inflight snapshot predates its last rebalancing.
    fn is_stale_for_symbol(&self, symbol: &Symbol, fetched_at: DateTime<Utc>) -> bool {
        self.equities
            .get(symbol)
            .and_then(Inventory::last_rebalancing)
            .is_some_and(|last_rebalancing| fetched_at < last_rebalancing)
    }

    /// Apply an inflight equity snapshot from the tokenization provider poll.
    ///
    /// Only sets inflight for symbols **present** in the maps. Symbols absent
    /// from the maps are left untouched — their inflight is managed exclusively
    /// by CQRS terminal events (`TransferOp::Complete`, `TransferOp::Cancel`).
    /// This prevents a race where the poll sees no pending request (e.g. after
    /// a fast rejection) and zeros inflight before the CQRS event arrives.
    ///
    /// Skips symbols whose `last_rebalancing` is more recent than `fetched_at`,
    /// because a stale poll could otherwise re-introduce inflight that was
    /// already cleared by a completed transfer.
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
        let mut view = self;

        for (symbol, &quantity) in mints {
            if view.is_stale_for_symbol(symbol, fetched_at) {
                debug!(
                    %symbol,
                    ?fetched_at,
                    "Skipping mint inflight snapshot: \
                     fetched before last rebalancing"
                );
                continue;
            }

            view = view.update_equity(
                symbol,
                Inventory::set_inflight(Venue::Hedging, quantity),
                now,
            )?;
        }

        for (symbol, &quantity) in redemptions {
            if view.is_stale_for_symbol(symbol, fetched_at) {
                debug!(
                    %symbol,
                    ?fetched_at,
                    "Skipping redemption inflight snapshot: \
                     fetched before last rebalancing"
                );
                continue;
            }

            view = view.update_equity(
                symbol,
                Inventory::set_inflight(Venue::MarketMaking, quantity),
                now,
            )?;
        }

        Ok(view)
    }

    /// Fold an [`InventorySnapshotEvent`] into this view under normal
    /// operation. Uses [`Inventory::on_snapshot`], which silently
    /// ignores stale snapshots (fetched before the last rebalancing)
    /// and snapshots that arrive while inflight balances are tracked --
    /// both return the unmodified view wrapped in `Ok`, not an error.
    /// A genuine [`InventoryViewError`] (arithmetic failure, cents
    /// conversion, etc.) is the only signal that callers should fall
    /// back to [`Self::force_apply_snapshot_event`] for recovery.
    ///
    /// Events that do not correspond to a tracked inventory slot
    /// (raw wallet reads used elsewhere for accounting) are no-ops.
    pub(crate) fn apply_snapshot_event(
        self,
        event: &InventorySnapshotEvent,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        use InventorySnapshotEvent::*;

        let fetched_at = event.timestamp();
        match event {
            OnchainEquity { balances, .. } => {
                balances
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(
                                Venue::MarketMaking,
                                *snapshot_balance,
                                fetched_at,
                            ),
                            now,
                        )
                    })
            }

            OnchainUsdc { usdc_balance, .. } => self.update_usdc(
                Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance, fetched_at),
                now,
            ),

            OffchainEquity { positions, .. } => {
                positions
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(Venue::Hedging, *snapshot_balance, fetched_at),
                            now,
                        )
                    })
            }

            OffchainUsd {
                usd_balance_cents, ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                self.update_usdc(
                    Inventory::on_snapshot(Venue::Hedging, usdc, fetched_at),
                    now,
                )
            }

            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. }
            | OffchainMarginSafeBuyingPower { .. } => Ok(self),

            InflightEquity {
                mints, redemptions, ..
            } => self.apply_inflight_snapshot(mints, redemptions, fetched_at, now),
        }
    }

    /// Recovery path for [`Self::apply_snapshot_event`] failures.
    /// Bypasses the inflight staleness guard via
    /// [`Inventory::force_on_snapshot`] so the view can catch up after
    /// a desync. `reason` is attached to the new balances so the
    /// force-write is auditable.
    ///
    /// `OffchainUsd` and `InflightEquity` intentionally reuse the
    /// non-forced conversion: silently inventing a Usdc from an invalid
    /// cents payload would corrupt financial state, so the original
    /// error resurfaces instead of being masked.
    pub(crate) fn force_apply_snapshot_event(
        self,
        event: &InventorySnapshotEvent,
        now: DateTime<Utc>,
        reason: Arc<InventoryViewError>,
    ) -> Result<Self, InventoryViewError> {
        use InventorySnapshotEvent::*;

        match event {
            OnchainEquity { balances, .. } => {
                balances
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::MarketMaking,
                                *snapshot_balance,
                                reason.clone(),
                            ),
                            now,
                        )
                    })
            }

            OnchainUsdc { usdc_balance, .. } => self.update_usdc(
                Inventory::force_on_snapshot(Venue::MarketMaking, *usdc_balance, reason),
                now,
            ),

            OffchainEquity { positions, .. } => {
                positions
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::Hedging,
                                *snapshot_balance,
                                reason.clone(),
                            ),
                            now,
                        )
                    })
            }

            OffchainUsd {
                usd_balance_cents, ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                self.update_usdc(
                    Inventory::force_on_snapshot(Venue::Hedging, usdc, reason),
                    now,
                )
            }

            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. }
            | OffchainMarginSafeBuyingPower { .. } => Ok(self),

            InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => self.apply_inflight_snapshot(mints, redemptions, *fetched_at, now),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use rain_math_float::Float;
    use std::collections::HashMap;

    use st0x_finance::Usdc;

    use super::*;
    use crate::wrapper::RATIO_ONE;
    use st0x_float_macro::float;

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
        assert!(inventory.ratio().unwrap().unwrap().eq(float!(0.5)).unwrap());
    }

    #[test]
    fn ratio_returns_one_when_all_onchain() {
        let inventory = make_inventory(100, 0, 0, 0);
        assert!(inventory.ratio().unwrap().unwrap().eq(float!(1)).unwrap());
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
        assert!(inventory.ratio().unwrap().unwrap().eq(float!(0.5)).unwrap());
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
            Usdc::new(float!(&available.to_string())),
            Usdc::new(float!(&inflight.to_string())),
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

        // Snapshot with the symbol present but stale fetched_at -- should NOT
        // update inflight because the snapshot predates last_rebalancing.
        let mut stale_redemptions = BTreeMap::new();
        stale_redemptions.insert(symbol.clone(), shares(5));

        let result = view
            .apply_inflight_snapshot(
                &BTreeMap::new(),
                &stale_redemptions,
                stale_fetched_at,
                Utc::now(),
            )
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert_eq!(
            inventory.onchain.unwrap().inflight(),
            shares(10),
            "Stale snapshot should preserve original inflight of 10, not update to 5"
        );
    }

    #[test]
    fn present_symbol_inflight_updated_when_fetched_after_last_rebalancing() {
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

        // Snapshot with symbol present and fresh fetched_at: should update inflight.
        let mut redemptions = BTreeMap::new();
        redemptions.insert(symbol.clone(), shares(5));

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &redemptions, fresh_fetched_at, Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert_eq!(
            inventory.onchain.unwrap().inflight(),
            shares(5),
            "Fresh snapshot should update MarketMaking inflight to the snapshot value"
        );
    }

    #[test]
    fn present_symbol_inflight_updated_when_no_last_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        // Snapshot with symbol present: should update inflight to the new value.
        let mut redemptions = BTreeMap::new();
        redemptions.insert(symbol.clone(), shares(5));

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &redemptions, Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert_eq!(
            inventory.onchain.unwrap().inflight(),
            shares(5),
            "Should update MarketMaking inflight to the snapshot value"
        );
    }

    #[test]
    fn absent_symbol_inflight_preserved_by_snapshot() {
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        // Empty snapshot (symbol absent from both maps) should not zero inflight.
        // Only CQRS terminal events (TransferOp::Complete/Cancel) zero inflight.
        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert_eq!(
            inventory.onchain.unwrap().inflight(),
            shares(10),
            "Absent symbol should preserve original MarketMaking inflight of 10"
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
    fn present_symbol_inflight_updated_by_snapshot() {
        let symbol = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        // When the symbol IS in the snapshot map, inflight is updated to
        // the snapshot's value.
        let mut redemptions = BTreeMap::new();
        redemptions.insert(symbol.clone(), shares(5));

        let result = view
            .apply_inflight_snapshot(&BTreeMap::new(), &redemptions, Utc::now(), Utc::now())
            .unwrap();

        let inventory = result.equities.get(&symbol).unwrap();
        assert_eq!(
            inventory.onchain.unwrap().inflight(),
            shares(5),
            "Present symbol should have MarketMaking inflight updated from 10 to 5"
        );
    }

    #[test]
    fn to_dto_converts_equities_and_usdc() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(100), shares(50))
            .with_usdc(Usdc::new(float!(10000)), Usdc::new(float!(5000)));

        let dto = view.to_dto();

        assert_eq!(dto.per_symbol.len(), 1);

        let aapl_dto = &dto.per_symbol[0];
        assert_eq!(aapl_dto.symbol, aapl);
        assert_eq!(
            aapl_dto.onchain_available,
            FractionalShares::new(float!(100))
        );
        assert_eq!(aapl_dto.onchain_inflight, FractionalShares::ZERO);
        assert_eq!(
            aapl_dto.offchain_available,
            FractionalShares::new(float!(50))
        );
        assert_eq!(aapl_dto.offchain_inflight, FractionalShares::ZERO);

        assert_eq!(dto.usdc.onchain_available, Usdc::new(float!(10000)));
        assert_eq!(dto.usdc.onchain_inflight, Usdc::ZERO);
        assert_eq!(dto.usdc.offchain_available, Usdc::new(float!(5000)));
        assert_eq!(dto.usdc.offchain_inflight, Usdc::ZERO);
    }

    #[test]
    fn to_dto_includes_inflight_amounts() {
        let tsla = Symbol::new("TSLA").unwrap();
        let view = InventoryView {
            equities: std::iter::once((tsla, make_inventory(80, 20, 40, 10))).collect(),
            usdc: usdc_make_inventory(5000, 1000, 3000, 500),
            last_updated: Utc::now(),
        };

        let dto = view.to_dto();

        let tsla_dto = &dto.per_symbol[0];
        assert_eq!(
            tsla_dto.onchain_available,
            FractionalShares::new(float!(80))
        );
        assert_eq!(tsla_dto.onchain_inflight, FractionalShares::new(float!(20)));
        assert_eq!(
            tsla_dto.offchain_available,
            FractionalShares::new(float!(40))
        );
        assert_eq!(
            tsla_dto.offchain_inflight,
            FractionalShares::new(float!(10))
        );

        assert_eq!(dto.usdc.onchain_available, Usdc::new(float!(5000)));
        assert_eq!(dto.usdc.onchain_inflight, Usdc::new(float!(1000)));
        assert_eq!(dto.usdc.offchain_available, Usdc::new(float!(3000)));
        assert_eq!(dto.usdc.offchain_inflight, Usdc::new(float!(500)));
    }

    #[test]
    fn to_dto_handles_uninitialized_venues() {
        let spy = Symbol::new("SPY").unwrap();
        let view = InventoryView {
            equities: std::iter::once((
                spy,
                Inventory {
                    onchain: Some(venue(75, 0)),
                    offchain: None,
                    last_rebalancing: None,
                },
            ))
            .collect(),
            usdc: Inventory::default(),
            last_updated: Utc::now(),
        };

        let dto = view.to_dto();

        let spy_dto = &dto.per_symbol[0];
        assert_eq!(spy_dto.onchain_available, FractionalShares::new(float!(75)));
        assert_eq!(spy_dto.offchain_available, FractionalShares::ZERO);
        assert_eq!(spy_dto.offchain_inflight, FractionalShares::ZERO);

        assert_eq!(dto.usdc.onchain_available, Usdc::ZERO);
        assert_eq!(dto.usdc.offchain_available, Usdc::ZERO);
    }
}
