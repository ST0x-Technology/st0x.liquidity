//! Inventory view for tracking cross-venue asset positions.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Add, Sub};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use st0x_config::ImbalanceThreshold;
use st0x_dto::{InFlightCash, InFlightEquity, SymbolInventory, UsdcInventory};
use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};
use st0x_finance::{Usd, Usdc};
use st0x_tokenization::IssuerRequestId;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped};

use super::snapshot::{InventoryObservationSource, InventorySnapshotEvent};
use super::venue_balance::{InventoryError, VenueBalance};
use crate::equity_redemption::RedemptionAggregateId;
use crate::usdc_rebalance::UsdcRebalanceId;

/// Error type for inventory view operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryViewError {
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
    #[error(transparent)]
    Usdc(#[from] InventoryError<Usdc>),
    #[error("float arithmetic error: {0}")]
    Float(#[from] FloatError),
    #[error("failed to convert USD balance cents {0} to USDC")]
    UsdBalanceConversion(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InventoryFreshnessRequirement {
    Equity,
    Usdc,
}

impl InventoryFreshnessRequirement {
    const fn sources(self) -> &'static [InventoryObservationSource] {
        match self {
            Self::Equity => &[
                InventoryObservationSource::InflightEquity,
                InventoryObservationSource::OnchainEquity,
                InventoryObservationSource::OffchainInventory,
            ],
            Self::Usdc => &[
                InventoryObservationSource::OnchainUsdc,
                InventoryObservationSource::OffchainInventory,
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InventorySourceFreshnessError {
    #[error("inventory source {observation_source:?} has never been observed")]
    Missing {
        observation_source: InventoryObservationSource,
    },
    #[error(
        "inventory source {observation_source:?} was last observed at {observed_at}, older than the {max_age:?} freshness window at {checked_at}"
    )]
    Stale {
        observation_source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
        checked_at: DateTime<Utc>,
        max_age: std::time::Duration,
    },
    #[error(
        "inventory source {observation_source:?} was observed at future time {observed_at}, after freshness check time {checked_at}"
    )]
    Future {
        observation_source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
        checked_at: DateTime<Utc>,
    },
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
        + std::fmt::Display
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
        + std::fmt::Display
        + std::fmt::Debug,
{
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
        + std::fmt::Display
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

    /// Atomically replace a venue with a fresh available balance and an exact
    /// recovered source reservation.
    ///
    /// This is narrower than [`Self::force_on_snapshot`]: it is used only when
    /// durable transfer state proves the reservation amount, so recovery keeps
    /// that inflight amount while replacing potentially stale available state.
    pub(crate) fn restore_reservation(
        venue: Venue,
        available: T,
        inflight: T,
    ) -> Box<dyn FnOnce(Self) -> Result<Self, InventoryError<T>> + Send> {
        Box::new(move |inventory| {
            let balance = VenueBalance::new(available, T::ZERO).set_inflight(inflight)?;

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
                    target: "inventory",
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

/// Locations where USDC may sit in transit between venues.
///
/// These slots track wallet balances observed by polling rather than
/// venue inventory. The underlying imbalance math
/// ([`InventoryView::check_usdc_imbalance_with_gross_offchain`]) operates
/// strictly on venue totals so wallet readings can never compensate a real
/// venue imbalance. Wallet-residue-driven suppression is deferred to a
/// broader orphan-state detection mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum InFlightCashLocation {
    /// USDC parked on the Ethereum wallet between Alpaca withdrawal and
    /// CCTP burn (or between CCTP mint and Alpaca deposit, depending on
    /// direction).
    EthereumWallet,
    /// USDC parked on the Base wallet outside the Raindex vaults
    /// (between vault withdrawal and CCTP burn, or between CCTP mint
    /// and vault deposit).
    BaseWallet,
}

/// A wallet-read USDC observation paired with the time it was fetched.
///
/// `fetched_at` discriminates concurrent or out-of-order snapshots so the
/// view ignores readings older than the one it currently holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InFlightCashEntry {
    pub(crate) amount: Usdc,
    pub(crate) fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct InFlightEquityEntry {
    pub(crate) amount: FractionalShares,
    pub(crate) fetched_at: DateTime<Utc>,
}

/// Locations where equity tokens may sit in transit between venues.
///
/// Like [`InFlightCashLocation`], these slots are populated from
/// wallet-read snapshots and never enter the imbalance math. They
/// give visibility into capital that has left one venue but has not
/// yet landed on the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum InFlightEquityLocation {
    /// Unwrapped equity tokens parked on the Base wallet (issuer
    /// tokens not yet wrapped into vault shares, or unwrapped tokens
    /// awaiting redemption journal to Alpaca).
    BaseWalletUnwrapped,
    /// Wrapped equity tokens parked on the Base wallet (vault shares
    /// awaiting deposit into Raindex, or shares withdrawn from the
    /// vault awaiting unwrapping).
    BaseWalletWrapped,
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
    /// Margin-safe buying power in cents from the offchain broker.
    #[serde(default)]
    buying_power_cents: Option<i64>,
    /// Settled (withdrawable) cash in cents from the offchain broker.
    /// Excludes T+1 unsettled equity-sale proceeds; this is the amount
    /// actually movable to Raindex during rebalancing.
    #[serde(default)]
    withdrawable_cash_cents: Option<i64>,
    /// Gross offchain USD balance in cents (before cash reserve subtraction).
    #[serde(default)]
    offchain_gross_usd_cents: Option<i64>,
    /// USDC token balance held in the Alpaca account.
    #[serde(default)]
    alpaca_usdc: Option<Usdc>,
    /// USDC observed at intermediate locations between the two venues.
    /// Populated from wallet-read snapshots and exposed for visibility
    /// into in-flight transit. Does not feed the imbalance math.
    ///
    /// Each entry carries the snapshot's `fetched_at` so out-of-order
    /// wallet polls cannot overwrite a fresher reading with a stale one.
    #[serde(default)]
    inflight_cash: HashMap<InFlightCashLocation, InFlightCashEntry>,
    /// Aggregate ID of the in-flight USDC rebalance, if any.
    ///
    /// Populated when a transfer initiates and cleared on terminal events.
    /// Recovery uses this to load the stalled aggregate from its store.
    #[serde(default)]
    active_usdc_rebalance: Option<UsdcRebalanceId>,
    /// Aggregate IDs of in-flight equity mints, keyed by symbol.
    ///
    /// Populated when a non-terminal mint event is processed, cleared on
    /// terminal mint events.
    #[serde(default)]
    active_mints: HashMap<Symbol, IssuerRequestId>,
    /// Aggregate IDs of in-flight equity redemptions, keyed by symbol.
    ///
    /// Populated when a non-terminal redemption event is processed, cleared
    /// on terminal redemption events.
    #[serde(default)]
    active_redemptions: HashMap<Symbol, RedemptionAggregateId>,
    /// Equity tokens observed at intermediate wallet locations between
    /// the two venues, keyed by `(symbol, location)`. Populated from
    /// wallet-read snapshots; does not feed the imbalance math.
    ///
    /// Wallet readings replace any prior value at the same location only when
    /// the incoming `fetched_at` is at least as recent as the existing entry's.
    /// Polls running concurrently against different RPC nodes can land out of
    /// order, so dropping older snapshots prevents a stale reading from
    /// overwriting a fresher one.
    #[serde(default)]
    inflight_equity: HashMap<(Symbol, InFlightEquityLocation), InFlightEquityEntry>,
    /// Symbols that appeared in the most recent inflight mint poll.
    /// Used to detect when a symbol disappears from the poll (request
    /// completed or rejected) so its inflight can be zeroed.
    #[serde(default)]
    previous_inflight_mint_symbols: HashSet<Symbol>,
    /// Symbols that appeared in the most recent inflight redemption poll.
    #[serde(default)]
    previous_inflight_redemption_symbols: HashSet<Symbol>,
    /// Latest absolute equity snapshot timestamp by symbol for the onchain venue.
    #[serde(default)]
    onchain_equity_snapshot_watermarks: HashMap<Symbol, DateTime<Utc>>,
    /// Latest absolute equity snapshot timestamp by symbol for the offchain venue.
    #[serde(default)]
    offchain_equity_snapshot_watermarks: HashMap<Symbol, DateTime<Utc>>,
    #[serde(default)]
    source_observed_at: BTreeMap<InventoryObservationSource, DateTime<Utc>>,
}

impl InventoryView {
    pub(crate) fn require_fresh_sources(
        &self,
        requirement: InventoryFreshnessRequirement,
        checked_at: DateTime<Utc>,
        max_age: std::time::Duration,
    ) -> Result<(), InventorySourceFreshnessError> {
        for &observation_source in requirement.sources() {
            let Some(&observed_at) = self.source_observed_at.get(&observation_source) else {
                return Err(InventorySourceFreshnessError::Missing { observation_source });
            };

            if observed_at > checked_at {
                return Err(InventorySourceFreshnessError::Future {
                    observation_source,
                    observed_at,
                    checked_at,
                });
            }

            let age = (checked_at - observed_at)
                .to_std()
                .unwrap_or(std::time::Duration::MAX);
            if age > max_age {
                return Err(InventorySourceFreshnessError::Stale {
                    observation_source,
                    observed_at,
                    checked_at,
                    max_age,
                });
            }
        }

        Ok(())
    }

    pub(crate) fn reset_for_snapshot_recovery(self) -> Self {
        Self {
            source_observed_at: self.source_observed_at,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_rebalancing_sources_observed_at(
        mut self,
        observed_at: DateTime<Utc>,
    ) -> Self {
        for source in [
            InventoryObservationSource::InflightEquity,
            InventoryObservationSource::OnchainEquity,
            InventoryObservationSource::OnchainUsdc,
            InventoryObservationSource::OffchainInventory,
        ] {
            self = self.record_source_observation(source, observed_at, observed_at);
        }
        self
    }

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

    /// Checks USDC imbalance using gross offchain cash when available.
    ///
    /// `reserved` cash is subtracted before `OffchainUsd` is stored for
    /// dashboard and spending-cap purposes. Rebalancing allocation should
    /// still use gross venue balances so the reserve does not make the system
    /// look artificially onchain-heavy. When a reserve is configured but the
    /// gross offchain snapshot is missing, this returns `None` rather than
    /// falling back to the reserve-adjusted venue balance — using the net
    /// value would be the exact regression this PR exists to prevent.
    ///
    /// Wallet readings (`inflight_cash`) never enter the imbalance math: the
    /// design keeps this check venue-only so wallet observations cannot mask
    /// or compensate a real venue imbalance. Suppression based on wallet
    /// residue is tracked separately by a broader orphan-state detection
    /// mechanism.
    pub(crate) fn check_usdc_imbalance_with_gross_offchain(
        &self,
        threshold: &ImbalanceThreshold,
        reserved: Option<Usd>,
    ) -> Result<Option<Imbalance<Usdc>>, InventoryViewError> {
        let Some(onchain_venue) = self.usdc.onchain.as_ref() else {
            return Ok(None);
        };
        let Some(offchain_venue) = self.usdc.offchain.as_ref() else {
            return Ok(None);
        };

        if onchain_venue.has_inflight()? || offchain_venue.has_inflight()? {
            return Ok(None);
        }

        let onchain = onchain_venue.total()?;
        let offchain = match (self.offchain_gross_usd_cents, reserved) {
            (Some(cents), _) => {
                Usdc::from_cents(cents).ok_or(InventoryViewError::UsdBalanceConversion(cents))?
            }
            (None, None) => offchain_venue.total()?,
            (None, Some(_)) => {
                tracing::warn!(
                    target: "rebalance",
                    "USDC imbalance check skipped: gross offchain cash snapshot is missing while a reserve is configured; refusing to compute ratio against reserve-adjusted net offchain"
                );
                return Ok(None);
            }
        };
        let total = (onchain + offchain)?;
        let total_float: Float = total.into();

        if total_float.is_zero()? {
            return Ok(None);
        }

        let onchain_float: Float = onchain.into();
        let ratio = (onchain_float / total_float)?;
        let lower = (threshold.target - threshold.deviation)?;
        let upper = (threshold.target + threshold.deviation)?;

        if ratio.lt(lower)? {
            let target_onchain = (total * threshold.target)?;
            let excess = (target_onchain - onchain)?;

            Ok(Some(Imbalance::TooMuchOffchain { excess }))
        } else if ratio.gt(upper)? {
            let target_onchain = (total * threshold.target)?;
            let excess = (onchain - target_onchain)?;

            Ok(Some(Imbalance::TooMuchOnchain { excess }))
        } else {
            Ok(None)
        }
    }

    /// Maximum USDC that may leave Alpaca while preserving the configured
    /// offchain reserve. Returns `None` when the broker did not report
    /// withdrawable cash, because settled cash is the outbound capacity source
    /// of truth. Returns `Some(Usdc::ZERO)` when withdrawable cash is at or
    /// below the reserve — the caller is expected to surface this as a
    /// distinct skip reason rather than treating it as "below minimum
    /// withdrawal."
    pub(crate) fn alpaca_to_base_usdc_capacity(
        &self,
        reserved: Option<Usd>,
    ) -> Result<Option<Usdc>, InventoryViewError> {
        let Some(withdrawable_cents) = self.withdrawable_cash_cents else {
            return Ok(None);
        };
        let withdrawable = Usdc::from_cents(withdrawable_cents)
            .ok_or(InventoryViewError::UsdBalanceConversion(withdrawable_cents))?;
        let reserved = reserved.map_or(Usdc::ZERO, |amount| Usdc::new(amount.inner()));

        if reserved.gt(&withdrawable)? {
            return Ok(Some(Usdc::ZERO));
        }

        Ok(Some((withdrawable - reserved)?))
    }

    /// Converts the in-memory inventory view to a DTO for dashboard serialization.
    pub(crate) fn to_dto(&self) -> st0x_dto::Inventory {
        let per_symbol = self
            .equities
            .keys()
            .chain(self.inflight_equity.keys().map(|(symbol, _)| symbol))
            .unique()
            .sorted()
            .map(|symbol| {
                let inventory = self.equities.get(symbol);
                let (onchain_available, onchain_inflight) = inventory
                    .map_or((FractionalShares::ZERO, FractionalShares::ZERO), |item| {
                        venue_balances(item.onchain)
                    });

                let (offchain_available, offchain_inflight) = inventory
                    .map_or((FractionalShares::ZERO, FractionalShares::ZERO), |item| {
                        venue_balances(item.offchain)
                    });

                let inflight_equity = InFlightEquity {
                    base_wallet_unwrapped: self
                        .inflight_equity
                        .get(&(symbol.clone(), InFlightEquityLocation::BaseWalletUnwrapped))
                        .map_or(FractionalShares::ZERO, |entry| entry.amount),
                    base_wallet_wrapped: self
                        .inflight_equity
                        .get(&(symbol.clone(), InFlightEquityLocation::BaseWalletWrapped))
                        .map_or(FractionalShares::ZERO, |entry| entry.amount),
                };

                SymbolInventory {
                    symbol: symbol.clone(),
                    onchain_available,
                    onchain_inflight,
                    offchain_available,
                    offchain_inflight,
                    inflight_equity,
                }
            })
            .collect();

        let (usdc_onchain_available, usdc_onchain_inflight) = venue_balances(self.usdc.onchain);

        let (usdc_offchain_available, usdc_offchain_inflight) = venue_balances(self.usdc.offchain);

        let withdrawable_cash = self.withdrawable_cash_cents.and_then(Usdc::from_cents);

        let offchain_gross = self.offchain_gross_usd_cents.and_then(Usdc::from_cents);

        let inflight_cash = InFlightCash {
            ethereum_wallet: self
                .inflight_cash
                .get(&InFlightCashLocation::EthereumWallet)
                .map(|entry| entry.amount),
            base_wallet: self
                .inflight_cash
                .get(&InFlightCashLocation::BaseWallet)
                .map(|entry| entry.amount),
        };

        st0x_dto::Inventory {
            per_symbol,
            usdc: UsdcInventory {
                onchain_available: usdc_onchain_available,
                onchain_inflight: usdc_onchain_inflight,
                offchain_available: usdc_offchain_available,
                offchain_inflight: usdc_offchain_inflight,
                offchain_gross,
                withdrawable_cash,
                alpaca_usdc: self.alpaca_usdc,
                inflight_cash,
            },
        }
    }
}

fn venue_balances<T>(venue: Option<VenueBalance<T>>) -> (T, T)
where
    T: Add<Output = Result<T, FloatError>>
        + Sub<Output = Result<T, FloatError>>
        + Copy
        + HasZero
        + std::fmt::Display,
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
            buying_power_cents: None,
            withdrawable_cash_cents: None,
            offchain_gross_usd_cents: None,
            alpaca_usdc: None,
            inflight_cash: HashMap::new(),
            active_usdc_rebalance: None,
            active_mints: HashMap::new(),
            active_redemptions: HashMap::new(),
            inflight_equity: HashMap::new(),
            previous_inflight_mint_symbols: HashSet::new(),
            previous_inflight_redemption_symbols: HashSet::new(),
            onchain_equity_snapshot_watermarks: HashMap::new(),
            offchain_equity_snapshot_watermarks: HashMap::new(),
            source_observed_at: BTreeMap::new(),
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

    /// Sets USDC inventory with explicit available *and* inflight balances per
    /// venue. Unlike [`Self::with_usdc`], this can seed the resume-desync state
    /// where inflight is reserved but the matching available debit was lost
    /// (e.g. a snapshot reset `available` to broker/chain reality while
    /// persisted inflight survived a restart) -- a state no single event
    /// produces on its own.
    #[cfg(test)]
    pub(crate) fn with_usdc_inflight(
        self,
        onchain_available: Usdc,
        onchain_inflight: Usdc,
        offchain_available: Usdc,
        offchain_inflight: Usdc,
    ) -> Self {
        Self {
            usdc: Inventory {
                onchain: Some(VenueBalance::new(onchain_available, onchain_inflight)),
                offchain: Some(VenueBalance::new(offchain_available, offchain_inflight)),
                last_rebalancing: None,
            },
            ..self
        }
    }

    /// Sets the gross offchain cash balance recorded by the inventory poller.
    #[cfg(test)]
    pub(crate) fn with_offchain_gross_usd_cents(self, cents: i64) -> Self {
        Self {
            offchain_gross_usd_cents: Some(cents),
            ..self
        }
    }

    /// Sets the offchain withdrawable cash balance reported by the broker.
    #[cfg(test)]
    pub(crate) fn with_withdrawable_cash_cents(self, cents: i64) -> Self {
        Self {
            withdrawable_cash_cents: Some(cents),
            ..self
        }
    }

    /// Returns the in-flight USDC observed at the given intermediate location.
    #[cfg(test)]
    pub(crate) fn inflight_cash_at(&self, location: InFlightCashLocation) -> Option<Usdc> {
        self.inflight_cash.get(&location).map(|entry| entry.amount)
    }

    /// Returns the in-flight equity tokens observed at the given
    /// intermediate location for a symbol.
    pub(crate) fn inflight_equity_at(
        &self,
        symbol: &Symbol,
        location: InFlightEquityLocation,
    ) -> Option<FractionalShares> {
        self.inflight_equity
            .get(&(symbol.clone(), location))
            .map(|entry| entry.amount)
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
            buying_power_cents: self.buying_power_cents,
            withdrawable_cash_cents: self.withdrawable_cash_cents,
            offchain_gross_usd_cents: self.offchain_gross_usd_cents,
            alpaca_usdc: self.alpaca_usdc,
            inflight_cash: self.inflight_cash,
            active_usdc_rebalance: self.active_usdc_rebalance,
            active_mints: self.active_mints,
            active_redemptions: self.active_redemptions,
            inflight_equity: self.inflight_equity,
            previous_inflight_mint_symbols: self.previous_inflight_mint_symbols,
            previous_inflight_redemption_symbols: self.previous_inflight_redemption_symbols,
            onchain_equity_snapshot_watermarks: self.onchain_equity_snapshot_watermarks,
            offchain_equity_snapshot_watermarks: self.offchain_equity_snapshot_watermarks,
            source_observed_at: self.source_observed_at,
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
            buying_power_cents: self.buying_power_cents,
            withdrawable_cash_cents: self.withdrawable_cash_cents,
            offchain_gross_usd_cents: self.offchain_gross_usd_cents,
            alpaca_usdc: self.alpaca_usdc,
            inflight_cash: self.inflight_cash,
            active_usdc_rebalance: self.active_usdc_rebalance,
            active_mints: self.active_mints,
            active_redemptions: self.active_redemptions,
            inflight_equity: self.inflight_equity,
            previous_inflight_mint_symbols: self.previous_inflight_mint_symbols,
            previous_inflight_redemption_symbols: self.previous_inflight_redemption_symbols,
            onchain_equity_snapshot_watermarks: self.onchain_equity_snapshot_watermarks,
            offchain_equity_snapshot_watermarks: self.offchain_equity_snapshot_watermarks,
            source_observed_at: self.source_observed_at,
        })
    }

    pub(crate) fn record_equity_snapshot_watermarks<'a>(
        mut self,
        venue: Venue,
        symbols: impl IntoIterator<Item = &'a Symbol>,
        fetched_at: DateTime<Utc>,
    ) -> Self {
        let watermarks = match venue {
            Venue::MarketMaking => &mut self.onchain_equity_snapshot_watermarks,
            Venue::Hedging => &mut self.offchain_equity_snapshot_watermarks,
        };

        for symbol in symbols {
            let watermark = watermarks.entry(symbol.clone()).or_insert(fetched_at);
            if fetched_at > *watermark {
                *watermark = fetched_at;
            }
        }

        self
    }

    fn equity_snapshot_watermark(&self, symbol: &Symbol, venue: Venue) -> Option<DateTime<Utc>> {
        let watermarks = match venue {
            Venue::MarketMaking => &self.onchain_equity_snapshot_watermarks,
            Venue::Hedging => &self.offchain_equity_snapshot_watermarks,
        };

        watermarks.get(symbol).copied()
    }

    fn equity_snapshot_would_apply(
        &self,
        symbol: &Symbol,
        venue: Venue,
        fetched_at: DateTime<Utc>,
    ) -> Result<bool, InventoryViewError> {
        if self
            .equity_snapshot_watermark(symbol, venue)
            .is_some_and(|watermark| fetched_at <= watermark)
        {
            return Ok(false);
        }

        let Some(inventory) = self.equities.get(symbol) else {
            return Ok(true);
        };

        if inventory.has_inflight()? {
            return Ok(false);
        }

        if let Some(last_rebalancing) = inventory.last_rebalancing()
            && fetched_at < last_rebalancing
        {
            return Ok(false);
        }

        Ok(true)
    }

    pub(crate) fn apply_equity_snapshot<'a>(
        self,
        venue: Venue,
        balances: impl IntoIterator<Item = (&'a Symbol, &'a FractionalShares)>,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let snapshot: Vec<(Symbol, FractionalShares)> = balances
            .into_iter()
            .map(|(symbol, balance)| (symbol.clone(), *balance))
            .collect();

        let present: HashSet<&Symbol> = snapshot.iter().map(|(symbol, _)| symbol).collect();

        // A venue snapshot is the complete picture of that venue at `fetched_at`:
        // a brokerage omits zero-share positions and an onchain poll covers every
        // discovered vault. Any symbol still tracked at this venue but absent from
        // the snapshot has therefore gone to zero, so apply an explicit zero
        // instead of leaving a stale balance behind. The same staleness guards
        // (`equity_snapshot_would_apply`) protect these zeroes from clobbering
        // fresher data or inflight transfers.
        let absent_zeroes: Vec<(Symbol, FractionalShares)> = self
            .equities
            .iter()
            .filter(|(symbol, inventory)| {
                inventory.get_venue(venue).is_some() && !present.contains(symbol)
            })
            .map(|(symbol, _)| (symbol.clone(), FractionalShares::ZERO))
            .collect();

        let (view, applied_symbols) = snapshot.iter().chain(absent_zeroes.iter()).try_fold(
            (self, Vec::new()),
            |(view, mut applied_symbols), (symbol, snapshot_balance)| {
                let should_record_watermark =
                    view.equity_snapshot_would_apply(symbol, venue, fetched_at)?;
                if !should_record_watermark {
                    return Ok::<_, InventoryViewError>((view, applied_symbols));
                }

                let view = view.update_equity(
                    symbol,
                    Inventory::on_snapshot(venue, *snapshot_balance, fetched_at),
                    now,
                )?;

                applied_symbols.push(symbol.clone());

                Ok::<_, InventoryViewError>((view, applied_symbols))
            },
        )?;

        Ok(view.record_equity_snapshot_watermarks(venue, applied_symbols.iter(), fetched_at))
    }

    /// Record the latest wallet-read USDC balance at an intermediate location.
    ///
    /// Wallet readings replace any prior value at the same location only when
    /// the incoming `fetched_at` is at least as recent as the existing entry's.
    /// Polls running concurrently against different RPC nodes can land out of
    /// order, so dropping older snapshots prevents a stale reading from
    /// overwriting a fresher one.
    pub(crate) fn set_inflight_cash(
        mut self,
        location: InFlightCashLocation,
        amount: Usdc,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Self {
        if let Some(existing) = self.inflight_cash.get(&location)
            && existing.fetched_at > fetched_at
        {
            warn!(
                target: "inventory",
                ?location,
                existing_fetched_at = ?existing.fetched_at,
                incoming_fetched_at = ?fetched_at,
                "ignoring stale inflight_cash snapshot",
            );
            return self;
        }

        self.inflight_cash
            .insert(location, InFlightCashEntry { amount, fetched_at });
        self.last_updated = now;
        self
    }

    /// Record the latest wallet-read equity balances at an intermediate
    /// location.
    ///
    /// Wallet readings replace any prior value at the same location only when
    /// the incoming `fetched_at` is at least as recent as the existing entry's.
    /// Polls running concurrently against different RPC nodes can land out of
    /// order, so dropping older snapshots prevents a stale reading from
    /// overwriting a fresher one.
    ///
    /// If any existing entry at this location is fresher than `fetched_at`,
    /// the entire snapshot is rejected without modification.
    ///
    /// Otherwise, symbols absent from `balances` are removed from the view,
    /// matching what the wallet reports.
    pub(crate) fn set_inflight_equity_at_location(
        mut self,
        location: InFlightEquityLocation,
        balances: &BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Self {
        // Check if any existing entries at this location are fresher than the
        // incoming snapshot. If so, reject it completely.
        let is_stale = self
            .inflight_equity
            .iter()
            .filter(|((_, existing_location), _)| *existing_location == location)
            .any(|(_, entry)| entry.fetched_at > fetched_at);

        if is_stale {
            warn!(
                target: "inventory",
                ?location,
                incoming_fetched_at = ?fetched_at,
                "ignoring stale inflight_equity snapshot for location",
            );
            return self;
        }

        self.inflight_equity
            .retain(|(_, existing_location), _| *existing_location != location);

        for (symbol, amount) in balances {
            self.inflight_equity.insert(
                (symbol.clone(), location),
                InFlightEquityEntry {
                    amount: *amount,
                    fetched_at,
                },
            );
        }

        self.last_updated = now;
        self
    }

    pub(crate) fn clear_equity_inflight(
        self,
        symbol: &Symbol,
        venue: Venue,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let Some(inventory) = self.equities.get(symbol).cloned() else {
            return Ok(self);
        };

        let cleared = Inventory::set_inflight(venue, FractionalShares::ZERO)(inventory)?;
        let cleared = Inventory::with_last_rebalancing(now)(cleared)?;

        let mut equities = self.equities;
        equities.insert(symbol.clone(), cleared);

        Ok(Self {
            equities,
            last_updated: now,
            usdc: self.usdc,
            buying_power_cents: self.buying_power_cents,
            withdrawable_cash_cents: self.withdrawable_cash_cents,
            offchain_gross_usd_cents: self.offchain_gross_usd_cents,
            alpaca_usdc: self.alpaca_usdc,
            inflight_cash: self.inflight_cash,
            active_usdc_rebalance: self.active_usdc_rebalance,
            active_mints: self.active_mints,
            active_redemptions: self.active_redemptions,
            inflight_equity: self.inflight_equity,
            previous_inflight_mint_symbols: self.previous_inflight_mint_symbols,
            previous_inflight_redemption_symbols: self.previous_inflight_redemption_symbols,
            onchain_equity_snapshot_watermarks: self.onchain_equity_snapshot_watermarks,
            offchain_equity_snapshot_watermarks: self.offchain_equity_snapshot_watermarks,
            source_observed_at: self.source_observed_at,
        })
    }

    pub(crate) fn clear_usdc_inflight(
        self,
        venue: Venue,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let cleared = Inventory::set_inflight(venue, Usdc::ZERO)(self.usdc)?;
        let cleared = Inventory::with_last_rebalancing(now)(cleared)?;

        Ok(Self {
            usdc: cleared,
            last_updated: now,
            equities: self.equities,
            buying_power_cents: self.buying_power_cents,
            withdrawable_cash_cents: self.withdrawable_cash_cents,
            offchain_gross_usd_cents: self.offchain_gross_usd_cents,
            alpaca_usdc: self.alpaca_usdc,
            inflight_cash: self.inflight_cash,
            active_usdc_rebalance: self.active_usdc_rebalance,
            active_mints: self.active_mints,
            active_redemptions: self.active_redemptions,
            inflight_equity: self.inflight_equity,
            previous_inflight_mint_symbols: self.previous_inflight_mint_symbols,
            previous_inflight_redemption_symbols: self.previous_inflight_redemption_symbols,
            onchain_equity_snapshot_watermarks: self.onchain_equity_snapshot_watermarks,
            offchain_equity_snapshot_watermarks: self.offchain_equity_snapshot_watermarks,
            source_observed_at: self.source_observed_at,
        })
    }

    /// Returns the aggregate ID of the in-flight USDC rebalance, if any.
    pub(crate) fn active_usdc_rebalance(&self) -> Option<&UsdcRebalanceId> {
        self.active_usdc_rebalance.as_ref()
    }

    /// Returns the aggregate ID of the in-flight mint for `symbol`, if any.
    ///
    /// Consumed by the wrapped-equity recovery dispatcher to load the
    /// stalled aggregate via `Store::load`.
    pub(crate) fn active_mint(&self, symbol: &Symbol) -> Option<&IssuerRequestId> {
        self.active_mints.get(symbol)
    }

    /// Returns the aggregate ID of the in-flight redemption for `symbol`, if any.
    ///
    /// Consumed by the wrapped-equity recovery dispatcher to load the
    /// stalled aggregate via `Store::load`.
    pub(crate) fn active_redemption(&self, symbol: &Symbol) -> Option<&RedemptionAggregateId> {
        self.active_redemptions.get(symbol)
    }

    /// Records `id` as the in-flight USDC rebalance.
    pub(crate) fn set_active_usdc_rebalance(self, id: UsdcRebalanceId) -> Self {
        Self {
            active_usdc_rebalance: Some(id),
            ..self
        }
    }

    /// Clears the in-flight USDC rebalance ID (no-op if already empty).
    pub(crate) fn clear_active_usdc_rebalance(self) -> Self {
        Self {
            active_usdc_rebalance: None,
            ..self
        }
    }

    /// Records `id` as the in-flight mint for `symbol`.
    pub(crate) fn set_active_mint(self, symbol: Symbol, id: IssuerRequestId) -> Self {
        let mut active_mints = self.active_mints;
        active_mints.insert(symbol, id);
        Self {
            active_mints,
            ..self
        }
    }

    /// Clears the in-flight mint ID for `symbol` (no-op if absent).
    pub(crate) fn clear_active_mint(self, symbol: &Symbol) -> Self {
        let mut active_mints = self.active_mints;
        active_mints.remove(symbol);
        Self {
            active_mints,
            ..self
        }
    }

    /// Records `id` as the in-flight redemption for `symbol`.
    pub(crate) fn set_active_redemption(self, symbol: Symbol, id: RedemptionAggregateId) -> Self {
        let mut active_redemptions = self.active_redemptions;
        active_redemptions.insert(symbol, id);
        Self {
            active_redemptions,
            ..self
        }
    }

    /// Clears the in-flight redemption ID for `symbol` (no-op if absent).
    pub(crate) fn clear_active_redemption(self, symbol: &Symbol) -> Self {
        let mut active_redemptions = self.active_redemptions;
        active_redemptions.remove(symbol);
        Self {
            active_redemptions,
            ..self
        }
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
    /// Sets inflight for symbols **present** in the maps. For symbols
    /// that were in the **previous** poll but are now **absent**, zeros
    /// their inflight — the pending request completed, was rejected, or
    /// was cancelled. Symbols that were never in any poll (CQRS-only
    /// inflight via `TransferOp::Start`) are left untouched, preventing
    /// a race where the poll fires before Alpaca detects the transfer.
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
        let mut this = self;
        let prev_mints = std::mem::take(&mut this.previous_inflight_mint_symbols);
        let prev_redemptions = std::mem::take(&mut this.previous_inflight_redemption_symbols);
        let mut view = this;

        // Set inflight for symbols present in the poll.
        for (symbol, &quantity) in mints {
            if view.is_stale_for_symbol(symbol, fetched_at) {
                debug!(
                    target: "inventory",
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
                    target: "inventory",
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

        // Zero inflight for symbols that were in the previous poll but
        // disappeared. These are requests that completed or were rejected.
        for symbol in &prev_mints {
            if !mints.contains_key(symbol) && !view.is_stale_for_symbol(symbol, fetched_at) {
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::Hedging, FractionalShares::ZERO),
                    now,
                )?;
            }
        }

        for symbol in &prev_redemptions {
            if !redemptions.contains_key(symbol) && !view.is_stale_for_symbol(symbol, fetched_at) {
                view = view.update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::MarketMaking, FractionalShares::ZERO),
                    now,
                )?;
            }
        }

        // Track current poll symbols for next cycle's cleanup.
        view.previous_inflight_mint_symbols = mints.keys().cloned().collect();
        view.previous_inflight_redemption_symbols = redemptions.keys().cloned().collect();

        Ok(view)
    }

    /// Remove a symbol from the previous inflight mint marker set.
    ///
    /// Called when a new mint transfer starts (MintAccepted event) to
    /// prevent the next inflight poll from incorrectly zeroing the new
    /// inflight. Without this, a poll that fires before Alpaca reflects
    /// the new pending request would see the symbol in `prev_mints` but
    /// absent from the current poll, and zero it.
    pub(crate) fn clear_previous_inflight_mint_marker(mut self, symbol: &Symbol) -> Self {
        self.previous_inflight_mint_symbols.remove(symbol);
        self
    }

    /// Remove a symbol from the previous inflight redemption marker set.
    ///
    /// Called when a new redemption transfer starts (VaultWithdrawPending
    /// event) for the same reason as
    /// [`Self::clear_previous_inflight_mint_marker`].
    pub(crate) fn clear_previous_inflight_redemption_marker(mut self, symbol: &Symbol) -> Self {
        self.previous_inflight_redemption_symbols.remove(symbol);
        self
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
                self.apply_equity_snapshot(Venue::MarketMaking, balances.iter(), fetched_at, now)
            }

            OnchainUsdc { usdc_balance, .. } => self.update_usdc(
                Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance, fetched_at),
                now,
            ),

            OffchainEquity { positions, .. } => {
                self.apply_equity_snapshot(Venue::Hedging, positions.iter(), fetched_at, now)
            }

            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                let updated = self.update_usdc(
                    Inventory::on_snapshot(Venue::Hedging, usdc, fetched_at),
                    now,
                )?;
                Ok(Self {
                    offchain_gross_usd_cents: *gross_usd_cents,
                    ..updated
                })
            }

            OffchainCashBuyingPower {
                cash_buying_power_cents,
                ..
            } => {
                debug!(
                    target: "inventory",
                    ?cash_buying_power_cents,
                    "apply_snapshot_event: OffchainCashBuyingPower"
                );
                Ok(Self {
                    buying_power_cents: *cash_buying_power_cents,
                    ..self
                })
            }

            OffchainCashWithdrawable {
                cash_withdrawable_cents,
                ..
            } => {
                debug!(
                    target: "inventory",
                    ?cash_withdrawable_cents,
                    "apply_snapshot_event: OffchainCashWithdrawable"
                );
                Ok(Self {
                    withdrawable_cash_cents: *cash_withdrawable_cents,
                    ..self
                })
            }

            AlpacaUsdc { usdc_balance, .. } => Ok(Self {
                alpaca_usdc: Some(*usdc_balance),
                ..self
            }),

            EthereumUsdc {
                usdc_balance,
                fetched_at,
            } => Ok(self.set_inflight_cash(
                InFlightCashLocation::EthereumWallet,
                *usdc_balance,
                *fetched_at,
                now,
            )),

            BaseWalletUsdc {
                usdc_balance,
                fetched_at,
            } => Ok(self.set_inflight_cash(
                InFlightCashLocation::BaseWallet,
                *usdc_balance,
                *fetched_at,
                now,
            )),

            BaseWalletUnwrappedEquity {
                balances,
                fetched_at,
            } => Ok(self.set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                balances,
                *fetched_at,
                now,
            )),

            BaseWalletWrappedEquity {
                balances,
                fetched_at,
            } => Ok(self.set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletWrapped,
                balances,
                *fetched_at,
                now,
            )),

            InflightEquity {
                mints, redemptions, ..
            } => self.apply_inflight_snapshot(mints, redemptions, fetched_at, now),

            SourceObserved {
                source,
                observed_at,
            } => Ok(self.record_source_observation(*source, *observed_at, now)),
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
            OnchainEquity {
                balances,
                fetched_at,
            } => balances
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
                .map(|view| {
                    view.record_equity_snapshot_watermarks(
                        Venue::MarketMaking,
                        balances.keys(),
                        *fetched_at,
                    )
                }),

            OnchainUsdc { usdc_balance, .. } => self.update_usdc(
                Inventory::force_on_snapshot(Venue::MarketMaking, *usdc_balance, reason),
                now,
            ),

            OffchainEquity {
                positions,
                fetched_at,
            } => positions
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
                .map(|view| {
                    view.record_equity_snapshot_watermarks(
                        Venue::Hedging,
                        positions.keys(),
                        *fetched_at,
                    )
                }),

            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                let updated = self.update_usdc(
                    Inventory::force_on_snapshot(Venue::Hedging, usdc, reason),
                    now,
                )?;
                Ok(Self {
                    offchain_gross_usd_cents: *gross_usd_cents,
                    ..updated
                })
            }

            OffchainCashBuyingPower {
                cash_buying_power_cents,
                ..
            } => Ok(Self {
                buying_power_cents: *cash_buying_power_cents,
                ..self
            }),

            OffchainCashWithdrawable {
                cash_withdrawable_cents,
                ..
            } => Ok(Self {
                withdrawable_cash_cents: *cash_withdrawable_cents,
                ..self
            }),

            AlpacaUsdc { usdc_balance, .. } => Ok(Self {
                alpaca_usdc: Some(*usdc_balance),
                ..self
            }),

            EthereumUsdc {
                usdc_balance,
                fetched_at,
            } => Ok(self.set_inflight_cash(
                InFlightCashLocation::EthereumWallet,
                *usdc_balance,
                *fetched_at,
                now,
            )),

            BaseWalletUsdc {
                usdc_balance,
                fetched_at,
            } => Ok(self.set_inflight_cash(
                InFlightCashLocation::BaseWallet,
                *usdc_balance,
                *fetched_at,
                now,
            )),

            BaseWalletUnwrappedEquity {
                balances,
                fetched_at,
            } => Ok(self.set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                balances,
                *fetched_at,
                now,
            )),

            BaseWalletWrappedEquity {
                balances,
                fetched_at,
            } => Ok(self.set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletWrapped,
                balances,
                *fetched_at,
                now,
            )),

            InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => self.apply_inflight_snapshot(mints, redemptions, *fetched_at, now),

            SourceObserved {
                source,
                observed_at,
            } => Ok(self.record_source_observation(*source, *observed_at, now)),
        }
    }

    fn record_source_observation(
        mut self,
        source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Self {
        let should_record = self
            .source_observed_at
            .get(&source)
            .is_none_or(|existing| observed_at > *existing);
        if should_record {
            self.source_observed_at.insert(source, observed_at);
            self.last_updated = now;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::{Duration, TimeZone, Utc};
    use proptest::prelude::*;
    use rain_math_float::Float;

    use st0x_finance::Usdc;
    use st0x_wrapper::RATIO_ONE;

    use super::*;
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
    fn equity_freshness_accepts_all_required_source_observations() {
        let now = Utc::now();
        let view = [
            InventoryObservationSource::InflightEquity,
            InventoryObservationSource::OnchainEquity,
            InventoryObservationSource::OffchainInventory,
        ]
        .into_iter()
        .fold(InventoryView::default(), |view, source| {
            view.apply_snapshot_event(
                &InventorySnapshotEvent::SourceObserved {
                    source,
                    observed_at: now,
                },
                now,
            )
            .unwrap()
        });

        view.require_fresh_sources(
            InventoryFreshnessRequirement::Equity,
            now,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
    }

    #[test]
    fn usdc_freshness_fails_closed_when_a_source_is_missing() {
        let now = Utc::now();

        let result = InventoryView::default().require_fresh_sources(
            InventoryFreshnessRequirement::Usdc,
            now,
            std::time::Duration::from_secs(60),
        );

        assert_eq!(
            result,
            Err(InventorySourceFreshnessError::Missing {
                observation_source: InventoryObservationSource::OnchainUsdc,
            })
        );
    }

    #[test]
    fn usdc_freshness_rejects_an_observation_older_than_the_window() {
        let now = Utc::now();
        let stale_at = now - Duration::seconds(61);
        let view = [
            (InventoryObservationSource::OnchainUsdc, stale_at),
            (InventoryObservationSource::OffchainInventory, now),
        ]
        .into_iter()
        .fold(InventoryView::default(), |view, (source, observed_at)| {
            view.apply_snapshot_event(
                &InventorySnapshotEvent::SourceObserved {
                    source,
                    observed_at,
                },
                now,
            )
            .unwrap()
        });

        let result = view.require_fresh_sources(
            InventoryFreshnessRequirement::Usdc,
            now,
            std::time::Duration::from_secs(60),
        );

        assert_eq!(
            result,
            Err(InventorySourceFreshnessError::Stale {
                observation_source: InventoryObservationSource::OnchainUsdc,
                observed_at: stale_at,
                checked_at: now,
                max_age: std::time::Duration::from_secs(60),
            })
        );
    }

    #[test]
    fn usdc_freshness_rejects_an_observation_from_the_future() {
        let now = Utc::now();
        let future_at = now + Duration::milliseconds(1);
        let view = [
            (InventoryObservationSource::OnchainUsdc, future_at),
            (InventoryObservationSource::OffchainInventory, now),
        ]
        .into_iter()
        .fold(InventoryView::default(), |view, (source, observed_at)| {
            view.apply_snapshot_event(
                &InventorySnapshotEvent::SourceObserved {
                    source,
                    observed_at,
                },
                now,
            )
            .unwrap()
        });

        let result = view.require_fresh_sources(
            InventoryFreshnessRequirement::Usdc,
            now,
            std::time::Duration::from_secs(60),
        );

        assert_eq!(
            result,
            Err(InventorySourceFreshnessError::Future {
                observation_source: InventoryObservationSource::OnchainUsdc,
                observed_at: future_at,
                checked_at: now,
            })
        );
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
            buying_power_cents: None,
            withdrawable_cash_cents: None,
            offchain_gross_usd_cents: None,
            alpaca_usdc: None,
            inflight_cash: HashMap::new(),
            active_usdc_rebalance: None,
            active_mints: HashMap::new(),
            active_redemptions: HashMap::new(),
            inflight_equity: HashMap::new(),
            previous_inflight_mint_symbols: HashSet::new(),
            previous_inflight_redemption_symbols: HashSet::new(),
            onchain_equity_snapshot_watermarks: HashMap::new(),
            offchain_equity_snapshot_watermarks: HashMap::new(),
            source_observed_at: BTreeMap::new(),
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
            buying_power_cents: None,
            withdrawable_cash_cents: None,
            offchain_gross_usd_cents: None,
            alpaca_usdc: None,
            inflight_cash: HashMap::new(),
            active_usdc_rebalance: None,
            active_mints: HashMap::new(),
            active_redemptions: HashMap::new(),
            inflight_equity: HashMap::new(),
            previous_inflight_mint_symbols: HashSet::new(),
            previous_inflight_redemption_symbols: HashSet::new(),
            onchain_equity_snapshot_watermarks: HashMap::new(),
            offchain_equity_snapshot_watermarks: HashMap::new(),
            source_observed_at: BTreeMap::new(),
        }
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

    /// Wallet-read events must populate `inflight_cash` rather than the
    /// venue inventory slots. The venue snapshot semantics ("wallet
    /// balances are a transfer-in-progress signal, not part of the
    /// imbalance math") depend on this separation.
    #[test]
    fn apply_snapshot_event_populates_inflight_cash_for_ethereum_usdc() {
        let view = InventoryView::default();
        let now = Utc::now();
        let balance = Usdc::new(float!(123));

        let updated = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::EthereumUsdc {
                    usdc_balance: balance,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            updated.inflight_cash_at(InFlightCashLocation::EthereumWallet),
            Some(balance),
            "EthereumUsdc must populate the Ethereum inflight cash slot",
        );
        assert_eq!(
            updated.inflight_cash_at(InFlightCashLocation::BaseWallet),
            None,
            "Ethereum event must not touch the BaseWallet slot",
        );
        assert_eq!(
            updated.usdc_available(Venue::MarketMaking),
            None,
            "EthereumUsdc must not initialize venue inventory",
        );
        assert_eq!(updated.usdc_available(Venue::Hedging), None);
    }

    #[test]
    fn apply_snapshot_event_populates_inflight_cash_for_base_wallet_usdc() {
        let view = InventoryView::default();
        let now = Utc::now();
        let balance = Usdc::new(float!(45));

        let updated = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUsdc {
                    usdc_balance: balance,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            updated.inflight_cash_at(InFlightCashLocation::BaseWallet),
            Some(balance),
            "BaseWalletUsdc must populate the BaseWallet inflight cash slot",
        );
        assert_eq!(
            updated.inflight_cash_at(InFlightCashLocation::EthereumWallet),
            None,
            "BaseWallet event must not touch the Ethereum slot",
        );
    }

    /// The two inflight tracking systems must remain independent: the
    /// per-venue `Inventory<Usdc>::inflight` (managed by transfer
    /// lifecycle events) and the location-keyed `inflight_cash` map
    /// (populated by wallet polls) describe different things and can
    /// legitimately coexist mid-transfer.
    #[test]
    fn venue_inflight_and_inflight_cash_are_tracked_independently() {
        let now = Utc::now();
        let view = make_usdc_view(700, 200, 100, 0)
            .apply_snapshot_event(
                &InventorySnapshotEvent::EthereumUsdc {
                    usdc_balance: Usdc::new(float!(50)),
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        // Venue-level inflight remains exactly as constructed
        assert_eq!(
            view.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::new(float!(200))),
        );
        // Wallet-level inflight is captured separately
        assert_eq!(
            view.inflight_cash_at(InFlightCashLocation::EthereumWallet),
            Some(Usdc::new(float!(50))),
        );
    }

    /// Wallet balances must NOT enter the imbalance math. The design
    /// explicitly keeps `check_usdc_imbalance_with_gross_offchain`
    /// operating on venue totals only, so wallet readings can never mask
    /// or compensate a real venue imbalance. Wallet-residue-driven
    /// suppression is deferred to a broader orphan-state detection
    /// mechanism, where distinguishing "in-flight" from
    /// "settled-with-baseline" requires transfer history.
    #[test]
    fn wallet_balances_do_not_enter_imbalance_math() {
        let now = Utc::now();
        let imbalance_without_wallet = make_usdc_view(900, 0, 100, 0)
            .check_usdc_imbalance_with_gross_offchain(&threshold("0.5", "0.3"), None)
            .unwrap();
        assert!(
            matches!(
                imbalance_without_wallet,
                Some(Imbalance::TooMuchOnchain { .. })
            ),
            "venue imbalance is detected without wallet noise, got {imbalance_without_wallet:?}",
        );

        let with_huge_wallet = make_usdc_view(900, 0, 100, 0)
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUsdc {
                    usdc_balance: Usdc::new(float!(10000)),
                    fetched_at: now,
                },
                now,
            )
            .unwrap();
        let imbalance_with_wallet = with_huge_wallet
            .check_usdc_imbalance_with_gross_offchain(&threshold("0.5", "0.3"), None)
            .unwrap();
        assert_eq!(
            imbalance_without_wallet, imbalance_with_wallet,
            "wallet readings must not alter the imbalance answer",
        );
    }

    /// `force_apply_snapshot_event` must wire the same wallet-read events
    /// so recovery paths produce identical inflight_cash bookkeeping.
    #[test]
    fn force_apply_snapshot_event_also_populates_inflight_cash() {
        let now = Utc::now();
        let reason = std::sync::Arc::new(InventoryViewError::UsdBalanceConversion(-1));
        let view = InventoryView::default()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::EthereumUsdc {
                    usdc_balance: Usdc::new(float!(7)),
                    fetched_at: now,
                },
                now,
                reason.clone(),
            )
            .unwrap()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUsdc {
                    usdc_balance: Usdc::new(float!(8)),
                    fetched_at: now,
                },
                now,
                reason,
            )
            .unwrap();

        assert_eq!(
            view.inflight_cash_at(InFlightCashLocation::EthereumWallet),
            Some(Usdc::new(float!(7))),
        );
        assert_eq!(
            view.inflight_cash_at(InFlightCashLocation::BaseWallet),
            Some(Usdc::new(float!(8))),
        );
    }

    /// A wallet poll whose `fetched_at` predates the entry already on file
    /// must not overwrite it. Polls running concurrently against different
    /// RPC nodes can land out of order; honouring the older one would let
    /// stale balances replace fresher ones.
    #[test]
    fn set_inflight_cash_ignores_stale_fetched_at() {
        let earlier = Utc::now();
        let later = earlier + Duration::seconds(30);
        let fresh_fetched_at = earlier;
        let stale_fetched_at = earlier - Duration::seconds(10);

        let view = InventoryView::default()
            .set_inflight_cash(
                InFlightCashLocation::EthereumWallet,
                Usdc::new(float!(100)),
                fresh_fetched_at,
                earlier,
            )
            .set_inflight_cash(
                InFlightCashLocation::EthereumWallet,
                Usdc::new(float!(7)),
                stale_fetched_at,
                later,
            );

        assert_eq!(
            view.inflight_cash_at(InFlightCashLocation::EthereumWallet),
            Some(Usdc::new(float!(100))),
            "stale snapshot must not overwrite fresher entry",
        );
        assert_eq!(
            view.last_updated, earlier,
            "dropping a stale snapshot must not advance last_updated",
        );
    }

    /// A snapshot whose `fetched_at` matches the existing entry must
    /// still replace it. Equal timestamps from the same poll cycle should
    /// not be treated as stale.
    #[test]
    fn set_inflight_cash_replaces_when_fetched_at_equals_existing() {
        let fetched_at = Utc::now();
        let now = fetched_at;

        let view = InventoryView::default()
            .set_inflight_cash(
                InFlightCashLocation::BaseWallet,
                Usdc::new(float!(50)),
                fetched_at,
                now,
            )
            .set_inflight_cash(
                InFlightCashLocation::BaseWallet,
                Usdc::new(float!(75)),
                fetched_at,
                now,
            );

        assert_eq!(
            view.inflight_cash_at(InFlightCashLocation::BaseWallet),
            Some(Usdc::new(float!(75))),
        );
    }

    #[test]
    fn set_inflight_equity_ignores_stale_fetched_at() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let earlier = Utc::now();
        let later = earlier + Duration::seconds(30);
        let fresh_fetched_at = earlier;
        let stale_fetched_at = earlier - Duration::seconds(10);

        let mut fresh_balances = BTreeMap::new();
        fresh_balances.insert(symbol_aapl.clone(), shares(100));

        let mut stale_balances = BTreeMap::new();
        stale_balances.insert(symbol_aapl.clone(), shares(7));

        let view = InventoryView::default()
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &fresh_balances,
                fresh_fetched_at,
                earlier,
            )
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &stale_balances,
                stale_fetched_at,
                later,
            );

        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            Some(shares(100)),
            "stale snapshot must not overwrite fresher entry",
        );
        assert_eq!(
            view.last_updated, earlier,
            "dropping a stale snapshot must not advance last_updated",
        );
    }

    #[test]
    fn set_inflight_equity_replaces_when_fetched_at_equals_existing() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let fetched_at = Utc::now();
        let now = fetched_at;

        let mut first_balances = BTreeMap::new();
        first_balances.insert(symbol_aapl.clone(), shares(50));

        let mut second_balances = BTreeMap::new();
        second_balances.insert(symbol_aapl.clone(), shares(75));

        let view = InventoryView::default()
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &first_balances,
                fetched_at,
                now,
            )
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &second_balances,
                fetched_at,
                now,
            );

        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            Some(shares(75)),
        );
    }

    #[test]
    fn apply_snapshot_event_populates_inflight_equity_for_base_wallet_unwrapped() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let mut balances = BTreeMap::new();
        balances.insert(symbol_aapl.clone(), shares(12));

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            Some(shares(12)),
            "BaseWalletUnwrappedEquity must populate the BaseWalletUnwrapped slot",
        );
        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletWrapped),
            None,
            "BaseWalletWrapped slot must remain untouched",
        );
        assert_eq!(
            view.equity_available(&symbol_aapl, Venue::MarketMaking),
            None,
            "wallet read must not initialize venue inventory",
        );
        assert_eq!(view.equity_available(&symbol_aapl, Venue::Hedging), None);
    }

    #[test]
    fn apply_snapshot_event_populates_inflight_equity_for_base_wallet_wrapped() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let mut balances = BTreeMap::new();
        balances.insert(symbol_aapl.clone(), shares(9));

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletWrapped),
            Some(shares(9)),
        );
        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            None,
        );
    }

    /// Wallet readings replace the prior balances at that location:
    /// symbols absent from the new map drop out of the inflight_equity
    /// map even if previously seen, because the chain reading is
    /// authoritative.
    #[test]
    fn apply_snapshot_event_replaces_inflight_equity_at_same_location() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let symbol_tsla = Symbol::new("TSLA").unwrap();
        let now = Utc::now();

        let mut first = BTreeMap::new();
        first.insert(symbol_aapl.clone(), shares(5));
        first.insert(symbol_tsla.clone(), shares(3));

        let after_first = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances: first,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        let mut second = BTreeMap::new();
        second.insert(symbol_aapl.clone(), shares(2));

        let after_second = after_first
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances: second,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            after_second
                .inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            Some(shares(2)),
            "AAPL balance must reflect the latest wallet read",
        );
        assert_eq!(
            after_second
                .inflight_equity_at(&symbol_tsla, InFlightEquityLocation::BaseWalletUnwrapped),
            None,
            "TSLA must drop out when absent from the latest wallet read",
        );
    }

    /// The two equity inflight tracking systems must remain independent:
    /// `Inventory<FractionalShares>::inflight` (managed by tokenization
    /// lifecycle events) and the location-keyed `inflight_equity` map
    /// (populated by wallet polls) describe different things and can
    /// legitimately coexist mid-transfer.
    #[test]
    fn venue_inflight_and_inflight_equity_are_tracked_independently() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let mut mints = BTreeMap::new();
        mints.insert(symbol_aapl.clone(), shares(15));

        let mut wallet_balances = BTreeMap::new();
        wallet_balances.insert(symbol_aapl.clone(), shares(4));

        let view = InventoryView::default()
            .apply_inflight_snapshot(&mints, &BTreeMap::new(), now, now)
            .unwrap()
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances: wallet_balances,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol_aapl, Venue::Hedging),
            Some(shares(15)),
            "venue-level inflight reflects mint snapshot",
        );
        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletWrapped),
            Some(shares(4)),
            "location-level inflight reflects the wallet read",
        );
    }

    /// Wallet equity balances must NOT enter the imbalance math --
    /// `check_equity_imbalance` operates on venue totals only, so wallet
    /// readings can never mask or compensate a real venue imbalance.
    #[test]
    fn wallet_equity_balances_do_not_enter_imbalance_math() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let baseline =
            InventoryView::default().with_equity(symbol_aapl.clone(), shares(90), shares(10));

        let imbalance_without_wallet = baseline
            .check_equity_imbalance(&symbol_aapl, &threshold("0.5", "0.3"), &one_to_one_ratio())
            .unwrap();
        assert!(
            matches!(
                imbalance_without_wallet,
                Some(Imbalance::TooMuchOnchain { .. })
            ),
            "venue imbalance is detected without wallet noise, got {imbalance_without_wallet:?}",
        );

        let mut wallet_balances = BTreeMap::new();
        wallet_balances.insert(symbol_aapl.clone(), shares(10_000));

        let with_huge_wallet = baseline
            .apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances: wallet_balances,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        let imbalance_with_wallet = with_huge_wallet
            .check_equity_imbalance(&symbol_aapl, &threshold("0.5", "0.3"), &one_to_one_ratio())
            .unwrap();
        assert_eq!(
            imbalance_without_wallet, imbalance_with_wallet,
            "wallet equity readings must not alter the imbalance answer",
        );
    }

    #[test]
    fn force_apply_snapshot_event_also_populates_inflight_equity() {
        let symbol_aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let reason = std::sync::Arc::new(InventoryViewError::UsdBalanceConversion(-1));

        let mut unwrapped = BTreeMap::new();
        unwrapped.insert(symbol_aapl.clone(), shares(2));
        let mut wrapped = BTreeMap::new();
        wrapped.insert(symbol_aapl.clone(), shares(3));

        let view = InventoryView::default()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances: unwrapped,
                    fetched_at: now,
                },
                now,
                reason.clone(),
            )
            .unwrap()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances: wrapped,
                    fetched_at: now,
                },
                now,
                reason,
            )
            .unwrap();

        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletUnwrapped),
            Some(shares(2)),
        );
        assert_eq!(
            view.inflight_equity_at(&symbol_aapl, InFlightEquityLocation::BaseWalletWrapped),
            Some(shares(3)),
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
    fn restore_reservation_replaces_stale_available_and_existing_inflight() {
        let inventory = make_inventory(50, 0, 100, 20);

        let restored =
            Inventory::restore_reservation(Venue::Hedging, shares(75), shares(30))(inventory)
                .unwrap();

        let offchain = restored.offchain.unwrap();
        assert_eq!(offchain.available(), shares(75));
        assert_eq!(offchain.inflight(), shares(30));
        assert_eq!(restored.onchain, Some(venue(50, 0)));
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
    }

    #[test]
    fn offchain_snapshot_zeroes_symbol_absent_from_complete_snapshot() {
        // A brokerage omits zero-share positions, so a configured symbol that
        // dropped to zero is absent from the snapshot. The live view must treat
        // the snapshot as complete and zero the offchain balance instead of
        // retaining the stale value, while leaving the onchain venue untouched.
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(90), shares(10))
            .with_equity(tsla.clone(), shares(50), shares(20));

        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), shares(10));

        let result = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            result.equity_available(&aapl, Venue::Hedging),
            Some(shares(10)),
            "present symbol keeps its reported offchain balance",
        );
        assert_eq!(
            result.equity_available(&tsla, Venue::Hedging),
            Some(shares(0)),
            "symbol absent from the complete offchain snapshot is zeroed",
        );
        assert_eq!(
            result.equity_available(&tsla, Venue::MarketMaking),
            Some(shares(50)),
            "the onchain venue of the absent symbol is untouched",
        );
        assert_eq!(
            result.equity_available(&aapl, Venue::MarketMaking),
            Some(shares(90)),
            "the onchain venue of the present symbol is untouched",
        );
    }

    #[test]
    fn onchain_snapshot_zeroes_symbol_absent_from_complete_snapshot() {
        // The same complete-snapshot semantics apply to the onchain venue: a
        // symbol tracked onchain but absent from a fresh OnchainEquity snapshot
        // has gone to zero onchain, leaving its offchain venue untouched.
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(90), shares(10))
            .with_equity(tsla.clone(), shares(50), shares(20));

        let mut balances = BTreeMap::new();
        balances.insert(aapl, shares(90));

        let result = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            result.equity_available(&tsla, Venue::MarketMaking),
            Some(shares(0)),
            "symbol absent from the complete onchain snapshot is zeroed",
        );
        assert_eq!(
            result.equity_available(&tsla, Venue::Hedging),
            Some(shares(20)),
            "the offchain venue of the absent symbol is untouched",
        );
    }

    #[test]
    fn snapshot_does_not_zero_absent_symbol_with_inflight() {
        // An absent symbol that has an inflight transfer must not be zeroed: the
        // staleness guard cannot distinguish a completed-but-unconfirmed transfer
        // from an unrelated change, so the stale balance is preserved.
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let now = Utc::now();

        let view = InventoryView {
            equities: [
                (
                    aapl.clone(),
                    Inventory {
                        onchain: Some(VenueBalance::new(shares(90), FractionalShares::ZERO)),
                        offchain: Some(VenueBalance::new(shares(10), FractionalShares::ZERO)),
                        last_rebalancing: None,
                    },
                ),
                (
                    tsla.clone(),
                    Inventory {
                        onchain: Some(VenueBalance::new(shares(50), FractionalShares::ZERO)),
                        offchain: Some(VenueBalance::new(shares(20), shares(5))),
                        last_rebalancing: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            ..InventoryView::default()
        };

        let mut positions = BTreeMap::new();
        positions.insert(aapl, shares(10));

        let result = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        assert_eq!(
            result.equity_available(&tsla, Venue::Hedging),
            Some(shares(20)),
            "absent symbol with inflight is not zeroed",
        );
        assert_eq!(
            result.equity_inflight(&tsla, Venue::Hedging),
            Some(shares(5)),
            "absent symbol inflight is preserved",
        );
    }

    #[test]
    fn stale_snapshot_does_not_zero_absent_symbol() {
        // A snapshot older than a symbol's recorded watermark must not zero it:
        // out-of-order polls can land late, and a stale complete snapshot would
        // otherwise wipe a fresher balance.
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let fresh = Utc::now();
        let stale = fresh - Duration::seconds(60);

        // A fresh snapshot establishes watermarks for both symbols.
        let mut fresh_positions = BTreeMap::new();
        fresh_positions.insert(aapl.clone(), shares(10));
        fresh_positions.insert(tsla.clone(), shares(20));

        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(90), shares(0))
            .with_equity(tsla.clone(), shares(50), shares(0))
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: fresh_positions,
                    fetched_at: fresh,
                },
                fresh,
            )
            .unwrap();

        // A stale snapshot omitting TSLA arrives late; it must not zero TSLA.
        let mut stale_positions = BTreeMap::new();
        stale_positions.insert(aapl, shares(10));

        let result = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: stale_positions,
                    fetched_at: stale,
                },
                stale,
            )
            .unwrap();

        assert_eq!(
            result.equity_available(&tsla, Venue::Hedging),
            Some(shares(20)),
            "stale snapshot must not zero a symbol with a fresher watermark",
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
    fn clear_previous_mint_marker_prevents_incorrect_zeroing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        // Poll 1: AAPL has a pending mint.
        let mut mints = BTreeMap::new();
        mints.insert(symbol.clone(), shares(10));

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .apply_inflight_snapshot(&mints, &BTreeMap::new(), now, now)
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10)),
        );

        // The old mint completes (inflight cleared via TransferOp::Complete).
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Complete, shares(10)),
                now,
            )
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol, Venue::Hedging),
            Some(FractionalShares::ZERO),
        );

        // A new mint starts (MintAccepted sets inflight via
        // TransferOp::Start) and clears the previous poll marker.
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(20)),
                now,
            )
            .unwrap()
            .clear_previous_inflight_mint_marker(&symbol);

        assert_eq!(
            view.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(20)),
        );

        // Poll 2: Alpaca hasn't reflected the new request yet (empty).
        // Without the marker clear, this would zero the new inflight.
        let view = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), now, now)
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(20)),
            "New inflight must be preserved when previous poll marker \
             was cleared by MintAccepted"
        );
    }

    #[test]
    fn clear_previous_redemption_marker_prevents_incorrect_zeroing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        // Poll 1: AAPL has a pending redemption.
        let mut redemptions = BTreeMap::new();
        redemptions.insert(symbol.clone(), shares(10));

        let view = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .apply_inflight_snapshot(&BTreeMap::new(), &redemptions, now, now)
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(10)),
        );

        // Old redemption completes.
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Complete, shares(10)),
                now,
            )
            .unwrap();

        // New redemption starts and clears the previous poll marker.
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(15)),
                now,
            )
            .unwrap()
            .clear_previous_inflight_redemption_marker(&symbol);

        // Poll 2: empty (Alpaca hasn't reflected the new request).
        let view = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), now, now)
            .unwrap();

        assert_eq!(
            view.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(15)),
            "New redemption inflight must be preserved when previous \
             poll marker was cleared by VaultWithdrawPending"
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
            buying_power_cents: None,
            withdrawable_cash_cents: None,
            offchain_gross_usd_cents: None,
            alpaca_usdc: None,
            inflight_cash: HashMap::new(),
            active_usdc_rebalance: None,
            active_mints: HashMap::new(),
            active_redemptions: HashMap::new(),
            inflight_equity: HashMap::new(),
            previous_inflight_mint_symbols: HashSet::new(),
            previous_inflight_redemption_symbols: HashSet::new(),
            onchain_equity_snapshot_watermarks: HashMap::new(),
            offchain_equity_snapshot_watermarks: HashMap::new(),
            source_observed_at: BTreeMap::new(),
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
            buying_power_cents: None,
            withdrawable_cash_cents: None,
            offchain_gross_usd_cents: None,
            alpaca_usdc: None,
            inflight_cash: HashMap::new(),
            active_usdc_rebalance: None,
            active_mints: HashMap::new(),
            active_redemptions: HashMap::new(),
            inflight_equity: HashMap::new(),
            previous_inflight_mint_symbols: HashSet::new(),
            previous_inflight_redemption_symbols: HashSet::new(),
            onchain_equity_snapshot_watermarks: HashMap::new(),
            offchain_equity_snapshot_watermarks: HashMap::new(),
            source_observed_at: BTreeMap::new(),
        };

        let dto = view.to_dto();

        let spy_dto = &dto.per_symbol[0];
        assert_eq!(spy_dto.onchain_available, FractionalShares::new(float!(75)));
        assert_eq!(spy_dto.offchain_available, FractionalShares::ZERO);
        assert_eq!(spy_dto.offchain_inflight, FractionalShares::ZERO);

        assert_eq!(dto.usdc.onchain_available, Usdc::ZERO);
        assert_eq!(dto.usdc.offchain_available, Usdc::ZERO);
    }

    /// `to_dto` must expose `inflight_cash` so the dashboard can see USDC
    /// observed in transit between venues. Locations that have not yet
    /// been polled remain `None`; observed locations expose their amount.
    #[test]
    fn to_dto_includes_inflight_cash() {
        let fetched_at = Utc::now();
        let view = InventoryView::default()
            .set_inflight_cash(
                InFlightCashLocation::EthereumWallet,
                Usdc::new(float!(250)),
                fetched_at,
                fetched_at,
            )
            .set_inflight_cash(
                InFlightCashLocation::BaseWallet,
                Usdc::ZERO,
                fetched_at,
                fetched_at,
            );

        let dto = view.to_dto();

        assert_eq!(
            dto.usdc.inflight_cash.ethereum_wallet,
            Some(Usdc::new(float!(250)))
        );
        assert_eq!(dto.usdc.inflight_cash.base_wallet, Some(Usdc::ZERO));
    }

    #[test]
    fn to_dto_includes_withdrawable_cash() {
        let view = InventoryView {
            withdrawable_cash_cents: Some(3_200_000),
            ..InventoryView::default()
        };

        let dto = view.to_dto();

        assert_eq!(dto.usdc.withdrawable_cash, Some(Usdc::new(float!(32000))));
    }

    #[test]
    fn to_dto_withdrawable_cash_is_none_when_broker_omitted_field() {
        let dto = InventoryView::default().to_dto();

        assert_eq!(dto.usdc.withdrawable_cash, None);
    }

    /// Locations that have never been observed must surface as `None` in
    /// the DTO so the dashboard can distinguish "not polled yet" from
    /// "observed as zero".
    #[test]
    fn to_dto_inflight_cash_is_none_for_unobserved_locations() {
        let dto = InventoryView::default().to_dto();

        assert_eq!(dto.usdc.inflight_cash.ethereum_wallet, None);
        assert_eq!(dto.usdc.inflight_cash.base_wallet, None);
    }

    #[test]
    fn to_dto_exports_alpaca_usdc_from_applied_snapshot_event() {
        let now = Utc::now();
        let balance = Usdc::new(float!(12.5));

        let dto = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::AlpacaUsdc {
                    usdc_balance: balance,
                    fetched_at: now,
                },
                now,
            )
            .unwrap()
            .to_dto();

        assert_eq!(
            dto.usdc.alpaca_usdc,
            Some(balance),
            "AlpacaUsdc snapshot event must survive apply_snapshot_event into the DTO",
        );
    }

    #[test]
    fn to_dto_exports_alpaca_usdc_from_force_applied_snapshot_event() {
        let now = Utc::now();
        let balance = Usdc::new(float!(12.5));
        let reason = Arc::new(InventoryViewError::UsdBalanceConversion(0));

        let dto = InventoryView::default()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::AlpacaUsdc {
                    usdc_balance: balance,
                    fetched_at: now,
                },
                now,
                reason,
            )
            .unwrap()
            .to_dto();

        assert_eq!(
            dto.usdc.alpaca_usdc,
            Some(balance),
            "AlpacaUsdc snapshot event must survive force_apply_snapshot_event into the DTO",
        );
    }

    #[test]
    fn to_dto_includes_inflight_equity() {
        let fetched_at = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();
        let mut unwrapped = BTreeMap::new();
        unwrapped.insert(aapl.clone(), shares(3));
        let mut wrapped = BTreeMap::new();
        wrapped.insert(aapl.clone(), shares(2));

        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(100), shares(50))
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &unwrapped,
                fetched_at,
                fetched_at,
            )
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletWrapped,
                &wrapped,
                fetched_at,
                fetched_at,
            );

        let dto = view.to_dto();
        let aapl_dto = &dto.per_symbol[0];

        assert_eq!(aapl_dto.symbol, aapl);
        assert_eq!(aapl_dto.inflight_equity.base_wallet_unwrapped, shares(3));
        assert_eq!(aapl_dto.inflight_equity.base_wallet_wrapped, shares(2));
    }

    #[test]
    fn to_dto_includes_wallet_only_equity_symbols() {
        let fetched_at = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();
        let mut wrapped = BTreeMap::new();
        wrapped.insert(aapl.clone(), shares(2));

        let view = InventoryView::default().set_inflight_equity_at_location(
            InFlightEquityLocation::BaseWalletWrapped,
            &wrapped,
            fetched_at,
            fetched_at,
        );

        let dto = view.to_dto();
        let aapl_dto = &dto.per_symbol[0];

        assert_eq!(aapl_dto.symbol, aapl);
        assert_eq!(aapl_dto.onchain_available, FractionalShares::ZERO);
        assert_eq!(aapl_dto.offchain_available, FractionalShares::ZERO);
        assert_eq!(
            aapl_dto.inflight_equity.base_wallet_unwrapped,
            FractionalShares::ZERO
        );
        assert_eq!(aapl_dto.inflight_equity.base_wallet_wrapped, shares(2));
    }

    #[test]
    fn inflight_from_transfer_survives_empty_polling_snapshot() {
        let symbol = Symbol::new("AAPL").unwrap();
        let transfer_quantity = shares(10);

        // Start with equity on both venues
        let view = InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50));

        // Simulate WithdrawnFromRaindex: trigger sets inflight
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, transfer_quantity),
                Utc::now(),
            )
            .unwrap();

        // Verify inflight is set in DTO
        let dto = view.to_dto();
        let aapl = &dto.per_symbol[0];
        assert_eq!(
            aapl.onchain_inflight, transfer_quantity,
            "Inflight should be set after transfer start"
        );

        // Simulate polling: Alpaca hasn't detected the transfer yet,
        // so the inflight snapshot has empty maps.
        let view = view
            .apply_inflight_snapshot(&BTreeMap::new(), &BTreeMap::new(), Utc::now(), Utc::now())
            .unwrap();

        // Inflight MUST still be present — the polling snapshot
        // should not clear inflight set by the transfer trigger.
        let dto = view.to_dto();
        let aapl = &dto.per_symbol[0];
        assert_eq!(
            aapl.onchain_inflight, transfer_quantity,
            "Inflight must survive an empty polling snapshot \
             (Alpaca hasn't detected the transfer yet)"
        );
    }

    #[test]
    fn inflight_from_transfer_survives_balance_snapshot() {
        let symbol = Symbol::new("AAPL").unwrap();
        let transfer_quantity = shares(10);

        let view = InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50));

        // Trigger sets inflight
        let view = view
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, transfer_quantity),
                Utc::now(),
            )
            .unwrap();

        // Balance polling reports updated onchain balance.
        // on_snapshot should skip because inflight is active.
        let view = view
            .update_equity(
                &symbol,
                Inventory::on_snapshot(Venue::MarketMaking, shares(40), Utc::now()),
                Utc::now(),
            )
            .unwrap();

        let dto = view.to_dto();
        let aapl = &dto.per_symbol[0];
        assert_eq!(
            aapl.onchain_inflight, transfer_quantity,
            "Inflight must survive a balance snapshot during active transfer"
        );
        assert_eq!(
            aapl.onchain_available,
            shares(40),
            "Available should reflect the transfer (50 - 10 moved to inflight)"
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct SetInflightCashCall {
        location: InFlightCashLocation,
        amount: Usdc,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    }

    fn arb_location() -> impl Strategy<Value = InFlightCashLocation> {
        use InFlightCashLocation::{BaseWallet, EthereumWallet};
        prop_oneof![Just(EthereumWallet), Just(BaseWallet)]
    }

    fn arb_usdc() -> impl Strategy<Value = Usdc> {
        (0i64..1_000_000_000)
            .prop_map(|cents| Usdc::from_cents(cents).expect("in-range cents are valid Usdc"))
    }

    /// Bounded so `Utc.timestamp_millis_opt` is always representable.
    fn arb_timestamp() -> impl Strategy<Value = DateTime<Utc>> {
        (0i64..i64::from(u32::MAX)).prop_map(|millis| {
            Utc.timestamp_millis_opt(millis)
                .single()
                .expect("bounded millis are representable")
        })
    }

    fn arb_call() -> impl Strategy<Value = SetInflightCashCall> {
        (arb_location(), arb_usdc(), arb_timestamp(), arb_timestamp()).prop_map(
            |(location, amount, fetched_at, now)| SetInflightCashCall {
                location,
                amount,
                fetched_at,
                now,
            },
        )
    }

    fn apply_calls(calls: &[SetInflightCashCall]) -> InventoryView {
        calls.iter().fold(InventoryView::default(), |view, call| {
            view.set_inflight_cash(call.location, call.amount, call.fetched_at, call.now)
        })
    }

    fn dto_slot(dto: &st0x_dto::Inventory, location: InFlightCashLocation) -> Option<Usdc> {
        use InFlightCashLocation::{BaseWallet, EthereumWallet};
        match location {
            EthereumWallet => dto.usdc.inflight_cash.ethereum_wallet,
            BaseWallet => dto.usdc.inflight_cash.base_wallet,
        }
    }

    const LOCATIONS: [InFlightCashLocation; 2] = [
        InFlightCashLocation::EthereumWallet,
        InFlightCashLocation::BaseWallet,
    ];

    proptest! {
        /// Each slot must equal the amount from the call with the maximum
        /// `fetched_at` for that location. `max_by_key` keeps the latest
        /// tie, matching the "equal timestamps replace" semantics.
        #[test]
        fn set_inflight_cash_keeps_freshest_per_location(
            calls in prop::collection::vec(arb_call(), 0..16),
        ) {
            let dto = apply_calls(&calls).to_dto();

            for location in LOCATIONS {
                let expected = calls
                    .iter()
                    .filter(|call| call.location == location)
                    .max_by_key(|call| call.fetched_at)
                    .map(|call| call.amount);

                prop_assert_eq!(dto_slot(&dto, location), expected);
            }
        }

        /// Writes targeting one location must never mutate the other slot.
        #[test]
        fn set_inflight_cash_does_not_touch_other_location(
            mut calls in prop::collection::vec(arb_call(), 1..16),
            untouched in arb_location(),
        ) {
            use InFlightCashLocation::{BaseWallet, EthereumWallet};

            let target = match untouched {
                EthereumWallet => BaseWallet,
                BaseWallet => EthereumWallet,
            };
            for call in &mut calls {
                call.location = target;
            }

            let dto = apply_calls(&calls).to_dto();
            prop_assert_eq!(dto_slot(&dto, untouched), None);
        }

        /// `last_updated` must equal the `now` of the most recent accepted
        /// call. Stale calls (fetched_at strictly older than the entry on
        /// file) must not advance the clock.
        #[test]
        fn set_inflight_cash_last_updated_advances_only_on_accept(
            calls in prop::collection::vec(arb_call(), 1..16),
        ) {
            let mut stored: HashMap<InFlightCashLocation, DateTime<Utc>> = HashMap::new();
            let mut expected: Option<DateTime<Utc>> = None;

            for call in &calls {
                if stored.get(&call.location).is_none_or(|seen| *seen <= call.fetched_at) {
                    stored.insert(call.location, call.fetched_at);
                    expected = Some(call.now);
                }
            }

            prop_assert_eq!(
                apply_calls(&calls).last_updated,
                expected.expect("first call always accepts against an empty map"),
            );
        }

        /// The DTO slot must equal the view's stored amount at every
        /// location for any sequence of writes.
        #[test]
        fn to_dto_round_trips_inflight_cash(
            calls in prop::collection::vec(arb_call(), 0..16),
        ) {
            let view = apply_calls(&calls);
            let dto = view.to_dto();

            for location in LOCATIONS {
                prop_assert_eq!(dto_slot(&dto, location), view.inflight_cash_at(location));
            }
        }
    }
}
