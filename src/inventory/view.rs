//! Inventory view for tracking cross-venue asset positions.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Add, Sub};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use st0x_config::ImbalanceThreshold;
use st0x_dto::{InFlightCash, InFlightEquity, SymbolInventory, UsdcInventory};
use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};
use st0x_finance::{Usd, Usdc};
use st0x_tokenization::IssuerRequestId;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped};

use super::divergence::{PersistentBrokerCashDivergence, PersistentBrokerDivergence};
use super::snapshot::InventorySnapshotEvent;
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

/// Why divergence recovery must leave a symbol alone this poll. Shared by
/// detection (freezes the counter) and the forced apply (aborts), so the
/// two can never disagree on what counts as busy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EquityReconcileBusy {
    /// Inflight at either venue, or an active mint/redemption owning the
    /// symbol's inflight slot.
    Transfer,
    /// An open hedge order's fill delta owns the balance.
    PendingHedgeOrder,
    /// A fill was applied to the view after the reading was fetched, so
    /// the reading is stale.
    FillAfterFetch,
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

    /// The skip conditions of [`Self::on_snapshot`], exposed so callers that
    /// track per-snapshot bookkeeping (the onchain USDC block watermark) can
    /// tell whether the closure will apply or silently skip. Single source
    /// of truth: the closure consults this same predicate.
    fn snapshot_would_apply(&self, fetched_at: DateTime<Utc>) -> Result<bool, FloatError> {
        if self.has_inflight()? {
            return Ok(false);
        }

        if let Some(last_rebalancing) = self.last_rebalancing
            && fetched_at < last_rebalancing
        {
            debug!(
                target: "inventory",
                ?fetched_at,
                ?last_rebalancing,
                "Rejecting stale snapshot that predates last rebalancing"
            );
            return Ok(false);
        }

        Ok(true)
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
            if !inventory.snapshot_would_apply(fetched_at)? {
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

/// A destination for USD-denominated balances tracked in a daily portfolio
/// snapshot (RAI-1457): the two live trading venues, plus the wallet-transit
/// points equity or cash may sit at in between.
///
/// USDC observed in wallet transit is never "wrapped" (no wrapped-USDC
/// concept exists onchain), so [`InFlightCashLocation::BaseWallet`] maps to
/// `BaseWalletUnwrapped` here. The primary key `(et_day, location, asset)`
/// prevents this from colliding with unwrapped equity at the same location,
/// since `asset` still distinguishes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum PortfolioLocation {
    MarketMaking,
    Hedging,
    EthereumWallet,
    BaseWalletUnwrapped,
    BaseWalletWrapped,
}

impl std::fmt::Display for PortfolioLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::MarketMaking => "market_making",
            Self::Hedging => "hedging",
            Self::EthereumWallet => "ethereum_wallet",
            Self::BaseWalletUnwrapped => "base_wallet_unwrapped",
            Self::BaseWalletWrapped => "base_wallet_wrapped",
        };
        write!(formatter, "{label}")
    }
}

/// The asset held at a [`PortfolioLocation`] in a daily portfolio snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum PortfolioAsset {
    Usdc,
    Equity(Symbol),
}

impl std::fmt::Display for PortfolioAsset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usdc => write!(formatter, "USDC"),
            Self::Equity(symbol) => write!(formatter, "{symbol}"),
        }
    }
}

/// A single observed balance at a `(location, asset)` pair, ready to be
/// marked with a USD price and captured into the daily portfolio snapshot's
/// `Captured` event. `available` and `inflight` are kept as independent
/// observed facts (per "No Denormalized Columns") -- nothing computed from
/// them is persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PortfolioBalanceRow {
    pub(crate) location: PortfolioLocation,
    pub(crate) asset: PortfolioAsset,
    pub(crate) available: Float,
    pub(crate) inflight: Float,
}

impl PartialEq for PortfolioBalanceRow {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
            && self.asset == other.asset
            && self.available.eq(other.available).unwrap_or(false)
            && self.inflight.eq(other.inflight).unwrap_or(false)
    }
}

/// Venues paired with their [`PortfolioLocation`] counterpart, in the fixed
/// order [`InventoryView::to_portfolio_snapshot_rows`] emits them.
/// Warn cadence for guard-starved offchain snapshots: every this many
/// consecutive skips of a symbol's Hedging equity snapshot (or of the
/// venue-level `OffchainUsd` snapshot), a `warn!` surfaces the starvation
/// that ADR 0015 accepted but left invisible at production log levels.
/// Observability only -- never changes which snapshots apply.
const OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY: u32 = 5;

const PORTFOLIO_VENUES: [(Venue, PortfolioLocation); 2] = [
    (Venue::MarketMaking, PortfolioLocation::MarketMaking),
    (Venue::Hedging, PortfolioLocation::Hedging),
];

/// Wallet-transit cash locations in the fixed order rows are emitted.
const PORTFOLIO_CASH_TRANSIT_LOCATIONS: [InFlightCashLocation; 2] = [
    InFlightCashLocation::EthereumWallet,
    InFlightCashLocation::BaseWallet,
];

/// Wallet-transit equity locations in the fixed order rows are emitted.
const PORTFOLIO_EQUITY_TRANSIT_LOCATIONS: [InFlightEquityLocation; 2] = [
    InFlightEquityLocation::BaseWalletUnwrapped,
    InFlightEquityLocation::BaseWalletWrapped,
];

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
    /// Local-clock time: the snapshot aggregate stamps `fetched_at` itself, so
    /// this is never the chain's or the broker's clock.
    #[serde(default)]
    onchain_equity_snapshot_watermarks: HashMap<Symbol, DateTime<Utc>>,
    /// Latest absolute equity snapshot timestamp by symbol for the offchain venue.
    /// Local-clock time, as above.
    #[serde(default)]
    offchain_equity_snapshot_watermarks: HashMap<Symbol, DateTime<Utc>>,
    /// Symbols with an open offchain (hedge) order.
    #[serde(default)]
    pending_offchain_order_symbols: HashSet<Symbol>,
    /// Local-clock time at which the most recent offchain fill was *applied to
    /// this view*, by symbol -- not when the broker executed it, which is
    /// earlier by the observation lag. It is compared against snapshot
    /// `fetched_at`, so it must be read from the same clock that stamps it.
    /// Stops a poll issued before the fill from landing after it and
    /// overwriting a correctly decremented balance with a pre-fill number.
    #[serde(default)]
    last_offchain_fill_applied_at: HashMap<Symbol, DateTime<Utc>>,
    /// Local-clock time at which the most recent offchain fill's mirrored
    /// USDC delta was applied to this view -- the venue-level cash analogue
    /// of `last_offchain_fill_applied_at`. Venue-level because the Hedging
    /// cash balance is one number, not per-symbol: a hedge fill on ANY
    /// symbol moves it, so the guard keying on this field must be equally
    /// broad. Same clock contract as the equity map: compared against
    /// snapshot `fetched_at`, so it must be read from the host clock that
    /// stamps it.
    #[serde(default)]
    last_offchain_cash_fill_applied_at: Option<DateTime<Utc>>,
    /// Consecutive skipped Hedging equity snapshots per symbol, reset when
    /// one applies. Pure observability: ADR 0015 accepted that its guards
    /// can starve a symbol's snapshots but noted the starvation was
    /// invisible at production log levels; these streaks surface it as a
    /// `warn!` every [`OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY`] consecutive
    /// skips.
    #[serde(default)]
    offchain_equity_snapshot_skip_streaks: HashMap<Symbol, u32>,
    /// Consecutive `OffchainUsd` snapshots skipped by the venue-level cash
    /// guards, reset when one passes them.
    #[serde(default)]
    offchain_usd_snapshot_skip_streak: u32,
    /// `fetched_at` of the freshest applied Hedging cash snapshot (ordinary
    /// or reconciled) -- the venue-level cash twin of
    /// `offchain_equity_snapshot_watermarks`. Consulted by
    /// `reconcile_offchain_usd`: an ordinary snapshot that applied between
    /// the escalation send and the reconcile apply owns the balance, and
    /// the reconcile's older broker reading must not overwrite it. The
    /// venue's other stamps cannot stand in -- the applied-cash-fill time
    /// tracks fills, not snapshots, and `last_rebalancing` is exactly what
    /// the reconcile's force path bypasses.
    #[serde(default)]
    offchain_usd_snapshot_watermark: Option<DateTime<Utc>>,
    /// Highest block number whose `OnchainEquity` snapshot has been applied,
    /// by symbol. Chain-native ordering, not a clock: a pinned vault read at
    /// block N provably contains every fill at a block <= N, so an
    /// `OnChainOrderFilled` delta covered by this watermark is already in
    /// the balance and must be skipped (ADR 0018).
    #[serde(default)]
    onchain_equity_snapshot_block_watermarks: HashMap<Symbol, u64>,
    /// Highest block number whose `OnchainUsdc` snapshot has been applied.
    /// Venue-level: the USDC balance is one number per venue.
    #[serde(default)]
    onchain_usdc_snapshot_block_watermark: Option<u64>,
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

    /// Extracts a flat list of observed balances for the daily portfolio
    /// snapshot (RAI-1457). Mirrors [`Self::to_dto`]'s per-venue iteration,
    /// but never merges wrapped/unwrapped equity or wallet-transit balances
    /// -- each remains its own row so a persisted snapshot cannot silently
    /// misstate a balance.
    ///
    /// Only locations that have actually been polled at least once are
    /// emitted: a venue that was never polled produces no row, while a venue
    /// polled to a genuine zero balance still produces a `0` row.
    /// Wallet-transit amounts (`inflight_cash`/`inflight_equity`) are
    /// point-in-time balances observed via wallet polling, not a venue
    /// split, so they are recorded on the row's `inflight` field with
    /// `available` at zero.
    ///
    /// Deployed capital counts the FULL book, including any configured cash
    /// reserve: reserved cash held at the broker is still money under
    /// management, not capital that has left the portfolio. The Hedging USDC
    /// row therefore uses `offchain_gross_usd_cents` (the pre-reserve broker
    /// balance) when known, falling back to the venue's own (reserve-adjusted
    /// where a reserve is configured) `available` balance only when no gross
    /// reading exists yet. An invalid gross reading is an error rather than a
    /// fallback, matching
    /// [`Self::check_usdc_imbalance_with_gross_offchain`]'s reasoning for why
    /// gross, not net, is the right number here.
    pub(crate) fn to_portfolio_snapshot_rows(
        &self,
    ) -> Result<Vec<PortfolioBalanceRow>, InventoryViewError> {
        let mut rows = Vec::new();

        for (symbol, inventory) in self.equities.iter().sorted_by_key(|(symbol, _)| *symbol) {
            for (venue, location) in PORTFOLIO_VENUES {
                if let Some(balance) = inventory.get_venue(venue) {
                    rows.push(PortfolioBalanceRow {
                        location,
                        asset: PortfolioAsset::Equity(symbol.clone()),
                        available: balance.available().into(),
                        inflight: balance.inflight().into(),
                    });
                }
            }
        }

        for (venue, location) in PORTFOLIO_VENUES {
            if let Some(balance) = self.usdc.get_venue(venue) {
                // Matches on `venue` (exactly 2 variants, `MarketMaking` and
                // `Hedging`), not `location`: `PORTFOLIO_VENUES` can only ever
                // produce those two `PortfolioLocation` values, so a wildcard
                // over the 5-variant `PortfolioLocation` would silently cover
                // wallet-transit variants this loop never actually sees.
                let available = match venue {
                    Venue::Hedging => match self.offchain_gross_usd_cents {
                        Some(cents) => Float::from(
                            Usdc::from_cents(cents)
                                .ok_or(InventoryViewError::UsdBalanceConversion(cents))?,
                        ),
                        None => balance.available().into(),
                    },
                    Venue::MarketMaking => balance.available().into(),
                };
                rows.push(PortfolioBalanceRow {
                    location,
                    asset: PortfolioAsset::Usdc,
                    available,
                    inflight: balance.inflight().into(),
                });
            }
        }

        for cash_location in PORTFOLIO_CASH_TRANSIT_LOCATIONS {
            if let Some(entry) = self.inflight_cash.get(&cash_location) {
                rows.push(PortfolioBalanceRow {
                    location: match cash_location {
                        InFlightCashLocation::EthereumWallet => PortfolioLocation::EthereumWallet,
                        InFlightCashLocation::BaseWallet => PortfolioLocation::BaseWalletUnwrapped,
                    },
                    asset: PortfolioAsset::Usdc,
                    available: Usdc::ZERO.into(),
                    inflight: entry.amount.into(),
                });
            }
        }

        let inflight_equity_symbols: std::collections::BTreeSet<&Symbol> = self
            .inflight_equity
            .keys()
            .map(|(symbol, _)| symbol)
            .collect();
        for symbol in inflight_equity_symbols {
            for equity_location in PORTFOLIO_EQUITY_TRANSIT_LOCATIONS {
                let Some(entry) = self.inflight_equity.get(&(symbol.clone(), equity_location))
                else {
                    continue;
                };
                rows.push(PortfolioBalanceRow {
                    location: match equity_location {
                        InFlightEquityLocation::BaseWalletUnwrapped => {
                            PortfolioLocation::BaseWalletUnwrapped
                        }
                        InFlightEquityLocation::BaseWalletWrapped => {
                            PortfolioLocation::BaseWalletWrapped
                        }
                    },
                    asset: PortfolioAsset::Equity(symbol.clone()),
                    available: FractionalShares::ZERO.into(),
                    inflight: entry.amount.into(),
                });
            }
        }

        Ok(rows)
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
            onchain_equity_snapshot_block_watermarks: HashMap::new(),
            onchain_usdc_snapshot_block_watermark: None,
            last_offchain_cash_fill_applied_at: None,
            offchain_equity_snapshot_skip_streaks: HashMap::new(),
            offchain_usd_snapshot_skip_streak: 0,
            offchain_usd_snapshot_watermark: None,
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
            pending_offchain_order_symbols: HashSet::new(),
            last_offchain_fill_applied_at: HashMap::new(),
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
    pub(crate) fn equity_available(
        &self,
        symbol: &Symbol,
        venue: Venue,
    ) -> Option<FractionalShares> {
        let inventory = self.equities.get(symbol)?;
        inventory.get_venue(venue).map(VenueBalance::available)
    }

    /// Whether divergence recovery must leave this symbol alone, and why.
    /// `None` means the symbol is quiet and the broker reading is
    /// comparable. The poller freezes the divergence counter on `Some`;
    /// the forced apply aborts on `Some`. Single predicate for both so
    /// detection and apply can never disagree on what counts as busy.
    pub(crate) fn equity_reconciliation_busy(
        &self,
        symbol: &Symbol,
        fetched_at: DateTime<Utc>,
    ) -> Result<Option<EquityReconcileBusy>, FloatError> {
        if self.equity_transfer_busy(symbol)? {
            return Ok(Some(EquityReconcileBusy::Transfer));
        }

        if self.has_pending_offchain_order(symbol) {
            return Ok(Some(EquityReconcileBusy::PendingHedgeOrder));
        }

        if self
            .last_offchain_fill_applied_at
            .get(symbol)
            .is_some_and(|filled_at| fetched_at < *filled_at)
        {
            return Ok(Some(EquityReconcileBusy::FillAfterFetch));
        }

        Ok(None)
    }

    /// Inflight at either venue, or an active mint/redemption owning the
    /// symbol's inflight slot.
    fn equity_transfer_busy(&self, symbol: &Symbol) -> Result<bool, FloatError> {
        let has_inflight = self
            .equities
            .get(symbol)
            .map(Inventory::has_inflight)
            .transpose()?
            .unwrap_or(false);

        Ok(has_inflight
            || self.active_mints.contains_key(symbol)
            || self.active_redemptions.contains_key(symbol))
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
            pending_offchain_order_symbols: self.pending_offchain_order_symbols,
            last_offchain_fill_applied_at: self.last_offchain_fill_applied_at,
            onchain_equity_snapshot_block_watermarks: self.onchain_equity_snapshot_block_watermarks,
            onchain_usdc_snapshot_block_watermark: self.onchain_usdc_snapshot_block_watermark,
            last_offchain_cash_fill_applied_at: self.last_offchain_cash_fill_applied_at,
            offchain_equity_snapshot_skip_streaks: self.offchain_equity_snapshot_skip_streaks,
            offchain_usd_snapshot_skip_streak: self.offchain_usd_snapshot_skip_streak,
            offchain_usd_snapshot_watermark: self.offchain_usd_snapshot_watermark,
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
            pending_offchain_order_symbols: self.pending_offchain_order_symbols,
            last_offchain_fill_applied_at: self.last_offchain_fill_applied_at,
            onchain_equity_snapshot_block_watermarks: self.onchain_equity_snapshot_block_watermarks,
            onchain_usdc_snapshot_block_watermark: self.onchain_usdc_snapshot_block_watermark,
            last_offchain_cash_fill_applied_at: self.last_offchain_cash_fill_applied_at,
            offchain_equity_snapshot_skip_streaks: self.offchain_equity_snapshot_skip_streaks,
            offchain_usd_snapshot_skip_streak: self.offchain_usd_snapshot_skip_streak,
            offchain_usd_snapshot_watermark: self.offchain_usd_snapshot_watermark,
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

    /// Advance the per-symbol onchain block watermarks after an equity
    /// snapshot applied. A no-op for the Hedging venue (broker reads have no
    /// block) and for legacy snapshots without a block number.
    fn record_onchain_equity_block_watermarks<'a>(
        mut self,
        venue: Venue,
        symbols: impl IntoIterator<Item = &'a Symbol>,
        block_number: Option<u64>,
    ) -> Self {
        let (Venue::MarketMaking, Some(block_number)) = (venue, block_number) else {
            return self;
        };

        for symbol in symbols {
            let watermark = self
                .onchain_equity_snapshot_block_watermarks
                .entry(symbol.clone())
                .or_insert(block_number);
            if block_number > *watermark {
                *watermark = block_number;
            }
        }

        self
    }

    /// Force-set the per-symbol onchain block watermarks to a forced read's
    /// block. Unlike [`Self::record_onchain_equity_block_watermarks`], this
    /// bypasses the monotonic maximum: the forced balance is authoritative,
    /// so a watermark left above the forced block would keep absorbing
    /// fills the forced balance does not contain.
    fn force_onchain_equity_block_watermarks<'a>(
        mut self,
        symbols: impl IntoIterator<Item = &'a Symbol>,
        block_number: Option<u64>,
    ) -> Self {
        let Some(block_number) = block_number else {
            return self;
        };

        for symbol in symbols {
            self.onchain_equity_snapshot_block_watermarks
                .insert(symbol.clone(), block_number);
        }

        self
    }

    /// Advance the venue-level onchain USDC block watermark after an
    /// `OnchainUsdc` snapshot applied.
    fn record_onchain_usdc_block_watermark(mut self, block_number: Option<u64>) -> Self {
        let Some(block_number) = block_number else {
            return self;
        };

        let advanced = self
            .onchain_usdc_snapshot_block_watermark
            .is_none_or(|watermark| block_number > watermark);
        if advanced {
            self.onchain_usdc_snapshot_block_watermark = Some(block_number);
        }

        self
    }

    /// Records an applied Hedging cash snapshot's `fetched_at`, keeping the
    /// watermark monotonic: unlike the equity path, the ordinary cash apply
    /// does not gate on this watermark, so an older-but-applying reading
    /// must not lower it.
    fn record_offchain_usd_snapshot_watermark(mut self, fetched_at: DateTime<Utc>) -> Self {
        let advanced = self
            .offchain_usd_snapshot_watermark
            .is_none_or(|watermark| fetched_at > watermark);
        if advanced {
            self.offchain_usd_snapshot_watermark = Some(fetched_at);
        }

        self
    }

    /// Whether a MarketMaking equity fill delta at `block_number` is already
    /// contained in an applied onchain snapshot (ADR 0018). Legacy fills
    /// without a block are never treated as absorbed.
    pub(crate) fn onchain_fill_absorbed_by_equity_snapshot(
        &self,
        symbol: &Symbol,
        block_number: Option<u64>,
    ) -> bool {
        let Some(block_number) = block_number else {
            return false;
        };

        self.onchain_equity_snapshot_block_watermarks
            .get(symbol)
            .is_some_and(|watermark| block_number <= *watermark)
    }

    /// Whether a MarketMaking USDC fill delta at `block_number` is already
    /// contained in an applied onchain USDC snapshot (ADR 0018).
    pub(crate) fn onchain_fill_absorbed_by_usdc_snapshot(&self, block_number: Option<u64>) -> bool {
        let Some(block_number) = block_number else {
            return false;
        };

        self.onchain_usdc_snapshot_block_watermark
            .is_some_and(|watermark| block_number <= watermark)
    }

    /// Marks a symbol as having an open offchain order, so offchain equity
    /// snapshots stop applying to it until the order reaches a terminal state.
    pub(crate) fn mark_offchain_order_pending(&mut self, symbol: Symbol) {
        self.pending_offchain_order_symbols.insert(symbol);
    }

    /// Whether a symbol has an open offchain order.
    ///
    /// Single source of truth for that state: it gates offchain equity
    /// snapshots here and equity rebalancing dispatch in the rebalancing
    /// trigger. One copy under one lock keeps the two from disagreeing.
    pub(crate) fn has_pending_offchain_order(&self, symbol: &Symbol) -> bool {
        self.pending_offchain_order_symbols.contains(symbol)
    }

    /// Replaces the set of symbols with an open offchain order, for rebuilding
    /// it from the `Position` projection at startup.
    pub(crate) fn set_pending_offchain_order_symbols(&mut self, symbols: HashSet<Symbol>) {
        self.pending_offchain_order_symbols = symbols;
    }

    /// Releases the snapshot block for a symbol whose offchain order reached a
    /// terminal state.
    ///
    /// `applied_fill_at` carries the local-clock time of a fill whose delta was
    /// applied to the mirror, and is `None` when nothing was applied (a failed
    /// or cancelled order, or a fill whose update errored). It must be a local
    /// reading, never the event's `broker_timestamp`: it is compared against
    /// snapshot `fetched_at`, which the snapshot aggregate stamps from this
    /// host's clock, so mixing the two would compare two unsynchronized clocks.
    /// `applied_fill_at` being `Some` records the fill application time on
    /// BOTH the symbol's equity guard and the venue-level cash guard: the
    /// fill's equity and mirrored-USDC legs apply atomically (`and_then`
    /// chaining in the reactor), so a stamped equity fill always implies a
    /// stamped cash fill. One entry point keeps the two stamps impossible to
    /// desynchronize.
    pub(crate) fn clear_offchain_order_pending(
        &mut self,
        symbol: &Symbol,
        applied_fill_at: Option<DateTime<Utc>>,
    ) {
        self.pending_offchain_order_symbols.remove(symbol);

        if let Some(applied_fill_at) = applied_fill_at {
            self.last_offchain_fill_applied_at
                .insert(symbol.clone(), applied_fill_at);
            self.last_offchain_cash_fill_applied_at = Some(applied_fill_at);
        }
    }

    /// A fresh default view retaining only the offchain-order guard state
    /// (pending orders and applied-fill times) and, for each gated symbol,
    /// the Hedging available balance that state guards.
    ///
    /// Snapshot-error recovery resets the view before force-applying the
    /// failed snapshot. The guard state must survive that reset: it is derived
    /// from the `Position` event stream, not from snapshots, and nothing
    /// re-seeds it after startup — wiping it would re-admit offchain snapshots
    /// for symbols whose hedge order is still open, re-opening the
    /// double-apply race. The delta-owned balance must survive with it: while
    /// the gate is held, snapshots are skipped, so nothing can repopulate a
    /// wiped balance — the symbol would sit uninitialized and imbalance
    /// detection would silently stop for it. Inflight is deliberately NOT
    /// carried (stuck inflight is the wedge class recovery exists to clear)
    /// and the onchain venue is left uninitialized (it is not delta-owned and
    /// nothing blocks its repopulation). Every other field is intentionally
    /// defaulted, which is why this uses functional-update syntax rather than
    /// an exhaustive literal: a future field should default here unless it is
    /// guard state.
    pub(crate) fn reset_preserving_offchain_order_state(&self) -> Self {
        let equities = self
            .equities
            .iter()
            .filter(|(symbol, _)| self.pending_offchain_order_symbols.contains(*symbol))
            .filter_map(|(symbol, inventory)| {
                inventory.get_venue(Venue::Hedging).map(|balance| {
                    (
                        symbol.clone(),
                        Inventory {
                            onchain: None,
                            offchain: Some(VenueBalance::new(
                                balance.available(),
                                FractionalShares::ZERO,
                            )),
                            last_rebalancing: None,
                        },
                    )
                })
            })
            .collect();

        Self {
            equities,
            pending_offchain_order_symbols: self.pending_offchain_order_symbols.clone(),
            last_offchain_fill_applied_at: self.last_offchain_fill_applied_at.clone(),
            last_offchain_cash_fill_applied_at: self.last_offchain_cash_fill_applied_at,
            ..Self::default()
        }
    }

    /// Note a skipped Hedging equity snapshot for `symbol`, warning every
    /// [`OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY`] consecutive skips. ADR 0015
    /// accepted guard starvation but left it invisible at production log
    /// levels; this is the missing signal. MarketMaking skips are not
    /// tracked: that venue's snapshots are only ever skipped by transfer
    /// inflight, which is already observable state.
    fn note_offchain_equity_snapshot_skip(mut self, symbol: &Symbol, venue: Venue) -> Self {
        if venue != Venue::Hedging {
            return self;
        }

        let streak = self
            .offchain_equity_snapshot_skip_streaks
            .entry(symbol.clone())
            .or_insert(0);
        *streak += 1;

        if streak.is_multiple_of(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY) {
            warn!(
                target: "inventory",
                %symbol,
                consecutive_skips = *streak,
                "Offchain equity snapshots for this symbol keep being \
                 skipped; its Hedging balance is not receiving broker truth"
            );
        }

        self
    }

    fn reset_offchain_equity_snapshot_skip(mut self, symbol: &Symbol, venue: Venue) -> Self {
        if venue == Venue::Hedging {
            self.offchain_equity_snapshot_skip_streaks.remove(symbol);
        }
        self
    }

    /// Note an `OffchainUsd` snapshot skipped by the venue-level cash
    /// guards, warning every [`OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY`]
    /// consecutive skips.
    fn note_offchain_usd_snapshot_skip(mut self) -> Self {
        self.offchain_usd_snapshot_skip_streak += 1;

        if self
            .offchain_usd_snapshot_skip_streak
            .is_multiple_of(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY)
        {
            warn!(
                target: "inventory",
                consecutive_skips = self.offchain_usd_snapshot_skip_streak,
                "Offchain USD snapshots keep being skipped; the Hedging cash \
                 balance is not receiving broker truth"
            );
        }

        self
    }

    /// The Hedging cash-balance twin of the ADR 0015 guards in
    /// [`Self::equity_snapshot_would_apply`]: the hedge fill applies a
    /// mirrored USDC delta at the Hedging venue, and the `OffchainUsd`
    /// snapshot comes from the same broker read as the equity positions, so
    /// the cash leg double-counts in exactly the window the equity guards
    /// close. The cash balance is venue-level, so guard 1 broadens from
    /// per-symbol membership to "any hedge order open" -- an open order on
    /// ANY symbol makes the venue's cash reading ambiguous.
    fn offchain_usd_snapshot_would_apply(&self, fetched_at: DateTime<Utc>) -> bool {
        if !self.pending_offchain_order_symbols.is_empty() {
            debug!(
                target: "inventory",
                pending_symbols = ?self.pending_offchain_order_symbols,
                ?fetched_at,
                "Skipping offchain USD snapshot: a hedge order is still open",
            );
            return false;
        }

        if self
            .last_offchain_cash_fill_applied_at
            .is_some_and(|applied_at| fetched_at < applied_at)
        {
            debug!(
                target: "inventory",
                ?fetched_at,
                last_cash_fill_applied_at = ?self.last_offchain_cash_fill_applied_at,
                "Skipping offchain USD snapshot fetched before the last \
                 applied hedge fill's cash leg",
            );
            return false;
        }

        true
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

        // Both venues have a second writer (the fill delta), but they are
        // reconciled in opposite directions. Offchain: no causal signal ties
        // a broker read to a fill, so the snapshot yields to the delta while
        // a hedge order owns the balance (the guards below). Onchain: both
        // writers derive from the same chain, so the DELTA yields instead --
        // a fill covered by an applied snapshot's block watermark is skipped
        // at the source (`onchain_fill_absorbed_by_*`, ADR 0018) and
        // snapshots here are never blocked. Exhaustive so a new venue forces
        // a decision on how its second writer is reconciled.
        match venue {
            Venue::MarketMaking => {}
            Venue::Hedging => {
                if self.pending_offchain_order_symbols.contains(symbol) {
                    debug!(
                        target: "inventory",
                        %symbol,
                        ?fetched_at,
                        "Skipping offchain equity snapshot: hedge order still open",
                    );
                    return Ok(false);
                }

                if self
                    .last_offchain_fill_applied_at
                    .get(symbol)
                    .is_some_and(|filled_at| fetched_at < *filled_at)
                {
                    debug!(
                        target: "inventory",
                        %symbol,
                        ?fetched_at,
                        "Skipping offchain equity snapshot: predates last applied fill",
                    );
                    return Ok(false);
                }
            }
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
        block_number: Option<u64>,
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
                // Block ordering is authoritative for onchain reads (ADR
                // 0018): a read pinned below the symbol's applied watermark
                // would set a balance missing fills the watermark already
                // absorbs. Per symbol, mirroring the `OnchainUsdc` arm's
                // venue-level check.
                if venue == Venue::MarketMaking
                    && let Some(block) = block_number
                    && view
                        .onchain_equity_snapshot_block_watermarks
                        .get(symbol)
                        .is_some_and(|watermark| block < *watermark)
                {
                    warn!(
                        target: "inventory",
                        %symbol,
                        block,
                        "Rejecting onchain equity snapshot pinned below the \
                         symbol's applied block watermark"
                    );
                    return Ok((view, applied_symbols));
                }

                let should_record_watermark =
                    view.equity_snapshot_would_apply(symbol, venue, fetched_at)?;
                if !should_record_watermark {
                    let view = view.note_offchain_equity_snapshot_skip(symbol, venue);
                    return Ok::<_, InventoryViewError>((view, applied_symbols));
                }

                let view = view
                    .update_equity(
                        symbol,
                        Inventory::on_snapshot(venue, *snapshot_balance, fetched_at),
                        now,
                    )?
                    .reset_offchain_equity_snapshot_skip(symbol, venue);

                applied_symbols.push(symbol.clone());

                Ok::<_, InventoryViewError>((view, applied_symbols))
            },
        )?;

        Ok(view
            .record_equity_snapshot_watermarks(venue, applied_symbols.iter(), fetched_at)
            .record_onchain_equity_block_watermarks(venue, applied_symbols.iter(), block_number))
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
            pending_offchain_order_symbols: self.pending_offchain_order_symbols,
            last_offchain_fill_applied_at: self.last_offchain_fill_applied_at,
            onchain_equity_snapshot_block_watermarks: self.onchain_equity_snapshot_block_watermarks,
            onchain_usdc_snapshot_block_watermark: self.onchain_usdc_snapshot_block_watermark,
            last_offchain_cash_fill_applied_at: self.last_offchain_cash_fill_applied_at,
            offchain_equity_snapshot_skip_streaks: self.offchain_equity_snapshot_skip_streaks,
            offchain_usd_snapshot_skip_streak: self.offchain_usd_snapshot_skip_streak,
            offchain_usd_snapshot_watermark: self.offchain_usd_snapshot_watermark,
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
            pending_offchain_order_symbols: self.pending_offchain_order_symbols,
            last_offchain_fill_applied_at: self.last_offchain_fill_applied_at,
            onchain_equity_snapshot_block_watermarks: self.onchain_equity_snapshot_block_watermarks,
            onchain_usdc_snapshot_block_watermark: self.onchain_usdc_snapshot_block_watermark,
            last_offchain_cash_fill_applied_at: self.last_offchain_cash_fill_applied_at,
            offchain_equity_snapshot_skip_streaks: self.offchain_equity_snapshot_skip_streaks,
            offchain_usd_snapshot_skip_streak: self.offchain_usd_snapshot_skip_streak,
            offchain_usd_snapshot_watermark: self.offchain_usd_snapshot_watermark,
        })
    }

    /// Returns the aggregate ID of the in-flight USDC rebalance, if any.
    #[cfg(test)]
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

    /// Force apply a broker confirmed offchain equity value after the
    /// poller escalated a persistent snapshot divergence.
    ///
    /// Bypasses the `last_rebalancing` and inflight staleness guards that
    /// wedged the symbol, but validates again, under the caller's write
    /// lock at apply time, that the symbol is still not busy and that no
    /// fresher snapshot applied since the escalation's reading was
    /// fetched. Either condition aborts unapplied; the poller reads the
    /// view back after the send and re-escalates once the symbol is quiet.
    /// Advances the symbol's Hedging watermark on success and
    /// deliberately does NOT stamp `last_rebalancing`: stamping it on
    /// every failed cleanup is what keeps the guards armed and the
    /// divergence unresolvable through ordinary snapshots.
    pub(crate) fn reconcile_offchain_equity(
        self,
        symbol: &Symbol,
        broker_position: FractionalShares,
        ledger_position: Option<FractionalShares>,
        consecutive_polls: u32,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        if self
            .equity_snapshot_watermark(symbol, Venue::Hedging)
            .is_some_and(|watermark| fetched_at <= watermark)
        {
            warn!(
                target: "inventory",
                %symbol,
                "Aborting offchain equity reconcile: a fresher snapshot \
                 already applied for this symbol"
            );
            return Ok(self);
        }

        if let Some(reason) = self.equity_reconciliation_busy(symbol, fetched_at)? {
            warn!(
                target: "inventory",
                %symbol,
                ?reason,
                "Aborting offchain equity reconcile; the poller will \
                 re-escalate once the symbol is quiet"
            );
            return Ok(self);
        }

        error!(
            target: "inventory",
            %symbol,
            ledger = ?ledger_position,
            broker = %broker_position,
            polls = consecutive_polls,
            "Force-reconciling offchain equity after persistent snapshot \
             divergence"
        );

        let witness = PersistentBrokerDivergence {
            symbol: symbol.clone(),
            ledger_value: ledger_position,
            broker_value: broker_position,
            polls: consecutive_polls,
        };

        let view = self.update_equity(
            symbol,
            Inventory::force_on_snapshot(Venue::Hedging, broker_position, witness),
            now,
        )?;

        Ok(view.record_equity_snapshot_watermarks(Venue::Hedging, [symbol], fetched_at))
    }

    /// Why cash divergence recovery must leave the venue alone this poll --
    /// the venue-level twin of [`Self::equity_reconciliation_busy`], shared
    /// by detection (freezes the counter) and the forced apply (aborts) so
    /// the two can never disagree. Reuses [`EquityReconcileBusy`]: the busy
    /// taxonomy is identical, only the scope widens from one symbol to the
    /// venue.
    pub(crate) fn cash_reconciliation_busy(
        &self,
        fetched_at: DateTime<Utc>,
    ) -> Result<Option<EquityReconcileBusy>, FloatError> {
        if self.usdc.has_inflight()? || self.active_usdc_rebalance.is_some() {
            return Ok(Some(EquityReconcileBusy::Transfer));
        }

        if !self.pending_offchain_order_symbols.is_empty() {
            return Ok(Some(EquityReconcileBusy::PendingHedgeOrder));
        }

        if self
            .last_offchain_cash_fill_applied_at
            .is_some_and(|filled_at| fetched_at < filled_at)
        {
            return Ok(Some(EquityReconcileBusy::FillAfterFetch));
        }

        Ok(None)
    }

    /// The venue-level cash twin of [`Self::reconcile_offchain_equity`]:
    /// force the broker's available cash over the view's Hedging USDC after
    /// the poller confirmed a persistent divergence. Aborts while the venue
    /// is busy; the poller re-escalates once it is quiet.
    pub(crate) fn reconcile_offchain_usd(
        self,
        usd_balance_cents: i64,
        gross_usd_cents: Option<i64>,
        ledger_usdc: Option<Usdc>,
        consecutive_polls: u32,
        fetched_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        if self
            .offchain_usd_snapshot_watermark
            .is_some_and(|watermark| fetched_at <= watermark)
        {
            warn!(
                target: "inventory",
                "Aborting offchain USD reconcile: a fresher cash snapshot \
                 already applied"
            );
            return Ok(self);
        }

        if let Some(reason) = self.cash_reconciliation_busy(fetched_at)? {
            warn!(
                target: "inventory",
                ?reason,
                "Aborting offchain USD reconcile; the poller will \
                 re-escalate once the venue is quiet"
            );
            return Ok(self);
        }

        let broker_usdc = Usdc::from_cents(usd_balance_cents)
            .ok_or(InventoryViewError::UsdBalanceConversion(usd_balance_cents))?;

        error!(
            target: "inventory",
            ledger = ?ledger_usdc,
            broker_cents = usd_balance_cents,
            polls = consecutive_polls,
            "Force-reconciling offchain USD after persistent snapshot \
             divergence"
        );

        let witness = PersistentBrokerCashDivergence {
            ledger_usdc,
            broker_usd_cents: usd_balance_cents,
            polls: consecutive_polls,
        };

        let view = self.update_usdc(
            Inventory::force_on_snapshot(Venue::Hedging, broker_usdc, witness),
            now,
        )?;

        Ok(Self {
            offchain_gross_usd_cents: gross_usd_cents,
            offchain_usd_snapshot_skip_streak: 0,
            ..view
        }
        .record_offchain_usd_snapshot_watermark(fetched_at))
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
            OnchainEquity {
                balances,
                block_number,
                ..
            } => self.apply_equity_snapshot(
                Venue::MarketMaking,
                balances.iter(),
                fetched_at,
                *block_number,
                now,
            ),

            OnchainUsdc {
                usdc_balance,
                block_number,
                ..
            } => {
                // Block ordering is authoritative for onchain reads (ADR
                // 0018): a read pinned below the applied watermark would set
                // a balance that does not contain fills the watermark
                // already absorbs, understating USDC until the next poll.
                if let (Some(block_number), Some(watermark)) =
                    (*block_number, self.onchain_usdc_snapshot_block_watermark)
                    && block_number < watermark
                {
                    warn!(
                        target: "inventory",
                        block_number,
                        watermark,
                        "Rejecting onchain USDC snapshot pinned below the \
                         applied block watermark"
                    );
                    return Ok(self);
                }

                // The closure silently skips when the snapshot cannot apply
                // (inflight, stale), so consult the same predicate first: the
                // block watermark must only advance for balances the view
                // actually took.
                let applies = self.usdc.snapshot_would_apply(fetched_at)?;
                let block_number = *block_number;
                let view = self.update_usdc(
                    Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance, fetched_at),
                    now,
                )?;
                Ok(if applies {
                    view.record_onchain_usdc_block_watermark(block_number)
                } else {
                    view
                })
            }

            OffchainEquity { positions, .. } => {
                self.apply_equity_snapshot(Venue::Hedging, positions.iter(), fetched_at, None, now)
            }

            OffchainEquityReconciled {
                symbol,
                position,
                ledger_position,
                consecutive_polls,
                ..
            } => self.reconcile_offchain_equity(
                symbol,
                *position,
                *ledger_position,
                *consecutive_polls,
                fetched_at,
                now,
            ),

            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                ..
            } => {
                // Gross is skipped along with the net balance: both come
                // from the same ambiguous broker read. The inventory-level
                // predicate mirrors what `on_snapshot` would silently skip
                // (inflight, stale), so those skips count toward the streak
                // instead of vanishing.
                if !self.offchain_usd_snapshot_would_apply(fetched_at)
                    || !self.usdc.snapshot_would_apply(fetched_at)?
                {
                    return Ok(self.note_offchain_usd_snapshot_skip());
                }

                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                let updated = self.update_usdc(
                    Inventory::on_snapshot(Venue::Hedging, usdc, fetched_at),
                    now,
                )?;
                Ok(Self {
                    offchain_gross_usd_cents: *gross_usd_cents,
                    offchain_usd_snapshot_skip_streak: 0,
                    ..updated
                }
                .record_offchain_usd_snapshot_watermark(fetched_at))
            }

            OffchainUsdReconciled {
                usd_balance_cents,
                gross_usd_cents,
                ledger_usdc,
                consecutive_polls,
                ..
            } => self.reconcile_offchain_usd(
                *usd_balance_cents,
                *gross_usd_cents,
                *ledger_usdc,
                *consecutive_polls,
                fetched_at,
                now,
            ),

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
                block_number,
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
                    // The forced balance is authoritative, so the block
                    // watermark must follow it exactly instead of retaining
                    // a higher block whose fills this balance does not
                    // contain.
                    view.record_equity_snapshot_watermarks(
                        Venue::MarketMaking,
                        balances.keys(),
                        *fetched_at,
                    )
                    .force_onchain_equity_block_watermarks(balances.keys(), *block_number)
                }),

            OnchainUsdc {
                usdc_balance,
                block_number,
                ..
            } => {
                let block_number = *block_number;
                self.update_usdc(
                    Inventory::force_on_snapshot(Venue::MarketMaking, *usdc_balance, reason),
                    now,
                )
                // Same as the equity arm above: the forced balance is
                // authoritative, so the watermark follows it exactly rather
                // than keeping the monotonic maximum.
                .map(|mut view| {
                    if let Some(block_number) = block_number {
                        view.onchain_usdc_snapshot_block_watermark = Some(block_number);
                    }
                    view
                })
            }

            OffchainEquity {
                positions,
                fetched_at,
            } => positions
                .iter()
                .try_fold(self, |view, (symbol, snapshot_balance)| {
                    // The force path bypasses the staleness guards, not the
                    // ownership one: a symbol with an open hedge order keeps
                    // its delta-owned balance, or the recovery path re-opens
                    // the snapshot-vs-fill double-count. Skipped symbols also
                    // keep their watermark un-advanced below, so the
                    // post-clear healing emission still applies.
                    if view.has_pending_offchain_order(symbol) {
                        return Ok(view);
                    }

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
                    let applied: Vec<&Symbol> = positions
                        .keys()
                        .filter(|symbol| !view.has_pending_offchain_order(symbol))
                        .collect();
                    view.record_equity_snapshot_watermarks(Venue::Hedging, applied, *fetched_at)
                }),

            // `reconcile_offchain_equity` validates busyness and the hedge
            // gate itself, so the recovery path routes through the same
            // logic instead of a second force write.
            OffchainEquityReconciled {
                symbol,
                position,
                ledger_position,
                consecutive_polls,
                fetched_at,
            } => self.reconcile_offchain_equity(
                symbol,
                *position,
                *ledger_position,
                *consecutive_polls,
                *fetched_at,
                now,
            ),

            // Same routing as the equity reconcile: the cash reconcile
            // validates venue busyness itself.
            OffchainUsdReconciled {
                usd_balance_cents,
                gross_usd_cents,
                ledger_usdc,
                consecutive_polls,
                fetched_at,
            } => self.reconcile_offchain_usd(
                *usd_balance_cents,
                *gross_usd_cents,
                *ledger_usdc,
                *consecutive_polls,
                *fetched_at,
                now,
            ),

            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
            } => {
                // The force path bypasses the staleness guards, not the
                // ownership one (mirrors the OffchainEquity arm above):
                // while any hedge order is open, the fill delta owns the
                // venue cash balance, and force-writing the ambiguous
                // mid-order reading would re-open the double-count on the
                // recovery path.
                if !self.pending_offchain_order_symbols.is_empty() {
                    warn!(
                        target: "inventory",
                        pending_symbols = ?self.pending_offchain_order_symbols,
                        "Skipping forced offchain USD snapshot: a hedge \
                         order is still open",
                    );
                    return Ok(self);
                }

                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                let updated = self.update_usdc(
                    Inventory::force_on_snapshot(Venue::Hedging, usdc, reason),
                    now,
                )?;
                Ok(Self {
                    offchain_gross_usd_cents: *gross_usd_cents,
                    ..updated
                }
                .record_offchain_usd_snapshot_watermark(*fetched_at))
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
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::{Duration, TimeZone, Utc};
    use proptest::prelude::*;
    use rain_math_float::Float;
    use uuid::Uuid;

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
            pending_offchain_order_symbols: HashSet::new(),
            last_offchain_fill_applied_at: HashMap::new(),
            onchain_equity_snapshot_block_watermarks: HashMap::new(),
            onchain_usdc_snapshot_block_watermark: None,
            last_offchain_cash_fill_applied_at: None,
            offchain_equity_snapshot_skip_streaks: HashMap::new(),
            offchain_usd_snapshot_skip_streak: 0,
            offchain_usd_snapshot_watermark: None,
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
            pending_offchain_order_symbols: HashSet::new(),
            last_offchain_fill_applied_at: HashMap::new(),
            onchain_equity_snapshot_block_watermarks: HashMap::new(),
            onchain_usdc_snapshot_block_watermark: None,
            last_offchain_cash_fill_applied_at: None,
            offchain_equity_snapshot_skip_streaks: HashMap::new(),
            offchain_usd_snapshot_skip_streak: 0,
            offchain_usd_snapshot_watermark: None,
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
    fn pending_offchain_order_skips_hedging_snapshot_without_advancing_watermark() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_equity(aapl.clone(), shares(20), shares(100));
        view.mark_offchain_order_pending(aapl.clone());

        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), shares(90));
        let snapshot = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: now,
        };

        let mut view = view.apply_snapshot_event(&snapshot, now).unwrap();
        assert_eq!(
            view.equity_available(&aapl, Venue::Hedging),
            Some(shares(100)),
            "snapshot must not apply while a hedge order is open",
        );

        // Re-delivering the *same* snapshot after the order terminates must
        // apply: the skip must not advance the watermark, or `fetched_at <=
        // watermark` would reject the retry. (Whether the aggregate re-emits
        // an unchanged value at all is a separate, currently-open concern —
        // its dedupe suppresses it; this pins the view-side precondition.)
        view.clear_offchain_order_pending(&aapl, None);
        let view = view.apply_snapshot_event(&snapshot, now).unwrap();
        assert_eq!(
            view.equity_available(&aapl, Venue::Hedging),
            Some(shares(90)),
            "the first snapshot after the order terminates must heal the balance",
        );
    }

    #[test]
    fn snapshot_predating_last_applied_fill_is_skipped() {
        let aapl = Symbol::new("AAPL").unwrap();
        let applied_at = Utc::now();

        // A 10-share sell was already applied to the mirror (100 -> 90).
        let mut view = InventoryView::default().with_equity(aapl.clone(), shares(20), shares(90));
        view.clear_offchain_order_pending(&aapl, Some(applied_at));

        // A poll read before the fill executed reports the pre-fill 100; its
        // event lands after the fill was applied. Applying it would resurrect
        // the pre-fill balance.
        let mut stale_positions = BTreeMap::new();
        stale_positions.insert(aapl.clone(), shares(100));
        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: stale_positions,
                    fetched_at: applied_at - Duration::seconds(1),
                },
                applied_at,
            )
            .unwrap();
        assert_eq!(
            view.equity_available(&aapl, Venue::Hedging),
            Some(shares(90)),
            "a snapshot fetched before the applied fill must not overwrite it",
        );

        // Boundary: the guard is strict (`fetched_at < applied_at`), so a
        // snapshot stamped at exactly the applied time is fresh and applies.
        // 85 is deliberately distinct from both 90 and 100 so application is
        // observable.
        let mut boundary_positions = BTreeMap::new();
        boundary_positions.insert(aapl.clone(), shares(85));
        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: boundary_positions,
                    fetched_at: applied_at,
                },
                applied_at,
            )
            .unwrap();
        assert_eq!(
            view.equity_available(&aapl, Venue::Hedging),
            Some(shares(85)),
            "a snapshot stamped exactly at the applied-fill time must apply",
        );
    }

    #[test]
    fn marketmaking_snapshot_unaffected_by_offchain_order_guards() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        // Populate both guard fields: an applied-fill time, then a re-opened
        // pending order.
        let mut view = InventoryView::default().with_equity(aapl.clone(), shares(50), shares(100));
        view.clear_offchain_order_pending(&aapl, Some(now));
        view.mark_offchain_order_pending(aapl.clone());

        // An onchain snapshot older than the applied fill, arriving while the
        // hedge order is open, must still apply: both guards are scoped to the
        // Hedging venue.
        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), shares(60));
        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances,
                    fetched_at: now - Duration::seconds(1),
                    block_number: None,
                },
                now,
            )
            .unwrap();
        assert_eq!(
            view.equity_available(&aapl, Venue::MarketMaking),
            Some(shares(60)),
            "MarketMaking snapshots must ignore the offchain-order guards: \
             the onchain venue's second writer is reconciled by absorbing \
             the fill delta (ADR 0018), never by blocking snapshots",
        );
        assert_eq!(
            view.equity_available(&aapl, Venue::Hedging),
            Some(shares(100)),
            "the offchain balance is untouched by an onchain snapshot",
        );
    }

    /// The hydration path replays persisted snapshot events through
    /// `apply_snapshot_event`, so applying onchain events with blocks must
    /// re-establish the absorption watermarks -- this is what closes the
    /// restart window for the onchain venue (blocks are durable, unlike the
    /// offchain guards' local-clock state).
    #[test]
    fn replayed_onchain_snapshots_restore_block_watermarks() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances: BTreeMap::from([(aapl.clone(), shares(60))]),
                    fetched_at: now,
                    block_number: Some(100),
                },
                now,
            )
            .unwrap()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::new(float!(8500)),
                    fetched_at: now,
                    block_number: Some(100),
                },
                now,
            )
            .unwrap();

        assert!(
            view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(100)),
            "a fill at the snapshot's block is absorbed"
        );
        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(101)),
            "a fill past the snapshot's block is not absorbed"
        );
        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, None),
            "a legacy fill without a block is never absorbed"
        );
        assert!(
            view.onchain_fill_absorbed_by_usdc_snapshot(Some(100)),
            "the USDC leg mirrors the equity behavior at the venue level"
        );
    }

    /// A skipped snapshot must not advance the block watermarks: the balance
    /// it carried was never taken, so a fill it contains is NOT yet in the
    /// view and its delta must still apply.
    #[test]
    fn skipped_onchain_snapshots_advance_no_block_watermarks() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        // Inflight at the equity inventory blocks the equity snapshot;
        // inflight at the USDC inventory blocks the USDC snapshot.
        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(50), shares(50))
            .update_equity(
                &aapl,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(5)),
                now,
            )
            .unwrap()
            .with_usdc_inflight(
                Usdc::new(float!(1000)),
                Usdc::new(float!(400)),
                Usdc::new(float!(1000)),
                Usdc::ZERO,
            );

        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances: BTreeMap::from([(aapl.clone(), shares(60))]),
                    fetched_at: now,
                    block_number: Some(100),
                },
                now,
            )
            .unwrap()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::new(float!(8500)),
                    fetched_at: now,
                    block_number: Some(100),
                },
                now,
            )
            .unwrap();

        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(100)),
            "an inflight-skipped equity snapshot must not mark fills absorbed"
        );
        assert!(
            !view.onchain_fill_absorbed_by_usdc_snapshot(Some(100)),
            "an inflight-skipped USDC snapshot must not mark fills absorbed"
        );
    }

    /// Snapshots can be delivered out of order (load-balanced RPC nodes can
    /// serve a later request from a lagging node). An older block must never
    /// lower an established watermark, or a fill between the two blocks
    /// would be absorbed twice.
    #[test]
    fn out_of_order_onchain_snapshots_never_lower_block_watermarks() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let apply = |view: InventoryView, block: u64, balance: i64, fetched_at: DateTime<Utc>| {
            view.apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances: BTreeMap::from([(aapl.clone(), shares(balance))]),
                    fetched_at,
                    block_number: Some(block),
                },
                now,
            )
            .unwrap()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::new(float!(&balance.to_string())),
                    fetched_at,
                    block_number: Some(block),
                },
                now,
            )
            .unwrap()
        };

        // Block 200 lands first, then a late snapshot from block 100 with a
        // fresher fetched_at. Block ordering is authoritative for onchain
        // reads: the late lower-block snapshot must be rejected outright --
        // its balance is missing fills the watermark already absorbs.
        let view = apply(InventoryView::default(), 200, 60, now);
        let view = apply(view, 100, 30, now + Duration::seconds(1));

        assert_eq!(
            view.equity_available(&aapl, Venue::MarketMaking),
            Some(shares(60)),
            "a below-watermark equity snapshot must not replace the balance"
        );
        assert_eq!(
            view.usdc_available(Venue::MarketMaking),
            Some(Usdc::new(float!(60))),
            "a below-watermark USDC snapshot must not replace the balance"
        );
        assert!(
            view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(150)),
            "the equity watermark must stay at the highest applied block"
        );
        assert!(
            view.onchain_fill_absorbed_by_usdc_snapshot(Some(150)),
            "the USDC watermark must stay at the highest applied block"
        );
        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(201)),
            "a fill past the highest applied block is still not absorbed"
        );
    }

    /// The recovery force path is the one writer allowed to move a balance
    /// BELOW the block watermark, so the watermark must follow the forced
    /// block exactly: keeping the monotonic maximum would leave fills
    /// between the forced block and the old watermark absorbed while the
    /// forced balance does not contain them.
    #[test]
    fn force_apply_resets_block_watermarks_to_the_forced_block() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let reason = Arc::new(InventoryViewError::UsdBalanceConversion(i64::MAX));

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances: BTreeMap::from([(aapl.clone(), shares(60))]),
                    fetched_at: now,
                    block_number: Some(200),
                },
                now,
            )
            .unwrap()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::new(float!(8500)),
                    fetched_at: now,
                    block_number: Some(200),
                },
                now,
            )
            .unwrap();

        let forced = view
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::OnchainEquity {
                    balances: BTreeMap::from([(aapl.clone(), shares(30))]),
                    fetched_at: now + Duration::seconds(1),
                    block_number: Some(100),
                },
                now,
                reason.clone(),
            )
            .unwrap()
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::new(float!(4000)),
                    fetched_at: now + Duration::seconds(1),
                    block_number: Some(100),
                },
                now,
                reason,
            )
            .unwrap();

        assert_eq!(
            forced.equity_available(&aapl, Venue::MarketMaking),
            Some(shares(30)),
            "the force path always writes the balance"
        );
        assert!(
            !forced.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(150)),
            "the equity watermark must drop to the forced block; a fill \
             above it is not absorbed by the forced balance"
        );
        assert!(
            forced.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(100)),
            "fills at or below the forced block are absorbed"
        );
        assert!(
            !forced.onchain_fill_absorbed_by_usdc_snapshot(Some(150)),
            "the USDC watermark must drop to the forced block"
        );
        assert!(forced.onchain_fill_absorbed_by_usdc_snapshot(Some(100)));
    }

    /// `#[serde(default)]` on the two block-watermark fields is what keeps
    /// a view payload persisted before this change loadable; legacy state
    /// must deserialize with empty watermarks and absorb nothing.
    #[test]
    fn legacy_view_payload_without_watermark_fields_absorbs_nothing() {
        let aapl = Symbol::new("AAPL").unwrap();

        let mut payload = serde_json::to_value(InventoryView::default()).unwrap();
        let map = payload.as_object_mut().unwrap();
        assert!(
            map.remove("onchain_equity_snapshot_block_watermarks")
                .is_some(),
            "field name drifted; this test no longer removes anything"
        );
        assert!(
            map.remove("onchain_usdc_snapshot_block_watermark")
                .is_some(),
            "field name drifted; this test no longer removes anything"
        );

        let view: InventoryView = serde_json::from_value(payload).unwrap();

        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(1)),
            "legacy state without watermark fields must absorb no equity fills"
        );
        assert!(
            !view.onchain_fill_absorbed_by_usdc_snapshot(Some(1)),
            "legacy state without watermark fields must absorb no USDC fills"
        );
    }

    /// The Hedging arm of the recorder must stay inert even if a future
    /// call site passes a block for a broker read: chain blocks order
    /// onchain reads only.
    #[test]
    fn hedging_venue_records_no_block_watermark() {
        let aapl = Symbol::new("AAPL").unwrap();

        let view = InventoryView::default().record_onchain_equity_block_watermarks(
            Venue::Hedging,
            [&aapl],
            Some(100),
        );

        assert!(
            !view.onchain_fill_absorbed_by_equity_snapshot(&aapl, Some(100)),
            "a Hedging recording must not create an onchain block watermark"
        );
    }

    /// The recovery force path bypasses the staleness guards by design, but
    /// it must not bypass the open-hedge gate: a symbol with an open order
    /// owns its balance via the fill delta, and force-writing the ambiguous
    /// mid-order snapshot value re-opens the double-count the gate exists to
    /// prevent -- on the recovery path, which exists precisely for when
    /// things already went wrong.
    #[test]
    fn force_apply_offchain_snapshot_respects_open_hedge_gate() {
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default()
            .with_equity(aapl.clone(), shares(20), shares(100))
            .with_equity(tsla.clone(), shares(10), shares(50));
        view.mark_offchain_order_pending(aapl.clone());

        let positions = BTreeMap::from([(aapl.clone(), shares(90)), (tsla.clone(), shares(45))]);
        let forced = view
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: now,
                },
                now,
                Arc::new(InventoryViewError::UsdBalanceConversion(0)),
            )
            .unwrap();

        assert_eq!(
            forced.equity_available(&aapl, Venue::Hedging),
            Some(shares(100)),
            "a symbol with an open hedge order owns its balance via the fill \
             delta; even the force path must not overwrite it"
        );
        assert_eq!(
            forced.equity_available(&tsla, Venue::Hedging),
            Some(shares(45)),
            "ungated symbols must still force-apply"
        );
    }

    #[test]
    fn reset_preserves_guard_state_and_delta_owned_balances_only() {
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let applied_at = Utc::now();

        let mut view = InventoryView::default()
            .with_equity(aapl.clone(), shares(20), shares(100))
            .with_equity(tsla.clone(), shares(10), shares(50));
        view.clear_offchain_order_pending(&aapl, Some(applied_at));
        view.mark_offchain_order_pending(aapl.clone());

        let mut reset = view.reset_preserving_offchain_order_state();

        assert!(
            reset.has_pending_offchain_order(&aapl),
            "the pending-order set must survive the reset"
        );
        assert_eq!(
            reset.equity_available(&aapl, Venue::Hedging),
            Some(shares(100)),
            "the gated symbol's delta-owned Hedging balance must survive the \
             reset — nothing can repopulate it while the gate is held"
        );
        assert_eq!(
            reset.equity_available(&aapl, Venue::MarketMaking),
            None,
            "the gated symbol's onchain venue is not delta-owned and must be \
             wiped like everything else"
        );
        assert_eq!(
            reset.equity_available(&tsla, Venue::Hedging),
            None,
            "ungated balances must not survive the reset"
        );

        // Guard 2 state must survive too: with the pending flag cleared, a
        // snapshot predating the preserved applied-fill time is still
        // rejected. Had the reset dropped it, this snapshot would apply.
        reset.clear_offchain_order_pending(&aapl, None);
        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), shares(50));
        let healed = reset
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: applied_at - Duration::seconds(1),
                },
                applied_at,
            )
            .unwrap();
        assert_eq!(
            healed.equity_available(&aapl, Venue::Hedging),
            Some(shares(100)),
            "a snapshot predating the preserved applied-fill time must still \
             be rejected after the reset — the preserved balance stays"
        );
    }

    /// The cash twin of the force-path gate above: while any hedge order is
    /// open, `force_apply_snapshot_event` must not write the venue-level
    /// `OffchainUsd` reading -- the fill delta owns the cash balance and the
    /// mid-order reading is ambiguous.
    #[test]
    fn force_apply_offchain_usd_respects_open_hedge_gate() {
        let aapl = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default()
            .with_equity(aapl.clone(), shares(20), shares(100))
            .with_usdc(Usdc::new(float!(5000)), Usdc::new(float!(5000)));
        view.mark_offchain_order_pending(aapl.clone());

        let reason = Arc::new(InventoryViewError::UsdBalanceConversion(0));
        let event = InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents: 999_900,
            gross_usd_cents: Some(999_900),
            fetched_at: now,
        };

        let gated = view
            .clone()
            .force_apply_snapshot_event(&event, now, reason.clone())
            .unwrap();
        assert_eq!(
            gated.usdc_available(Venue::Hedging),
            Some(Usdc::new(float!(5000))),
            "the force path must not write the venue cash while a hedge \
             order is open"
        );
        assert_eq!(
            gated.offchain_gross_usd_cents, None,
            "the gross reading comes from the same ambiguous read and must \
             be skipped with it"
        );

        view.clear_offchain_order_pending(&aapl, None);
        let ungated = view
            .force_apply_snapshot_event(&event, now, reason)
            .unwrap();
        assert_eq!(
            ungated.usdc_available(Venue::Hedging),
            Some(Usdc::new(float!(9999))),
            "with no order open the force path applies normally"
        );
    }

    /// The venue-level cash guard state must survive the recovery reset the
    /// same way the equity guard state does: it is fed by Position events
    /// and nothing re-seeds it after startup.
    #[test]
    fn reset_preserves_cash_fill_guard() {
        let aapl = Symbol::new("AAPL").unwrap();
        let applied_at = Utc::now();

        let mut view = InventoryView::default()
            .with_equity(aapl.clone(), shares(20), shares(100))
            .with_usdc(Usdc::new(float!(5000)), Usdc::new(float!(5000)));
        view.clear_offchain_order_pending(&aapl, Some(applied_at));

        let reset = view.reset_preserving_offchain_order_state();

        // With no order open, only the preserved cash-fill time can reject
        // this pre-fill reading. Had the reset dropped it, the snapshot
        // would initialize the wiped venue to the stale value.
        let healed = reset
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents: 500_000,
                    gross_usd_cents: None,
                    fetched_at: applied_at - Duration::seconds(1),
                },
                applied_at,
            )
            .unwrap();
        assert_eq!(
            healed.usdc_available(Venue::Hedging),
            None,
            "a cash snapshot predating the preserved applied-fill time must \
             still be rejected after the reset; Some means the guard was \
             wiped"
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
                    block_number: None,
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
            pending_offchain_order_symbols: HashSet::new(),
            last_offchain_fill_applied_at: HashMap::new(),
            onchain_equity_snapshot_block_watermarks: HashMap::new(),
            onchain_usdc_snapshot_block_watermark: None,
            last_offchain_cash_fill_applied_at: None,
            offchain_equity_snapshot_skip_streaks: HashMap::new(),
            offchain_usd_snapshot_skip_streak: 0,
            offchain_usd_snapshot_watermark: None,
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
            pending_offchain_order_symbols: HashSet::new(),
            last_offchain_fill_applied_at: HashMap::new(),
            onchain_equity_snapshot_block_watermarks: HashMap::new(),
            onchain_usdc_snapshot_block_watermark: None,
            last_offchain_cash_fill_applied_at: None,
            offchain_equity_snapshot_skip_streaks: HashMap::new(),
            offchain_usd_snapshot_skip_streak: 0,
            offchain_usd_snapshot_watermark: None,
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

    #[test]
    fn to_portfolio_snapshot_rows_empty_view_produces_no_rows() {
        let rows = InventoryView::default()
            .to_portfolio_snapshot_rows()
            .unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn to_portfolio_snapshot_rows_populated_view_produces_correct_tuples() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = InventoryView::default()
            .with_equity(aapl.clone(), shares(100), shares(50))
            .with_usdc(Usdc::new(float!(10000)), Usdc::new(float!(5000)));

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        assert_eq!(rows.len(), 4);
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::MarketMaking,
            asset: PortfolioAsset::Equity(aapl.clone()),
            available: shares(100).into(),
            inflight: FractionalShares::ZERO.into(),
        }));
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::Hedging,
            asset: PortfolioAsset::Equity(aapl),
            available: shares(50).into(),
            inflight: FractionalShares::ZERO.into(),
        }));
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::MarketMaking,
            asset: PortfolioAsset::Usdc,
            available: Usdc::new(float!(10000)).into(),
            inflight: Usdc::ZERO.into(),
        }));
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::Hedging,
            asset: PortfolioAsset::Usdc,
            available: Usdc::new(float!(5000)).into(),
            inflight: Usdc::ZERO.into(),
        }));
    }

    /// A symbol polled to a genuine zero balance at a venue must still
    /// produce a `0` row -- only a venue that was never polled (`None`) is
    /// skipped.
    #[test]
    fn to_portfolio_snapshot_rows_genuinely_zero_balance_still_produces_a_row() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view =
            InventoryView::default().with_equity(aapl.clone(), FractionalShares::ZERO, shares(10));

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::MarketMaking,
            asset: PortfolioAsset::Equity(aapl),
            available: FractionalShares::ZERO.into(),
            inflight: FractionalShares::ZERO.into(),
        }));
    }

    /// A venue that was never polled (`None`) must not appear as a row at
    /// all -- distinct from a polled-to-zero balance.
    #[test]
    fn to_portfolio_snapshot_rows_never_polled_venue_produces_no_row() {
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
            ..InventoryView::default()
        };

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].location, PortfolioLocation::MarketMaking);
        assert!(
            !rows
                .iter()
                .any(|row| row.location == PortfolioLocation::Hedging)
        );
    }

    /// Wrapped and unwrapped equity balances for the same symbol must never
    /// collapse into one row -- they are different units until normalized by
    /// the vault ratio.
    #[test]
    fn to_portfolio_snapshot_rows_wrapped_and_unwrapped_equity_never_collapse() {
        let aapl = Symbol::new("AAPL").unwrap();
        let fetched_at = Utc::now();
        let mut unwrapped = BTreeMap::new();
        unwrapped.insert(aapl.clone(), shares(5));
        let mut wrapped = BTreeMap::new();
        wrapped.insert(aapl.clone(), shares(7));

        let view = InventoryView::default()
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

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::BaseWalletUnwrapped,
            asset: PortfolioAsset::Equity(aapl.clone()),
            available: FractionalShares::ZERO.into(),
            inflight: shares(5).into(),
        }));
        assert!(rows.contains(&PortfolioBalanceRow {
            location: PortfolioLocation::BaseWalletWrapped,
            asset: PortfolioAsset::Equity(aapl),
            available: FractionalShares::ZERO.into(),
            inflight: shares(7).into(),
        }));
    }

    /// Wallet-transit USDC (`inflight_cash`) is emitted as its own row per
    /// populated location, recorded on `inflight` with `available` at zero.
    #[test]
    fn to_portfolio_snapshot_rows_includes_inflight_cash() {
        let fetched_at = Utc::now();
        let view = InventoryView::default().set_inflight_cash(
            InFlightCashLocation::EthereumWallet,
            Usdc::new(float!(250)),
            fetched_at,
            fetched_at,
        );

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].location, PortfolioLocation::EthereumWallet);
        assert_eq!(rows[0].asset, PortfolioAsset::Usdc);
        let expected_available: Float = Usdc::ZERO.into();
        let expected_inflight: Float = Usdc::new(float!(250)).into();
        assert!(rows[0].available.eq(expected_available).unwrap());
        assert!(rows[0].inflight.eq(expected_inflight).unwrap());
    }

    /// A configured reserve makes `usdc.offchain.available` the
    /// reserve-adjusted (net) balance -- deployed capital must still count
    /// the full, pre-reserve broker cash, so the Hedging row uses
    /// `offchain_gross_usd_cents`, not that net venue balance.
    #[test]
    fn to_portfolio_snapshot_rows_hedging_usdc_uses_gross_cash_not_reserve_adjusted() {
        let view = InventoryView::default()
            // The reserve-adjusted venue balance the poller stored after
            // subtracting a configured reserve from the broker's real cash.
            .with_usdc(Usdc::ZERO, Usdc::new(float!(800)))
            .with_offchain_gross_usd_cents(100_000);

        let rows = view.to_portfolio_snapshot_rows().unwrap();

        let hedging_row = rows
            .iter()
            .find(|row| {
                row.location == PortfolioLocation::Hedging && row.asset == PortfolioAsset::Usdc
            })
            .expect("Hedging USDC row must be present");
        let expected_gross: Float = Usdc::new(float!(1000)).into();
        assert!(
            hedging_row.available.eq(expected_gross).unwrap(),
            "Hedging USDC capital must use gross broker cash (1000), not the \
             reserve-adjusted venue balance (800)"
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

    fn reconciled_event(
        symbol: &Symbol,
        broker: FractionalShares,
        ledger: Option<FractionalShares>,
        fetched_at: DateTime<Utc>,
    ) -> InventorySnapshotEvent {
        InventorySnapshotEvent::OffchainEquityReconciled {
            symbol: symbol.clone(),
            position: broker,
            fetched_at,
            ledger_position: ledger,
            consecutive_polls: 3,
        }
    }

    /// A phantom Hedging credit protected by a freshly stamped
    /// `last_rebalancing` that ordinary snapshots cannot pierce. The
    /// reconcile event must force the broker value through, clear inflight,
    /// and advance the watermark.
    #[test]
    fn reconcile_event_force_applies_over_stale_rebalancing_guard() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        // Phantom 136 at Hedging; clear_equity_inflight stamps
        // last_rebalancing = now, the guard that wedges the view.
        let view = InventoryView::default()
            .with_equity(spym.clone(), shares(0), shares(136))
            .clear_equity_inflight(&spym, Venue::Hedging, now)
            .unwrap();

        // An ordinary snapshot fetched before the stamp stays rejected.
        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(0))]),
                    fetched_at: now - Duration::seconds(1),
                },
                now,
            )
            .unwrap();
        assert_eq!(
            view.equity_available(&spym, Venue::Hedging),
            Some(shares(136)),
            "precondition: the ordinary snapshot path must stay wedged"
        );

        let reconcile_fetched_at = now + Duration::seconds(1);
        let healed = view
            .apply_snapshot_event(
                &reconciled_event(&spym, shares(0), Some(shares(136)), reconcile_fetched_at),
                now,
            )
            .unwrap();

        assert_eq!(
            healed.equity_available(&spym, Venue::Hedging),
            Some(shares(0)),
            "the reconcile must force the broker value through the guard"
        );
        assert_eq!(
            healed.equity_inflight(&spym, Venue::Hedging),
            Some(FractionalShares::ZERO),
            "the force path clears Hedging inflight"
        );

        // Watermark advanced to the reconcile's fetched_at: a snapshot
        // stamped at or before it must not apply...
        let replayed = healed
            .clone()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(99))]),
                    fetched_at: reconcile_fetched_at,
                },
                now,
            )
            .unwrap();
        assert_eq!(
            replayed.equity_available(&spym, Venue::Hedging),
            Some(shares(0)),
            "a snapshot at the reconcile watermark must be rejected"
        );

        // ...while a strictly newer one applies.
        let fresher = healed
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(2))]),
                    fetched_at: reconcile_fetched_at + Duration::seconds(1),
                },
                now,
            )
            .unwrap();
        assert_eq!(
            fresher.equity_available(&spym, Venue::Hedging),
            Some(shares(2)),
            "a strictly newer snapshot must apply after the reconcile"
        );
    }

    /// The reconcile must not stamp `last_rebalancing`: a snapshot fetched
    /// after the reconcile but before its apply time must still apply.
    /// Stamping (as `clear_equity_inflight` does) would reject that
    /// snapshot and recreate the wedge the reconcile just cleared.
    #[test]
    fn reconcile_does_not_stamp_last_rebalancing() {
        let spym = Symbol::new("SPYM").unwrap();
        let fetched_at = Utc::now();
        let heal_applied_at = fetched_at + Duration::seconds(10);

        let view = InventoryView::default()
            .with_equity(spym.clone(), shares(0), shares(136))
            .reconcile_offchain_equity(
                &spym,
                shares(0),
                Some(shares(136)),
                3,
                fetched_at,
                heal_applied_at,
            )
            .unwrap();

        // Fetched after the reconcile's watermark but before the
        // reconcile's apply time: rejected iff last_rebalancing was stamped.
        let updated = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(5))]),
                    fetched_at: fetched_at + Duration::seconds(1),
                },
                heal_applied_at,
            )
            .unwrap();

        assert_eq!(
            updated.equity_available(&spym, Venue::Hedging),
            Some(shares(5)),
            "a snapshot after the reconcile must apply; the reconcile must \
             not stamp last_rebalancing"
        );
    }

    /// A fresher ordinary snapshot that applied between the escalation
    /// send and the reconcile apply owns the balance: the reconcile's
    /// older reading must not overwrite it, at or below the watermark.
    #[test]
    fn reconcile_aborts_when_watermark_is_newer_than_reading() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(5))]),
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        let older = view
            .clone()
            .apply_snapshot_event(
                &reconciled_event(
                    &spym,
                    shares(0),
                    Some(shares(5)),
                    now - Duration::seconds(1),
                ),
                now,
            )
            .unwrap();
        assert_eq!(
            older.equity_available(&spym, Venue::Hedging),
            Some(shares(5)),
            "a reconcile older than the watermark must not overwrite the \
             fresher snapshot"
        );

        let at_watermark = view
            .apply_snapshot_event(
                &reconciled_event(&spym, shares(0), Some(shares(5)), now),
                now,
            )
            .unwrap();
        assert_eq!(
            at_watermark.equity_available(&spym, Venue::Hedging),
            Some(shares(5)),
            "a reconcile at the watermark must be rejected like the \
             ordinary path"
        );
    }

    #[test]
    fn reconcile_aborts_when_symbol_acquired_inflight_between_escalation_and_apply() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .with_equity(spym.clone(), shares(50), shares(136))
            .update_equity(
                &spym,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap();

        let result = view
            .apply_snapshot_event(&reconciled_event(&spym, shares(0), None, now), now)
            .unwrap();

        assert_eq!(
            result.equity_available(&spym, Venue::Hedging),
            Some(shares(126)),
            "the reconcile must abort while an inflight transfer owns the balance"
        );
        assert_eq!(
            result.equity_inflight(&spym, Venue::Hedging),
            Some(shares(10)),
            "the aborted reconcile must not clear the live inflight"
        );
    }

    #[test]
    fn reconcile_aborts_when_reading_predates_last_applied_fill() {
        let spym = Symbol::new("SPYM").unwrap();
        let fetched_at = Utc::now();
        let fill_applied_at = fetched_at + Duration::seconds(1);

        // A fill applied after the escalation's reading was fetched: the
        // reconcile would overwrite the fill's delta with a stale value.
        let mut view = InventoryView::default().with_equity(spym.clone(), shares(0), shares(126));
        view.clear_offchain_order_pending(&spym, Some(fill_applied_at));

        let result = view
            .apply_snapshot_event(
                &reconciled_event(&spym, shares(0), Some(shares(136)), fetched_at),
                fill_applied_at,
            )
            .unwrap();

        assert_eq!(
            result.equity_available(&spym, Venue::Hedging),
            Some(shares(126)),
            "the reconcile must abort when a fill applied after the reading \
             was fetched"
        );
    }

    #[test]
    fn reconcile_aborts_when_symbol_acquired_active_mint_between_escalation_and_apply() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .with_equity(spym.clone(), shares(0), shares(136))
            .set_active_mint(spym.clone(), st0x_tokenization::issuer_request_id("mint"));

        let result = view
            .apply_snapshot_event(&reconciled_event(&spym, shares(0), None, now), now)
            .unwrap();

        assert_eq!(
            result.equity_available(&spym, Venue::Hedging),
            Some(shares(136)),
            "the reconcile must abort while a mint owns the symbol's inflight slot"
        );
    }

    #[test]
    fn reconcile_aborts_when_symbol_acquired_active_redemption_between_escalation_and_apply() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let view = InventoryView::default()
            .with_equity(spym.clone(), shares(0), shares(136))
            .set_active_redemption(spym.clone(), RedemptionAggregateId(Uuid::new_v4()));

        let result = view
            .apply_snapshot_event(&reconciled_event(&spym, shares(0), None, now), now)
            .unwrap();

        assert_eq!(
            result.equity_available(&spym, Venue::Hedging),
            Some(shares(136)),
            "the reconcile must abort while a redemption owns the symbol's \
             inflight slot"
        );
    }

    /// Mirror of `force_apply_offchain_snapshot_respects_open_hedge_gate`:
    /// even the reconcile force path must not overwrite a balance owned by
    /// an open hedge order's fill delta.
    #[test]
    fn reconcile_respects_open_hedge_gate() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_equity(spym.clone(), shares(0), shares(136));
        view.mark_offchain_order_pending(spym.clone());

        let gated = view
            .apply_snapshot_event(&reconciled_event(&spym, shares(0), None, now), now)
            .unwrap();
        assert_eq!(
            gated.equity_available(&spym, Venue::Hedging),
            Some(shares(136)),
            "an open hedge order owns the balance; the reconcile must abort"
        );

        // The aborted reconcile must not burn the watermark: once the order
        // terminates, delivering the same reconcile event again applies it.
        let mut released = gated;
        released.clear_offchain_order_pending(&spym, None);
        let healed = released
            .apply_snapshot_event(&reconciled_event(&spym, shares(0), None, now), now)
            .unwrap();
        assert_eq!(
            healed.equity_available(&spym, Venue::Hedging),
            Some(shares(0)),
            "the reconcile must apply once the hedge order terminated"
        );
    }

    #[test]
    fn equity_reconciliation_busy_covers_each_busy_source() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let not_busy = InventoryView::default().with_equity(spym.clone(), shares(0), shares(10));
        assert_eq!(
            not_busy.equity_reconciliation_busy(&spym, now).unwrap(),
            None
        );

        let inflight = not_busy
            .clone()
            .update_equity(
                &spym,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(5)),
                now,
            )
            .unwrap();
        assert_eq!(
            inflight.equity_reconciliation_busy(&spym, now).unwrap(),
            Some(EquityReconcileBusy::Transfer)
        );

        let minting = not_busy
            .clone()
            .set_active_mint(spym.clone(), st0x_tokenization::issuer_request_id("mint"));
        assert_eq!(
            minting.equity_reconciliation_busy(&spym, now).unwrap(),
            Some(EquityReconcileBusy::Transfer)
        );

        let redeeming = not_busy
            .clone()
            .set_active_redemption(spym.clone(), RedemptionAggregateId(Uuid::new_v4()));
        assert_eq!(
            redeeming.equity_reconciliation_busy(&spym, now).unwrap(),
            Some(EquityReconcileBusy::Transfer)
        );

        let mut hedging = not_busy.clone();
        hedging.mark_offchain_order_pending(spym.clone());
        assert_eq!(
            hedging.equity_reconciliation_busy(&spym, now).unwrap(),
            Some(EquityReconcileBusy::PendingHedgeOrder)
        );

        let mut fill_applied_after_reading = not_busy;
        fill_applied_after_reading
            .clear_offchain_order_pending(&spym, Some(now + Duration::seconds(1)));
        assert_eq!(
            fill_applied_after_reading
                .equity_reconciliation_busy(&spym, now)
                .unwrap(),
            Some(EquityReconcileBusy::FillAfterFetch)
        );
        assert_eq!(
            fill_applied_after_reading
                .equity_reconciliation_busy(&spym, now + Duration::seconds(2))
                .unwrap(),
            None
        );
    }

    fn usdc_cents(cents: i64) -> Usdc {
        Usdc::from_cents(cents).unwrap()
    }

    fn usd_reconciled_event(
        broker_cents: i64,
        ledger: Option<Usdc>,
        fetched_at: DateTime<Utc>,
    ) -> InventorySnapshotEvent {
        InventorySnapshotEvent::OffchainUsdReconciled {
            usd_balance_cents: broker_cents,
            gross_usd_cents: Some(broker_cents),
            fetched_at,
            ledger_usdc: ledger,
            consecutive_polls: 3,
        }
    }

    /// A phantom Hedging cash balance protected by a freshly stamped
    /// `last_rebalancing` that ordinary snapshots cannot pierce. The
    /// reconcile event must force the broker value through, clear inflight,
    /// and carry the gross reading with the net.
    #[test]
    fn cash_reconcile_forces_broker_value_through_guard() {
        let now = Utc::now();
        let view = InventoryView::default()
            .with_usdc(Usdc::ZERO, usdc_cents(50_000))
            .clear_usdc_inflight(Venue::Hedging, now)
            .unwrap();

        let wedged = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents: 0,
                    gross_usd_cents: Some(0),
                    fetched_at: now - Duration::seconds(1),
                },
                now,
            )
            .unwrap();
        assert_eq!(
            wedged.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "precondition: the ordinary cash snapshot path must stay wedged"
        );
        assert_eq!(
            wedged.offchain_gross_usd_cents, None,
            "a skipped cash snapshot must not apply its gross reading either"
        );

        let healed = wedged
            .apply_snapshot_event(
                &usd_reconciled_event(0, Some(usdc_cents(50_000)), now + Duration::seconds(1)),
                now,
            )
            .unwrap();
        assert_eq!(
            healed.usdc_available(Venue::Hedging),
            Some(Usdc::ZERO),
            "the reconcile must force the broker cash value through the guard"
        );
        assert_eq!(
            healed.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "the force path clears Hedging cash inflight"
        );
        assert_eq!(
            healed.offchain_gross_usd_cents,
            Some(0),
            "the reconcile applies the gross reading alongside the net"
        );
    }

    /// The cash twin of `reconcile_aborts_when_watermark_is_newer_than_reading`:
    /// an ordinary cash snapshot that applied between the escalation send
    /// and the reconcile apply owns the balance, and the reconcile's older
    /// broker reading must not overwrite it, at or below the watermark.
    #[test]
    fn cash_reconcile_aborts_when_watermark_is_newer_than_reading() {
        let now = Utc::now();

        let view = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents: 50_000,
                    gross_usd_cents: Some(50_000),
                    fetched_at: now,
                },
                now,
            )
            .unwrap();

        let older = view
            .clone()
            .apply_snapshot_event(
                &usd_reconciled_event(0, Some(usdc_cents(50_000)), now - Duration::seconds(1)),
                now,
            )
            .unwrap();
        assert_eq!(
            older.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "a cash reconcile older than the watermark must not overwrite \
             the fresher snapshot"
        );
        assert_eq!(
            older.offchain_gross_usd_cents,
            Some(50_000),
            "the aborted reconcile must not overwrite the fresher gross \
             reading either"
        );

        let at_watermark = view
            .apply_snapshot_event(&usd_reconciled_event(0, Some(usdc_cents(50_000)), now), now)
            .unwrap();
        assert_eq!(
            at_watermark.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "a cash reconcile at the watermark must be rejected like the \
             ordinary path"
        );
    }

    /// An applied cash reconcile stamps the watermark itself: redelivering
    /// the same reconcile event (same `fetched_at`) must be rejected by the
    /// freshness guard instead of force-writing again.
    #[test]
    fn cash_reconcile_stamps_the_watermark_it_checks() {
        let now = Utc::now();

        let healed = InventoryView::default()
            .with_usdc(Usdc::ZERO, usdc_cents(50_000))
            .apply_snapshot_event(&usd_reconciled_event(0, Some(usdc_cents(50_000)), now), now)
            .unwrap();
        assert_eq!(
            healed.usdc_available(Venue::Hedging),
            Some(Usdc::ZERO),
            "precondition: the first reconcile must apply"
        );

        // A fill delta lands after the heal; a replay of the same reconcile
        // reading must not overwrite it with the pre-fill broker value.
        let after_fill = healed
            .update_usdc(
                Inventory::available(Venue::Hedging, Operator::Add, usdc_cents(1_000)),
                now,
            )
            .unwrap()
            .apply_snapshot_event(&usd_reconciled_event(0, Some(usdc_cents(50_000)), now), now)
            .unwrap();
        assert_eq!(
            after_fill.usdc_available(Venue::Hedging),
            Some(usdc_cents(1_000)),
            "a replayed reconcile at its own watermark must be rejected"
        );
    }

    #[test]
    fn cash_reconcile_aborts_while_usdc_inflight() {
        let now = Utc::now();
        let view = InventoryView::default().with_usdc_inflight(
            Usdc::ZERO,
            Usdc::ZERO,
            usdc_cents(50_000),
            usdc_cents(10_000),
        );

        let result = view
            .apply_snapshot_event(&usd_reconciled_event(0, Some(usdc_cents(50_000)), now), now)
            .unwrap();

        assert_eq!(
            result.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "the reconcile must abort while a USDC transfer owns the balance"
        );
        assert_eq!(
            result.usdc_inflight(Venue::Hedging),
            Some(usdc_cents(10_000)),
            "the aborted reconcile must not clear the live inflight"
        );
    }

    #[test]
    fn cash_reconcile_aborts_while_usdc_rebalance_active() {
        let now = Utc::now();
        let view = InventoryView::default()
            .with_usdc(Usdc::ZERO, usdc_cents(50_000))
            .set_active_usdc_rebalance(UsdcRebalanceId(Uuid::new_v4()));

        let result = view
            .apply_snapshot_event(&usd_reconciled_event(0, Some(usdc_cents(50_000)), now), now)
            .unwrap();

        assert_eq!(
            result.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "the reconcile must abort while a USDC rebalance aggregate is live"
        );
    }

    /// The venue-level twin of `reconcile_respects_open_hedge_gate`: an open
    /// hedge order on ANY symbol makes the venue's cash reading ambiguous,
    /// and the aborted reconcile must not burn the watermark.
    #[test]
    fn cash_reconcile_aborts_while_any_hedge_order_open() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_usdc(Usdc::ZERO, usdc_cents(50_000));
        view.mark_offchain_order_pending(spym.clone());

        let gated = view
            .apply_snapshot_event(&usd_reconciled_event(0, None, now), now)
            .unwrap();
        assert_eq!(
            gated.usdc_available(Venue::Hedging),
            Some(usdc_cents(50_000)),
            "an open hedge order owns the cash balance; the reconcile must abort"
        );

        let mut released = gated;
        released.clear_offchain_order_pending(&spym, None);
        let healed = released
            .apply_snapshot_event(&usd_reconciled_event(0, None, now), now)
            .unwrap();
        assert_eq!(
            healed.usdc_available(Venue::Hedging),
            Some(Usdc::ZERO),
            "the reconcile must apply once the hedge order terminated"
        );
    }

    #[test]
    fn cash_reconcile_aborts_when_reading_predates_last_applied_cash_fill() {
        let spym = Symbol::new("SPYM").unwrap();
        let fetched_at = Utc::now();
        let fill_applied_at = fetched_at + Duration::seconds(1);

        let mut view = InventoryView::default().with_usdc(Usdc::ZERO, usdc_cents(40_000));
        view.clear_offchain_order_pending(&spym, Some(fill_applied_at));

        let result = view
            .apply_snapshot_event(
                &usd_reconciled_event(0, Some(usdc_cents(40_000)), fetched_at),
                fill_applied_at,
            )
            .unwrap();

        assert_eq!(
            result.usdc_available(Venue::Hedging),
            Some(usdc_cents(40_000)),
            "the reconcile must abort when a cash fill applied after the \
             reading was fetched"
        );
    }

    #[test]
    fn cash_reconciliation_busy_covers_each_busy_source() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let not_busy = InventoryView::default().with_usdc(Usdc::ZERO, usdc_cents(50_000));
        assert_eq!(not_busy.cash_reconciliation_busy(now).unwrap(), None);

        let inflight = not_busy.clone().with_usdc_inflight(
            Usdc::ZERO,
            Usdc::ZERO,
            usdc_cents(50_000),
            usdc_cents(1_000),
        );
        assert_eq!(
            inflight.cash_reconciliation_busy(now).unwrap(),
            Some(EquityReconcileBusy::Transfer)
        );

        let rebalancing = not_busy
            .clone()
            .set_active_usdc_rebalance(UsdcRebalanceId(Uuid::new_v4()));
        assert_eq!(
            rebalancing.cash_reconciliation_busy(now).unwrap(),
            Some(EquityReconcileBusy::Transfer)
        );

        let mut hedging = not_busy.clone();
        hedging.mark_offchain_order_pending(spym.clone());
        assert_eq!(
            hedging.cash_reconciliation_busy(now).unwrap(),
            Some(EquityReconcileBusy::PendingHedgeOrder)
        );

        let mut fill_applied_after_reading = not_busy;
        fill_applied_after_reading
            .clear_offchain_order_pending(&spym, Some(now + Duration::seconds(1)));
        assert_eq!(
            fill_applied_after_reading
                .cash_reconciliation_busy(now)
                .unwrap(),
            Some(EquityReconcileBusy::FillAfterFetch)
        );
        assert_eq!(
            fill_applied_after_reading
                .cash_reconciliation_busy(now + Duration::seconds(2))
                .unwrap(),
            None
        );
    }

    /// ADR 0015 accepted guard starvation but left it invisible at
    /// production log levels; the skip streak is the missing signal. The
    /// warn must fire only at the cadence, not on every skip.
    #[test]
    #[tracing_test::traced_test]
    fn offchain_usd_snapshot_skip_streak_warns_at_cadence() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_usdc(Usdc::ZERO, usdc_cents(50_000));
        view.mark_offchain_order_pending(spym);

        let snapshot = InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents: 0,
            gross_usd_cents: Some(0),
            fetched_at: now,
        };

        for _ in 0..(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY - 1) {
            view = view.apply_snapshot_event(&snapshot, now).unwrap();
        }
        assert_eq!(
            view.offchain_usd_snapshot_skip_streak,
            OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY - 1
        );
        assert!(
            !logs_contain("Offchain USD snapshots keep being skipped"),
            "below the cadence no starvation warning may fire"
        );

        let view = view.apply_snapshot_event(&snapshot, now).unwrap();
        assert_eq!(
            view.offchain_usd_snapshot_skip_streak,
            OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY
        );
        assert!(
            logs_contain("Offchain USD snapshots keep being skipped"),
            "the warn must fire once the streak reaches the cadence"
        );
    }

    #[test]
    #[tracing_test::traced_test]
    fn offchain_usd_snapshot_skip_streak_resets_on_apply() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_usdc(Usdc::ZERO, usdc_cents(50_000));
        view.mark_offchain_order_pending(spym.clone());

        let snapshot = InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents: 0,
            gross_usd_cents: Some(0),
            fetched_at: now,
        };

        for _ in 0..(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY - 1) {
            view = view.apply_snapshot_event(&snapshot, now).unwrap();
        }

        // An applied snapshot ends the starvation streak: the guard lifted
        // and broker truth reached the balance.
        view.clear_offchain_order_pending(&spym, None);
        view = view.apply_snapshot_event(&snapshot, now).unwrap();
        assert_eq!(
            view.offchain_usd_snapshot_skip_streak, 0,
            "an applied snapshot must reset the skip streak"
        );

        view.mark_offchain_order_pending(spym);
        for _ in 0..(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY - 1) {
            view = view.apply_snapshot_event(&snapshot, now).unwrap();
        }
        assert!(
            !logs_contain("Offchain USD snapshots keep being skipped"),
            "interrupted streaks must not accumulate across an apply"
        );
    }

    #[test]
    #[tracing_test::traced_test]
    fn offchain_equity_snapshot_skip_streak_warns_at_cadence() {
        let spym = Symbol::new("SPYM").unwrap();
        let now = Utc::now();

        let mut view = InventoryView::default().with_equity(spym.clone(), shares(0), shares(10));
        view.mark_offchain_order_pending(spym.clone());

        let snapshot = InventorySnapshotEvent::OffchainEquity {
            positions: BTreeMap::from([(spym.clone(), shares(0))]),
            fetched_at: now,
        };

        for _ in 0..(OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY - 1) {
            view = view.apply_snapshot_event(&snapshot, now).unwrap();
        }
        assert!(
            !logs_contain("Offchain equity snapshots for this symbol keep being"),
            "below the cadence no starvation warning may fire"
        );

        view = view.apply_snapshot_event(&snapshot, now).unwrap();
        assert_eq!(
            view.offchain_equity_snapshot_skip_streaks.get(&spym),
            Some(&OFFCHAIN_SNAPSHOT_SKIP_WARN_EVERY)
        );
        assert!(
            logs_contain("Offchain equity snapshots for this symbol keep being"),
            "the warn must fire once the symbol's streak reaches the cadence"
        );

        // An applied snapshot drops the symbol's streak entirely.
        view.clear_offchain_order_pending(&spym, None);
        let view = view
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainEquity {
                    positions: BTreeMap::from([(spym.clone(), shares(0))]),
                    fetched_at: now + Duration::seconds(1),
                },
                now,
            )
            .unwrap();
        assert_eq!(
            view.offchain_equity_snapshot_skip_streaks.get(&spym),
            None,
            "an applied snapshot must clear the symbol's skip streak"
        );
    }
}
