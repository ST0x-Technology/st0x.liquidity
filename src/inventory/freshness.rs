//! Day-scoped poll-freshness tracking.
//!
//! `hydration_gap` (`crate::portfolio_snapshot::write`) gates the daily
//! portfolio snapshot capture on PRESENCE: every required `(location, asset)`
//! balance exists in the live `InventoryView`. On restart, the view hydrates
//! from persisted `InventorySnapshot` state, so presence passes immediately
//! even though the CURRENT process has not polled anything yet -- if the
//! daily capture fires before the first poll cycle, it would otherwise
//! persist an immutable snapshot built from a prior run's stale balances
//! (the aggregate rejects a second `Capture`, so there is no fixing it after
//! the fact).
//!
//! [`PollFreshness`] closes that hole with a FRESHNESS signal: a per-slot
//! timestamp, updated by the poll loop
//! (`crate::inventory::polling::InventoryPollingService`) on every successful
//! fetch, independent of `InventorySnapshot`'s change-suppression. An earlier
//! attempt tied freshness to that aggregate's own event watermark, which
//! freezes whenever a poll observes an unchanged balance -- a static book
//! would eventually block every future capture, not just a stale-hydrated
//! one, so that attempt was reverted.
//!
//! The gate ([`Self::observed_since`]) is DAY-SCOPED, not merely "observed at
//! some point this process run": a slot only counts as fresh for a given
//! capture if its stored timestamp is at or after that capture's target ET
//! day's midnight. Plain run-scoped membership would let a slot observed once
//! early in a long-running process's life stay "fresh" for every later day's
//! capture too, even after the venue that fed it stops polling entirely --
//! reopening the same stale-capture hole this module exists to close, just on
//! a longer timescale than the restart case above. Callers pass the floor
//! explicitly (see `crate::portfolio_snapshot::write::freshness_gap`) rather
//! than this module hard-coding a wall-clock window, so the gate stays
//! decoupled from the (config-driven) poll interval.
//!
//! Keyed by `(PortfolioLocation, PortfolioAsset)`, the SAME identity
//! `InventoryView::to_portfolio_snapshot_rows` and the presence gate use --
//! keying by location alone would collapse the two co-located rows at
//! `PortfolioLocation::BaseWalletUnwrapped` (USDC and unwrapped equity are
//! distinct assets tracked independently at that one location).

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::warn;

use crate::inventory::view::{PortfolioAsset, PortfolioLocation};

type LocationFreshness = HashMap<PortfolioAsset, DateTime<Utc>>;
type FreshnessMap = HashMap<PortfolioLocation, LocationFreshness>;

/// Shared state behind [`PollFreshness`]'s `Arc`: the per-slot observation
/// timestamps, plus `created_at` -- this tracker's own construction time, used
/// by `crate::portfolio_snapshot::write::freshness_defer_exhausted` to grant a
/// freshly-restarted process its own defer grace window instead of judging it
/// solely against the target day's capture boundary.
struct Inner {
    created_at: DateTime<Utc>,
    map: Mutex<FreshnessMap>,
}

/// Tracks the most recent successful-poll timestamp for each
/// `(PortfolioLocation, PortfolioAsset)` slot. See the module doc for why the
/// read side ([`Self::observed_since`]) is day-scoped, not plain membership.
///
/// `std::sync::Mutex`, not `tokio::sync::Mutex`: every operation is a
/// synchronous map read/write and no lock is ever held across an `.await`
/// (mirrors the existing `std` mutexes in
/// `crate::inventory::polling::InventoryPollingService`).
#[derive(Clone)]
pub(crate) struct PollFreshness(Arc<Inner>);

impl PollFreshness {
    /// An empty tracker, `created_at` stamped to the current instant: every
    /// slot reads un-observed until [`Self::observe`] marks it, which is
    /// exactly what makes the freshness signal restart-safe -- startup
    /// hydration alone can never make a slot read fresh.
    pub(crate) fn new() -> Self {
        Self::with_created_at(Utc::now())
    }

    /// Test-only: builds a tracker whose [`Self::created_at`] is an injected
    /// instant rather than the real construction time, so
    /// `freshness_defer_exhausted`'s restart-anchored grace window can be
    /// tested deterministically instead of racing the real wall clock.
    #[cfg(test)]
    pub(crate) fn new_at(created_at: DateTime<Utc>) -> Self {
        Self::with_created_at(created_at)
    }

    fn with_created_at(created_at: DateTime<Utc>) -> Self {
        Self(Arc::new(Inner {
            created_at,
            map: Mutex::new(HashMap::new()),
        }))
    }

    /// When this tracker was constructed (a fresh, empty map) -- i.e. this
    /// process run's start, or an injected instant in tests.
    pub(crate) fn created_at(&self) -> DateTime<Utc> {
        self.0.created_at
    }

    /// Marks `(location, asset)` as observed by a successful poll just now.
    /// Idempotent: re-observing an already-fresh slot (e.g. a second poll
    /// tick of an unchanged balance) simply refreshes its timestamp.
    pub(crate) fn observe(&self, location: PortfolioLocation, asset: PortfolioAsset) {
        self.lock()
            .entry(location)
            .or_default()
            .insert(asset, Utc::now());
    }

    /// Whether `(location, asset)` was observed by a successful poll at or
    /// after `floor`. See the module doc: this is deliberately day-scoped,
    /// not plain membership, so a slot's freshness is judged against the
    /// specific capture it is gating, not merely "observed at some point
    /// since process start".
    pub(crate) fn observed_since(
        &self,
        location: PortfolioLocation,
        asset: &PortfolioAsset,
        floor: DateTime<Utc>,
    ) -> bool {
        self.lock()
            .get(&location)
            .and_then(|assets| assets.get(asset))
            .is_some_and(|observed_at| *observed_at >= floor)
    }

    fn lock(&self) -> MutexGuard<'_, FreshnessMap> {
        self.0.map.lock().unwrap_or_else(|poisoned| {
            warn!(
                target: "inventory",
                "Poll freshness tracker was poisoned; recovering state"
            );
            poisoned.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use st0x_execution::Symbol;

    use super::*;

    fn symbol(value: &str) -> Symbol {
        Symbol::new(value).unwrap()
    }

    /// Any `floor` at or before construction sees no slots observed --
    /// including the earliest representable instant, so this doubles as an
    /// "observed ever" check.
    #[test]
    fn new_instance_has_no_slots_observed() {
        let poll_freshness = PollFreshness::new();

        assert!(!poll_freshness.observed_since(
            PortfolioLocation::MarketMaking,
            &PortfolioAsset::Usdc,
            DateTime::<Utc>::MIN_UTC
        ));
    }

    #[test]
    fn observe_marks_the_exact_slot_observed_since_before_the_call() {
        let before = Utc::now();
        let poll_freshness = PollFreshness::new();

        poll_freshness.observe(PortfolioLocation::MarketMaking, PortfolioAsset::Usdc);

        assert!(poll_freshness.observed_since(
            PortfolioLocation::MarketMaking,
            &PortfolioAsset::Usdc,
            before
        ));
    }

    /// The core new behavior this module's day-scoping fix exists to provide:
    /// a slot observed once does not read as fresh forever -- only for floors
    /// at or before its stored timestamp.
    #[test]
    fn observed_since_returns_false_once_the_floor_moves_past_the_stored_timestamp() {
        let poll_freshness = PollFreshness::new();

        poll_freshness.observe(PortfolioLocation::MarketMaking, PortfolioAsset::Usdc);

        let floor_in_the_future = Utc::now() + chrono::Duration::hours(1);
        assert!(
            !poll_freshness.observed_since(
                PortfolioLocation::MarketMaking,
                &PortfolioAsset::Usdc,
                floor_in_the_future
            ),
            "a slot observed before the floor must not read as fresh for that floor"
        );
    }

    #[test]
    fn observing_one_location_does_not_mark_a_different_location_for_the_same_asset() {
        let poll_freshness = PollFreshness::new();

        poll_freshness.observe(PortfolioLocation::MarketMaking, PortfolioAsset::Usdc);

        assert!(!poll_freshness.observed_since(
            PortfolioLocation::Hedging,
            &PortfolioAsset::Usdc,
            DateTime::<Utc>::MIN_UTC
        ));
    }

    /// The exact regression this module exists to prevent: keying by
    /// location alone would collapse USDC and unwrapped equity at
    /// `BaseWalletUnwrapped` into one slot.
    #[test]
    fn base_wallet_unwrapped_usdc_and_equity_are_tracked_as_distinct_slots() {
        let poll_freshness = PollFreshness::new();

        poll_freshness.observe(PortfolioLocation::BaseWalletUnwrapped, PortfolioAsset::Usdc);

        assert!(poll_freshness.observed_since(
            PortfolioLocation::BaseWalletUnwrapped,
            &PortfolioAsset::Usdc,
            DateTime::<Utc>::MIN_UTC
        ));
        assert!(!poll_freshness.observed_since(
            PortfolioLocation::BaseWalletUnwrapped,
            &PortfolioAsset::Equity(symbol("AAPL")),
            DateTime::<Utc>::MIN_UTC
        ));
    }

    #[test]
    fn re_observing_an_already_fresh_slot_advances_its_timestamp() {
        let poll_freshness = PollFreshness::new();

        poll_freshness.observe(PortfolioLocation::Hedging, PortfolioAsset::Usdc);
        let floor_between_the_two_observations = Utc::now();
        poll_freshness.observe(PortfolioLocation::Hedging, PortfolioAsset::Usdc);

        assert!(
            poll_freshness.observed_since(
                PortfolioLocation::Hedging,
                &PortfolioAsset::Usdc,
                floor_between_the_two_observations
            ),
            "the second observe must refresh the stored timestamp, not just no-op"
        );
    }

    #[test]
    fn cloned_handle_shares_the_same_underlying_state() {
        let poll_freshness = PollFreshness::new();
        let clone = poll_freshness.clone();

        clone.observe(PortfolioLocation::Hedging, PortfolioAsset::Usdc);

        assert!(poll_freshness.observed_since(
            PortfolioLocation::Hedging,
            &PortfolioAsset::Usdc,
            DateTime::<Utc>::MIN_UTC
        ));
    }

    #[test]
    fn new_stamps_created_at_to_the_construction_instant() {
        let before = Utc::now();
        let poll_freshness = PollFreshness::new();
        let after = Utc::now();

        assert!(poll_freshness.created_at() >= before);
        assert!(poll_freshness.created_at() <= after);
    }
}
