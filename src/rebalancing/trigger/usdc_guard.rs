//! One USDC cash guard per corridor chain (ADR 0003, amended).
//!
//! A corridor admits a new transfer only while no other transfer holds it,
//! and at most one Alpaca-outbound transfer holds a guard across all
//! corridors, because Alpaca's withdrawable cash and its USDC inflight are
//! shared. A release removes only its own transfer, so it never frees a guard
//! another transfer still holds.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use st0x_evm::Chain;

use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceId};

#[derive(Debug, Default)]
pub(crate) struct UsdcCashGuards(Mutex<CashGuardState>);

impl UsdcCashGuards {
    /// Claims `chain`'s guard for `id`. Refused while another transfer holds
    /// the chain, while an unclassified startup candidate latches every
    /// chain, or, for an Alpaca-outbound transfer, while another chain holds
    /// one. A claim by the transfer already holding the chain succeeds and
    /// leaves that hold in place on drop.
    pub(super) fn try_claim(
        self: &Arc<Self>,
        chain: Chain,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
    ) -> Option<CashGuardClaim> {
        let fresh = self.state().claim(chain, id, direction)?;

        Some(CashGuardClaim {
            guards: Arc::clone(self),
            id: id.clone(),
            release_on_drop: fresh,
        })
    }

    /// Keeps `chain` held by `id`, whatever else holds it.
    pub(super) fn hold(&self, chain: Chain, id: &UsdcRebalanceId, direction: RebalanceDirection) {
        self.state()
            .holders
            .entry(chain)
            .or_default()
            .insert(id.clone(), direction);
    }

    /// Frees whatever `id` holds; other holders keep their guards.
    pub(super) fn release(&self, id: &UsdcRebalanceId) {
        self.state().holders.retain(|_, holders| {
            holders.remove(id);
            !holders.is_empty()
        });
    }

    /// Blocks every corridor until a restart: a startup candidate whose
    /// corridor is unknown may hold any of them.
    pub(super) fn latch_unclassified(&self) {
        self.state().unclassified = true;
    }

    #[cfg(test)]
    pub(crate) fn is_held(&self, chain: Chain) -> bool {
        let state = self.state();
        state.unclassified || state.holders.contains_key(&chain)
    }

    /// Stands in for every holder's terminal event.
    #[cfg(test)]
    pub(crate) fn release_every_holder(&self) {
        self.state().holders.clear();
    }

    fn state(&self) -> MutexGuard<'_, CashGuardState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A guard claimed by the trigger or a manual resume. Dropping it releases
/// the claim unless it was defused after the transfer's job was queued.
pub(super) struct CashGuardClaim {
    guards: Arc<UsdcCashGuards>,
    id: UsdcRebalanceId,
    release_on_drop: bool,
}

impl CashGuardClaim {
    pub(super) fn defuse(mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for CashGuardClaim {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.guards.release(&self.id);
        }
    }
}

#[derive(Debug, Default)]
struct CashGuardState {
    holders: BTreeMap<Chain, HashMap<UsdcRebalanceId, RebalanceDirection>>,
    unclassified: bool,
}

impl CashGuardState {
    /// Adds `id` to `chain`'s holders when admitted; `Some(true)` when the
    /// hold is new.
    fn claim(
        &mut self,
        chain: Chain,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
    ) -> Option<bool> {
        if !self.admits(chain, id, direction) {
            return None;
        }

        let fresh = self
            .holders
            .entry(chain)
            .or_default()
            .insert(id.clone(), direction)
            .is_none();

        Some(fresh)
    }

    fn admits(&self, chain: Chain, id: &UsdcRebalanceId, direction: RebalanceDirection) -> bool {
        if self.unclassified {
            return false;
        }

        let chain_taken = self
            .holders
            .get(&chain)
            .is_some_and(|holders| holders.keys().any(|holder| holder != id));
        if chain_taken {
            return false;
        }

        match direction {
            RebalanceDirection::BaseToAlpaca => true,
            RebalanceDirection::AlpacaToBase => !self
                .holders
                .iter()
                .filter(|(held_chain, _)| **held_chain != chain)
                .flat_map(|(_, holders)| holders.iter())
                .any(|(holder, held_direction)| {
                    holder != id && *held_direction == RebalanceDirection::AlpacaToBase
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn new_id() -> UsdcRebalanceId {
        UsdcRebalanceId(Uuid::new_v4())
    }

    #[test]
    fn test_guard_releases_on_drop() {
        let guards = Arc::new(UsdcCashGuards::default());

        {
            let guard = guards
                .try_claim(Chain::Base, &new_id(), RebalanceDirection::BaseToAlpaca)
                .unwrap();
            assert!(guards.is_held(Chain::Base));
            drop(guard);
        }

        assert!(!guards.is_held(Chain::Base));
    }

    #[test]
    fn test_guard_defuse_prevents_release() {
        let guards = Arc::new(UsdcCashGuards::default());

        {
            let guard = guards
                .try_claim(Chain::Base, &new_id(), RebalanceDirection::BaseToAlpaca)
                .unwrap();
            assert!(guards.is_held(Chain::Base));
            guard.defuse();
        }

        // Should still be in progress after defused guard dropped
        assert!(guards.is_held(Chain::Base));
    }

    #[test]
    fn test_guard_try_claim_fails_when_already_claimed() {
        let guards = Arc::new(UsdcCashGuards::default());

        let _guard = guards
            .try_claim(Chain::Base, &new_id(), RebalanceDirection::BaseToAlpaca)
            .unwrap();

        let second_claim =
            guards.try_claim(Chain::Base, &new_id(), RebalanceDirection::BaseToAlpaca);
        assert!(second_claim.is_none());
    }

    #[test]
    fn cash_guard_on_one_corridor_does_not_block_another() {
        let guards = Arc::new(UsdcCashGuards::default());
        let base = new_id();
        guards
            .try_claim(Chain::Base, &base, RebalanceDirection::BaseToAlpaca)
            .expect("a free corridor admits a transfer")
            .defuse();

        let _robinhood = guards
            .try_claim(
                Chain::Robinhood,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
            )
            .expect("another corridor must admit its own transfer");
        let None = guards.try_claim(Chain::Base, &new_id(), RebalanceDirection::BaseToAlpaca)
        else {
            panic!("a second transfer on a held corridor must be refused");
        };

        let again = guards
            .try_claim(Chain::Base, &base, RebalanceDirection::BaseToAlpaca)
            .expect("the holder's own claim succeeds");
        drop(again);
        assert!(
            guards.is_held(Chain::Base),
            "the holder's own claim must not release its hold on drop"
        );
    }

    #[test]
    fn alpaca_outbound_waits_for_another_corridors_alpaca_outbound() {
        let guards = Arc::new(UsdcCashGuards::default());
        guards.hold(Chain::Base, &new_id(), RebalanceDirection::AlpacaToBase);

        let None = guards.try_claim(
            Chain::Robinhood,
            &new_id(),
            RebalanceDirection::AlpacaToBase,
        ) else {
            panic!("Alpaca's cash is shared: one outbound transfer at a time");
        };
        guards
            .try_claim(
                Chain::Robinhood,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
            )
            .expect("an inbound transfer does not draw on Alpaca's cash");
    }
}
