//! One USDC cash guard per corridor chain (ADR 0003, amended).
//!
//! A corridor admits a new transfer only while no other transfer holds it,
//! and at most one Alpaca-outbound transfer holds a guard across all
//! corridors, because Alpaca's withdrawable cash and its USDC inflight are
//! shared. A release removes only its own transfer, so it never frees a guard
//! another transfer still holds.
//!
//! Every hold carries its transfer's amount, published with the holder in
//! one step; chain-to-Alpaca sizing on the other corridors reads the sum of
//! inbound amounts as cash on its way to Alpaca.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rain_math_float::FloatError;
use tracing::warn;

use st0x_evm::Chain;
use st0x_finance::{HasZero, Usdc};

use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceId};

#[derive(Debug, Default)]
pub(crate) struct UsdcCashGuards(Mutex<CashGuardState>);

impl UsdcCashGuards {
    /// Claims `chain`'s guard for `id` with its `amount`. Refused while
    /// another transfer holds the chain, while a transfer whose corridor could
    /// not be read latches every chain, or, for an Alpaca-outbound transfer,
    /// while another chain holds one. A claim by the transfer already holding
    /// the chain succeeds, replaces its amount and leaves that hold in place on
    /// drop.
    pub(super) fn try_claim(
        self: &Arc<Self>,
        chain: Chain,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        amount: Usdc,
    ) -> Result<CashGuardClaim, ClaimRefusal> {
        let fresh = self.state().claim(chain, id, Hold { direction, amount })?;

        Ok(CashGuardClaim {
            guards: Arc::clone(self),
            id: id.clone(),
            release_on_drop: fresh,
        })
    }

    /// Keeps `chain` held by `id` with its `amount`, whatever else holds it.
    pub(super) fn hold(
        &self,
        chain: Chain,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        amount: Usdc,
    ) {
        self.state()
            .holders
            .entry(chain)
            .or_default()
            .insert(id.clone(), Hold { direction, amount });
    }

    /// The chain whose guard `id` holds, if any.
    pub(super) fn held_chain(&self, id: &UsdcRebalanceId) -> Option<Chain> {
        self.state()
            .holders
            .iter()
            .find_map(|(chain, holders)| holders.contains_key(id).then_some(*chain))
    }

    /// Frees whatever `id` holds; other holders keep their guards.
    pub(crate) fn release(&self, id: &UsdcRebalanceId) {
        self.state().holders.retain(|_, holders| {
            holders.remove(id);
            !holders.is_empty()
        });
    }

    /// The amounts of chain-to-Alpaca transfers holding a corridor other than
    /// `chain`: cash on its way to Alpaca that `chain`'s sizing has not seen
    /// land yet.
    pub(super) fn inbound_elsewhere(&self, chain: Chain) -> Result<Usdc, FloatError> {
        self.state()
            .holders
            .iter()
            .filter(|(held_chain, _)| **held_chain != chain)
            .flat_map(|(_, holders)| holders.values())
            .filter(|hold| hold.direction == RebalanceDirection::BaseToAlpaca)
            .try_fold(Usdc::ZERO, |sum, hold| sum + hold.amount)
    }

    /// Blocks every corridor until a restart: a transfer whose corridor
    /// could not be read may hold any of them.
    pub(super) fn latch_unclassified(&self) {
        self.state().unclassified = true;
    }

    /// Blocks every corridor while `id`'s aggregate fails to load (a store
    /// error, possibly transient); [`Self::resolve_unread`] lifts it.
    pub(super) fn block_unread(&self, id: &UsdcRebalanceId) {
        self.state().unread.insert(id.clone());
    }

    /// Transfers blocking every corridor until their aggregate loads.
    pub(super) fn unread_ids(&self) -> Vec<UsdcRebalanceId> {
        self.state().unread.iter().cloned().collect()
    }

    /// Lifts `id`'s unread block, holding `holder`'s chain for it with its
    /// amount when the loaded state still holds a guard.
    pub(super) fn resolve_unread(
        &self,
        id: &UsdcRebalanceId,
        holder: Option<(Chain, RebalanceDirection, Usdc)>,
    ) {
        let mut state = self.state();
        state.unread.remove(id);
        if let Some((chain, direction, amount)) = holder {
            state
                .holders
                .entry(chain)
                .or_default()
                .insert(id.clone(), Hold { direction, amount });
        }
    }

    /// Whether a transfer whose corridor could not be read latches every
    /// corridor.
    pub(super) fn is_latched(&self) -> bool {
        self.state().unclassified
    }

    /// Whether a transfer holds `chain`; the every-corridor latch is read
    /// with [`Self::is_latched`].
    #[cfg(test)]
    pub(crate) fn is_held(&self, chain: Chain) -> bool {
        self.state().holders.contains_key(&chain)
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

/// Why a corridor refused a claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClaimRefusal {
    /// Another transfer holds the corridor.
    CorridorHeld,
    /// Another corridor holds an Alpaca-outbound transfer; Alpaca's cash is
    /// shared, so a second one must wait.
    AlpacaOutboundElsewhere,
    /// A transfer whose corridor could not be read (at startup or at
    /// runtime) latches every corridor until a restart.
    Unclassified,
    /// A transfer's aggregate failed to load; every corridor waits until
    /// the sweep reads it.
    CorridorUnread,
}

impl std::fmt::Display for ClaimRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CorridorHeld => formatter.write_str("another transfer holds the corridor"),
            Self::AlpacaOutboundElsewhere => {
                formatter.write_str("another corridor holds an Alpaca-outbound transfer")
            }
            Self::CorridorUnread => formatter.write_str(
                "a transfer's aggregate failed to load; every corridor waits for the sweep to \
                 read it",
            ),
            Self::Unclassified => formatter.write_str(
                "a transfer whose corridor could not be read latches every corridor until a \
                 restart",
            ),
        }
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
    /// Replaces the claimed transfer's amount: the trigger re-sizes under its
    /// claim before it queues the job.
    pub(super) fn resize(&self, amount: Usdc) {
        let resized = self
            .guards
            .state()
            .holders
            .values_mut()
            .find_map(|holders| holders.get_mut(&self.id))
            .map(|hold| hold.amount = amount)
            .is_some();

        if !resized {
            warn!(target: "rebalance", id = %self.id, "Resize found no hold for its claim");
        }
    }

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
    holders: BTreeMap<Chain, HashMap<UsdcRebalanceId, Hold>>,
    unclassified: bool,
    unread: HashSet<UsdcRebalanceId>,
}

/// A holder's direction and initiated amount, stored as one entry so no
/// reader sees a holder without its amount.
#[derive(Debug, Clone, Copy)]
struct Hold {
    direction: RebalanceDirection,
    amount: Usdc,
}

impl CashGuardState {
    /// Adds `id` to `chain`'s holders when admitted; `Ok(true)` when the
    /// hold is new.
    fn claim(
        &mut self,
        chain: Chain,
        id: &UsdcRebalanceId,
        hold: Hold,
    ) -> Result<bool, ClaimRefusal> {
        self.admits(chain, id, hold.direction)?;

        let fresh = self
            .holders
            .entry(chain)
            .or_default()
            .insert(id.clone(), hold)
            .is_none();

        Ok(fresh)
    }

    fn admits(
        &self,
        chain: Chain,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
    ) -> Result<(), ClaimRefusal> {
        if self.unclassified {
            return Err(ClaimRefusal::Unclassified);
        }

        if !self.unread.is_empty() {
            return Err(ClaimRefusal::CorridorUnread);
        }

        // Checked before the corridor, so a caller that tolerates a held
        // corridor (a manual resume) still meets the outbound rule.
        let outbound_elsewhere = match direction {
            RebalanceDirection::BaseToAlpaca => false,
            RebalanceDirection::AlpacaToBase => self
                .holders
                .iter()
                .filter(|(held_chain, _)| **held_chain != chain)
                .flat_map(|(_, holders)| holders.iter())
                .any(|(holder, hold)| {
                    holder != id && hold.direction == RebalanceDirection::AlpacaToBase
                }),
        };
        if outbound_elsewhere {
            return Err(ClaimRefusal::AlpacaOutboundElsewhere);
        }

        let chain_taken = self
            .holders
            .get(&chain)
            .is_some_and(|holders| holders.keys().any(|holder| holder != id));
        if chain_taken {
            return Err(ClaimRefusal::CorridorHeld);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use st0x_float_macro::float;

    use super::*;

    fn new_id() -> UsdcRebalanceId {
        UsdcRebalanceId(Uuid::new_v4())
    }

    #[test]
    fn test_guard_releases_on_drop() {
        let guards = Arc::new(UsdcCashGuards::default());

        {
            let guard = guards
                .try_claim(
                    Chain::Base,
                    &new_id(),
                    RebalanceDirection::BaseToAlpaca,
                    Usdc::ZERO,
                )
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
                .try_claim(
                    Chain::Base,
                    &new_id(),
                    RebalanceDirection::BaseToAlpaca,
                    Usdc::ZERO,
                )
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
            .try_claim(
                Chain::Base,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
                Usdc::ZERO,
            )
            .unwrap();

        let second_claim = guards.try_claim(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        );
        assert!(matches!(second_claim, Err(ClaimRefusal::CorridorHeld)));
    }

    #[test]
    fn cash_guard_on_one_corridor_does_not_block_another() {
        let guards = Arc::new(UsdcCashGuards::default());
        let base = new_id();
        guards
            .try_claim(
                Chain::Base,
                &base,
                RebalanceDirection::BaseToAlpaca,
                Usdc::ZERO,
            )
            .expect("a free corridor admits a transfer")
            .defuse();

        let _robinhood = guards
            .try_claim(
                Chain::Robinhood,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
                Usdc::ZERO,
            )
            .expect("another corridor must admit its own transfer");
        let Err(ClaimRefusal::CorridorHeld) = guards.try_claim(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        ) else {
            panic!("a second transfer on a held corridor must be refused");
        };

        let again = guards
            .try_claim(
                Chain::Base,
                &base,
                RebalanceDirection::BaseToAlpaca,
                Usdc::ZERO,
            )
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
        guards.hold(
            Chain::Base,
            &new_id(),
            RebalanceDirection::AlpacaToBase,
            Usdc::ZERO,
        );

        let Err(ClaimRefusal::AlpacaOutboundElsewhere) = guards.try_claim(
            Chain::Robinhood,
            &new_id(),
            RebalanceDirection::AlpacaToBase,
            Usdc::ZERO,
        ) else {
            panic!("Alpaca's cash is shared: one outbound transfer at a time");
        };
        guards
            .try_claim(
                Chain::Robinhood,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
                Usdc::ZERO,
            )
            .expect("an inbound transfer does not draw on Alpaca's cash");
    }

    #[test]
    fn a_held_corridor_refuses_the_opposite_direction_too() {
        let guards = Arc::new(UsdcCashGuards::default());
        guards.hold(
            Chain::Base,
            &new_id(),
            RebalanceDirection::AlpacaToBase,
            Usdc::ZERO,
        );

        let Err(ClaimRefusal::CorridorHeld) = guards.try_claim(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        ) else {
            panic!("both directions of a corridor share its guard");
        };
    }

    #[test]
    fn releasing_one_of_two_holders_keeps_the_corridor_held() {
        let guards = Arc::new(UsdcCashGuards::default());
        let first = new_id();
        guards.hold(
            Chain::Base,
            &first,
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        );
        guards.hold(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        );

        guards.release(&first);

        let Err(ClaimRefusal::CorridorHeld) = guards.try_claim(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            Usdc::ZERO,
        ) else {
            panic!("the other holder must keep the corridor held");
        };
    }

    fn usdc(amount: i64) -> Usdc {
        Usdc::new(float!(&amount.to_string()))
    }

    #[test]
    fn inbound_elsewhere_sums_other_corridors_inbound_amounts() {
        let guards = Arc::new(UsdcCashGuards::default());
        guards.hold(
            Chain::Base,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            usdc(50),
        );
        guards.hold(
            Chain::HyperEvm,
            &new_id(),
            RebalanceDirection::AlpacaToBase,
            usdc(30),
        );
        guards.hold(
            Chain::Robinhood,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            usdc(20),
        );

        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(50),
            "outbound and own holds add nothing"
        );
        assert_eq!(guards.inbound_elsewhere(Chain::HyperEvm).unwrap(), usdc(70));
    }

    /// Each publishing call counts its amount from the call itself; `Hold`
    /// stores direction and amount as one entry.
    #[test]
    fn every_hold_counts_its_amount_from_the_call_that_publishes_it() {
        let guards = Arc::new(UsdcCashGuards::default());

        let _claim = guards
            .try_claim(
                Chain::Base,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
                usdc(50),
            )
            .unwrap();
        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(50)
        );

        guards.hold(
            Chain::Ethereum,
            &new_id(),
            RebalanceDirection::BaseToAlpaca,
            usdc(30),
        );
        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(80)
        );

        let unread = new_id();
        guards.block_unread(&unread);
        guards.resolve_unread(
            &unread,
            Some((Chain::HyperEvm, RebalanceDirection::BaseToAlpaca, usdc(20))),
        );
        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(100)
        );
    }

    /// A resume claiming the hold its own transfer already has replaces the
    /// amount with the persisted one.
    #[test]
    fn a_claim_by_the_holder_replaces_its_amount() {
        let guards = Arc::new(UsdcCashGuards::default());
        let id = new_id();
        guards.hold(Chain::Base, &id, RebalanceDirection::BaseToAlpaca, usdc(50));

        let claim = guards
            .try_claim(Chain::Base, &id, RebalanceDirection::BaseToAlpaca, usdc(40))
            .unwrap();
        drop(claim);

        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(40)
        );
    }

    #[test]
    fn release_drops_the_amount() {
        let guards = Arc::new(UsdcCashGuards::default());
        let id = new_id();
        guards.hold(Chain::Base, &id, RebalanceDirection::BaseToAlpaca, usdc(50));

        guards.release(&id);

        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            Usdc::ZERO
        );
    }

    /// The trigger re-sizes under its claim before the enqueue: the re-sized
    /// amount is the one that counts.
    #[test]
    fn resizing_a_claim_replaces_its_amount() {
        let guards = Arc::new(UsdcCashGuards::default());
        let claim = guards
            .try_claim(
                Chain::Base,
                &new_id(),
                RebalanceDirection::BaseToAlpaca,
                usdc(50),
            )
            .unwrap();

        claim.resize(usdc(40));

        assert_eq!(
            guards.inbound_elsewhere(Chain::Robinhood).unwrap(),
            usdc(40)
        );
    }
}
