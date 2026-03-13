//! USDC-specific trigger types and logic.

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use st0x_float_macro::float;
use tracing::{debug, trace, warn};

use st0x_finance::Usdc;

use super::TriggeredOperation;
use crate::inventory::{BroadcastingInventory, Imbalance, ImbalanceThreshold};

/// Minimum USDC amount for Alpaca withdrawals.
/// Alpaca requires $50 USD minimum, but due to USDC/USD spread (~17bps observed in live tests),
/// we use $51 to ensure we always meet the minimum after conversion slippage.
pub(crate) static ALPACA_MINIMUM_WITHDRAWAL: LazyLock<Usdc> =
    LazyLock::new(|| Usdc::new(float!(51)));

/// Why a USDC trigger check did not produce an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UsdcTriggerSkip {
    /// No USDC imbalance detected.
    NoImbalance,
    /// Imbalance exists but amount is below Alpaca's minimum withdrawal ($51).
    BelowMinimumWithdrawal { excess: Usdc },
    /// Arithmetic error during imbalance calculation.
    ArithmeticError,
}

/// RAII guard that holds a USDC in-progress claim.
/// Automatically releases the claim on drop unless `defuse` is called.
pub(super) struct InProgressGuard {
    /// Shared reference to the in-progress flag for cleanup on drop.
    in_progress: Arc<AtomicBool>,
    /// When true, the guard will not release the claim on drop.
    defused: bool,
}

impl InProgressGuard {
    /// Attempts to claim the USDC in-progress slot.
    /// Returns `None` if already claimed by another operation.
    pub(super) fn try_claim(in_progress: Arc<AtomicBool>) -> Option<Self> {
        let was_in_progress =
            in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);

        if was_in_progress.is_err() {
            return None;
        }

        Some(Self {
            in_progress,
            defused: false,
        })
    }

    /// Prevents the guard from releasing the claim on drop.
    /// Call this after successfully sending the operation.
    pub(super) fn defuse(mut self) {
        self.defused = true;
    }
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        if !self.defused {
            self.in_progress.store(false, Ordering::SeqCst);
        }
    }
}

/// Checks inventory for USDC imbalance and returns the appropriate bridging operation.
///
/// Returns `UsdcAlpacaToBase` if there's too much USDC in Alpaca that needs to be bridged to Base,
/// or `UsdcBaseToAlpaca` if there's too much USDC on Base that needs to be bridged to Alpaca.
pub(super) async fn check_imbalance_and_build_operation(
    threshold: &ImbalanceThreshold,
    inventory: &Arc<BroadcastingInventory>,
    usdc_limit: Option<Usdc>,
) -> Result<TriggeredOperation, UsdcTriggerSkip> {
    let imbalance = {
        let inventory = inventory.read().await;
        inventory.check_usdc_imbalance(threshold).map_err(|error| {
            warn!(
                ?error,
                "USDC imbalance check failed due to arithmetic error"
            );
            UsdcTriggerSkip::ArithmeticError
        })?
    };

    let Some(imbalance) = imbalance else {
        trace!("No USDC imbalance detected (balanced, partial data, or inflight)");
        return Err(UsdcTriggerSkip::NoImbalance);
    };

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => {
            let capped = cap_usdc(excess, usdc_limit);

            if capped
                .lt(&ALPACA_MINIMUM_WITHDRAWAL)
                .map_err(|_| UsdcTriggerSkip::NoImbalance)?
            {
                debug!(
                    excess = ?capped,
                    minimum = ?*ALPACA_MINIMUM_WITHDRAWAL,
                    "USDC imbalance below Alpaca minimum withdrawal, skipping"
                );
                Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess: capped })
            } else {
                Ok(TriggeredOperation::UsdcAlpacaToBase { amount: capped })
            }
        }
        Imbalance::TooMuchOnchain { excess } => Ok(TriggeredOperation::UsdcBaseToAlpaca {
            amount: cap_usdc(excess, usdc_limit),
        }),
    }
}

fn cap_usdc(amount: Usdc, usdc_limit: Option<Usdc>) -> Usdc {
    let Some(cap) = usdc_limit else {
        return amount;
    };

    if amount.gt(&cap).unwrap_or(false) {
        warn!(
            computed = ?amount,
            limit = ?cap,
            "USDC rebalancing amount capped by operational limit"
        );
        cap
    } else {
        amount
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;

    use st0x_dto::ServerMessage;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::InventoryView;

    #[test]
    fn test_guard_releases_on_drop() {
        let in_progress = Arc::new(AtomicBool::new(false));

        {
            let guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.load(Ordering::SeqCst));
            drop(guard);
        }

        assert!(!in_progress.load(Ordering::SeqCst));
    }

    #[test]
    fn test_guard_defuse_prevents_release() {
        let in_progress = Arc::new(AtomicBool::new(false));

        {
            let guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.load(Ordering::SeqCst));
            guard.defuse();
        }

        // Should still be in progress after defused guard dropped
        assert!(in_progress.load(Ordering::SeqCst));
    }

    #[test]
    fn test_guard_try_claim_fails_when_already_claimed() {
        let in_progress = Arc::new(AtomicBool::new(false));

        let _guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();

        let second_claim = InProgressGuard::try_claim(Arc::clone(&in_progress));
        assert!(second_claim.is_none());
    }

    #[tokio::test]
    async fn test_balanced_inventory_returns_no_imbalance() {
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None).await;

        assert_eq!(result, Err(UsdcTriggerSkip::NoImbalance));
    }

    #[test]
    fn alpaca_minimum_withdrawal_is_51_usdc() {
        assert_eq!(
            *ALPACA_MINIMUM_WITHDRAWAL,
            Usdc::new(float!(51)),
            "Alpaca minimum withdrawal should be $51 to account for ~17bps USDC/USD spread"
        );
    }

    #[tokio::test]
    async fn test_excess_below_minimum_returns_below_minimum_withdrawal() {
        // 90 offchain, 10 onchain = 90% offchain
        // Excess to reach 50% target = 90 - 50 = 40 USDC (below $51 minimum)
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(10)), Usdc::new(float!(90)));

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None).await;

        assert!(
            matches!(result, Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess }) if excess.inner().lt(float!(51)).unwrap_or(false)),
            "Expected BelowMinimumWithdrawal, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_excess_above_minimum_triggers_alpaca_to_base() {
        // 500 offchain, 100 onchain = 83% offchain
        // To reach 50% target: need 300 each, so excess = 500 - 300 = 200 USDC
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)));

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None).await;

        assert!(
            matches!(result, Ok(TriggeredOperation::UsdcAlpacaToBase { amount }) if !amount.inner().lt(float!(51)).unwrap_or(true)),
            "Expected UsdcAlpacaToBase with amount >= 51, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_excess_at_exactly_minimum_triggers_alpaca_to_base() {
        // Edge case: excess is exactly $51
        // Total = 102, target = 51 each
        // If offchain = 102, onchain = 0, excess = 102 - 51 = 51
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(0)), Usdc::new(float!(102)));

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None).await;

        assert!(
            matches!(result, Ok(TriggeredOperation::UsdcAlpacaToBase { amount }) if amount == Usdc::new(float!(51))),
            "Expected UsdcAlpacaToBase with amount = 51, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_too_much_onchain_has_no_minimum_restriction() {
        // When there's too much onchain (Base), we deposit to Alpaca.
        // Deposits have no minimum restriction (verified in production at 0.01 USDC).
        // 10 offchain, 90 onchain = 10% offchain (below 30% lower bound)
        // Excess = 90 - 50 = 40 USDC (would be below withdrawal minimum, but deposits are fine)
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(90)), Usdc::new(float!(10)));

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None).await;

        assert!(
            matches!(result, Ok(TriggeredOperation::UsdcBaseToAlpaca { amount }) if amount == Usdc::new(float!(40))),
            "Expected UsdcBaseToAlpaca with amount = 40 (no minimum for deposits), got {result:?}"
        );
    }

    #[tokio::test]
    async fn operational_limits_cap_usdc_amount() {
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)));
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(100)));

        let result = check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit).await;

        assert!(
            matches!(result, Ok(TriggeredOperation::UsdcAlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Operational limit should cap USDC transfer to 100, got {result:?}"
        );
    }

    #[tokio::test]
    async fn capped_usdc_rebalancing_leaves_remaining_imbalance_triggerable() {
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(100)));

        // 100 onchain / 500 offchain -> 83% offchain, excess = 200
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500))),
            event_sender,
        ));

        let first = check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit).await;
        assert!(
            matches!(first, Ok(TriggeredOperation::UsdcAlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "First transfer capped to 100, got {first:?}"
        );

        // After transferring 100: 200 onchain / 400 offchain -> 67% offchain
        // Still above 70% threshold? No - 400/600 = 66.7%, within 30%-70%. No trigger.
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let after_first = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(Usdc::new(float!(200)), Usdc::new(float!(400))),
            event_sender,
        ));

        let second =
            check_imbalance_and_build_operation(&threshold, &after_first, usdc_limit).await;
        assert_eq!(
            second,
            Err(UsdcTriggerSkip::NoImbalance),
            "After 100 USDC transfer, 66.7% offchain is within bounds"
        );

        // But if only 50 was transferred: 150 onchain / 450 offchain -> 75% offchain
        // Still above 70%, so triggers again
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let partially_resolved = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(Usdc::new(float!(150)), Usdc::new(float!(450))),
            event_sender,
        ));

        let third =
            check_imbalance_and_build_operation(&threshold, &partially_resolved, usdc_limit).await;
        assert!(
            matches!(third, Ok(TriggeredOperation::UsdcAlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Remaining imbalance triggers another capped transfer, got {third:?}"
        );
    }

    #[tokio::test]
    async fn capped_amount_below_minimum_skips_withdrawal() {
        // excess = $200 (above $51 minimum), but limit = $30 caps it below minimum
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500))),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(30)));

        let result = check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit).await;

        assert!(
            matches!(
                result,
                Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess })
                    if excess == Usdc::new(float!(30))
            ),
            "Cap of $30 should produce BelowMinimumWithdrawal, got {result:?}"
        );
    }
}
