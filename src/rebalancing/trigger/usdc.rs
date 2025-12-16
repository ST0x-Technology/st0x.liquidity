//! USDC-specific trigger types and logic.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::RwLock;

use super::TriggeredOperation;
use crate::inventory::{Imbalance, ImbalanceThreshold, InventoryView};

/// Why a USDC trigger check did not produce an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UsdcTriggerSkip {
    /// No USDC imbalance detected.
    NoImbalance,
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
    inventory: &Arc<RwLock<InventoryView>>,
) -> Result<TriggeredOperation, UsdcTriggerSkip> {
    let imbalance = {
        let inventory = inventory.read().await;
        inventory.check_usdc_imbalance(threshold)
    };

    let imbalance = imbalance.ok_or(UsdcTriggerSkip::NoImbalance)?;

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => {
            Ok(TriggeredOperation::UsdcAlpacaToBase { amount: excess })
        }
        Imbalance::TooMuchOnchain { excess } => {
            Ok(TriggeredOperation::UsdcBaseToAlpaca { amount: excess })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory).await;

        assert_eq!(result, Err(UsdcTriggerSkip::NoImbalance));
    }
}
