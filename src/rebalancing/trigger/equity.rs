//! Equity-specific trigger types and logic.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::inventory::{Imbalance, ImbalanceThreshold, InventoryView};
use crate::symbol::cache::SymbolCache;
use st0x_broker::Symbol;

use super::TriggeredOperation;

/// Why an equity trigger check did not produce an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EquityTriggerSkip {
    /// Another operation for this symbol is already in progress.
    AlreadyInProgress,
    /// No imbalance detected for this symbol.
    NoImbalance,
    /// Token address not found in cache (required for redemption).
    TokenNotInCache,
}

/// RAII guard that holds an equity in-progress claim.
/// Automatically releases the claim on drop unless `defuse` is called.
pub(super) struct InProgressGuard {
    /// The symbol this guard holds a claim for.
    symbol: Symbol,
    /// Shared reference to the in-progress set for cleanup on drop.
    in_progress: Arc<RwLock<HashSet<Symbol>>>,
    /// When true, the guard will not release the claim on drop.
    defused: bool,
}

impl InProgressGuard {
    /// Attempts to claim the in-progress slot for a symbol.
    /// Returns `None` if already claimed by another operation.
    pub(super) fn try_claim(
        symbol: Symbol,
        in_progress: Arc<RwLock<HashSet<Symbol>>>,
    ) -> Option<Self> {
        {
            let mut guard = match in_progress.write() {
                Ok(g) => g,
                Err(poison) => poison.into_inner(),
            };

            if guard.contains(&symbol) {
                return None;
            }

            guard.insert(symbol.clone());
        }

        Some(Self {
            symbol,
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
            let mut guard = match self.in_progress.write() {
                Ok(g) => g,
                Err(poison) => poison.into_inner(),
            };
            guard.remove(&self.symbol);
        }
    }
}

/// Checks inventory for equity imbalance and returns the appropriate rebalancing operation.
///
/// Returns `Mint` if there's too much offchain equity that needs to be tokenized,
/// or `Redemption` if there's too much onchain equity that needs to be redeemed.
pub(super) fn check_imbalance_and_build_operation(
    symbol: &Symbol,
    threshold: ImbalanceThreshold,
    inventory: &Arc<RwLock<InventoryView>>,
    symbol_cache: &SymbolCache,
) -> Result<TriggeredOperation, EquityTriggerSkip> {
    let imbalance = {
        let inventory = match inventory.read() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };

        let mut thresholds = HashMap::new();
        thresholds.insert(symbol.clone(), threshold);

        inventory
            .check_equity_imbalances(&thresholds)
            .into_iter()
            .find(|(s, _)| s == symbol)
            .map(|(_, imb)| imb)
    };

    let imbalance = imbalance.ok_or(EquityTriggerSkip::NoImbalance)?;

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => Ok(TriggeredOperation::Mint {
            symbol: symbol.clone(),
            quantity: excess,
        }),
        Imbalance::TooMuchOnchain { excess } => {
            let token = symbol_cache
                .get_address(&symbol.to_string())
                .ok_or(EquityTriggerSkip::TokenNotInCache)?;

            Ok(TriggeredOperation::Redemption {
                symbol: symbol.clone(),
                quantity: excess,
                token,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_guard_releases_on_drop() {
        let in_progress = Arc::new(RwLock::new(HashSet::new()));
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let guard =
                InProgressGuard::try_claim(symbol.clone(), Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.read().unwrap().contains(&symbol));
            drop(guard);
        }

        assert!(!in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn test_guard_defuse_prevents_release() {
        let in_progress = Arc::new(RwLock::new(HashSet::new()));
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let guard =
                InProgressGuard::try_claim(symbol.clone(), Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.read().unwrap().contains(&symbol));
            guard.defuse();
        }

        // Should still be in progress after defused guard dropped
        assert!(in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn test_guard_try_claim_fails_when_already_claimed() {
        let in_progress = Arc::new(RwLock::new(HashSet::new()));
        let symbol = Symbol::new("AAPL").unwrap();

        let _guard = InProgressGuard::try_claim(symbol.clone(), Arc::clone(&in_progress)).unwrap();

        let second_claim = InProgressGuard::try_claim(symbol, Arc::clone(&in_progress));
        assert!(second_claim.is_none());
    }

    #[test]
    fn test_balanced_inventory_returns_no_imbalance() {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let symbol_cache = SymbolCache::default();
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &Symbol::new("AAPL").unwrap(),
            threshold,
            &inventory,
            &symbol_cache,
        );
        assert_eq!(result, Err(EquityTriggerSkip::NoImbalance));
    }
}
