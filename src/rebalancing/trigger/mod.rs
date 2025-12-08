//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod usdc;

use alloy::primitives::Address;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::warn;

use crate::inventory::{ImbalanceThreshold, InventoryView};
use crate::shares::FractionalShares;
use crate::symbol::cache::SymbolCache;
use crate::threshold::Usdc;
use st0x_broker::Symbol;

pub(crate) use equity::EquityTriggerSkip;
pub(crate) use usdc::UsdcTriggerSkip;

/// Configuration for the rebalancing trigger.
#[derive(Debug, Clone)]
pub(crate) struct RebalancingTriggerConfig {
    pub(crate) equity_threshold: ImbalanceThreshold,
    pub(crate) usdc_threshold: ImbalanceThreshold,
    pub(crate) wallet: Address,
}

/// Operations triggered by inventory imbalances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggeredOperation {
    /// Mint tokenized equity (too much offchain).
    Mint {
        symbol: Symbol,
        quantity: FractionalShares,
    },
    /// Redeem tokenized equity (too much onchain).
    Redemption {
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
    },
    /// Move USDC from Alpaca to Base (too much offchain).
    UsdcAlpacaToBase { amount: Usdc },
    /// Move USDC from Base to Alpaca (too much onchain).
    UsdcBaseToAlpaca { amount: Usdc },
}

/// Trigger that monitors inventory and sends rebalancing operations.
pub(crate) struct RebalancingTrigger {
    config: RebalancingTriggerConfig,
    symbol_cache: SymbolCache,
    inventory: Arc<RwLock<InventoryView>>,
    equity_in_progress: Arc<RwLock<HashSet<Symbol>>>,
    usdc_in_progress: Arc<AtomicBool>,
    sender: mpsc::Sender<TriggeredOperation>,
}

impl RebalancingTrigger {
    pub(crate) fn new(
        config: RebalancingTriggerConfig,
        symbol_cache: SymbolCache,
        inventory: Arc<RwLock<InventoryView>>,
        sender: mpsc::Sender<TriggeredOperation>,
    ) -> Self {
        Self {
            config,
            symbol_cache,
            inventory,
            equity_in_progress: Arc::new(RwLock::new(HashSet::new())),
            usdc_in_progress: Arc::new(AtomicBool::new(false)),
            sender,
        }
    }

    /// Checks inventory for equity imbalance and triggers operation if needed.
    /// Returns the triggered operation on success, or the reason it was skipped.
    pub(crate) fn check_and_trigger_equity(
        &self,
        symbol: &Symbol,
    ) -> Result<TriggeredOperation, EquityTriggerSkip> {
        let guard = equity::InProgressGuard::try_claim(
            symbol.clone(),
            Arc::clone(&self.equity_in_progress),
        )
        .ok_or(EquityTriggerSkip::AlreadyInProgress)?;

        let operation = equity::check_imbalance_and_build_operation(
            symbol,
            self.config.equity_threshold,
            &self.inventory,
            &self.symbol_cache,
        )?;

        self.sender
            .try_send(operation.clone())
            .map_err(|e| {
                warn!(error = %e, "Failed to send triggered operation");
                // Guard drops here, releasing the claim
            })
            .ok();

        guard.defuse();
        Ok(operation)
    }

    /// Checks inventory for USDC imbalance and triggers operation if needed.
    /// Returns the triggered operation on success, or the reason it was skipped.
    pub(crate) fn check_and_trigger_usdc(&self) -> Result<TriggeredOperation, UsdcTriggerSkip> {
        let guard = usdc::InProgressGuard::try_claim(Arc::clone(&self.usdc_in_progress))
            .ok_or(UsdcTriggerSkip::AlreadyInProgress)?;

        let operation = usdc::check_imbalance_and_build_operation(
            &self.config.usdc_threshold,
            &self.inventory,
        )?;

        self.sender
            .try_send(operation.clone())
            .map_err(|e| {
                warn!(error = %e, "Failed to send USDC triggered operation");
                // Guard drops here, releasing the claim
            })
            .ok();

        guard.defuse();
        Ok(operation)
    }

    /// Clears the in-progress flag for an equity symbol.
    pub(crate) fn clear_equity_in_progress(&self, symbol: &Symbol) {
        let mut guard = match self.equity_in_progress.write() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };
        guard.remove(symbol);
    }

    /// Clears the in-progress flag for USDC rebalancing.
    pub(crate) fn clear_usdc_in_progress(&self) {
        self.usdc_in_progress.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::atomic::Ordering;

    fn make_trigger() -> RebalancingTrigger {
        let (sender, _receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let symbol_cache = SymbolCache::default();
        let config = RebalancingTriggerConfig {
            equity_threshold: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc_threshold: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            wallet: address!("0x1234567890123456789012345678901234567890"),
        };

        RebalancingTrigger::new(config, symbol_cache, inventory, sender)
    }

    #[test]
    fn test_in_progress_symbol_returns_error() {
        let trigger = make_trigger();
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let result = trigger.check_and_trigger_equity(&symbol);
        assert_eq!(result, Err(EquityTriggerSkip::AlreadyInProgress));
    }

    #[test]
    fn test_usdc_in_progress_returns_error() {
        let trigger = make_trigger();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        let result = trigger.check_and_trigger_usdc();
        assert_eq!(result, Err(UsdcTriggerSkip::AlreadyInProgress));
    }

    #[test]
    fn test_clear_equity_in_progress() {
        let trigger = make_trigger();
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        trigger.clear_equity_in_progress(&symbol);

        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn test_clear_usdc_in_progress() {
        let trigger = make_trigger();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        trigger.clear_usdc_in_progress();

        assert!(!trigger.usdc_in_progress.load(Ordering::SeqCst));
    }

    #[test]
    fn test_balanced_inventory_returns_no_imbalance() {
        let trigger = make_trigger();
        let symbol = Symbol::new("AAPL").unwrap();

        let equity_result = trigger.check_and_trigger_equity(&symbol);
        assert_eq!(equity_result, Err(EquityTriggerSkip::NoImbalance));

        let usdc_result = trigger.check_and_trigger_usdc();
        assert_eq!(usdc_result, Err(UsdcTriggerSkip::NoImbalance));
    }
}
