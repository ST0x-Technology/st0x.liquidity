//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod usdc;

use alloy::primitives::Address;
use async_trait::async_trait;
use cqrs_es::{EventEnvelope, Query};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::inventory::{ImbalanceThreshold, InventoryView, InventoryViewError};
use crate::lifecycle::Lifecycle;
use crate::position::{Position, PositionEvent};
use crate::shares::{ArithmeticError, FractionalShares};
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

    /// Applies a position event to inventory and checks for rebalancing trigger.
    fn apply_position_event_and_check(&self, symbol: &Symbol, event: &PositionEvent) {
        if let Err(e) = self.apply_position_event_to_inventory(symbol, event) {
            warn!(symbol = %symbol, error = %e, "Failed to apply position event to inventory");
            return;
        }

        self.log_equity_trigger_result(symbol);
    }

    fn log_equity_trigger_result(&self, symbol: &Symbol) {
        match self.check_and_trigger_equity(symbol) {
            Ok(op) => {
                debug!(symbol = %symbol, operation = ?op, "Triggered rebalancing from position event");
            }
            Err(EquityTriggerSkip::AlreadyInProgress) => {
                debug!(symbol = %symbol, "Skipped equity trigger: already in progress");
            }
            Err(EquityTriggerSkip::NoImbalance) => {}
            Err(EquityTriggerSkip::TokenNotInCache) => {
                warn!(symbol = %symbol, "Skipped equity trigger: token not in cache");
            }
        }
    }

    fn apply_position_event_to_inventory(
        &self,
        symbol: &Symbol,
        event: &PositionEvent,
    ) -> Result<(), InventoryViewError> {
        let mut inventory = match self.inventory.write() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };

        let new_inventory = inventory.clone().apply_position_event(symbol, event)?;

        *inventory = new_inventory;
        drop(inventory);

        Ok(())
    }
}

#[async_trait]
impl Query<Lifecycle<Position, ArithmeticError<FractionalShares>>> for RebalancingTrigger {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<Position, ArithmeticError<FractionalShares>>>],
    ) {
        let Ok(symbol) = Symbol::new(aggregate_id) else {
            warn!(aggregate_id = %aggregate_id, "Invalid symbol in position aggregate_id");
            return;
        };

        for envelope in events {
            self.apply_position_event_and_check(&symbol, &envelope.payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{TxHash, address};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use st0x_broker::Direction;
    use std::sync::atomic::Ordering;

    use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
    use crate::position::TradeId;

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

    fn shares(n: i64) -> FractionalShares {
        FractionalShares(Decimal::from(n))
    }

    fn make_onchain_fill(amount: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            amount,
            direction,
            price_usdc: dec!(150.0),
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        }
    }

    fn make_offchain_fill(shares_filled: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OffChainOrderFilled {
            execution_id: ExecutionId(1),
            shares_filled,
            direction,
            broker_order_id: BrokerOrderId("ORD1".to_string()),
            price_cents: PriceCents(15000),
            broker_timestamp: Utc::now(),
        }
    }

    fn make_trigger_with_inventory(
        inventory: InventoryView,
    ) -> (RebalancingTrigger, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(inventory));
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

        (
            RebalancingTrigger::new(config, symbol_cache, inventory, sender),
            receiver,
        )
    }

    #[test]
    fn position_event_for_unknown_symbol_logs_error_without_panic() {
        let (trigger, _receiver) = make_trigger_with_inventory(InventoryView::default());
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_onchain_fill(shares(10), Direction::Buy);

        // Should handle the error gracefully without panicking.
        trigger.apply_position_event_and_check(&symbol, &event);

        // Verify no operation was triggered.
        let result = trigger.check_and_trigger_equity(&symbol);
        assert_eq!(result, Err(EquityTriggerSkip::NoImbalance));
    }

    #[test]
    fn position_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, _receiver) = make_trigger_with_inventory(inventory);

        // Apply onchain buy - should add to onchain available.
        let event = make_onchain_fill(shares(50), Direction::Buy);
        trigger.apply_position_event_and_check(&symbol, &event);

        // Apply offchain buy - should add to offchain available.
        let event = make_offchain_fill(shares(50), Direction::Buy);
        trigger.apply_position_event_and_check(&symbol, &event);

        // Now inventory has 50 onchain, 50 offchain = balanced at 50%.
        let result = trigger.check_and_trigger_equity(&symbol);
        assert_eq!(result, Err(EquityTriggerSkip::NoImbalance));
    }

    #[test]
    fn position_event_maintaining_balance_triggers_nothing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut inventory = InventoryView::default().with_equity(symbol.clone());

        // Build balanced initial state: 50 onchain, 50 offchain.
        inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(50), Direction::Buy))
            .unwrap();
        inventory = inventory
            .apply_position_event(&symbol, &make_offchain_fill(shares(50), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory);

        // Apply a small buy that maintains balance (5 shares onchain).
        // After: 55 onchain, 50 offchain = 52.4% ratio, within 30-70% bounds.
        let event = make_onchain_fill(shares(5), Direction::Buy);
        trigger.apply_position_event_and_check(&symbol, &event);

        // No operation should be triggered.
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn position_event_causing_imbalance_triggers_mint() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut inventory = InventoryView::default().with_equity(symbol.clone());

        // Build imbalanced state: 20 onchain, 80 offchain = 20% onchain ratio.
        // This is below the 30% lower threshold (50% - 20% deviation).
        inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(20), Direction::Buy))
            .unwrap();
        inventory = inventory
            .apply_position_event(&symbol, &make_offchain_fill(shares(80), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory);

        // Apply a small event that triggers the imbalance check.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        trigger.apply_position_event_and_check(&symbol, &event);

        // Mint should be triggered because too much offchain.
        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint operation, got {triggered:?}"
        );
    }
}
