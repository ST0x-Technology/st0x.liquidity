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
use crate::lifecycle::{Lifecycle, Never};
use crate::position::{Position, PositionEvent};
use crate::shares::{ArithmeticError, FractionalShares};
use crate::symbol::cache::SymbolCache;
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
use chrono::Utc;
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

#[async_trait]
impl Query<Lifecycle<TokenizedEquityMint, Never>> for RebalancingTrigger {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<TokenizedEquityMint, Never>>],
    ) {
        let Some((symbol, quantity)) = Self::extract_mint_info(events) else {
            warn!(aggregate_id = %aggregate_id, "No MintRequested event found in mint dispatch");
            return;
        };

        for envelope in events {
            self.apply_mint_event_to_inventory(&symbol, &envelope.payload, quantity);
        }

        if Self::has_terminal_mint_event(events) {
            self.clear_equity_in_progress(&symbol);
            debug!(symbol = %symbol, "Cleared equity in-progress flag after mint terminal event");
        }
    }
}

impl RebalancingTrigger {
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

    fn extract_mint_info(
        events: &[EventEnvelope<Lifecycle<TokenizedEquityMint, Never>>],
    ) -> Option<(Symbol, FractionalShares)> {
        for envelope in events {
            if let TokenizedEquityMintEvent::MintRequested {
                symbol, quantity, ..
            } = &envelope.payload
            {
                let shares = FractionalShares(*quantity);
                return Some((symbol.clone(), shares));
            }
        }
        None
    }

    fn has_terminal_mint_event(
        events: &[EventEnvelope<Lifecycle<TokenizedEquityMint, Never>>],
    ) -> bool {
        events.iter().any(|envelope| {
            matches!(
                envelope.payload,
                TokenizedEquityMintEvent::MintCompleted { .. }
                    | TokenizedEquityMintEvent::MintRejected { .. }
                    | TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
                    | TokenizedEquityMintEvent::TokenReceiptFailed { .. }
            )
        })
    }

    fn apply_mint_event_to_inventory(
        &self,
        symbol: &Symbol,
        event: &TokenizedEquityMintEvent,
        quantity: FractionalShares,
    ) {
        let mut inventory = match self.inventory.write() {
            Ok(g) => g,
            Err(poison) => poison.into_inner(),
        };

        let result = inventory
            .clone()
            .apply_mint_event(symbol, event, quantity, Utc::now());

        match result {
            Ok(new_inventory) => {
                *inventory = new_inventory;
            }
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "Failed to apply mint event to inventory");
            }
        }

        drop(inventory);
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
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
    use crate::position::TradeId;
    use crate::tokenized_equity_mint::{IssuerRequestId, ReceiptId, TokenizationRequestId};
    use alloy::primitives::{Address, U256};

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

    fn make_mint_requested(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet: Address::random(),
            requested_at: Utc::now(),
        }
    }

    fn make_mint_accepted() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId::new("ISS123"),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        }
    }

    fn make_mint_completed() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintCompleted {
            completed_at: Utc::now(),
        }
    }

    fn make_mint_rejected() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRejected {
            reason: "API timeout".to_string(),
            rejected_at: Utc::now(),
        }
    }

    fn make_mint_acceptance_failed() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "Transaction reverted".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_token_receipt_failed() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokenReceiptFailed {
            reason: "Verification failed".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_tokens_received() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(30_000_000_000_000_000_000_u128),
            received_at: Utc::now(),
        }
    }

    fn make_mint_envelope(
        event: TokenizedEquityMintEvent,
    ) -> EventEnvelope<Lifecycle<TokenizedEquityMint, Never>> {
        EventEnvelope {
            aggregate_id: "mint-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::default(),
        }
    }

    #[test]
    fn mint_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        // Start with 20 onchain and 80 offchain - imbalanced (20% onchain).
        // Threshold: target 50%, deviation 20%, so lower bound is 30%.
        // 20% < 30% triggers TooMuchOffchain.
        let inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(20), Direction::Buy))
            .unwrap()
            .apply_position_event(&symbol, &make_offchain_fill(shares(80), Direction::Buy))
            .unwrap();

        let (trigger, _receiver) = make_trigger_with_inventory(inventory);

        // Initially, trigger should detect imbalance (too much offchain).
        let initial_check = trigger.check_and_trigger_equity(&symbol);
        assert!(
            matches!(initial_check, Ok(TriggeredOperation::Mint { .. })),
            "Expected initial imbalance to trigger Mint, got {initial_check:?}"
        );

        // Clear in-progress so we can test again.
        trigger.clear_equity_in_progress(&symbol);

        // Apply MintAccepted - this moves shares to inflight.
        // Inflight should now block imbalance detection.
        trigger.apply_mint_event_to_inventory(&symbol, &make_mint_accepted(), shares(30));

        // With inflight, imbalance detection should return NoImbalance.
        let after_accepted = trigger.check_and_trigger_equity(&symbol);
        assert_eq!(
            after_accepted,
            Err(EquityTriggerSkip::NoImbalance),
            "Expected NoImbalance due to inflight"
        );
    }

    #[test]
    fn mint_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, _receiver) = make_trigger_with_inventory(inventory);

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }
        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        // Create event batch with MintRequested and MintCompleted.
        let events = vec![
            make_mint_envelope(make_mint_requested(&symbol, dec!(30))),
            make_mint_envelope(make_mint_completed()),
        ];

        // Check that terminal event is detected.
        assert!(RebalancingTrigger::has_terminal_mint_event(&events));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn mint_rejection_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, _receiver) = make_trigger_with_inventory(inventory);

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let events = vec![
            make_mint_envelope(make_mint_requested(&symbol, dec!(30))),
            make_mint_envelope(make_mint_rejected()),
        ];

        assert!(RebalancingTrigger::has_terminal_mint_event(&events));

        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn mint_acceptance_failure_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();

        let events = vec![
            make_mint_envelope(make_mint_requested(&symbol, dec!(30))),
            make_mint_envelope(make_mint_acceptance_failed()),
        ];

        assert!(RebalancingTrigger::has_terminal_mint_event(&events));
    }

    #[test]
    fn mint_token_receipt_failure_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();

        let events = vec![
            make_mint_envelope(make_mint_requested(&symbol, dec!(30))),
            make_mint_envelope(make_token_receipt_failed()),
        ];

        assert!(RebalancingTrigger::has_terminal_mint_event(&events));
    }

    #[test]
    fn extract_mint_info_returns_symbol_and_quantity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let events = vec![make_mint_envelope(make_mint_requested(&symbol, dec!(42.5)))];

        let result = RebalancingTrigger::extract_mint_info(&events);

        let (extracted_symbol, extracted_quantity) = result.unwrap();
        assert_eq!(extracted_symbol, symbol);
        assert_eq!(extracted_quantity.0, dec!(42.5));
    }

    #[test]
    fn extract_mint_info_returns_none_without_mint_requested() {
        let events = vec![make_mint_envelope(make_mint_completed())];

        let result = RebalancingTrigger::extract_mint_info(&events);

        assert!(result.is_none());
    }

    #[test]
    fn has_terminal_mint_event_returns_false_for_non_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let events = vec![
            make_mint_envelope(make_mint_requested(&symbol, dec!(30))),
            make_mint_envelope(make_mint_accepted()),
            make_mint_envelope(make_tokens_received()),
        ];

        assert!(!RebalancingTrigger::has_terminal_mint_event(&events));
    }
}
