//! Equity-specific trigger types and logic.

use std::collections::HashSet;
use std::sync::Arc;

use alloy::primitives::Address;
use st0x_execution::Symbol;
use tokio::sync::RwLock;

use super::TriggeredOperation;
use crate::inventory::{Imbalance, ImbalanceThreshold, InventoryView};

/// Why an equity trigger check did not produce an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EquityTriggerSkip {
    /// No imbalance detected for this symbol.
    NoImbalance,
}

/// RAII guard that holds an equity in-progress claim.
/// Automatically releases the claim on drop unless `defuse` is called.
pub(super) struct InProgressGuard {
    /// The symbol this guard holds a claim for.
    symbol: Symbol,
    /// Shared reference to the in-progress set for cleanup on drop.
    in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    /// When true, the guard will not release the claim on drop.
    defused: bool,
}

impl InProgressGuard {
    /// Attempts to claim the in-progress slot for a symbol.
    /// Returns `None` if already claimed by another operation.
    pub(super) fn try_claim(
        symbol: Symbol,
        in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
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
pub(super) async fn check_imbalance_and_build_operation(
    symbol: &Symbol,
    threshold: &ImbalanceThreshold,
    inventory: &Arc<RwLock<InventoryView>>,
    token_address: Address,
) -> Result<TriggeredOperation, EquityTriggerSkip> {
    let imbalance = {
        let inventory = inventory.read().await;
        inventory.check_equity_imbalance(symbol, threshold)
    };

    let imbalance = imbalance.ok_or(EquityTriggerSkip::NoImbalance)?;

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => Ok(TriggeredOperation::Mint {
            symbol: symbol.clone(),
            quantity: excess,
        }),
        Imbalance::TooMuchOnchain { excess } => Ok(TriggeredOperation::Redemption {
            symbol: symbol.clone(),
            quantity: excess,
            token: token_address,
        }),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, address};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use st0x_execution::Direction;

    use st0x_execution::FractionalShares;

    use super::*;
    use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
    use crate::position::{PositionEvent, TradeId};

    fn shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
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
            block_timestamp: chrono::Utc::now(),
            seen_at: chrono::Utc::now(),
        }
    }

    fn make_offchain_fill(shares_filled: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OffChainOrderFilled {
            execution_id: ExecutionId(1),
            shares_filled,
            direction,
            broker_order_id: BrokerOrderId("ORD1".to_string()),
            price_cents: PriceCents(15000),
            broker_timestamp: chrono::Utc::now(),
        }
    }

    fn make_imbalanced_view(
        symbol: &Symbol,
        onchain: i64,
        offchain: i64,
    ) -> Arc<RwLock<InventoryView>> {
        let view = InventoryView::default()
            .with_equity(symbol.clone())
            .apply_position_event(symbol, &make_onchain_fill(shares(onchain), Direction::Buy))
            .unwrap()
            .apply_position_event(
                symbol,
                &make_offchain_fill(shares(offchain), Direction::Buy),
            )
            .unwrap();

        Arc::new(RwLock::new(view))
    }

    #[test]
    fn test_guard_releases_on_drop() {
        let in_progress = Arc::new(std::sync::RwLock::new(HashSet::new()));
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
        let in_progress = Arc::new(std::sync::RwLock::new(HashSet::new()));
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
        let in_progress = Arc::new(std::sync::RwLock::new(HashSet::new()));
        let symbol = Symbol::new("AAPL").unwrap();

        let _guard = InProgressGuard::try_claim(symbol.clone(), Arc::clone(&in_progress)).unwrap();

        let second_claim = InProgressGuard::try_claim(symbol, Arc::clone(&in_progress));
        assert!(second_claim.is_none());
    }

    #[tokio::test]
    async fn test_balanced_inventory_returns_no_imbalance() {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &Symbol::new("AAPL").unwrap(),
            &threshold,
            &inventory,
            Address::ZERO,
        )
        .await;

        assert_eq!(result, Err(EquityTriggerSkip::NoImbalance));
    }

    #[tokio::test]
    async fn test_too_much_offchain_returns_mint() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = make_imbalanced_view(&symbol, 20, 80);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        let result =
            check_imbalance_and_build_operation(&symbol, &threshold, &inventory, Address::ZERO)
                .await;

        assert!(matches!(result, Ok(TriggeredOperation::Mint { .. })));
    }

    #[tokio::test]
    async fn test_too_much_onchain_returns_redemption_with_token() {
        let symbol = Symbol::new("AAPL").unwrap();
        let token_address = address!("0x1234567890123456789012345678901234567890");
        let inventory = make_imbalanced_view(&symbol, 80, 20);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        let result =
            check_imbalance_and_build_operation(&symbol, &threshold, &inventory, token_address)
                .await;

        let Ok(TriggeredOperation::Redemption { token, .. }) = result else {
            panic!("Expected Redemption, got {result:?}");
        };
        assert_eq!(token, token_address);
    }
}
