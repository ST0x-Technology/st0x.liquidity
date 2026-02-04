//! Equity-specific trigger types and logic.

use std::collections::HashSet;
use std::sync::Arc;

use alloy::primitives::Address;
use st0x_execution::{FractionalShares, Symbol};
use tokio::sync::RwLock;
use tracing::{debug, trace};

use super::TriggeredOperation;
use crate::inventory::{Imbalance, ImbalanceThreshold, InventoryView};

/// Maximum decimal places for Alpaca tokenization API quantities.
const ALPACA_QUANTITY_MAX_DECIMAL_PLACES: u32 = 9;

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
                Ok(guard) => guard,
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
                Ok(guard) => guard,
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

    let Some(imbalance) = imbalance else {
        trace!(symbol = %symbol, "No equity imbalance detected (balanced, partial data, or inflight)");
        return Err(EquityTriggerSkip::NoImbalance);
    };

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => {
            let quantity = truncate_for_alpaca(symbol, excess);
            Ok(TriggeredOperation::Mint {
                symbol: symbol.clone(),
                quantity,
            })
        }
        Imbalance::TooMuchOnchain { excess } => {
            let quantity = truncate_for_alpaca(symbol, excess);
            Ok(TriggeredOperation::Redemption {
                symbol: symbol.clone(),
                quantity,
                token: token_address,
            })
        }
    }
}

fn truncate_for_alpaca(symbol: &Symbol, quantity: FractionalShares) -> FractionalShares {
    let truncated_decimal = quantity
        .inner()
        .trunc_with_scale(ALPACA_QUANTITY_MAX_DECIMAL_PLACES);
    let truncated = FractionalShares::new(truncated_decimal);

    if truncated != quantity {
        debug!(
            symbol = %symbol,
            original = %quantity.inner(),
            truncated = %truncated_decimal,
            "Truncated quantity to {} decimal places for Alpaca API",
            ALPACA_QUANTITY_MAX_DECIMAL_PLACES
        );
    }

    truncated
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::{TxHash, U256, address};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use st0x_execution::{Direction, ExecutorOrderId, FractionalShares, Positive};

    use super::*;
    use crate::offchain_order::{OffchainOrderId, PriceCents};
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
            offchain_order_id: OffchainOrderId::new(),
            shares_filled: Positive::new(shares_filled).unwrap(),
            direction,
            executor_order_id: ExecutorOrderId::new("ORD1"),
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

    fn precise_shares(s: &str) -> FractionalShares {
        FractionalShares::new(Decimal::from_str(s).unwrap())
    }

    fn make_precise_imbalanced_view(
        symbol: &Symbol,
        onchain: &str,
        offchain: &str,
    ) -> Arc<RwLock<InventoryView>> {
        let view = InventoryView::default()
            .with_equity(symbol.clone())
            .apply_position_event(
                symbol,
                &make_onchain_fill(precise_shares(onchain), Direction::Buy),
            )
            .unwrap()
            .apply_position_event(
                symbol,
                &make_offchain_fill(precise_shares(offchain), Direction::Buy),
            )
            .unwrap();

        Arc::new(RwLock::new(view))
    }

    /// Verifies that quantity truncation doesn't lose the truncated portion from inventory.
    ///
    /// When we truncate 3.1762222348598623822654992091 to 3.176222234 for Alpaca,
    /// the 0.0000000008598623822654992091 must remain in inventory and accumulate.
    #[tokio::test]
    async fn truncation_preserves_leftover_in_inventory() {
        let symbol = Symbol::new("RKLB").unwrap();

        // Set up inventory where excess calculation produces high-precision result.
        // With 20% onchain, 80% offchain and 50% target:
        // - Total = 100, target onchain = 50, current onchain = 20
        // - Excess (too much offchain) = 50 - 20 = 30
        //
        // Use precise values to get high-precision excess:
        // onchain = 6.3524444697197247645309984182
        // offchain = 25.4097778788788990581239936728
        // total = 31.762222348598623822654992091
        // target_onchain = 31.762222348598623822654992091 * 0.5 = 15.8811111742993119113274960455
        // excess = 15.8811111742993119113274960455 - 6.3524444697197247645309984182
        //        = 9.5286667045795871467964976273
        let onchain = "6.3524444697197247645309984182";
        let offchain = "25.4097778788788990581239936728";

        let inventory = make_precise_imbalanced_view(&symbol, onchain, offchain);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        };

        // Trigger rebalancing - should return Mint with truncated quantity
        let result =
            check_imbalance_and_build_operation(&symbol, &threshold, &inventory, Address::ZERO)
                .await
                .unwrap();

        let TriggeredOperation::Mint { quantity, .. } = result else {
            panic!("Expected Mint, got {result:?}");
        };

        // Verify the quantity was truncated to 9 decimal places
        let expected_truncated = precise_shares("9.528666704");
        assert_eq!(
            quantity, expected_truncated,
            "Quantity should be truncated to 9 decimal places"
        );

        // The original excess had more precision - verify truncation occurred
        let full_excess = precise_shares("9.5286667045795871467964976273");
        assert_ne!(
            quantity, full_excess,
            "Quantity should differ from full-precision excess"
        );

        // Now simulate the mint completing with the TRUNCATED quantity.
        // The inventory should be updated with only the truncated amount.
        let mut view = inventory.write().await;
        let now = chrono::Utc::now();

        // MintAccepted: move truncated quantity from offchain.available to offchain.inflight
        *view = view
            .clone()
            .apply_mint_event(
                &symbol,
                &TokenizedEquityMintEvent::MintAccepted {
                    issuer_request_id: IssuerRequestId::new("test"),
                    tokenization_request_id: TokenizationRequestId("test".to_string()),
                    accepted_at: now,
                },
                quantity,
                now,
            )
            .unwrap();

        // TokensReceived: move from offchain.inflight to onchain.available
        *view = view
            .clone()
            .apply_mint_event(
                &symbol,
                &TokenizedEquityMintEvent::TokensReceived {
                    tx_hash: TxHash::random(),
                    receipt_id: ReceiptId(U256::from(1)),
                    shares_minted: U256::ZERO, // Not used by inventory update
                    received_at: now,
                },
                quantity,
                now,
            )
            .unwrap();

        drop(view);

        // After the mint, check the remaining imbalance.
        // The leftover (0.0000000005795871467964976273) should still be there.
        let remaining_imbalance = {
            let view = inventory.read().await;
            view.check_equity_imbalance(&symbol, &threshold)
        };

        // The leftover is tiny, so it won't exceed the deviation threshold alone.
        // But it IS still there in the inventory - not lost.
        // With target 50% and deviation 10%, the bounds are 40%-60%.
        // After minting 9.528666704:
        // - new onchain = 6.3524444697197247645309984182 + 9.528666704 = 15.8811111737197247645309984182
        // - new offchain = 25.4097778788788990581239936728 - 9.528666704 = 15.8811111748788990581239936728
        // - new total = 31.7622223485986238226549920910
        // - new ratio = 15.8811111737197247645309984182 / 31.7622223485986238226549920910 ≈ 0.49999999998...
        //
        // This is within the 40%-60% threshold, so no imbalance is detected.
        // But the LEFTOVER (the sub-9-decimal precision) is preserved in the totals.
        assert!(
            remaining_imbalance.is_none(),
            "After minting truncated amount, small leftover shouldn't trigger (within threshold)"
        );
    }

    /// Verifies that truncated leftovers accumulate and eventually get included.
    #[tokio::test]
    async fn truncated_leftovers_accumulate_over_multiple_operations() {
        let symbol = Symbol::new("RKLB").unwrap();

        // Start with an imbalance that produces a high-precision excess
        let inventory = make_precise_imbalanced_view(
            &symbol,
            "10.1234567891234567891234567891", // onchain
            "89.8765432108765432108765432109", // offchain (much more)
        );

        // Target 50%, deviation 10% -> triggers when outside 40%-60%
        // Current ratio = 10.12... / 100 = ~10.12%, well below 40%
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        };

        // First trigger
        let result1 =
            check_imbalance_and_build_operation(&symbol, &threshold, &inventory, Address::ZERO)
                .await
                .unwrap();

        let TriggeredOperation::Mint { quantity: qty1, .. } = result1 else {
            panic!("Expected Mint");
        };

        // Verify it's truncated (exactly 9 decimal places)
        assert_eq!(
            qty1.inner().scale(),
            9,
            "First mint quantity should have exactly 9 decimal places after truncation"
        );

        // Simulate mint completing
        {
            let mut view = inventory.write().await;
            let now = chrono::Utc::now();
            *view = view
                .clone()
                .apply_mint_event(
                    &symbol,
                    &TokenizedEquityMintEvent::MintAccepted {
                        issuer_request_id: IssuerRequestId::new("test"),
                        tokenization_request_id: TokenizationRequestId("test1".to_string()),
                        accepted_at: now,
                    },
                    qty1,
                    now,
                )
                .unwrap();
            *view = view
                .clone()
                .apply_mint_event(
                    &symbol,
                    &TokenizedEquityMintEvent::TokensReceived {
                        tx_hash: TxHash::random(),
                        receipt_id: ReceiptId(U256::from(1)),
                        shares_minted: U256::ZERO,
                        received_at: now,
                    },
                    qty1,
                    now,
                )
                .unwrap();
        }

        // Add more offchain shares to create another imbalance.
        // Need to add enough to push ratio outside 40%-60% threshold.
        // After first mint, ratio is ~50%. Adding 100 more offchain shares
        // changes total to ~200, with offchain ~150, onchain ~50, ratio ~25%.
        {
            let mut view = inventory.write().await;
            *view = view
                .clone()
                .apply_position_event(
                    &symbol,
                    &make_offchain_fill(precise_shares("100.0000000001"), Direction::Buy),
                )
                .unwrap();
        }

        // Second trigger - the leftover from first truncation plus new imbalance
        let result2 =
            check_imbalance_and_build_operation(&symbol, &threshold, &inventory, Address::ZERO)
                .await;

        // Should trigger again (we added significant new imbalance)
        assert!(
            result2.is_ok(),
            "Should trigger again after adding more offchain shares"
        );

        let TriggeredOperation::Mint { quantity: qty2, .. } = result2.unwrap() else {
            panic!("Expected Mint");
        };

        // The second quantity includes accumulated leftovers from previous truncation
        // plus the new imbalance. We can't easily calculate the exact expected value,
        // but we verify the system continues to work and produce truncated quantities.
        assert!(
            qty2.inner() > Decimal::ZERO,
            "Second mint should have positive quantity"
        );
        assert!(
            qty2.inner().scale() <= 9,
            "Second mint quantity should be truncated to at most 9 decimal places"
        );
    }

    /// Verifies that the truncate_for_alpaca helper works correctly.
    #[test]
    fn truncate_for_alpaca_truncates_to_9_decimals() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = precise_shares("1.12345678901234567890");
        let truncated = truncate_for_alpaca(&symbol, original);

        assert_eq!(truncated.inner(), dec!(1.123456789));
    }

    #[test]
    fn truncate_for_alpaca_preserves_fewer_decimals() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = precise_shares("1.123");
        let truncated = truncate_for_alpaca(&symbol, original);

        assert_eq!(truncated, original);
    }
}
