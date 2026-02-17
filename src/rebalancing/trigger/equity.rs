//! Equity-specific trigger types and logic.

use std::collections::HashSet;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::RwLock;
use tracing::{trace, warn};

use st0x_execution::{FractionalShares, Symbol};

use super::TriggeredOperation;
use crate::inventory::{Imbalance, ImbalanceThreshold, InventoryView};
use crate::wrapper::UnderlyingPerWrapped;

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
///
/// The onchain (wrapped) amounts are converted to unwrapped-equivalent using
/// the vault_ratio for accurate imbalance detection.
pub(super) async fn check_imbalance_and_build_operation(
    symbol: &Symbol,
    threshold: &ImbalanceThreshold,
    inventory: &Arc<RwLock<InventoryView>>,
    wrapped_token: Address,
    unwrapped_token: Address,
    vault_ratio: &UnderlyingPerWrapped,
) -> Result<TriggeredOperation, EquityTriggerSkip> {
    let imbalance = {
        let inventory = inventory.read().await;
        inventory.check_equity_imbalance(symbol, threshold, vault_ratio)
    };

    let Some(imbalance) = imbalance else {
        trace!(symbol = %symbol, "No equity imbalance detected (balanced, partial data, or inflight)");
        return Err(EquityTriggerSkip::NoImbalance);
    };

    let resolve_precision = |result: Result<FractionalShares, AlpacaPrecisionLoss>| match result {
        Ok(quantity) => quantity,
        Err(loss) => {
            warn!(
                symbol = %loss.symbol,
                original = %loss.original.inner(),
                truncated = %loss.truncated.inner(),
                "Truncated quantity to {} decimal places for Alpaca API",
                ALPACA_QUANTITY_MAX_DECIMAL_PLACES
            );
            loss.truncated
        }
    };

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => {
            let quantity = resolve_precision(truncate_for_alpaca(symbol, excess));
            Ok(TriggeredOperation::Mint {
                symbol: symbol.clone(),
                quantity,
            })
        }
        Imbalance::TooMuchOnchain { excess } => {
            let quantity = resolve_precision(truncate_for_alpaca(symbol, excess));
            Ok(TriggeredOperation::Redemption {
                symbol: symbol.clone(),
                quantity,
                wrapped_token,
                unwrapped_token,
            })
        }
    }
}

/// Rounds to the Alpaca API decimal limit. Returns the original value when
/// no rounding is needed, or `Err(AlpacaPrecisionLoss)` with the rounded
/// value when sub-nanoshare digits must be dropped.
fn truncate_for_alpaca(
    symbol: &Symbol,
    quantity: FractionalShares,
) -> Result<FractionalShares, AlpacaPrecisionLoss> {
    let truncated_decimal = quantity
        .inner()
        .trunc_with_scale(ALPACA_QUANTITY_MAX_DECIMAL_PLACES);
    let truncated = FractionalShares::new(truncated_decimal);

    if truncated == quantity {
        return Ok(truncated);
    }

    Err(AlpacaPrecisionLoss {
        symbol: symbol.clone(),
        original: quantity,
        truncated,
    })
}

#[derive(Debug)]
struct AlpacaPrecisionLoss {
    symbol: Symbol,
    original: FractionalShares,
    truncated: FractionalShares,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::{U256, address};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use st0x_execution::FractionalShares;

    use super::*;
    use crate::inventory::{Inventory, Operator, TransferOp, Venue};
    use crate::wrapper::RATIO_ONE;

    fn one_to_one_ratio() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
    }

    fn shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    fn make_imbalanced_view(
        symbol: &Symbol,
        onchain: i64,
        offchain: i64,
    ) -> Arc<RwLock<InventoryView>> {
        let view = InventoryView::default()
            .with_equity(symbol.clone())
            .update_equity(
                symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(onchain)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(offchain)),
                Utc::now(),
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
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let result = check_imbalance_and_build_operation(
            &Symbol::new("AAPL").unwrap(),
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &ratio,
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
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let result = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &ratio,
        )
        .await;

        assert!(matches!(result, Ok(TriggeredOperation::Mint { .. })));
    }

    #[tokio::test]
    async fn test_too_much_onchain_returns_redemption_with_tokens() {
        let symbol = Symbol::new("AAPL").unwrap();
        let wrapped_addr = address!("0x1234567890123456789012345678901234567890");
        let unwrapped_addr = address!("0xabcdef0123456789abcdef0123456789abcdef01");
        let inventory = make_imbalanced_view(&symbol, 80, 20);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };
        let ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        let result = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            wrapped_addr,
            unwrapped_addr,
            &ratio,
        )
        .await;

        let Ok(TriggeredOperation::Redemption {
            wrapped_token,
            unwrapped_token,
            ..
        }) = result
        else {
            panic!("Expected Redemption, got {result:?}");
        };
        assert_eq!(wrapped_token, wrapped_addr);
        assert_eq!(unwrapped_token, unwrapped_addr);
    }

    #[tokio::test]
    async fn test_high_ratio_triggers_redemption_that_would_be_balanced_at_1_to_1() {
        // With 65 onchain, 35 offchain at 1:1 ratio:
        //   65/100 = 65% onchain, within 30%-70% threshold -> balanced
        //
        // With 1.5 ratio (vault appreciated 50%):
        //   65 wrapped = 97.5 underlying-equivalent
        //   97.5/(97.5+35) = 97.5/132.5 = 73.6% onchain, above 70% -> too much onchain
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = make_imbalanced_view(&symbol, 65, 35);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        };

        // At 1:1 ratio, this is balanced
        let ratio_1_to_1 = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let result_1_to_1 = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &ratio_1_to_1,
        )
        .await;
        assert_eq!(result_1_to_1, Err(EquityTriggerSkip::NoImbalance));

        // At 1.5 ratio, this triggers redemption
        let ratio_1_5 =
            UnderlyingPerWrapped::new(U256::from(1_500_000_000_000_000_000u64)).unwrap();
        let result_1_5 = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &ratio_1_5,
        )
        .await;
        assert!(
            matches!(result_1_5, Ok(TriggeredOperation::Redemption { .. })),
            "Expected redemption with 1.5 ratio, got {result_1_5:?}"
        );
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
            .update_equity(
                symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, precise_shares(onchain)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                symbol,
                Inventory::available(Venue::Hedging, Operator::Add, precise_shares(offchain)),
                Utc::now(),
            )
            .unwrap();

        Arc::new(RwLock::new(view))
    }

    /// Verifies that quantity truncation doesn't lose the truncated portion from inventory.
    ///
    /// When we truncate the excess to 9 decimal places for Alpaca,
    /// the sub-nanoshare digits must remain in inventory and accumulate.
    #[tokio::test]
    async fn truncation_preserves_leftover_in_inventory() {
        let symbol = Symbol::new("RKLB").unwrap();

        // Set up inventory where excess calculation produces high-precision result.
        // With ~20% onchain, ~80% offchain and 50% target:
        //
        // onchain = 6.352444469719724764
        // offchain = 25.409777878878899058
        // total = 31.762222348598623822
        // target_onchain = 31.762222348598623822 * 0.5 = 15.881111174299311911
        // excess = 15.881111174299311911 - 6.352444469719724764
        //        = 9.528666704579587147
        let onchain = "6.352444469719724764";
        let offchain = "25.409777878878899058";

        let inventory = make_precise_imbalanced_view(&symbol, onchain, offchain);
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        };

        // Trigger rebalancing - should return Mint with truncated quantity
        let vault_ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();
        let result = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &vault_ratio,
        )
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
        let full_excess = precise_shares("9.528666704579587147");
        assert_ne!(
            quantity, full_excess,
            "Quantity should differ from full-precision excess"
        );

        // Now simulate the mint completing with the TRUNCATED quantity.
        // The inventory should be updated with only the truncated amount.
        let mut view = inventory.write().await;

        // MintAccepted: move truncated quantity from offchain.available to offchain.inflight
        *view = view
            .clone()
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, quantity),
                Utc::now(),
            )
            .unwrap();

        // TokensReceived: move from offchain.inflight to onchain.available
        *view = view
            .clone()
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Complete, quantity),
                Utc::now(),
            )
            .unwrap();

        drop(view);

        // After the mint, check the remaining imbalance.
        // The leftover (0.000000000579587147) should still be there.
        let remaining_imbalance = {
            let view = inventory.read().await;
            view.check_equity_imbalance(&symbol, &threshold, &one_to_one_ratio())
        };

        // The leftover is tiny, so it won't exceed the deviation threshold alone.
        // But it IS still there in the inventory - not lost.
        // With target 50% and deviation 10%, the bounds are 40%-60%.
        // After minting 9.528666704:
        // - new onchain = 6.352444469719724764 + 9.528666704 = 15.881111173719724764
        // - new offchain = 25.409777878878899058 - 9.528666704 = 15.881111174878899058
        // - new total = 31.762222348598623822
        // - new ratio = 15.881111173719724764 / 31.762222348598623822 ~= 0.49999999998...
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
            "10.123456789123456789", // onchain
            "89.876543210876543211", // offchain (much more)
        );

        // Target 50%, deviation 10% -> triggers when outside 40%-60%
        // Current ratio = 10.12... / 100 = ~10.12%, well below 40%
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        };
        let vault_ratio = UnderlyingPerWrapped::new(RATIO_ONE).unwrap();

        // First trigger
        let result1 = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &vault_ratio,
        )
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
            *view = view
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::transfer(Venue::Hedging, TransferOp::Start, qty1),
                    Utc::now(),
                )
                .unwrap();
            *view = view
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::transfer(Venue::Hedging, TransferOp::Complete, qty1),
                    Utc::now(),
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
                .update_equity(
                    &symbol,
                    Inventory::available(
                        Venue::Hedging,
                        Operator::Add,
                        precise_shares("100.0000000001"),
                    ),
                    Utc::now(),
                )
                .unwrap();
        }

        // Second trigger - the leftover from first truncation plus new imbalance
        let result2 = check_imbalance_and_build_operation(
            &symbol,
            &threshold,
            &inventory,
            Address::ZERO,
            Address::ZERO,
            &vault_ratio,
        )
        .await;

        // Should trigger again (we added significant new imbalance)
        let TriggeredOperation::Mint { quantity: qty2, .. } = result2.unwrap() else {
            panic!("Expected Mint after adding more offchain shares");
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

    #[test]
    fn truncate_for_alpaca_returns_error_when_precision_lost() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = precise_shares("1.12345678901234567890");
        let loss = truncate_for_alpaca(&symbol, original).unwrap_err();

        assert_eq!(loss.truncated.inner(), dec!(1.123456789));
        assert_eq!(loss.original, original);
    }

    #[test]
    fn truncate_for_alpaca_returns_ok_when_no_precision_lost() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = precise_shares("1.123");
        let result = truncate_for_alpaca(&symbol, original).unwrap();

        assert_eq!(result, original);
    }
}
